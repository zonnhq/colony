#!/usr/bin/env python3
"""Split inference worker for the Project Colony killer test.

A model is split across two nodes. The primary node holds the lower layers and
the secondary node holds the upper layers. The nodes sit behind NAT and cannot
reach each other, so every activation tensor travels over a WebSocket to the
Coordinator, which relays it to the peer.

The worker ships a tiny standard library WebSocket client so it has no third
party network dependency. Two engines are available:

  mock  real numpy matrix math split across the two nodes. Proves the relay and
        the split compute end to end without downloading a model. Output is
        deterministic gibberish, clearly labelled.
  real  a genuine transformers model split by layer. Supports Qwen2,
        Llama/SmolLM2, and GPT-2 architectures. Release builds bundle the
        runtime; source runs need torch and transformers installed.

Protocol over the relay:
  binary frame  a serialized numpy array (an activation tensor, or a single
                next-token id going back to the primary)
  text frame    JSON control: {"type":"result","text":...} from the secondary
                when generation finishes, or {"type":"stop"} from the Coordinator
"""

import argparse
import base64
import io
import json
import os
import socket
import struct
import sys
import threading
import time
from urllib.parse import urlparse

import numpy as np


# --------------------------------------------------------------------------- #
# Minimal WebSocket client (RFC 6455, client side only).
# --------------------------------------------------------------------------- #

OP_TEXT = 0x1
OP_BINARY = 0x2
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA


class WSClient:
    def __init__(self, url):
        parsed = urlparse(url)
        self.host = parsed.hostname
        self.port = parsed.port or 80
        self.path = parsed.path or "/"
        if parsed.query:
            self.path += "?" + parsed.query
        self.sock = None
        self.recv_buffer = b""

    def connect(self):
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
        self.recv_buffer = b""
        self.sock = socket.create_connection((self.host, self.port), timeout=60)
        key = base64.b64encode(os.urandom(16)).decode()
        req = (
            f"GET {self.path} HTTP/1.1\r\n"
            f"Host: {self.host}:{self.port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        )
        self.sock.sendall(req.encode())
        resp = b""
        while b"\r\n\r\n" not in resp:
            chunk = self.sock.recv(1024)
            if not chunk:
                raise ConnectionError("relay closed during handshake")
            resp += chunk
        if b" 101 " not in resp.split(b"\r\n", 1)[0]:
            raise ConnectionError(f"relay handshake failed: {resp.splitlines()[0]!r}")
        _, self.recv_buffer = resp.split(b"\r\n\r\n", 1)
        # Model initialization on the peer can take minutes on first use. The
        # connect timeout must not become a job execution timeout after the
        # WebSocket handshake succeeds.
        self.sock.settimeout(None)

    def _recv_exact(self, n):
        buf = self.recv_buffer[:n]
        self.recv_buffer = self.recv_buffer[n:]
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("relay closed")
            buf += chunk
        return buf

    def _send_frame(self, opcode, payload):
        header = bytearray([0x80 | opcode])
        length = len(payload)
        if length < 126:
            header.append(0x80 | length)
        elif length < 65536:
            header.append(0x80 | 126)
            header += struct.pack("!H", length)
        else:
            header.append(0x80 | 127)
            header += struct.pack("!Q", length)
        mask = os.urandom(4)
        header += mask
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(bytes(header) + masked)

    def send_binary(self, data):
        self._send_frame(OP_BINARY, data)

    def send_text(self, text):
        self._send_frame(OP_TEXT, text.encode("utf-8"))

    def recv(self):
        """Return (opcode, payload), transparently answering pings."""
        while True:
            b0, b1 = self._recv_exact(2)
            opcode = b0 & 0x0F
            masked = b1 & 0x80
            length = b1 & 0x7F
            if length == 126:
                length = struct.unpack("!H", self._recv_exact(2))[0]
            elif length == 127:
                length = struct.unpack("!Q", self._recv_exact(8))[0]
            mask = self._recv_exact(4) if masked else None
            payload = self._recv_exact(length) if length else b""
            if mask:
                payload = bytes(p ^ mask[i % 4] for i, p in enumerate(payload))
            if opcode == OP_PING:
                self._send_frame(OP_PONG, payload)
                continue
            if opcode == OP_PONG:
                continue
            return opcode, payload

    def close(self):
        try:
            if self.sock:
                self._send_frame(OP_CLOSE, b"")
                self.sock.close()
        except OSError:
            pass

    def reconnect(self, max_wait=300):
        # Bounded, not infinite: once the Coordinator has given up on this job
        # (see the Coordinator's own reconnect grace window) it stops
        # accepting new connections for it, so retrying forever here would
        # just leave an orphaned process spinning on a job nobody is waiting
        # on anymore.
        deadline = time.monotonic() + max_wait
        while True:
            try:
                self.connect()
                return
            except OSError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(2)

    def send_text_resilient(self, text, attempts=5):
        for attempt in range(attempts):
            try:
                self.send_text(text)
                return
            except OSError:
                if attempt == attempts - 1:
                    raise
                self.reconnect()

    def send_binary_resilient(self, data, attempts=5):
        for attempt in range(attempts):
            try:
                self.send_binary(data)
                return
            except OSError:
                if attempt == attempts - 1:
                    raise
                self.reconnect()

    def recv_resilient(self, attempts=5):
        for attempt in range(attempts):
            try:
                return self.recv()
            except (OSError, ConnectionError):
                if attempt == attempts - 1:
                    raise
                self.reconnect()


# --------------------------------------------------------------------------- #
# Tensor serialization using the self describing numpy .npy format.
# --------------------------------------------------------------------------- #

def pack(array):
    buf = io.BytesIO()
    np.save(buf, np.asarray(array))
    return buf.getvalue()


def unpack(data):
    return np.load(io.BytesIO(data))


# --------------------------------------------------------------------------- #
# Mock engine: real split matrix math, no model download.
# --------------------------------------------------------------------------- #

class MockEngine:
    """A tiny two stage network. The primary embeds tokens and applies the first
    projection, the secondary applies the second projection and picks the next
    token. It is not a language model, but it exercises the exact same split and
    relay as the real path with deterministic output."""

    vocab = 256
    dim = 32
    eos = -1  # never emitted, generation stops on the token budget

    def __init__(self):
        rng = np.random.default_rng(1234)
        self.embed = rng.standard_normal((self.vocab, self.dim)).astype(np.float32)
        self.w1 = rng.standard_normal((self.dim, self.dim)).astype(np.float32)
        self.w2 = rng.standard_normal((self.dim, self.vocab)).astype(np.float32)

    def encode(self, text):
        return list(text.encode("utf-8"))

    def decode(self, ids):
        return bytes(max(0, min(255, i)) for i in ids).decode("utf-8", errors="replace")

    def lower(self, ids):
        x = self.embed[np.asarray(ids) % self.vocab]
        return np.tanh(x @ self.w1).astype(np.float32)

    def upper(self, hidden):
        logits = hidden[-1] @ self.w2
        # Map to a printable ASCII byte so the relayed result is readable.
        return 32 + int(np.argmax(logits)) % 95


# --------------------------------------------------------------------------- #
# Real engine: a GPT-2 family model split by layer. Reference path, needs torch.
# --------------------------------------------------------------------------- #

class RealEngine:
    def __init__(self, model_name, split):
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer

        configured_threads = int(os.environ.get("COLONY_TORCH_THREADS", "0") or 0)
        torch_threads = configured_threads if configured_threads > 0 else min(4, max(1, os.cpu_count() or 1))
        torch.set_num_threads(torch_threads)
        try:
            torch.set_num_interop_threads(1)
        except RuntimeError:
            pass

        self.torch = torch
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForCausalLM.from_pretrained(model_name)
        self.model.eval()
        self.dtype = next(self.model.parameters()).dtype
        self.eos = self.tokenizer.eos_token_id if self.tokenizer.eos_token_id is not None else -1

        # Support the GPT-2 family and modern Llama-style decoder models such
        # as Qwen2 and SmolLM2. Everything stays on CPU for portability.
        if hasattr(self.model, "transformer"):
            self.layout = "gpt2"
            self.t = self.model.transformer
            self.layers = self.t.h
        elif hasattr(self.model, "model") and hasattr(self.model.model, "layers"):
            self.layout = "decoder"
            self.t = self.model.model
            self.layers = self.t.layers
        else:
            raise ValueError(
                f"unsupported model architecture {self.model.config.model_type!r}; "
                "choose a Qwen2, Llama/SmolLM2, or GPT-2 model"
            )
        self.n_layers = len(self.layers)
        self.split_at = max(1, int(self.n_layers * split))

    @staticmethod
    def _block_hidden(output):
        # Transformers 4 returned a tuple from GPT-2 blocks. Transformers 5
        # returns the hidden-state tensor directly.
        if isinstance(output, (tuple, list)):
            return output[0]
        return output

    def encode(self, text):
        if text and getattr(self.tokenizer, "chat_template", None):
            encoded = self.tokenizer.apply_chat_template(
                [{"role": "user", "content": text}],
                tokenize=True,
                add_generation_prompt=True,
            )
            if isinstance(encoded, dict) or hasattr(encoded, "get"):
                input_ids = encoded.get("input_ids")
                if input_ids is not None:
                    return input_ids
            ids = getattr(encoded, "ids", None)
            return ids if ids is not None else encoded
        return self.tokenizer.encode(text)

    def decode(self, ids):
        return self.tokenizer.decode(ids, skip_special_tokens=True)

    def lower(self, ids):
        torch = self.torch
        with torch.no_grad():
            input_ids = torch.tensor([ids], dtype=torch.long)
            if self.layout == "decoder":
                hidden = self.t.embed_tokens(input_ids)
                position_ids, mask, position_embeddings = self._decoder_inputs(hidden)
                for block in self.layers[: self.split_at]:
                    hidden = self._block_hidden(block(
                        hidden,
                        attention_mask=mask,
                        position_ids=position_ids,
                        position_embeddings=position_embeddings,
                        use_cache=False,
                    ))
                return hidden.squeeze(0).float().numpy().astype(np.float32)

            positions = torch.arange(0, len(ids), dtype=torch.long).unsqueeze(0)
            hidden = self.t.wte(input_ids) + self.t.wpe(positions)
            for block in self.layers[: self.split_at]:
                hidden = self._block_hidden(block(hidden))
        return hidden.squeeze(0).numpy().astype(np.float32)

    def upper(self, hidden):
        torch = self.torch
        with torch.no_grad():
            h = torch.tensor(hidden, dtype=self.dtype).unsqueeze(0)
            if self.layout == "decoder":
                position_ids, mask, position_embeddings = self._decoder_inputs(h)
                for block in self.layers[self.split_at :]:
                    h = self._block_hidden(block(
                        h,
                        attention_mask=mask,
                        position_ids=position_ids,
                        position_embeddings=position_embeddings,
                        use_cache=False,
                    ))
                h = self.t.norm(h)
            else:
                for block in self.layers[self.split_at :]:
                    h = self._block_hidden(block(h))
                h = self.t.ln_f(h)
            logits = self.model.lm_head(h)
            return int(torch.argmax(logits[0, -1]).item())

    def _decoder_inputs(self, hidden):
        torch = self.torch
        length = hidden.shape[1]
        position_ids = torch.arange(length, device=hidden.device).unsqueeze(0)
        mask = torch.full(
            (length, length),
            torch.finfo(hidden.dtype).min,
            dtype=hidden.dtype,
            device=hidden.device,
        )
        mask = torch.triu(mask, diagonal=1).unsqueeze(0).unsqueeze(0)
        position_embeddings = self.t.rotary_emb(hidden, position_ids)
        return position_ids, mask, position_embeddings


def build_engine(engine, model_name, split):
    if engine == "real":
        return RealEngine(model_name, split)
    return MockEngine()


# --------------------------------------------------------------------------- #
# Roles.
# --------------------------------------------------------------------------- #

def run_primary(ws, engine, prompt, attempts=5):
    tokens = engine.encode(prompt)
    relayed = 0
    while True:
        hidden = engine.lower(tokens)
        frame = pack(hidden)
        ws.send_binary_resilient(frame)
        relayed += 1
        # A public tunnel can recycle long-lived TCP connections. Rejoin the
        # same relay role and resend the activation that had no reply, up to
        # `attempts` times, so more than one drop in a row does not sink the
        # whole job the way a single retry would.
        opcode = payload = None
        for attempt in range(attempts):
            try:
                opcode, payload = ws.recv()
                break
            except (OSError, ConnectionError):
                if attempt == attempts - 1:
                    raise
                ws.reconnect()
                ws.send_binary(frame)
        if opcode == OP_TEXT:
            msg = json.loads(payload.decode("utf-8"))
            if msg.get("type") == "stop":
                break
        elif opcode == OP_BINARY:
            next_id = int(unpack(payload))
            tokens.append(next_id)
        elif opcode == OP_CLOSE:
            break
    print(f"primary: relayed {relayed} activation tensors", flush=True)


def run_secondary(ws, engine, prompt, max_new_tokens):
    prompt_ids = engine.encode(prompt)
    generated = []
    while True:
        try:
            opcode, payload = ws.recv()
        except (OSError, ConnectionError):
            # reconnect() itself is bounded: it raises once the Coordinator
            # has given up on this job and stops accepting new connections
            # for it, which is exactly what should end this loop too.
            ws.reconnect()
            continue
        if opcode == OP_TEXT:
            msg = json.loads(payload.decode("utf-8"))
            if msg.get("type") == "stop":
                break
            continue
        if opcode == OP_CLOSE:
            break
        hidden = unpack(payload)
        next_id = engine.upper(hidden)
        generated.append(next_id)
        ws.send_text_resilient(json.dumps({
            "type": "status",
            "status": f"Generated {len(generated)} of {max_new_tokens} tokens",
        }))
        if len(generated) >= max_new_tokens or next_id == engine.eos:
            text = engine.decode(prompt_ids + generated)
            if isinstance(engine, MockEngine):
                text = "[mock split-inference] " + engine.decode(generated)
            ws.send_text_resilient(json.dumps({"type": "result", "text": text}))
            print(f"secondary: produced {len(generated)} tokens", flush=True)
            break
        ws.send_binary_resilient(pack(np.array(next_id)))


def load_engine_with_progress(ws, engine_name, model_name, split, interval=10):
    loaded = {}

    def load_engine():
        try:
            loaded["engine"] = build_engine(engine_name, model_name, split)
        except BaseException as exc:  # noqa: BLE001 transfer the exception to the main thread
            loaded["error"] = exc

    loader = threading.Thread(target=load_engine, name="model-loader", daemon=True)
    loader.start()
    while loader.is_alive():
        status = json.dumps({"type": "status", "status": f"Loading model {model_name}"})
        try:
            ws.send_text(status)
        except OSError:
            ws.reconnect()
            ws.send_text(status)
        loader.join(interval)
    if "error" in loaded:
        raise loaded["error"]
    ws.send_text_resilient(json.dumps({"type": "status", "status": "Model loaded; starting inference"}))
    return loaded["engine"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", required=True, choices=["primary", "secondary"])
    parser.add_argument("--relay-ws", required=True)
    parser.add_argument("--engine", default="mock", choices=["mock", "real"])
    parser.add_argument("--model", default="gpt2")
    parser.add_argument("--prompt", default="")
    parser.add_argument("--max-new-tokens", type=int, default=20)
    parser.add_argument("--split", type=float, default=0.5)
    args = parser.parse_args()

    ws = WSClient(args.relay_ws)
    ws.connect()
    print(f"{args.role}: connected to relay; loading the {args.engine} engine", flush=True)

    try:
        engine = load_engine_with_progress(ws, args.engine, args.model, args.split)
        print(f"{args.role}: model loaded; starting the {args.engine} engine", flush=True)

        if args.role == "primary":
            run_primary(ws, engine, args.prompt)
        else:
            run_secondary(ws, engine, args.prompt, args.max_new_tokens)
    except Exception as exc:  # noqa: BLE001 report failures back through the relay
        try:
            ws.send_text(json.dumps({"type": "error", "error": f"{args.role}: {exc}"}))
        except OSError:
            pass
        print(f"{args.role}: error: {exc}", file=sys.stderr, flush=True)
        ws.close()
        sys.exit(1)

    ws.close()


if __name__ == "__main__":
    main()
