import json
import time
import unittest
from unittest.mock import patch

import inference_worker


class RecordingWebSocket:
    def __init__(self):
        self.messages = []

    def send_text(self, text):
        self.messages.append(json.loads(text))

    def send_text_resilient(self, text, attempts=5):
        self.send_text(text)


class EngineLoadingTest(unittest.TestCase):
    def test_reports_progress_while_engine_loads(self):
        ws = RecordingWebSocket()
        expected = object()

        def delayed_engine(*_args):
            time.sleep(0.06)
            return expected

        with patch.object(inference_worker, "build_engine", side_effect=delayed_engine):
            actual = inference_worker.load_engine_with_progress(
                ws, "real", "test/model", 0.5, interval=0.01
            )

        self.assertIs(actual, expected)
        self.assertGreaterEqual(len(ws.messages), 3)
        self.assertEqual(ws.messages[0]["status"], "Loading model test/model")
        self.assertEqual(ws.messages[-1]["status"], "Model loaded; starting inference")


if __name__ == "__main__":
    unittest.main()
