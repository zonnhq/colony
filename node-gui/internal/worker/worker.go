// Package worker launches the Python inference worker for the LLM test. The node
// receives a job command from the Coordinator, then runs the worker as a
// subprocess pointed at the Coordinator relay WebSocket.
package worker

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v3/process"
)

const bytesPerGB = 1024 * 1024 * 1024

// Command mirrors the job spec the Coordinator sends in the heartbeat response.
type Command struct {
	JobID        string  `json:"job_id"`
	Role         string  `json:"role"`
	Model        string  `json:"model"`
	Prompt       string  `json:"prompt"`
	Engine       string  `json:"engine"`
	MaxNewTokens int     `json:"max_new_tokens"`
	Split        float64 `json:"split"`
	RelayPort    string  `json:"relay_port"`
	RelayPath    string  `json:"relay_path"`
	RelayURL     string  `json:"relay_url"`
}

// scriptPath resolves the worker script from an explicit override, the node's
// data directory, or next to the released desktop binary.
func scriptPath() (string, error) {
	if s := os.Getenv("COLONY_WORKER_SCRIPT"); s != "" {
		if _, err := os.Stat(s); err != nil {
			return "", fmt.Errorf("worker script not found at %s", s)
		}
		return s, nil
	}

	executable, _ := os.Executable()
	return findScriptPath(executable, runtime.GOOS)
}

func findScriptPath(executable, goos string) (string, error) {
	var candidates []string
	if dataDir := os.Getenv("COLONY_HOME"); dataDir != "" {
		candidates = append(candidates, filepath.Join(dataDir, "inference_worker.py"))
	} else if home, err := os.UserHomeDir(); err == nil {
		candidates = append(candidates, filepath.Join(home, ".colony", "inference_worker.py"))
	}
	if executable != "" {
		appDir := filepath.Dir(executable)
		candidates = append(candidates, filepath.Join(appDir, "inference_worker.py"))
		if goos == "darwin" {
			candidates = append(candidates, filepath.Clean(filepath.Join(appDir, "..", "Resources", "inference_worker.py")))
		}
	}
	if goos == "linux" {
		candidates = append(candidates, "/usr/lib/zonn/inference_worker.py")
	}
	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("worker script not found; place inference_worker.py in the Zonn Node folder or set COLONY_WORKER_SCRIPT")
}

func pythonCommand() (string, []string, error) {
	if p := os.Getenv("COLONY_PYTHON"); p != "" {
		return p, nil, nil
	}
	candidates := []struct {
		name string
		args []string
	}{
		{name: "python3"},
		{name: "python"},
	}
	if runtime.GOOS == "windows" {
		candidates = []struct {
			name string
			args []string
		}{
			{name: "py", args: []string{"-3"}},
			{name: "python"},
			{name: "python3"},
		}
	}
	for _, candidate := range candidates {
		if path, err := exec.LookPath(candidate.name); err == nil {
			return path, candidate.args, nil
		}
	}
	return "", nil, fmt.Errorf("Python 3 was not found; install Python 3 or set COLONY_PYTHON")
}

func bundledWorkerPath(appExecutable, goos string) (string, error) {
	if configured := os.Getenv("COLONY_WORKER_EXECUTABLE"); configured != "" {
		if info, err := os.Stat(configured); err == nil && !info.IsDir() {
			return configured, nil
		}
		return "", fmt.Errorf("bundled worker not found at %s", configured)
	}

	name := "inference-worker"
	if goos == "windows" {
		name += ".exe"
	}
	var candidates []string
	if appExecutable != "" {
		appDir := filepath.Dir(appExecutable)
		candidates = append(candidates, filepath.Join(appDir, name))
		if goos == "darwin" {
			candidates = append(candidates, filepath.Clean(filepath.Join(appDir, "..", "Resources", name)))
		}
	}
	if goos == "linux" {
		candidates = append(candidates, filepath.Join("/usr/lib/zonn", name))
	}
	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
	}
	return "", os.ErrNotExist
}

func workerCommand(engine string) (string, []string, error) {
	executable, _ := os.Executable()
	if bundled, err := bundledWorkerPath(executable, runtime.GOOS); err == nil {
		return bundled, nil, nil
	}

	script, err := scriptPath()
	if err != nil {
		return "", nil, fmt.Errorf("bundled inference worker is missing and Python fallback is unavailable: %w", err)
	}
	python, pythonArgs, err := pythonCommand()
	if err != nil {
		return "", nil, err
	}
	return python, append(pythonArgs, script), nil
}

// Usage is the worker process's live resource draw, which is how much of the
// contribution the Zone is actually using on this node.
type Usage struct {
	CPUCores float64
	RAMGB    float64
}

// Launch runs the worker for one job and blocks until it exits. Output is passed
// to logfn line by line. While the worker runs, sampleFn is called about once a
// second with the process's live CPU and memory draw, so the node can show what
// of the contribution the Zone is using; it is called with a zero Usage once
// the worker exits. sampleFn may be nil.
func Launch(ctx context.Context, cmd Command, coordinatorHost string, logfn func(level, message string), sampleFn func(Usage)) error {
	relayPath := cmd.RelayPath
	if relayPath == "" {
		relayPath = "/relay"
	}
	wsURL, err := commandRelayURL(cmd, coordinatorHost, relayPath)
	if err != nil {
		return err
	}

	args := []string{
		"--role", cmd.Role,
		"--relay-ws", wsURL,
		"--engine", cmd.Engine,
		"--model", cmd.Model,
		"--max-new-tokens", strconv.Itoa(cmd.MaxNewTokens),
		"--split", strconv.FormatFloat(cmd.Split, 'f', -1, 64),
	}
	if cmd.Role == "primary" {
		args = append(args, "--prompt", cmd.Prompt)
	}

	logfn("INFO", fmt.Sprintf("Starting %s worker for job %s", cmd.Role, cmd.JobID))

	var out bytes.Buffer
	program, prefixArgs, err := workerCommand(cmd.Engine)
	if err != nil {
		return err
	}
	proc := exec.CommandContext(ctx, program, append(prefixArgs, args...)...)
	proc.Env = append(os.Environ(),
		"HF_HUB_DISABLE_SYMLINKS_WARNING=1",
		"HF_HUB_DISABLE_XET=1",
	)
	proc.Stdout = &out
	proc.Stderr = &out
	if err := proc.Start(); err != nil {
		return fmt.Errorf("start worker: %w", err)
	}

	// Sample the worker's usage until it exits, then reset to zero.
	done := make(chan struct{})
	if sampleFn != nil {
		go sampleUsage(proc.Process.Pid, sampleFn, done)
	}

	waitErr := proc.Wait()
	close(done)
	if sampleFn != nil {
		sampleFn(Usage{})
	}

	// out is only written by the process, and it has now exited, so reading it
	// here is race free.
	for _, line := range strings.Split(strings.TrimRight(out.String(), "\n"), "\n") {
		if strings.TrimSpace(line) != "" {
			logfn("INFO", "worker: "+line)
		}
	}
	if waitErr != nil {
		detail := lastWorkerError(out.String())
		if detail != "" {
			return fmt.Errorf("worker exited: %w: %s", waitErr, detail)
		}
		return fmt.Errorf("worker exited: %w", waitErr)
	}
	logfn("INFO", fmt.Sprintf("Worker for job %s finished", cmd.JobID))
	return nil
}

func lastWorkerError(output string) string {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(strings.TrimSuffix(lines[i], "\r"))
		if line == "" || strings.Contains(line, "Loading weights:") {
			continue
		}
		if len(line) > 500 {
			line = line[:500]
		}
		return line
	}
	return ""
}

func commandRelayURL(cmd Command, coordinatorHost, relayPath string) (string, error) {
	raw := cmd.RelayURL
	if raw == "" {
		raw = fmt.Sprintf("ws://%s:%s%s", coordinatorHost, cmd.RelayPort, relayPath)
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" {
		return "", fmt.Errorf("invalid relay URL %q", raw)
	}
	query := u.Query()
	query.Set("job", cmd.JobID)
	query.Set("role", cmd.Role)
	u.RawQuery = query.Encode()
	return u.String(), nil
}

// sampleUsage reports the worker process's CPU and memory draw about once a
// second until done is closed. gopsutil Percent needs a prior call to establish
// a baseline, so the first tick is primed and skipped.
func sampleUsage(pid int, sampleFn func(Usage), done chan struct{}) {
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		return
	}
	_, _ = p.Percent(0) // prime the CPU baseline
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			var u Usage
			if pct, err := p.Percent(0); err == nil {
				u.CPUCores = pct / 100.0 // gopsutil reports 100 percent as one core
			}
			if mi, err := p.MemoryInfo(); err == nil && mi != nil {
				u.RAMGB = float64(mi.RSS) / bytesPerGB
			}
			sampleFn(u)
		}
	}
}
