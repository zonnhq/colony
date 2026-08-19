package worker

import (
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestScriptPathUsesColonyHome(t *testing.T) {
	dir := t.TempDir()
	want := filepath.Join(dir, "inference_worker.py")
	if err := os.WriteFile(want, []byte("# test worker\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("COLONY_HOME", dir)
	t.Setenv("COLONY_WORKER_SCRIPT", "")
	got, err := scriptPath()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("script path = %q, want %q", got, want)
	}
}

func TestScriptPathUsesReleasedBinaryDirectory(t *testing.T) {
	dir := t.TempDir()
	want := filepath.Join(dir, "inference_worker.py")
	if err := os.WriteFile(want, []byte("# packaged worker\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("COLONY_HOME", filepath.Join(dir, "empty-data-dir"))
	got, err := findScriptPath(filepath.Join(dir, "colony-node.exe"), "windows")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("script path = %q, want %q", got, want)
	}
}

func TestBundledWorkerPathUsesReleasedBinaryDirectory(t *testing.T) {
	dir := t.TempDir()
	want := filepath.Join(dir, "inference-worker.exe")
	if err := os.WriteFile(want, []byte("test worker"), 0o700); err != nil {
		t.Fatal(err)
	}
	got, err := bundledWorkerPath(filepath.Join(dir, "zonn-node.exe"), "windows")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("bundled worker path = %q, want %q", got, want)
	}
}

func TestBundledWorkerPathUsesMacResources(t *testing.T) {
	dir := t.TempDir()
	resources := filepath.Join(dir, "Zonn Node.app", "Contents", "Resources")
	if err := os.MkdirAll(resources, 0o755); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(resources, "inference-worker")
	if err := os.WriteFile(want, []byte("test worker"), 0o700); err != nil {
		t.Fatal(err)
	}
	app := filepath.Join(dir, "Zonn Node.app", "Contents", "MacOS", "Zonn Node")
	got, err := bundledWorkerPath(app, "darwin")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("bundled worker path = %q, want %q", got, want)
	}
}

func TestRealEngineUsesBundledWorker(t *testing.T) {
	dir := t.TempDir()
	want := filepath.Join(dir, "inference-worker")
	if err := os.WriteFile(want, []byte("test worker"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("COLONY_WORKER_EXECUTABLE", want)

	program, args, err := workerCommand("real")
	if err != nil {
		t.Fatal(err)
	}
	if program != want || len(args) != 0 {
		t.Fatalf("real worker command = %q %v, want bundled %q", program, args, want)
	}
}

func TestCommandRelayURLUsesPublicOverride(t *testing.T) {
	cmd := Command{JobID: "job 1", Role: "secondary", RelayURL: "wss://relay.example.test/tensor"}
	got, err := commandRelayURL(cmd, "private-host", "/relay")
	if err != nil {
		t.Fatal(err)
	}
	want := "wss://relay.example.test/tensor?job=job+1&role=secondary"
	if got != want {
		t.Fatalf("relay URL = %q, want %q", got, want)
	}
}

func TestLastWorkerErrorReturnsUsefulFinalLine(t *testing.T) {
	output := "primary: connected\nLoading weights: 100%\nprimary: error: relay closed\n"
	if got := lastWorkerError(output); got != "primary: error: relay closed" {
		t.Fatalf("lastWorkerError = %q", got)
	}
}

// TestSampleUsageReadsLiveProcess proves the usage sampler reports a running
// process's real draw: a short CPU bound child should show non zero memory and,
// with any luck, non zero CPU. This is the measurement behind "Zone use of
// your contribution".
func TestSampleUsageReadsLiveProcess(t *testing.T) {
	// A CPU bound Python loop that runs for a few seconds so the sampler gets a
	// couple of ticks after priming its baseline.
	python, prefix, err := pythonCommand()
	if err != nil {
		t.Skip(err)
	}
	proc := exec.Command(python, append(prefix, "-c", "x=0\nfor i in range(60000000): x+=i\nimport time\ntime.sleep(3)")...)
	if err := proc.Start(); err != nil {
		t.Skipf("could not start python worker stand in: %v", err)
	}
	defer func() {
		_ = proc.Process.Kill()
		_ = proc.Wait()
	}()

	var mu sync.Mutex
	var maxRAM, maxCPU float64
	done := make(chan struct{})
	go sampleUsage(proc.Process.Pid, func(u Usage) {
		mu.Lock()
		if u.RAMGB > maxRAM {
			maxRAM = u.RAMGB
		}
		if u.CPUCores > maxCPU {
			maxCPU = u.CPUCores
		}
		mu.Unlock()
	}, done)

	time.Sleep(3 * time.Second)
	close(done)

	mu.Lock()
	defer mu.Unlock()
	if maxRAM <= 0 {
		t.Fatalf("expected non zero memory draw from a live process, got %.4f GB", maxRAM)
	}
	t.Logf("sampled worker draw: up to %.3f cores, %.4f GB", maxCPU, maxRAM)
}
