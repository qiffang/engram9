package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestBundleAgentModeRejected(t *testing.T) {
	// Build the binary into a temp dir.
	bin := filepath.Join(t.TempDir(), "engram9-mcp")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = "."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}

	bundleDir := t.TempDir()
	os.MkdirAll(filepath.Join(bundleDir, "semantic"), 0o755)

	cmd := exec.Command(bin, "-bundle", bundleDir, "-mode", "agent")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected non-zero exit for -bundle -mode agent")
	}
	if !strings.Contains(string(out), "read-only") {
		t.Errorf("expected 'read-only' in error output, got: %s", out)
	}
}

// TestCompileModeRequiresEventBound guards the A1 safety fail-fast: compile mode
// must refuse to start without -event-bound. Omitting it would default the bound
// to 0 and (per compile.go) serve read_events_since UNBOUNDED — silently
// dropping the snapshot bound. Missing config must fail loudly, not change
// semantics.
func TestCompileModeRequiresEventBound(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "engram9-mcp")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	dataDir := t.TempDir()
	receipt := filepath.Join(t.TempDir(), "receipt.jsonl")

	// Missing -event-bound → must exit non-zero with a clear message.
	cmd := exec.Command(bin, "-data", dataDir, "-mode", "compile", "-turn-id", "T", "-receipt", receipt)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected non-zero exit for compile mode without -event-bound")
	}
	if !strings.Contains(string(out), "event-bound") {
		t.Errorf("expected 'event-bound' in error output, got: %s", out)
	}
}
