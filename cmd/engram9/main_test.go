package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func writeCLIFile(t *testing.T, root, rel, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestRunMigrateOKFCheckAndWrite(t *testing.T) {
	root := t.TempDir()
	writeCLIFile(t, root, "semantic/a.md", "<!-- compiled_from: evt_001 -->\n# A\n\nSee [[procedural/b]].\n")
	writeCLIFile(t, root, "procedural/b.md", `---
type: procedure
title: B
description: B page
timestamp: "2026-06-16T12:00:00Z"
memory_type: procedural
source_events: [evt_002]
trust_tier: T1
---
# B
`)

	if code := runMigrateOKF([]string{"--check", root}); code != 1 {
		t.Fatalf("check code=%d, want 1", code)
	}
	if code := runMigrateOKF([]string{"--write", "--backup=false", root}); code != 0 {
		t.Fatalf("write code=%d, want 0", code)
	}
	if code := runValidate([]string{"--strict", root}); code != 0 {
		t.Fatalf("validate after migration code=%d, want 0", code)
	}
	if code := runMigrateOKF([]string{"--check", root}); code != 0 {
		t.Fatalf("check after migration code=%d, want 0", code)
	}
}

func TestRunMigrateOKFRejectsWriteAndCheck(t *testing.T) {
	root := t.TempDir()
	if code := runMigrateOKF([]string{"--write", "--check", root}); code != 2 {
		t.Fatalf("code=%d, want 2", code)
	}
}

func TestRunRepoScanWritesOutput(t *testing.T) {
	root := t.TempDir()
	runCLIGit(t, root, "init")
	runCLIGit(t, root, "config", "user.email", "test@example.com")
	runCLIGit(t, root, "config", "user.name", "Test User")
	writeCLIFile(t, root, "pkg/fuse/a.go", "package fuse\n\ntype A struct{}\n")
	runCLIGit(t, root, "add", "-A")
	runCLIGit(t, root, "commit", "-m", "initial")

	out := filepath.Join(t.TempDir(), "facts")
	if code := runRepo([]string{"scan", "--path", root, "--scope", "pkg/fuse", "--out", out}); code != 0 {
		t.Fatalf("repo scan code=%d, want 0", code)
	}
	if _, err := os.Stat(filepath.Join(out, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(out, "facts.jsonl")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(out, "snippets.jsonl")); err != nil {
		t.Fatal(err)
	}
}

func runCLIGit(t *testing.T, root string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func TestEngram9McpCommandFromEnv(t *testing.T) {
	t.Setenv("ENGRAM9_MCP_COMMAND", "")
	if got := engram9McpCommandFromEnv(); got != "engram9-mcp" {
		t.Fatalf("unset ENGRAM9_MCP_COMMAND must default to engram9-mcp; got %q", got)
	}
	t.Setenv("ENGRAM9_MCP_COMMAND", "/opt/engram9/engram9-mcp")
	if got := engram9McpCommandFromEnv(); got != "/opt/engram9/engram9-mcp" {
		t.Fatalf("explicit ENGRAM9_MCP_COMMAND must be honored; got %q", got)
	}
}
