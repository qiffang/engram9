package main

import (
	"bufio"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/qiffang/engram9/internal/storage"
)

// TestCompileReadEventsSnapshotBoundClamp is the DISCRIMINATING in-repo evidence
// for A1 (snapshot-bound). It drives the REAL engram9-mcp binary in compile mode
// cross-process: seed 5 events, pass -event-bound 3, call read_events_since with
// cursor=0 exactly once, and assert the served page is CLAMPED to exactly 3
// events with new_cursor=3, and that the receipt records new_cursor=3.
//
// This is discriminating: a non-clamping implementation (serve all events past
// the cursor) would return 5 events / new_cursor=5 and FAIL this test. The
// full-Claude E2E cannot discriminate this — the clamp executes entirely in
// engram9-mcp, which Claude does not participate in — so the discriminating
// evidence for A1 lives here at the cross-process mcp layer (per architect
// ruling on PR #42).
func TestCompileReadEventsSnapshotBoundClamp(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "engram9-mcp")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = "."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}

	// Seed 5 events into a fresh data dir.
	dataDir := t.TempDir()
	store, err := storage.NewFS(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := store.AppendEvent(storage.Event{
			ID:            eventID(i),
			Content:       "event content " + eventID(i),
			Actor:         "user",
			Source:        "test",
			Durability:    "long-term",
			Actionability: "informational",
			SourceType:    "user",
			EvidenceKind:  "direct_statement",
			TrustTier:     1,
		}); err != nil {
			t.Fatal(err)
		}
	}

	receiptPath := filepath.Join(t.TempDir(), "receipt.jsonl")
	const turnID = "TURN_CLAMP"
	cmd := exec.Command(bin,
		"-data", dataDir,
		"-mode", "compile",
		"-turn-id", turnID,
		"-event-bound", "3",
		"-receipt", receiptPath,
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)

	// initialize
	writeRPC(t, stdin, 1, "initialize", map[string]any{"protocolVersion": 1})
	readRPCResult(t, scanner, 1)

	// tools/call read_events_since cursor=0
	writeRPC(t, stdin, 2, "tools/call", map[string]any{
		"name":      "read_events_since",
		"arguments": map[string]any{"cursor": 0},
	})
	result := readRPCResult(t, scanner, 2)

	// The tool result text is the marshaled EventsPage.
	var callResult struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(result, &callResult); err != nil {
		t.Fatalf("decode tools/call result: %v (%s)", err, result)
	}
	if callResult.IsError || len(callResult.Content) == 0 {
		t.Fatalf("read_events_since errored: %s", result)
	}
	var page storage.EventsPage
	if err := json.Unmarshal([]byte(callResult.Content[0].Text), &page); err != nil {
		t.Fatalf("decode EventsPage: %v (%s)", err, callResult.Content[0].Text)
	}

	// DISCRIMINATING assertions: clamp to the event bound (3), NOT the full 5.
	if len(page.Events) != 3 {
		t.Fatalf("snapshot-bound clamp: expected 3 events (bound), got %d (a non-clamping impl returns 5)", len(page.Events))
	}
	if page.NewCursor != 3 {
		t.Fatalf("snapshot-bound clamp: expected new_cursor=3 (bound), got %d", page.NewCursor)
	}

	// Receipt must record turn_id + cursor_in=0 + new_cursor=3 (bounded).
	entries := readReceipt(t, receiptPath)
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 receipt entry, got %d", len(entries))
	}
	if entries[0].TurnID != turnID || entries[0].CursorIn != 0 || entries[0].NewCursor != 3 {
		t.Fatalf("receipt = %+v, want {turn_id:%s cursor_in:0 new_cursor:3}", entries[0], turnID)
	}
}

func eventID(i int) string {
	return "evt_" + string(rune('a'+i))
}

func writeRPC(t *testing.T, w interface{ Write([]byte) (int, error) }, id int, method string, params any) {
	t.Helper()
	req := map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(append(data, '\n')); err != nil {
		t.Fatal(err)
	}
}

// readRPCResult reads lines until it finds the response for id, returning the
// raw result JSON.
func readRPCResult(t *testing.T, scanner *bufio.Scanner, id int) json.RawMessage {
	t.Helper()
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var resp struct {
			ID     int             `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(line, &resp); err != nil {
			continue
		}
		if resp.ID != id {
			continue
		}
		if resp.Error != nil {
			t.Fatalf("rpc id=%d error: %s", id, resp.Error.Message)
		}
		return resp.Result
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	t.Fatalf("no response for id=%d", id)
	return nil
}

type receiptEntry struct {
	TurnID    string `json:"turn_id"`
	CursorIn  uint64 `json:"cursor_in"`
	NewCursor uint64 `json:"new_cursor"`
}

func readReceipt(t *testing.T, path string) []receiptEntry {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open receipt: %v", err)
	}
	defer f.Close()
	var entries []receiptEntry
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var e receiptEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("decode receipt line: %v", err)
		}
		entries = append(entries, e)
	}
	return entries
}
