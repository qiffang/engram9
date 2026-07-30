package agent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeReceipt writes the given entries as JSONL to a temp receipt file.
func writeReceipt(t *testing.T, entries []CompileReceiptEntry) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, compileReceiptName)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, e := range entries {
		line, _ := json.Marshal(e)
		f.Write(append(line, '\n'))
	}
	return path
}

// D2 happy path: exactly one entry with this turn_id and cursor_in==expected
// advances the cursor to the receipt's new_cursor (clamped to the bound).
func TestValidateCompileReceipt_SingleValidRead(t *testing.T) {
	path := writeReceipt(t, []CompileReceiptEntry{
		{TurnID: "turnA", CursorIn: 5, NewCursor: 10},
	})
	nc, err := validateCompileReceipt(path, "turnA", 5, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nc != 10 {
		t.Fatalf("new cursor = %d, want 10", nc)
	}
}

// D1: a receipt planted by a FABRICATED prior turn_id whose cursor_in coincides
// with expected must NOT advance the cursor — the current turn did zero reads,
// so no entry carries this turn's nonce (adversary-2 D1's exact scenario).
func TestValidateCompileReceipt_StaleReceiptInjectionRejected(t *testing.T) {
	path := writeReceipt(t, []CompileReceiptEntry{
		{TurnID: "PRIOR_TIMED_OUT_TURN", CursorIn: 5, NewCursor: 10}, // stale, cursor_in matches
	})
	_, err := validateCompileReceipt(path, "turnB", 5, 20) // this turn read nothing
	if err == nil {
		t.Fatal("expected error: stale receipt must not advance cursor")
	}
	if !strings.Contains(err.Error(), "no_valid_receipt") {
		t.Fatalf("error = %q, want no_valid_receipt", err.Error())
	}
}

// D2: two successful reads in one turn → unknown(multi_read), cursor unchanged.
func TestValidateCompileReceipt_MultiReadRejected(t *testing.T) {
	path := writeReceipt(t, []CompileReceiptEntry{
		{TurnID: "turnA", CursorIn: 5, NewCursor: 10},
		{TurnID: "turnA", CursorIn: 10, NewCursor: 15},
	})
	_, err := validateCompileReceipt(path, "turnA", 5, 20)
	if err == nil || !strings.Contains(err.Error(), "multi_read") {
		t.Fatalf("error = %v, want multi_read", err)
	}
}

// D2: a failed read appends nothing, so failed-then-success leaves exactly one
// entry and the cursor advances. (The mcp side only appends on success; here we
// model that the receipt has just the one successful entry.)
func TestValidateCompileReceipt_FailedThenSuccessCountsOne(t *testing.T) {
	path := writeReceipt(t, []CompileReceiptEntry{
		{TurnID: "turnA", CursorIn: 5, NewCursor: 12}, // the one successful read
	})
	nc, err := validateCompileReceipt(path, "turnA", 5, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nc != 12 {
		t.Fatalf("new cursor = %d, want 12", nc)
	}
}

// A turn that never called read_events_since (no receipt file) →
// unknown(no_valid_receipt), cursor unchanged.
func TestValidateCompileReceipt_NoReceiptFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), compileReceiptName)
	_, err := validateCompileReceipt(missing, "turnA", 5, 20)
	if err == nil || !strings.Contains(err.Error(), "no_valid_receipt") {
		t.Fatalf("error = %v, want no_valid_receipt", err)
	}
}

// cursor_in mismatch (agent read from the wrong start) → rejected.
func TestValidateCompileReceipt_CursorInMismatch(t *testing.T) {
	path := writeReceipt(t, []CompileReceiptEntry{
		{TurnID: "turnA", CursorIn: 7, NewCursor: 12}, // read started at 7, expected 5
	})
	_, err := validateCompileReceipt(path, "turnA", 5, 20)
	if err == nil || !strings.Contains(err.Error(), "no_valid_receipt") {
		t.Fatalf("error = %v, want no_valid_receipt", err)
	}
}

// A1/clamp: a receipt new_cursor beyond the pre-turn event bound is clamped down
// to the bound (defense-in-depth alongside the mcp-side snapshot bounding).
func TestValidateCompileReceipt_ClampsToEventBound(t *testing.T) {
	path := writeReceipt(t, []CompileReceiptEntry{
		{TurnID: "turnA", CursorIn: 5, NewCursor: 99},
	})
	nc, err := validateCompileReceipt(path, "turnA", 5, 12) // bound = 12
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nc != 12 {
		t.Fatalf("new cursor = %d, want clamp to 12", nc)
	}
}
