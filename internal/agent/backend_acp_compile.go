package agent

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/qiffang/engram9/internal/storage"
)

// CompileReceiptEntry is one line in the compile receipt file. The compile-mode
// engram9-mcp appends one entry per SUCCESSFUL read_events_since call; the
// handler reads them back after the turn to validate cursor advancement.
//
// Protocol (design #41 §5, adversary-2 D1/D2):
//   - TurnID stamps freshness: only entries whose TurnID equals the nonce
//     generated for THIS turn are accepted; a stale entry from a timed-out
//     prior turn can never validate regardless of cursor coincidence.
//   - Exactly one entry with the current TurnID is required; zero →
//     unknown(no_valid_receipt), more than one → unknown(multi_read).
//   - CursorIn must equal the handler's expected start cursor.
type CompileReceiptEntry struct {
	TurnID    string `json:"turn_id"`
	CursorIn  uint64 `json:"cursor_in"`
	NewCursor uint64 `json:"new_cursor"`
}

// compileReceiptName is the receipt file the compile-mode engram9-mcp appends to
// (inside the staging dir). The handler reads it back after the turn to
// validate cursor advancement.
const compileReceiptName = "compile-receipt.jsonl"

// RunCompile runs one compile cycle through acpmux with a compile-mode
// engram9-mcp, advancing the cursor only via a validated turn-scoped receipt.
//
// Protocol (design #41 §5, adversary-2 D1/D2 + adversary-1 A1/A2):
//   - Capture the pre-turn event count as the snapshot bound and generate a
//     per-turn freshness nonce BEFORE spawning (anchor precedes mutation).
//   - engram9-mcp serves read_events_since only within [cursor, bound) and
//     appends one receipt entry {turn_id, cursor_in, new_cursor} per successful
//     read.
//   - After the turn, require exactly one receipt entry carrying THIS turn_id
//     with cursor_in == cursor. Zero → no_valid_receipt; more than one →
//     multi_read; either way the cursor does not advance and staging is
//     discarded (store unchanged).
func (b *ACPBackend) RunCompile(ctx context.Context, cursor uint64) (CompileResult, error) {
	// Capture the pre-turn event count (snapshot bound) from the live store.
	store, err := storage.NewFS(b.dataDir)
	if err != nil {
		return CompileResult{}, fmt.Errorf("open store for event bound: %w", err)
	}
	page, err := store.ReadEventsSince(0)
	if err != nil {
		return CompileResult{}, fmt.Errorf("read event count: %w", err)
	}
	eventBound := page.NewCursor

	turnID, err := newTurnID()
	if err != nil {
		return CompileResult{}, fmt.Errorf("generate turn id: %w", err)
	}

	prompt := buildCompilePrompt(cursor)

	// validatedCursor is filled by the preMerge hook if the receipt is valid.
	var validatedCursor uint64
	receiptValidated := false

	opts := acpTurnOptions{
		mcpArgs: func(stagingDir string) []string {
			return []string{
				"-data", stagingDir,
				"-mode", "compile",
				"-turn-id", turnID,
				"-event-bound", fmt.Sprintf("%d", eventBound),
				"-receipt", filepath.Join(stagingDir, compileReceiptName),
			}
		},
		preMerge: func(stagingDir string) error {
			nc, verr := validateCompileReceipt(filepath.Join(stagingDir, compileReceiptName), turnID, cursor, eventBound)
			if verr != nil {
				return verr
			}
			validatedCursor = nc
			receiptValidated = true
			return nil
		},
	}

	result, err := b.runACPTurnFullOpts(ctx, prompt, ValidateOptions{AllowDelete: false}, opts)
	if err != nil {
		return CompileResult{Summary: result.Summary}, err
	}
	if len(result.Violations) > 0 {
		messages := make([]string, len(result.Violations))
		for i, v := range result.Violations {
			messages[i] = v.String()
		}
		return CompileResult{Summary: result.Summary}, fmt.Errorf("validation failed: %s", joinSemicolon(messages))
	}
	if !receiptValidated {
		// Should be unreachable (preMerge returns error when invalid), but guard
		// against cursor advance without a validated receipt.
		return CompileResult{Summary: result.Summary, NewCursor: cursor}, nil
	}

	return CompileResult{Summary: result.Summary, NewCursor: validatedCursor}, nil
}

// validateCompileReceipt reads the receipt file and enforces the single-read +
// freshness + cursor_in invariants. It returns the validated new cursor
// (clamped to [expected, eventBound]) or an error naming the closed-enum
// unknown reason.
func validateCompileReceipt(path, turnID string, expected, eventBound uint64) (uint64, error) {
	entries, err := readReceiptEntries(path)
	if err != nil {
		return 0, fmt.Errorf("unknown(no_valid_receipt): read receipt: %w", err)
	}

	// Count only entries stamped with THIS turn's nonce; stale entries from a
	// prior (e.g. timed-out) turn can never advance the cursor (D1).
	var mine []CompileReceiptEntry
	for _, e := range entries {
		if e.TurnID == turnID {
			mine = append(mine, e)
		}
	}
	switch {
	case len(mine) == 0:
		return 0, fmt.Errorf("unknown(no_valid_receipt): no receipt entry for this turn")
	case len(mine) > 1:
		return 0, fmt.Errorf("unknown(multi_read): %d read_events_since calls in one turn", len(mine))
	}
	entry := mine[0]
	if entry.CursorIn != expected {
		return 0, fmt.Errorf("unknown(no_valid_receipt): receipt cursor_in=%d != expected=%d", entry.CursorIn, expected)
	}

	// Clamp: never backward, never beyond the pre-turn event bound.
	nc := entry.NewCursor
	if nc < expected {
		nc = expected
	}
	if nc > eventBound {
		nc = eventBound
	}
	return nc, nil
}

// readReceiptEntries parses the JSONL receipt file. A missing file yields zero
// entries (no successful read happened), which the caller treats as
// no_valid_receipt.
func readReceiptEntries(path string) ([]CompileReceiptEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var entries []CompileReceiptEntry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var e CompileReceiptEntry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("malformed receipt line: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, scanner.Err()
}

// newTurnID returns a random hex nonce for one compile turn.
func newTurnID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func joinSemicolon(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += "; "
		}
		out += p
	}
	return out
}

// buildCompilePrompt builds the compile-cycle user message. It instructs
// exactly one read_events_since call (single-read invariant, D2).
func buildCompilePrompt(cursor uint64) string {
	return fmt.Sprintf(`Run a full compile cycle.

Current compile cursor: %d. Call read_events_since with cursor=%d EXACTLY ONCE to get the unprocessed events for this cycle; do not call it again.

Execute all three phases:
1. Distill new events into wiki
2. Sleep pruning (archive stale pages per memory-type rules)
3. Rebuild index

Report what you did when finished.

%s`, cursor, cursor, compileSystemPrompt)
}
