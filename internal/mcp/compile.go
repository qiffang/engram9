package mcp

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/qiffang/engram9/internal/storage"
)

// appendReceipt appends a receipt entry as one JSON line to the receipt file.
// The receipt schema (turn_id, cursor_in, new_cursor) is defined by the handler
// side as agent.CompileReceiptEntry; here we emit the same wire shape without
// importing that package (mcp is imported BY agent, so it cannot import agent).
func (s *Server) appendReceipt(cursorIn, newCursor uint64) error {
	if s.receiptPath == "" {
		return fmt.Errorf("compile mode: no receipt path configured")
	}
	entry := struct {
		TurnID    string `json:"turn_id"`
		CursorIn  uint64 `json:"cursor_in"`
		NewCursor uint64 `json:"new_cursor"`
	}{TurnID: s.turnID, CursorIn: cursorIn, NewCursor: newCursor}
	line, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(s.receiptPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		return err
	}
	return nil
}

// execCompileReadEventsSince serves events in the snapshot-bounded window
// [cursor, eventBound) and appends a receipt entry on success. Bounding to the
// pre-turn eventBound makes post-start /remember appends structurally invisible
// to this turn (invariant 10 / A1). The receipt is written only after the read
// succeeds, so failed reads append nothing (D2: failed reads don't count).
func (s *Server) execCompileReadEventsSince(args map[string]any) (string, error) {
	cursorIn, err := uint64Arg(args, "cursor")
	if err != nil {
		return "", err
	}

	page, err := s.store.ReadEventsSince(cursorIn)
	if err != nil {
		return "", err
	}

	// Snapshot-bound: drop any events at or beyond the pre-turn event bound, and
	// never report a new_cursor past it. events are 0-indexed from cursorIn, so
	// an event at absolute index i lives at page.Events[i-cursorIn].
	bounded := page
	if s.eventBound > 0 && page.NewCursor > s.eventBound {
		keep := 0
		if s.eventBound > cursorIn {
			keep = int(s.eventBound - cursorIn)
		}
		if keep > len(page.Events) {
			keep = len(page.Events)
		}
		bounded = &storage.EventsPage{
			Events:    page.Events[:keep],
			NewCursor: s.eventBound,
		}
	}

	if err := s.appendReceipt(cursorIn, bounded.NewCursor); err != nil {
		return "", fmt.Errorf("write compile receipt: %w", err)
	}

	data, err := json.Marshal(bounded)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// execCompileArchiveWikiPage archives a page during sleep-pruning.
func (s *Server) execCompileArchiveWikiPage(args map[string]any) (string, error) {
	path, ok := args["path"].(string)
	if !ok || path == "" {
		return "", fmt.Errorf("path is required")
	}
	if err := validatePath(path); err != nil {
		return "", err
	}
	reason, _ := args["reason"].(string)
	if err := s.store.ArchiveWikiPage(path, reason); err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"status": "ok", "archived": "%s"}`, path), nil
}

// execCompileRebuildIndex regenerates the wiki index.
func (s *Server) execCompileRebuildIndex() (string, error) {
	if err := s.store.RebuildIndex(); err != nil {
		return "", err
	}
	return `{"status": "ok", "index": "rebuilt"}`, nil
}

// uint64Arg extracts a uint64 argument from a decoded JSON tool-call args map.
// JSON numbers decode to float64; a raw integer string is also accepted.
func uint64Arg(args map[string]any, key string) (uint64, error) {
	switch v := args[key].(type) {
	case float64:
		if v < 0 {
			return 0, fmt.Errorf("%s must be non-negative", key)
		}
		return uint64(v), nil
	case json.Number:
		n, err := v.Int64()
		if err != nil || n < 0 {
			return 0, fmt.Errorf("%s must be a non-negative integer", key)
		}
		return uint64(n), nil
	case nil:
		return 0, fmt.Errorf("%s is required", key)
	default:
		return 0, fmt.Errorf("%s must be an integer", key)
	}
}
