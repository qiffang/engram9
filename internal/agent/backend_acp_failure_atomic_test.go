package agent

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/qiffang/engram9/internal/storage"
	"github.com/stretchr/testify/require"
)

// This file holds the handler-level failure-atomic (A2) tests. They are
// DETERMINISTIC and drive the REAL engram9-mcp compile binary to mutate a
// staging copy (write a page + archive-move a page + issue N read_events_since
// calls producing a real receipt), then run the REAL validate → validateCompileReceipt
// → merge-gate decision that runACPTurnFullOpts uses, and assert that on an
// invalid receipt the ENTIRE staging turn is discarded — the live store
// (active + archive + index + .meta + cursor) is byte-for-byte unchanged.
//
// SCOPE (honest naming, per #41 §10.2c + reviewer ruling): these are
// HANDLER-LEVEL failure-atomic tests. The A2 invariant (invalid receipt → whole
// discard) lives entirely in the engram9 Go merge-gate; the acpmux relay does
// not participate in the validate-or-discard decision. These tests therefore do
// NOT drive acpmux and MUST NOT be described as acpmux-in-loop / full-loop e2e.
// The acpmux-in-loop variant is a required follow-up (needs a scriptable-mock
// acpmux provider) tracked against canon9-ai #40.

// mcpBinary builds engram9-mcp into a temp dir and returns its path.
func mcpBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "engram9-mcp")
	repoRoot, err := filepath.Abs("../..")
	require.NoError(t, err)
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/engram9-mcp")
	cmd.Dir = repoRoot
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Run(), "build engram9-mcp")
	return bin
}

// mcpCall is one JSON-RPC tools/call to run against the compile-mode binary.
type mcpCall struct {
	name string
	args map[string]any
}

// driveCompileMCP starts the real engram9-mcp compile-mode binary on stagingDir
// with the given turnID/eventBound/receiptPath and executes the given tool calls
// in order over stdio. It returns after the calls complete. This is the real
// cross-process MCP surface an agent would drive; here the test drives it
// deterministically instead of relying on an LLM.
func driveCompileMCP(t *testing.T, bin, stagingDir, turnID string, eventBound uint64, receiptPath string, calls []mcpCall) {
	t.Helper()
	cmd := exec.Command(bin,
		"-data", stagingDir,
		"-mode", "compile",
		"-turn-id", turnID,
		"-event-bound", uitoa(eventBound),
		"-receipt", receiptPath,
	)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)

	writeMCP(t, stdin, 1, "initialize", map[string]any{"protocolVersion": 1})
	readMCP(t, scanner, 1)

	for i, c := range calls {
		id := i + 2
		writeMCP(t, stdin, id, "tools/call", map[string]any{"name": c.name, "arguments": c.args})
		result := readMCP(t, scanner, id)
		// Surface tool errors (except where the caller expects them, e.g. a
		// deliberately empty read window still succeeds).
		var callResult struct {
			IsError bool `json:"isError"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		}
		_ = json.Unmarshal(result, &callResult)
		if callResult.IsError {
			t.Fatalf("tool %q returned error: %s", c.name, firstText(callResult.Content))
		}
	}
}

func firstText(content []struct {
	Text string `json:"text"`
}) string {
	if len(content) == 0 {
		return ""
	}
	return content[0].Text
}

// runFailureAtomicScenario is the shared harness. It seeds a live store, snapshots
// it, copies it to staging (as runACPTurnFullOpts does), drives the real compile
// binary to make staged mutations + emit a receipt, then runs the REAL
// validate → validateCompileReceipt → merge decision and asserts the live store
// is untouched when the receipt is invalid.
func runFailureAtomicScenario(t *testing.T, turnID string, cursor, eventBound uint64, calls []mcpCall, wantReceiptErr string) {
	t.Helper()
	bin := mcpBinary(t)

	// Live store: 2 events + a stale active page (to be archived) + index.
	liveDir := t.TempDir()
	store, err := storage.NewFS(liveDir)
	require.NoError(t, err)
	for _, id := range []string{"evt_001", "evt_002"} {
		_, err := store.AppendEvent(storage.Event{
			ID: id, Content: "content " + id, Actor: "user", Source: "test",
			Durability: "long-term", Actionability: "informational",
			SourceType: "user", EvidenceKind: "direct_statement", TrustTier: 1,
		})
		require.NoError(t, err)
	}
	require.NoError(t, store.WriteWikiPageWithMeta(
		"semantic/projects/stale.md",
		"<!-- compiled_from: evt_000 -->\n<!-- last_compiled: 2026-07-01T00:00:00Z -->\n# stale\n\nprune me\n",
		[]string{"evt_000"}, 1,
	))
	require.NoError(t, store.RebuildIndex())

	beforeHash := hashTree(t, liveDir)
	cursorBefore := store.GetCompileCursor()

	// Stage = copy of live (exactly what runACPTurnFullOpts does).
	stagingDir := t.TempDir()
	require.NoError(t, copyDir(liveDir, stagingDir))

	receiptPath := filepath.Join(stagingDir, compileReceiptName)
	// Drive the real binary to mutate STAGING and emit the receipt.
	driveCompileMCP(t, bin, stagingDir, turnID, eventBound, receiptPath, calls)

	// Sanity: the staged mutations really happened (so the discard is meaningful).
	_, statActive := os.Stat(filepath.Join(stagingDir, "wiki", "semantic", "projects", "stale.md"))
	require.True(t, os.IsNotExist(statActive), "staging: stale.md should have been archived away from active")
	_, statArchived := os.Stat(filepath.Join(stagingDir, "wiki", "archive", "semantic", "projects", "stale.md"))
	require.NoError(t, statArchived, "staging: stale.md should exist under archive/")

	// Run the SHARED, ordered commit gate that production uses
	// (commitStagingTurn: validate → preMerge → merge). The preMerge hook is the
	// real compile receipt validation. Because the receipt is invalid, preMerge
	// returns an error BEFORE merge, so mergeWiki must never run — and an ordering
	// regression (merge before preMerge) would mutate the live store and fail the
	// whole-tree assertion below.
	b := &ACPBackend{dataDir: liveDir, validator: NewWikiValidator(DefaultACPMaxDiffBytes)}
	var receiptValidated bool
	res, turnErr := b.commitStagingTurn(stagingDir, "", ValidateOptions{AllowPairedArchiveMove: true}, acpTurnOptions{
		preMerge: func(sd string) error {
			_, verr := validateCompileReceipt(filepath.Join(sd, compileReceiptName), turnID, cursor, eventBound)
			if verr == nil {
				receiptValidated = true
			}
			return verr
		},
	})
	require.Error(t, turnErr, "commit gate must fail on an invalid receipt")
	require.Contains(t, turnErr.Error(), wantReceiptErr)
	require.False(t, receiptValidated, "receipt must not validate")
	require.False(t, res.Merged, "no merge may occur on an invalid-receipt turn")

	// Whole-tree assertion: live store byte-for-byte identical.
	afterHash := hashTree(t, liveDir)
	require.Equal(t, beforeHash, afterHash,
		"invalid-receipt turn must leave the live store unchanged (active+archive+index+.meta)")
	require.Equal(t, cursorBefore, store.GetCompileCursor(), "compile cursor must be unchanged")

	// The archived page's move must NOT have reached the live store.
	_, liveActive := os.Stat(filepath.Join(liveDir, "wiki", "semantic", "projects", "stale.md"))
	require.NoError(t, liveActive, "live: stale.md must remain active (archive move rolled back)")
	_, liveArchived := os.Stat(filepath.Join(liveDir, "wiki", "archive", "semantic", "projects", "stale.md"))
	require.True(t, os.IsNotExist(liveArchived), "live: no archived copy may appear (whole staging discarded)")
}

// TestRunCompileOrchestrationDiscardsOnInvalidReceipt drives the REAL
// runCompile → runACPTurnFullOpts → commitStagingTurn path end-to-end through a
// fake acpmux (AcpmuxCommand seam, same pattern as the ingest scripted-acpmux
// tests). The fake acpmux completes a turn WITHOUT spawning engram9-mcp, so no
// receipt is written → no_valid_receipt → the real orchestration must discard
// staging and NOT merge. This locks the real spawn/staging/validate→preMerge→
// merge ORDERING (not just the leaf commit function): an implementation that
// merged before validating the receipt, or ignored the preMerge error, would
// mutate the live store and fail this test.
func TestRunCompileOrchestrationDiscardsOnInvalidReceipt(t *testing.T) {
	dataDir := t.TempDir()
	store, err := storage.NewFS(dataDir)
	require.NoError(t, err)
	require.NoError(t, store.WriteWikiPageWithMeta(
		"semantic/projects/keep.md",
		"<!-- compiled_from: evt_000 -->\n<!-- last_compiled: 2026-07-01T00:00:00Z -->\n# keep\n\nlive content\n",
		[]string{"evt_000"}, 1,
	))
	require.NoError(t, store.RebuildIndex())
	beforeHash := hashTree(t, dataDir)
	cursorBefore := store.GetCompileCursor()

	// Fake acpmux: answer the ACP handshake and complete the prompt, but never
	// spawn engram9-mcp — so the compile receipt file is never written.
	scriptPath := filepath.Join(t.TempDir(), "acpmux")
	script := "#!/bin/sh\nset -eu\n" + `
read _init
echo '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}'
read _initnotif
read _session
echo '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"session"}}'
read _prompt
echo '{"jsonrpc":"2.0","id":3,"result":{"text":"done (no receipt written)"}}'
`
	require.NoError(t, os.WriteFile(scriptPath, []byte(script), 0o755))

	b, err := NewACPBackend(dataDir, ACPBackendConfig{
		Provider: "claude", AcpmuxCommand: scriptPath, TurnTimeout: 10 * time.Second,
	})
	require.NoError(t, err)

	res, err := b.runCompile(context.Background(), 0, "compile please")
	require.Error(t, err, "invalid (missing) receipt must fail the turn")
	require.Contains(t, err.Error(), "no_valid_receipt")
	require.Equal(t, uint64(0), res.NewCursor, "cursor must not advance on an invalid-receipt turn")

	// The live store must be byte-for-byte unchanged (nothing merged).
	require.Equal(t, beforeHash, hashTree(t, dataDir),
		"real runCompile orchestration must discard staging on an invalid receipt (no merge)")
	require.Equal(t, cursorBefore, store.GetCompileCursor(), "cursor unchanged on disk")
}

// TestCompileFailureAtomic_MultiRead: two read_events_since calls in one turn →
// multi_read → invalid receipt → whole staging (incl. the staged write and the
// archive paired-move) discarded. Discriminating: a merge-before-validate or a
// leak-past-receipt implementation would mutate the live store and FAIL.
func TestCompileFailureAtomic_MultiRead(t *testing.T) {
	runFailureAtomicScenario(t, "TURN_MR", 0, 2, []mcpCall{
		{name: "read_events_since", args: map[string]any{"cursor": 0}},
		{name: "write_wiki_page", args: map[string]any{
			"path":    "semantic/projects/new.md",
			"content": "<!-- compiled_from: evt_001 -->\n<!-- last_compiled: 2026-07-30T00:00:00Z -->\n# new\n\ndistilled\n",
		}},
		{name: "archive_wiki_page", args: map[string]any{"path": "semantic/projects/stale.md", "reason": "pruned"}},
		{name: "read_events_since", args: map[string]any{"cursor": 0}}, // second read → multi_read
	}, "multi_read")
}

// TestCompileFailureAtomic_NoValidReceipt: the agent makes staged mutations
// (write + archive) but NEVER calls read_events_since → zero receipt entries →
// no_valid_receipt → whole staging discarded.
func TestCompileFailureAtomic_NoValidReceipt(t *testing.T) {
	runFailureAtomicScenario(t, "TURN_NR", 0, 2, []mcpCall{
		{name: "write_wiki_page", args: map[string]any{
			"path":    "semantic/projects/new.md",
			"content": "<!-- compiled_from: evt_001 -->\n<!-- last_compiled: 2026-07-30T00:00:00Z -->\n# new\n\ndistilled\n",
		}},
		{name: "archive_wiki_page", args: map[string]any{"path": "semantic/projects/stale.md", "reason": "pruned"}},
		// no read_events_since at all → no receipt entry for this turn.
	}, "no_valid_receipt")
}

// --- small local helpers (kept separate from the e2e-tagged file) ---

func writeMCP(t *testing.T, w interface{ Write([]byte) (int, error) }, id int, method string, params any) {
	t.Helper()
	req := map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}
	data, err := json.Marshal(req)
	require.NoError(t, err)
	_, err = w.Write(append(data, '\n'))
	require.NoError(t, err)
}

func readMCP(t *testing.T, scanner *bufio.Scanner, id int) json.RawMessage {
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

func hashTree(t *testing.T, root string) string {
	t.Helper()
	h := sha256.New()
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		h.Write([]byte(rel))
		h.Write([]byte{0})
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		h.Write(data)
		h.Write([]byte{0})
		return nil
	})
	require.NoError(t, err)
	return hex.EncodeToString(h.Sum(nil))
}

func uitoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}
