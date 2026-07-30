//go:build e2e_acp

// Package agent E2E tests for the ACP compile/query capabilities.
//
// These tests spawn REAL cross-process infrastructure: acpmux drives a real
// Claude adapter, which connects to a real engram9-mcp subprocess over stdio
// MCP. They are the hard-gate evidence for #41 (COMPILE/QUERY_BACKEND=acp with
// zero API-key dependency). Because they require a working `acpmux`, `claude`
// (authenticated), and `engram9-mcp` on PATH, they are gated behind the
// `e2e_acp` build tag and skipped by default in CI/unit runs.
//
// Run:
//
//	go test -tags e2e_acp -run TestE2E -v -timeout 20m ./internal/agent/
//
// Prereqs (asserted by the test; it FAILS loudly rather than silently skips if a
// prereq is a config error, but SKIPS if the binaries are simply absent):
//   - acpmux on PATH (or ACPMUX_CMD)
//   - claude on PATH, logged in (Claude adapter)
//   - engram9-mcp built and on PATH (the test builds it into a temp dir on PATH)
//   - NO API key required (this is the whole point)
package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/qiffang/engram9/internal/storage"
	"github.com/stretchr/testify/require"
)

// e2eTurnTimeout bounds one real Claude turn. Real turns finish in ~1-3 min;
// 8 min leaves headroom without letting a hung turn wedge the suite.
const e2eTurnTimeout = 8 * time.Minute

// buildEngram9MCPOnPath compiles cmd/engram9-mcp into a temp dir and prepends
// that dir to PATH for the current process, so the session/new MCP launch
// (command: "engram9-mcp") resolves to the just-built binary. Returns the dir.
func buildEngram9MCPOnPath(t *testing.T) string {
	t.Helper()
	binDir := t.TempDir()
	out := filepath.Join(binDir, "engram9-mcp")
	// The module root is two levels up from internal/agent.
	repoRoot, err := filepath.Abs("../..")
	require.NoError(t, err)
	cmd := exec.Command("go", "build", "-o", out, "./cmd/engram9-mcp")
	cmd.Dir = repoRoot
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Run(), "build engram9-mcp")

	oldPath := os.Getenv("PATH")
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+oldPath)
	// Sanity: it must now resolve.
	_, err = exec.LookPath("engram9-mcp")
	require.NoError(t, err, "engram9-mcp must be on PATH after build")
	return binDir
}

// requireE2ETooling skips (not fails) when the real ACP toolchain is absent, so
// unit-only environments don't hard-fail; a present-but-broken tool is surfaced
// by the turn itself.
func requireE2ETooling(t *testing.T) string {
	t.Helper()
	acpmux := os.Getenv("ACPMUX_CMD")
	if acpmux == "" {
		acpmux = "acpmux"
	}
	if _, err := exec.LookPath(acpmux); err != nil {
		t.Skipf("acpmux not found (%v); skipping real ACP E2E", err)
	}
	if _, err := exec.LookPath("claude"); err != nil {
		t.Skipf("claude adapter not found (%v); skipping real ACP E2E", err)
	}
	return acpmux
}

// seedEvent builds a fully-populated raw event with a deterministic ID.
func seedEvent(id, content string) storage.Event {
	return storage.Event{
		ID:            id,
		Timestamp:     "2026-07-29T00:00:00Z",
		Actor:         "user",
		Content:       content,
		Source:        "conversation",
		Durability:    "long-term",
		Actionability: "informational",
		SourceType:    "user",
		EvidenceKind:  "direct_statement",
		TrustTier:     1,
	}
}

// newSeededDataDir creates a fresh data dir with the given events already
// appended to the raw log (cursor starts at 0). Returns the dir.
func newSeededDataDir(t *testing.T, events ...storage.Event) string {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.NewFS(dir)
	require.NoError(t, err)
	for _, ev := range events {
		_, err := store.AppendEvent(ev)
		require.NoError(t, err)
	}
	return dir
}

// hashDir returns a stable content hash of every file under root (path+bytes),
// used to prove a query turn mutates nothing.
func hashDir(t *testing.T, root string) string {
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

func newE2EBackend(t *testing.T, dataDir, acpmux string) *ACPBackend {
	t.Helper()
	b, err := NewACPBackend(dataDir, ACPBackendConfig{
		Provider:      "claude",
		AcpmuxCommand: acpmux,
		TurnTimeout:   e2eTurnTimeout,
	})
	require.NoError(t, err)
	return b
}

// TestE2ECompileClaude runs a real compile cycle through acpmux+Claude and
// asserts the receipt protocol and cursor advancement over a seeded store.
//
// Scope note (no overclaim): this test verifies that a real Claude compile turn
// reads events via the receipt-emitting tool and advances the cursor to the
// pre-turn bound, distilling events into the wiki. It does NOT discriminate the
// snapshot-bound CLAMP — that clamp executes entirely inside engram9-mcp (Claude
// does not participate in it), so wrapping Claude around it adds no discriminating
// power. The discriminating snapshot-bound (A1) evidence is
// TestCompileReadEventsSnapshotBoundClamp in cmd/engram9-mcp (real binary,
// 5 events, -event-bound 3 → clamped to 3; a non-clamping impl fails there).
func TestE2ECompileClaude(t *testing.T) {
	acpmux := requireE2ETooling(t)
	buildEngram9MCPOnPath(t)

	dataDir := newSeededDataDir(t,
		seedEvent("evt_001", "The drive9 project uses FUSE for the client filesystem layer."),
		seedEvent("evt_002", "drive9 stores objects in a content-addressed store keyed by SHA-256."),
	)

	store, err := storage.NewFS(dataDir)
	require.NoError(t, err)
	page, err := store.ReadEventsSince(0)
	require.NoError(t, err)
	require.Equal(t, uint64(2), page.NewCursor, "seeded event count")

	b := newE2EBackend(t, dataDir, acpmux)
	ctx, cancel := context.WithTimeout(context.Background(), e2eTurnTimeout+time.Minute)
	defer cancel()

	res, err := b.RunCompile(ctx, 0)
	require.NoError(t, err, "compile turn should succeed: %s", res.Summary)

	// Cursor advanced to the pre-turn bound (all seeded events consumed).
	require.Equal(t, uint64(2), res.NewCursor, "cursor advances to snapshot bound")

	// The wiki must now contain at least one distilled page mentioning drive9.
	hits, err := store.SearchWiki("drive9")
	require.NoError(t, err)
	require.NotEmpty(t, hits, "compile must distill events into wiki pages (found no 'drive9' mention)")
	t.Logf("compile E2E: cursor 0 -> %d, %d wiki lines mention drive9", res.NewCursor, len(hits))
}

// TestE2EQueryClaude runs a real read-only query through acpmux+Claude and
// asserts (A3) zero store mutation — including .meta access telemetry — and
// (A4) pending-event recall parity: an event passed as recentEvents but NOT in
// the wiki is still reflected in the answer.
func TestE2EQueryClaude(t *testing.T) {
	acpmux := requireE2ETooling(t)
	buildEngram9MCPOnPath(t)

	dataDir := newSeededDataDir(t) // no raw events needed for a pure recall query

	// Seed one wiki page + its sidecar so read_wiki_page has real content and a
	// .meta sidecar we can prove is not rewritten.
	store, err := storage.NewFS(dataDir)
	require.NoError(t, err)
	require.NoError(t, store.WriteWikiPageWithMeta(
		"semantic/projects/drive9.md",
		"# drive9\n\ndrive9 is a distributed filesystem. It uses a FUSE client.\n",
		[]string{"evt_seed"}, 1,
	))
	require.NoError(t, store.RebuildIndex())

	beforeHash := hashDir(t, dataDir)

	b := newE2EBackend(t, dataDir, acpmux)
	ctx, cancel := context.WithTimeout(context.Background(), e2eTurnTimeout+time.Minute)
	defer cancel()

	// A pending event NOT written to the wiki — recall parity must surface it.
	pending := []storage.Event{
		seedEvent("evt_pending", "URGENT: drive9 v2.5 was just released today with a new write-back cache."),
	}

	res, err := b.RunQuery(ctx,
		"What is the most recent drive9 release and what did it add?",
		map[string]string{"asked_by": "e2e"},
		pending,
	)
	require.NoError(t, err, "query turn should succeed")
	require.NotEmpty(t, res.Answer, "query must return an answer")

	// A4: the answer must reflect the pending (not-yet-compiled) event.
	ans := strings.ToLower(res.Answer)
	require.True(t,
		strings.Contains(ans, "v2.5") || strings.Contains(ans, "write-back") || strings.Contains(ans, "write back"),
		"answer must reflect the pending event (recall parity); got: %s", res.Answer)

	// A3: zero store mutation — the entire data dir must be byte-identical.
	afterHash := hashDir(t, dataDir)
	require.Equal(t, beforeHash, afterHash,
		"query must not mutate the store (A3: no .meta access telemetry writeback)")
	t.Logf("query E2E: zero-mutation verified; answer reflects pending event")
}
