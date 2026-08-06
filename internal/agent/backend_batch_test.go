package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestACPBackendRunBatchIngestSuccess(t *testing.T) {
	backend := newScriptedACPBackend(t, `
read initialize
echo '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}'
read initialized
read session
echo '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"session"}}'
read prompt
printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"text":"EVENT a INTEGRATED pages: semantic/a.md\nEVENT b SKIPPED reason: duplicate"}}'
`)
	batch := makeBatch([]PendingEvent{{ID: "a", Text: "alpha"}, {ID: "b", Text: "beta"}}, 0)
	wikiMu := &sync.Mutex{}
	var rebuildCalls atomic.Int64

	result, err := backend.RunBatchIngest(context.Background(), batch, wikiMu, func() error {
		require.False(t, wikiMu.TryLock(), "rebuild must run while wikiMu is held")
		rebuildCalls.Add(1)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, "success", result.Status)
	require.Equal(t, batch.ID, result.BatchID)
	require.Equal(t, int64(1), rebuildCalls.Load())
	require.Equal(t, []EventResult{
		{EventID: "a", Status: "integrated", Pages: []string{"semantic/a.md"}},
		{EventID: "b", Status: "skipped", Reason: "duplicate"},
	}, result.EventResults)
	require.False(t, result.IndexStale)
	require.Nil(t, result.Error)
	require.True(t, wikiMu.TryLock())
	wikiMu.Unlock()
}

func TestACPBackendRunBatchIngestPreservesValidationSummary(t *testing.T) {
	backend := newScriptedACPBackend(t, `
read initialize
echo '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}'
read initialized
read session
cwd=$(printf '%s' "$session" | sed -E 's/.*"cwd":"([^"]+)".*/\1/')
echo invalid > "$cwd/wiki/semantic/bad.txt"
echo '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"session"}}'
read prompt
echo '{"jsonrpc":"2.0","id":3,"result":{"text":"EVENT a INTEGRATED pages: semantic/a.md"}}'
`)
	batch := makeBatch([]PendingEvent{{ID: "a", Text: "alpha"}}, 0)
	var rebuilt atomic.Bool

	result, err := backend.RunBatchIngest(context.Background(), batch, &sync.Mutex{}, func() error {
		rebuilt.Store(true)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, "failed", result.Status)
	require.Contains(t, result.Summary, "EVENT a INTEGRATED")
	require.NotEmpty(t, result.Violations)
	require.False(t, rebuilt.Load())
	require.Empty(t, result.EventResults)
}

func TestACPBackendRunBatchIngestClassifiesProcessFailure(t *testing.T) {
	backend := newScriptedACPBackend(t, `exit 7`)
	batch := makeBatch([]PendingEvent{{ID: "a"}}, 0)
	result, err := backend.RunBatchIngest(context.Background(), batch, &sync.Mutex{}, func() error { return nil })
	require.NoError(t, err)
	require.Equal(t, "failed", result.Status)
	require.Error(t, result.Error)
	require.Equal(t, "crash", result.errorClass())
}

func TestACPBackendRunBatchIngestClassifiesObservedOAuthExpiry(t *testing.T) {
	backend := newScriptedACPBackend(t, `
read initialize
echo '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}'
read initialized
read session
echo '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"session"}}'
read prompt
printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"text":"Let me write these pages.\nFailed to authenticate. API Error: 401 {\"type\":\"error\",\"error\":{\"type\":\"authentication_error\",\"message\":\"OAuth access token has expired. Re-authenticate to continue.\"}}"}}'
`)
	batch := makeBatch([]PendingEvent{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}}, 0)

	result, err := backend.RunBatchIngest(context.Background(), batch, &sync.Mutex{}, func() error { return nil })

	require.NoError(t, err)
	require.Equal(t, "success", result.Status)
	require.Len(t, result.EventResults, 4)
	for _, eventResult := range result.EventResults {
		require.Equal(t, "unknown", eventResult.Status)
		require.Equal(t, UnknownReasonAuthExpired, eventResult.Reason)
		require.NotEmpty(t, eventResult.TranscriptPath)
		require.FileExists(t, eventResult.TranscriptPath)
	}
}

func TestACPBackendRunBatchIngestRejectsMissingPromptCompletion(t *testing.T) {
	backend := newScriptedACPBackend(t, `
read initialize
echo '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}'
read initialized
read session
echo '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"session"}}'
read prompt
`)
	batch := makeBatch([]PendingEvent{{ID: "a"}}, 0)
	result, err := backend.RunBatchIngest(context.Background(), batch, &sync.Mutex{}, func() error { return nil })
	require.NoError(t, err)
	require.Equal(t, "failed", result.Status)
	require.ErrorContains(t, result.Error, "prompt response missing")
	require.Equal(t, "crash", result.errorClass())
}

func TestACPBackendRunBatchIngestMapsTransportEOFToShutdownCancellation(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), "prompt-read")
	backend := newScriptedACPBackend(t, fmt.Sprintf(`
read initialize
echo '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}'
read initialized
read session
echo '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"session"}}'
read prompt
: > %q
read blocked
	`, markerPath))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultCh := make(chan BatchResult, 1)
	errCh := make(chan error, 1)
	go func() {
		result, err := backend.RunBatchIngest(ctx, makeBatch([]PendingEvent{{ID: "a"}}, 2), &sync.Mutex{}, func() error { return nil })
		resultCh <- result
		errCh <- err
	}()
	require.Eventually(t, func() bool {
		_, err := os.Stat(markerPath)
		return err == nil
	}, time.Second, 10*time.Millisecond)

	cancel()

	select {
	case result := <-resultCh:
		require.NoError(t, <-errCh)
		require.Equal(t, "failed", result.Status)
		require.ErrorIs(t, result.Error, context.Canceled)
		require.True(t, result.IsShutdownCancel())
	case <-time.After(time.Second):
		t.Fatal("RunBatchIngest did not return after cancellation")
	}
}

func TestACPBackendRunBatchIngestReportsIndexStale(t *testing.T) {
	backend := newScriptedACPBackend(t, `
read initialize
echo '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}'
read initialized
read session
echo '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"session"}}'
read prompt
echo '{"jsonrpc":"2.0","id":3,"result":{"text":"EVENT a SKIPPED reason: no-op"}}'
`)
	result, err := backend.RunBatchIngest(context.Background(), makeBatch([]PendingEvent{{ID: "a"}}, 0), &sync.Mutex{}, func() error {
		return os.ErrPermission
	})
	require.NoError(t, err)
	require.Equal(t, "success", result.Status)
	require.True(t, result.IndexStale)
}

func TestACPBackendRunBatchIngestWaitsForNonCancellableRebuild(t *testing.T) {
	backend := newScriptedACPBackend(t, `
read initialize
echo '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}'
read initialized
read session
echo '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"session"}}'
read prompt
echo '{"jsonrpc":"2.0","id":3,"result":{"text":"EVENT a SKIPPED reason: no-op"}}'
`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rebuildFinished := atomic.Bool{}
	startedAt := time.Now()
	result, err := backend.RunBatchIngest(ctx, makeBatch([]PendingEvent{{ID: "a"}}, 0), &sync.Mutex{}, func() error {
		cancel()
		time.Sleep(30 * time.Millisecond)
		rebuildFinished.Store(true)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, "success", result.Status)
	require.True(t, rebuildFinished.Load())
	require.GreaterOrEqual(t, time.Since(startedAt), 30*time.Millisecond)
	require.GreaterOrEqual(t, result.DurationMs, int64(30))
}

func TestACPBackendRunBatchIngestContextAwareLock(t *testing.T) {
	backend := newScriptedACPBackend(t, `exit 1`)
	wikiMu := &sync.Mutex{}
	wikiMu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	result, err := backend.RunBatchIngest(ctx, makeBatch([]PendingEvent{{ID: "a"}}, 0), wikiMu, func() error { return nil })
	require.NoError(t, err)
	require.ErrorIs(t, result.Error, context.DeadlineExceeded)

	wikiMu.Unlock()
	require.Eventually(t, func() bool {
		if !wikiMu.TryLock() {
			return false
		}
		wikiMu.Unlock()
		return true
	}, time.Second, 10*time.Millisecond)
}

func TestACPBackendRunBatchIngestRejectsMissingDependencies(t *testing.T) {
	backend := &ACPBackend{}
	_, err := backend.RunBatchIngest(context.Background(), Batch{}, nil, func() error { return nil })
	require.ErrorContains(t, err, "wiki mutex")
	_, err = backend.RunBatchIngest(context.Background(), Batch{}, &sync.Mutex{}, nil)
	require.ErrorContains(t, err, "rebuild")
}

func TestBatchResultShutdownCancellation(t *testing.T) {
	require.True(t, (BatchResult{Error: context.Canceled}).IsShutdownCancel())
	require.False(t, (BatchResult{Error: context.DeadlineExceeded}).IsShutdownCancel())
	require.Equal(t, "timeout", (BatchResult{Error: context.DeadlineExceeded}).errorClass())
	require.Equal(t, "validation", (BatchResult{Violations: []Violation{{Path: "bad.md"}}}).errorClass())
}

func newScriptedACPBackend(t *testing.T, scriptBody string) *ACPBackend {
	t.Helper()
	dataDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "wiki", "semantic"), 0o755))
	scriptPath := filepath.Join(t.TempDir(), "acpmux")
	require.NoError(t, os.WriteFile(scriptPath, []byte("#!/bin/sh\nset -eu\n"+scriptBody), 0o755))
	backend, err := NewACPBackend(dataDir, ACPBackendConfig{
		// Engram9McpCommand is set to a resolvable stand-in (the same fixture
		// script) so NewACPBackend's fail-closed companion check (task #66) is
		// satisfied without relying on an ambient-PATH engram9-mcp. These tests
		// use scripted ACP responses and never launch the real companion.
		Provider: "claude", AcpmuxCommand: scriptPath, Engram9McpCommand: scriptPath, TurnTimeout: time.Second,
	})
	require.NoError(t, err)
	return backend
}
