package agent

import (
	"strings"
	"testing"
	"time"

	"github.com/qiffang/engram9/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestNormalizeToPendingEventUsesPersistedContext(t *testing.T) {
	event := storage.Event{
		ID:          "evt-1",
		Timestamp:   "2026-07-19T10:11:12.123456789Z",
		Content:     "remember this",
		Actor:       "fallback",
		ContextJSON: `{"actor":"alice","custom":"value"}`,
		SourceType:  "user",
	}

	got := NormalizeToPendingEvent(event)
	require.Equal(t, "evt-1", got.ID)
	require.Equal(t, "remember this", got.Text)
	require.Equal(t, map[string]string{"actor": "alice", "custom": "value"}, got.Context)
	require.Equal(t, time.Date(2026, 7, 19, 10, 11, 12, 123456789, time.UTC), got.CreatedAt)
}

func TestNormalizeToPendingEventFallbacks(t *testing.T) {
	tests := []struct {
		name        string
		event       storage.Event
		wantContext map[string]string
	}{
		{
			name: "pre migration fields",
			event: storage.Event{
				Actor: "alice", Source: "chat", SessionID: "s1", ActiveProject: "canon9",
				ActiveTask: "batch", SourceType: "user",
			},
			wantContext: map[string]string{
				"actor": "alice", "source": "chat", "session_id": "s1", "active_project": "canon9",
				"active_task": "batch", "source_type": "user",
			},
		},
		{
			name:        "malformed persisted context",
			event:       storage.Event{ContextJSON: `{"actor":`, Actor: "fallback", SourceType: "user"},
			wantContext: map[string]string{"actor": "fallback", "source_type": "user"},
		},
		{
			name:        "null persisted context",
			event:       storage.Event{ContextJSON: "null", SourceType: "user"},
			wantContext: nil,
		},
		{
			name:        "empty request context",
			event:       storage.Event{SourceType: "user"},
			wantContext: nil,
		},
		{
			name:        "explicit empty object",
			event:       storage.Event{ContextJSON: `{}`, Actor: "ignored", SourceType: "user"},
			wantContext: map[string]string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := NormalizeToPendingEvent(test.event)
			require.Equal(t, test.wantContext, got.Context)
			require.True(t, got.CreatedAt.IsZero())
		})
	}
}

func TestFormBatchesDeterministicOrderingAndCaps(t *testing.T) {
	base := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	events := []PendingEvent{
		{ID: "c", Text: "cccc", CreatedAt: base.Add(time.Second)},
		{ID: "b", Text: "bbbb", CreatedAt: base},
		{ID: "a", Text: "aaaa", CreatedAt: base},
	}
	limits := BatchLimits{MaxEventsPerBatch: 2, MaxTokensPerBatch: 100, MaxBytesPerBatch: 100}

	got := FormBatches(events, limits)
	require.Len(t, got, 2)
	require.Equal(t, []string{"a", "b"}, []string{got[0].Events[0].ID, got[0].Events[1].ID})
	require.Equal(t, []string{"c"}, []string{got[1].Events[0].ID})
	require.Equal(t, "1eb7c54d52831bbf", got[0].ID)
	require.Equal(t, 16, len(got[0].ID))

	again := FormBatches(events, limits)
	require.Equal(t, got, again)
}

func TestFormBatchesAppliesByteAndTokenCaps(t *testing.T) {
	events := []PendingEvent{
		{ID: "a", Text: "12345678"},
		{ID: "b", Text: "12345678"},
	}

	byBytes := FormBatches(events, BatchLimits{MaxEventsPerBatch: 20, MaxTokensPerBatch: 100, MaxBytesPerBatch: 20})
	require.Len(t, byBytes, 2)

	byTokens := FormBatches(events, BatchLimits{MaxEventsPerBatch: 20, MaxTokensPerBatch: 3, MaxBytesPerBatch: 100})
	require.Len(t, byTokens, 2)
}

func TestFormBatchesAllowsSoloOversizeEvent(t *testing.T) {
	event := PendingEvent{ID: "large", Text: strings.Repeat("x", 100)}
	got := FormBatches([]PendingEvent{event}, BatchLimits{MaxEventsPerBatch: 1, MaxTokensPerBatch: 1, MaxBytesPerBatch: 1})
	require.Len(t, got, 1)
	require.Equal(t, 104, got[0].ByteSize)
	require.Equal(t, 26, got[0].TokenEst)
}

func TestFormBatchesInputValidation(t *testing.T) {
	require.Nil(t, FormBatches(nil, DefaultBatchLimits))
	for _, limits := range []BatchLimits{
		{MaxEventsPerBatch: 0, MaxTokensPerBatch: 1, MaxBytesPerBatch: 1},
		{MaxEventsPerBatch: 1, MaxTokensPerBatch: 0, MaxBytesPerBatch: 1},
		{MaxEventsPerBatch: 1, MaxTokensPerBatch: 1, MaxBytesPerBatch: 0},
	} {
		require.Panics(t, func() { FormBatches(nil, limits) })
	}
}

func TestEventByteSizeIncludesDeterministicContext(t *testing.T) {
	event := PendingEvent{Text: "hello", Context: map[string]string{"z": "last", "a": "first"}}
	require.Equal(t, len("hello")+len("a=first; z=last"), EventByteSize(event))
	require.Equal(t, "a=first; z=last", serializeContext(event.Context))
	require.Equal(t, "none", serializeContext(nil))
}

func TestBuildBatchPromptIncludesOrderedEventsAndOutputContract(t *testing.T) {
	createdAt := time.Date(2026, 7, 19, 10, 11, 12, 123456789, time.FixedZone("offset", 8*60*60))
	batch := makeBatch([]PendingEvent{
		{ID: "evt-a", Text: "alpha", Context: map[string]string{"z": "2", "a": "1"}, CreatedAt: createdAt},
		{ID: "evt-b", Text: "beta"},
	}, 0)

	prompt := BuildBatchPrompt(batch)
	require.Contains(t, prompt, "Process the following 2 events as a batch")
	require.Contains(t, prompt, "Timestamp: 2026-07-19T02:11:12.123456789Z")
	require.Contains(t, prompt, "Context: a=1; z=2")
	require.Contains(t, prompt, "Context: none")
	require.Less(t, strings.Index(prompt, "### Event 1: evt-a"), strings.Index(prompt, "### Event 2: evt-b"))
	require.Contains(t, prompt, "EVENT {eventID} INTEGRATED pages:")
	require.Contains(t, prompt, "Frontmatter compiled_from: must list ALL event IDs")
}

func TestParseEventResults(t *testing.T) {
	batch := makeBatch([]PendingEvent{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}}, 0)
	summary := strings.Join([]string{
		"EVENT a INTEGRATED pages: semantic/a.md",
		"EVENT b SKIPPED reason: duplicate",
		"EVENT c FAILED reason: cannot classify",
		"EVENT a FAILED reason: later result wins",
		"EVENT foreign INTEGRATED pages: semantic/foreign.md",
		"EVENT d INTEGRATED missing-prefix",
	}, "\n")

	got := parseEventResults(batch, summary, "transcripts/test.log")
	require.Equal(t, []EventResult{
		{EventID: "a", Status: "failed_by_agent", Reason: "later result wins"},
		{EventID: "b", Status: "skipped", Reason: "duplicate"},
		{EventID: "c", Status: "failed_by_agent", Reason: "cannot classify"},
		{EventID: "d", Status: "unknown", Reason: UnknownReasonNoPerEventVerdict, TranscriptPath: "transcripts/test.log"},
	}, got)
}

// task #38 (PR-A): once producers size each event under DefaultMaxTokensPerBatch/k,
// FormBatches actually packs >=k events into ONE batch — previously (24k events >
// 12k cap) every batch held exactly one event, which masked the multi-event
// verdict path. This asserts both halves: >=4 same-size events pack into one
// batch at the real default limits, AND each gets its own distinct verdict with
// no cross-talk or loss.
func TestFormBatchesPacksMultipleEventsAndEachGetsOwnVerdict(t *testing.T) {
	// FormBatches estimates tokens as bytes/4. Size each event at ~cap/5 tokens
	// so >=5 fit under the default token cap (and comfortably >=4).
	perEventChars := DefaultMaxTokensPerBatch / 5 * 4
	events := []PendingEvent{
		{ID: "e1", Text: strings.Repeat("a", perEventChars)},
		{ID: "e2", Text: strings.Repeat("b", perEventChars)},
		{ID: "e3", Text: strings.Repeat("c", perEventChars)},
		{ID: "e4", Text: strings.Repeat("d", perEventChars)},
	}
	batches := FormBatches(events, DefaultBatchLimits)
	require.Len(t, batches, 1, "4 events sized at cap/5 must pack into ONE batch (not 1-per-batch)")
	require.Len(t, batches[0].Events, 4)

	// Each event gets its own distinct verdict — no cross-talk, none lost.
	summary := strings.Join([]string{
		"EVENT e1 INTEGRATED pages: semantic/e1.md",
		"EVENT e2 SKIPPED reason: duplicate",
		"EVENT e3 FAILED reason: cannot classify",
		"EVENT e4 INTEGRATED pages: semantic/e4.md",
	}, "\n")
	got := parseEventResults(batches[0], summary, "transcripts/test.log")
	require.Equal(t, []EventResult{
		{EventID: "e1", Status: "integrated", Pages: []string{"semantic/e1.md"}},
		{EventID: "e2", Status: "skipped", Reason: "duplicate"},
		{EventID: "e3", Status: "failed_by_agent", Reason: "cannot classify"},
		{EventID: "e4", Status: "integrated", Pages: []string{"semantic/e4.md"}},
	}, got)
}

// task #38 (PR-A, adversary-1 blocker #1): FormBatches sizes each event as
// text + serialized context (EventByteSize), so an autopilot budget that reserves
// only the TEXT at capacity/k leaves no room for context and k near-limit events
// overflow the batch. This asserts the contract the autopilot reserve must honor:
// events whose TEXT is (cap/k - contextTokens) each, PLUS a realistic /remember
// context, still pack k into ONE batch under DefaultBatchLimits.
func TestFourNearLimitEventsWithContextPackIntoOneBatch(t *testing.T) {
	// Mirror the /remember context autopilot sends (see wiki.py _remember_context).
	ctx := map[string]string{
		"actor":          "drive9-autopilot",
		"source":         "repo_scan",
		"active_project": "qiffang/drive9",
		"active_task":    "pkg/fuse",
	}
	contextBytes := len(serializeContext(ctx))
	k := 4
	perEventTokens := DefaultMaxTokensPerBatch / k
	// Autopilot reserves the context cost (bytes/4) PLUS a safety margin from the
	// per-event text budget — mirrors wiki.py _ingest_context_overhead_tokens
	// (contextBytes/4 + WIKI_INGEST_CONTEXT_SAFETY_TOKENS=64). The margin is what
	// keeps text+context under BOTH the token and byte caps (48000 = 12000*4, so
	// the byte cap binds without it).
	const contextSafetyTokens = 64
	textBudgetTokens := perEventTokens - (contextBytes/4 + contextSafetyTokens)
	textBytes := textBudgetTokens * 4 // FormBatches estimates tokens as bytes/4

	events := make([]PendingEvent, k)
	for i := 0; i < k; i++ {
		events[i] = PendingEvent{
			ID:      "e" + string(rune('1'+i)),
			Text:    strings.Repeat("x", textBytes),
			Context: ctx,
		}
	}
	batches := FormBatches(events, DefaultBatchLimits)
	require.Len(t, batches, 1,
		"4 events whose text reserves the context overhead must pack into ONE batch")
	require.Len(t, batches[0].Events, k)

	// And prove the reserve is load-bearing: without it (text = full cap/k), the
	// same 4 events plus context DO overflow into more than one batch.
	unreserved := make([]PendingEvent, k)
	for i := 0; i < k; i++ {
		unreserved[i] = PendingEvent{
			ID:      "u" + string(rune('1'+i)),
			Text:    strings.Repeat("x", perEventTokens*4),
			Context: ctx,
		}
	}
	require.Greater(t, len(FormBatches(unreserved, DefaultBatchLimits)), 1,
		"sanity: without the context reserve, 4 cap/k-text events + context must NOT fit one batch")
}

func TestParseEventResultsUnknownReasons(t *testing.T) {
	batch := makeBatch([]PendingEvent{{ID: "a"}, {ID: "b"}}, 0)

	require.Equal(t, UnknownReasonTurnEndedNoOutput, parseEventResults(batch, "", "transcripts/test.log")[0].Reason)
	require.Equal(t, UnknownReasonMalformedVerdict, parseEventResults(batch, "EVENT foreign SKIPPED reason: no", "transcripts/test.log")[0].Reason)
	require.Equal(t, UnknownReasonNoPerEventVerdict, parseEventResults(batch, "unstructured summary", "transcripts/test.log")[0].Reason)
}

func TestMakeBatchRecomputesRetryMetadata(t *testing.T) {
	events := []PendingEvent{{ID: "b", Text: "1234"}, {ID: "a", Text: "5678"}}
	got := makeBatch(events, 2)
	require.Equal(t, "1eb7c54d52831bbf", got.ID)
	require.Equal(t, 16, got.ByteSize)
	require.Equal(t, 4, got.TokenEst)
	require.Equal(t, 2, got.Depth)
}
