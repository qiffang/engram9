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
