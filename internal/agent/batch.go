package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/qiffang/engram9/internal/storage"
)

const (
	DefaultMaxEventsPerBatch = 20
	DefaultMaxTokensPerBatch = 12000
	DefaultMaxBytesPerBatch  = 48000
)

type BatchLimits struct {
	MaxEventsPerBatch int
	MaxTokensPerBatch int
	MaxBytesPerBatch  int
}

var DefaultBatchLimits = BatchLimits{
	MaxEventsPerBatch: DefaultMaxEventsPerBatch,
	MaxTokensPerBatch: DefaultMaxTokensPerBatch,
	MaxBytesPerBatch:  DefaultMaxBytesPerBatch,
}

type PendingEvent struct {
	ID        string
	Text      string
	Context   map[string]string
	CreatedAt time.Time
}

type Batch struct {
	ID       string
	Events   []PendingEvent
	TokenEst int
	ByteSize int
	Depth    int
}

type EventResult struct {
	EventID string   `json:"event_id"`
	Status  string   `json:"status"`
	Pages   []string `json:"pages,omitempty"`
	Reason  string   `json:"reason,omitempty"`
	// TranscriptPath points to the persisted ACP turn transcript. Set for
	// every "unknown" outcome (never empty there) so operators can triage
	// silent agent failures without hunting for logs.
	TranscriptPath string `json:"transcript_path,omitempty"`
}

// Closed reason enum for "unknown" outcomes. Classification failure maps to
// UnknownReasonUnclassified — an empty reason is structurally impossible.
const (
	UnknownReasonAuthExpired        = "auth_expired"               // provider OAuth token expired during the turn
	UnknownReasonNoPerEventVerdict  = "no_per_event_verdict"       // agent output has verdict lines, none for this event
	UnknownReasonMalformedVerdict   = "malformed_verdict"          // verdict lines present but none match batch event IDs
	UnknownReasonTurnEndedNoOutput  = "turn_ended_no_output"       // agent turn completed with empty output
	UnknownReasonParserError        = "parser_error"               // verdict parsing failed structurally
	UnknownReasonRecoveredFromCrash = "recovered_from_frontmatter" // event was in-flight at process death; frontmatter shows page contact but no verdict
	UnknownReasonUnclassified       = "unclassified"               // fallback — never ""
)

// IsValidUnknownReason reports whether reason is a member of the closed
// unknown-reason enum. The store normalizes anything else to
// UnknownReasonUnclassified at its write AND read boundaries.
func IsValidUnknownReason(reason string) bool {
	switch reason {
	case UnknownReasonAuthExpired, UnknownReasonNoPerEventVerdict, UnknownReasonMalformedVerdict,
		UnknownReasonTurnEndedNoOutput, UnknownReasonParserError,
		UnknownReasonRecoveredFromCrash, UnknownReasonUnclassified:
		return true
	default:
		return false
	}
}

// Transcript path markers used when no real transcript file exists. Exported
// so consumers can match them without magic strings.
const (
	TranscriptUnavailableMarker = "transcript-unavailable"  // caller had no transcript to persist
	TranscriptWriteFailedMarker = "transcript-write-failed" // persistence attempted and failed
)

func NormalizeToPendingEvent(event storage.Event) PendingEvent {
	createdAt := time.Time{}
	for _, layout := range []string{time.RFC3339, time.RFC3339Nano} {
		parsed, err := time.Parse(layout, event.Timestamp)
		if err == nil {
			createdAt = parsed
			break
		}
	}

	var contextInfo map[string]string
	if event.ContextJSON != "" && event.ContextJSON != "null" {
		var parsed map[string]string
		if err := json.Unmarshal([]byte(event.ContextJSON), &parsed); err == nil && parsed != nil {
			contextInfo = parsed
		}
	}
	if contextInfo == nil {
		fallback := make(map[string]string)
		addContextValue(fallback, "actor", event.Actor)
		addContextValue(fallback, "source", event.Source)
		addContextValue(fallback, "session_id", event.SessionID)
		addContextValue(fallback, "active_project", event.ActiveProject)
		addContextValue(fallback, "active_task", event.ActiveTask)
		if len(fallback) > 0 {
			addContextValue(fallback, "source_type", event.SourceType)
			contextInfo = fallback
		}
	}

	return PendingEvent{
		ID:        event.ID,
		Text:      event.Content,
		Context:   contextInfo,
		CreatedAt: createdAt,
	}
}

func addContextValue(contextInfo map[string]string, key, value string) {
	if value != "" {
		contextInfo[key] = value
	}
}

func serializeContext(contextInfo map[string]string) string {
	if len(contextInfo) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(contextInfo))
	for key := range contextInfo {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+contextInfo[key])
	}
	return strings.Join(parts, "; ")
}

func EventByteSize(event PendingEvent) int {
	return len(event.Text) + len(serializeContext(event.Context))
}

func FormBatches(events []PendingEvent, limits BatchLimits) []Batch {
	if limits.MaxEventsPerBatch <= 0 || limits.MaxTokensPerBatch <= 0 || limits.MaxBytesPerBatch <= 0 {
		panic("batch limits must be positive")
	}
	if len(events) == 0 {
		return nil
	}

	ordered := append([]PendingEvent(nil), events...)
	sort.SliceStable(ordered, func(left, right int) bool {
		if ordered[left].CreatedAt.Equal(ordered[right].CreatedAt) {
			return ordered[left].ID < ordered[right].ID
		}
		return ordered[left].CreatedAt.Before(ordered[right].CreatedAt)
	})

	var batches []Batch
	start := 0
	byteSize := 0
	tokenEstimate := 0
	for index, event := range ordered {
		eventBytes := EventByteSize(event)
		eventTokens := eventBytes / 4
		currentCount := index - start
		wouldExceed := currentCount > 0 && (currentCount+1 > limits.MaxEventsPerBatch ||
			byteSize+eventBytes > limits.MaxBytesPerBatch ||
			tokenEstimate+eventTokens > limits.MaxTokensPerBatch)
		if wouldExceed {
			batches = append(batches, makeBatch(ordered[start:index], 0))
			start = index
			byteSize = 0
			tokenEstimate = 0
		}
		byteSize += eventBytes
		tokenEstimate += eventTokens
	}
	batches = append(batches, makeBatch(ordered[start:], 0))
	return batches
}

func makeBatch(events []PendingEvent, depth int) Batch {
	ids := make([]string, len(events))
	byteSize := 0
	tokenEstimate := 0
	for index, event := range events {
		ids[index] = event.ID
		eventBytes := EventByteSize(event)
		byteSize += eventBytes
		tokenEstimate += eventBytes / 4
	}
	sort.Strings(ids)
	digest := sha256.Sum256([]byte(strings.Join(ids, ",")))
	return Batch{
		ID:       hex.EncodeToString(digest[:])[:16],
		Events:   append([]PendingEvent(nil), events...),
		TokenEst: tokenEstimate,
		ByteSize: byteSize,
		Depth:    depth,
	}
}

func BuildBatchPrompt(batch Batch) string {
	var prompt strings.Builder
	fmt.Fprintf(&prompt, "You are the Ingest Agent. Process the following %d events as a batch.\n\n", len(batch.Events))
	prompt.WriteString("## Events to integrate (ordered by time):\n\n")
	for index, event := range batch.Events {
		fmt.Fprintf(
			&prompt,
			"### Event %d: %s\nTimestamp: %s\nContext: %s\n%s\n\n",
			index+1,
			event.ID,
			event.CreatedAt.UTC().Format(time.RFC3339Nano),
			serializeContext(event.Context),
			event.Text,
		)
	}
	prompt.WriteString("## Instructions\n")
	prompt.WriteString(integrateSystemPrompt)
	prompt.WriteString(`

Key batch rules:
- Process ALL events in order. Each event MUST be reflected in wiki or explicitly skipped.
- For each event, report: INTEGRATED (pages touched) or SKIPPED (reason).
- Multiple events about the same topic should be merged into coherent wiki updates (not separate edits per event).
- Frontmatter compiled_from: must list ALL event IDs that contributed to each page update.
- You may read the wiki index once, then batch your writes. Minimize redundant reads.
- If you encounter an error writing one page, continue with remaining events.

## Required output format (after all tool calls):
For each event, emit exactly one line:
EVENT {eventID} INTEGRATED pages: {path1}, {path2}
EVENT {eventID} SKIPPED reason: {reason}
EVENT {eventID} FAILED reason: {reason}
`)
	return prompt.String()
}

var eventResultLinePattern = regexp.MustCompile(`^EVENT\s+(\S+)\s+(INTEGRATED|SKIPPED|FAILED)\s+(.+)$`)

func parseEventResults(batch Batch, summary string, transcriptPath string) []EventResult {
	batchIDs := make(map[string]struct{}, len(batch.Events))
	for _, event := range batch.Events {
		batchIDs[event.ID] = struct{}{}
	}

	parsed := make(map[string]EventResult, len(batch.Events))
	validEventLines := 0
	for _, rawLine := range strings.Split(summary, "\n") {
		matches := eventResultLinePattern.FindStringSubmatch(strings.TrimSpace(rawLine))
		if matches == nil {
			continue
		}
		result, ok := parseEventResultLine(matches[1], matches[2], matches[3])
		if !ok {
			continue
		}
		validEventLines++
		if _, ok := batchIDs[result.EventID]; !ok {
			continue
		}
		parsed[result.EventID] = result
	}

	defaultReason := UnknownReasonNoPerEventVerdict
	if providerAuthenticationExpired(summary) {
		defaultReason = UnknownReasonAuthExpired
	} else if strings.TrimSpace(summary) == "" {
		defaultReason = UnknownReasonTurnEndedNoOutput
	} else if validEventLines > 0 && len(parsed) == 0 {
		defaultReason = UnknownReasonMalformedVerdict
	}
	if defaultReason == "" {
		defaultReason = UnknownReasonUnclassified
	}
	if transcriptPath == "" {
		transcriptPath = TranscriptUnavailableMarker
	}

	results := make([]EventResult, 0, len(batch.Events))
	for _, event := range batch.Events {
		result, ok := parsed[event.ID]
		if !ok {
			result = EventResult{EventID: event.ID, Status: "unknown", Reason: defaultReason, TranscriptPath: transcriptPath}
		}
		results = append(results, result)
	}
	return results
}

// providerAuthenticationExpired recognizes the terminal Claude Code failure
// shape observed in production. It intentionally requires all three signals
// (HTTP 401, authentication failure, and an expired/re-authenticate marker) so
// an event whose prose merely discusses OAuth does not get misclassified.
func providerAuthenticationExpired(summary string) bool {
	normalized := strings.ToLower(summary)
	if !strings.Contains(normalized, "api error: 401") {
		return false
	}
	authFailure := strings.Contains(normalized, "failed to authenticate") ||
		strings.Contains(normalized, "authentication_error")
	expired := strings.Contains(normalized, "oauth access token has expired") ||
		strings.Contains(normalized, "re-authenticate to continue")
	return authFailure && expired
}

func parseEventResultLine(eventID, status, tail string) (EventResult, bool) {
	tail = strings.TrimSpace(tail)
	switch status {
	case "INTEGRATED":
		const prefix = "pages:"
		if !strings.HasPrefix(tail, prefix) {
			return EventResult{}, false
		}
		var pages []string
		for _, page := range strings.Split(strings.TrimSpace(strings.TrimPrefix(tail, prefix)), ",") {
			if page = strings.TrimSpace(page); page != "" {
				pages = append(pages, page)
			}
		}
		if len(pages) == 0 {
			return EventResult{}, false
		}
		return EventResult{EventID: eventID, Status: "integrated", Pages: pages}, true
	case "SKIPPED", "FAILED":
		const prefix = "reason:"
		if !strings.HasPrefix(tail, prefix) {
			return EventResult{}, false
		}
		reason := strings.TrimSpace(strings.TrimPrefix(tail, prefix))
		if reason == "" {
			return EventResult{}, false
		}
		resultStatus := "skipped"
		if status == "FAILED" {
			resultStatus = "failed_by_agent"
		}
		return EventResult{EventID: eventID, Status: resultStatus, Reason: reason}, true
	default:
		return EventResult{}, false
	}
}
