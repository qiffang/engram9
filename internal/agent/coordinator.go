package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	DefaultFlushInterval   = 30 * time.Second
	DefaultFlushThreshold  = 20
	DefaultEnvelopeTimeout = 40 * time.Minute
	DefaultResumeDelay     = 60 * time.Second
	startupWarmupDelay     = 5 * time.Second
	coordinatorHardStop    = 10 * time.Second
)

type batchIngestBackend interface {
	RunBatchIngest(context.Context, Batch, *sync.Mutex, func() error) (BatchResult, error)
	TurnTimeout() time.Duration
}

type pendingEventStore interface {
	ListPendingEvents() ([]PendingEvent, error)
	PendingCount() (int, error)
	HasPending() (bool, error)
	NotifyAppended(string)
	WriteStatus(StatusEntry) error
	WriteTombstone(string) error
	GetEventStatus(string) (EventStatus, error)
	RecoverFromCrash(string) error
	Counts() StatusCounts
	UnknownByReason() map[string]int
}

type WikiIndexRebuilder interface {
	RebuildWikiIndex() error
}

type WikiIndexRebuilderFunc func() error

func (rebuild WikiIndexRebuilderFunc) RebuildWikiIndex() error {
	return rebuild()
}

type CoordinatorConfig struct {
	FlushInterval   time.Duration
	FlushThreshold  int
	EnvelopeTimeout time.Duration
	ResumeDelay     time.Duration
}

// ActiveBatchState is explicitly process-local and ephemeral. The single
// durable truth is per-event state in the store: on process death, in-flight
// events revert to pending via frontmatter recovery and the next flush
// regroups them. Batch identity/progress is never persisted and never
// survives a restart — each flush attempt legitimately receives a fresh
// envelope ("new flush attempt", not "resumed batch").
type ActiveBatchState struct {
	BatchID      string    `json:"-"`
	EventIDs     []string  `json:"event_ids"`
	StartedAt    time.Time `json:"-"`
	BatchIndex   int       `json:"-"`
	TotalBatches int       `json:"-"`
}

type BatchError struct {
	BatchID    string    `json:"batch_id,omitempty"`
	ErrorClass string    `json:"error_class"`
	Message    string    `json:"message"`
	Timestamp  time.Time `json:"timestamp"`
}

// Flush stop reasons. StoppedReason is always one of these constants (or ""
// when the flush ran to completion). TestFlushStopReasonsDocumented asserts
// this comment stays in sync with the constants below:
// "envelope_timeout", "stopped", "internal_error_limit", "store_error",
// "unknown_streak".
const (
	StopReasonEnvelopeTimeout    = "envelope_timeout"
	StopReasonStopped            = "stopped"
	StopReasonInternalErrorLimit = "internal_error_limit"
	StopReasonStoreError         = "store_error"
	StopReasonUnknownStreak      = "unknown_streak"
)

// unknownStreakLimit trips the unknown-streak breaker: N consecutive
// completed batches where every event outcome is "unknown" and nothing was
// integrated/skipped/agent-failed. Suspends the coordinator (invariant #17
// semantics) instead of burning further ACP turns on a silently failing
// agent (incident 2026-07-26, task #66).
const unknownStreakLimit = 3

// UnknownStreakStop captures triage evidence when the unknown-streak breaker
// trips: the reason distribution and the (bounded) transcript paths of the
// streak's batches.
type UnknownStreakStop struct {
	Batches         int            `json:"batches"`
	ReasonCounts    map[string]int `json:"reason_counts"`
	TranscriptPaths []string       `json:"transcript_paths"`
}

type FlushResult struct {
	BatchesAttempted  int    `json:"batches_attempted"`
	BatchesSucceeded  int    `json:"batches_succeeded"`
	BatchesFailed     int    `json:"batches_failed"`
	EventsIntegrated  int    `json:"events_integrated"`
	EventsSkipped     int    `json:"events_skipped"`
	EventsFailed      int    `json:"events_failed"`
	EventsUnknown     int    `json:"events_unknown"`
	EventsRemaining   int    `json:"events_remaining"`
	IndexStale        bool   `json:"index_stale"`
	StoppedReason     string `json:"stopped_reason"`
	StatusWriteErrors int    `json:"status_write_errors"`
}

type BatchProgressEvent struct {
	BatchID       string        `json:"batch_id"`
	BatchIndex    int           `json:"batch_index"`
	TotalBatches  int           `json:"total_batches"`
	EventsInBatch int           `json:"events_in_batch"`
	Status        string        `json:"status"`
	ElapsedMs     int64         `json:"elapsed_ms"`
	EventResults  []EventResult `json:"event_results,omitempty"`
}

type CurrentBatchStatus struct {
	ID            string    `json:"id"`
	EventIDs      []string  `json:"event_ids"`
	Index         int       `json:"index"`
	TotalBatches  int       `json:"total_batches"`
	EventsInBatch int       `json:"events_in_batch"`
	StartedAt     time.Time `json:"started_at"`
	ElapsedMs     int64     `json:"elapsed_ms"`
}

type LastFlushStatus struct {
	CompletedAt      time.Time `json:"completed_at"`
	BatchesSucceeded int       `json:"batches_succeeded"`
	BatchesFailed    int       `json:"batches_failed"`
	StoppedReason    string    `json:"stopped_reason"`
}

type CoordinatorStatus struct {
	Health             string              `json:"health"`
	GeneratedAt        time.Time           `json:"generated_at"`
	TotalEvents        int                 `json:"total_events"`
	Pending            int                 `json:"pending"`
	InProgress         int                 `json:"in_progress"`
	Integrated         int                 `json:"integrated"`
	Skipped            int                 `json:"skipped"`
	Failed             int                 `json:"failed"`
	Unknown            int                 `json:"unknown"`
	ActionableFailures int                 `json:"actionable_failures"`
	DeferredEvents     int                 `json:"deferred_events"`
	DeferredIDs        []string            `json:"deferred_ids"`
	IndexStale         bool                `json:"index_stale"`
	StatusWriteErrors  int                 `json:"status_write_errors"`
	CurrentBatch       *CurrentBatchStatus `json:"current_batch"`
	LastFlush          *LastFlushStatus    `json:"last_flush"`
	LastError          *BatchError         `json:"last_error"`
	UnknownByReason    map[string]int      `json:"unknown_by_reason,omitempty"`
	UnknownStreak      *UnknownStreakStop  `json:"unknown_streak,omitempty"`
	Suspended          bool                `json:"-"`
	EventsRemaining    int                 `json:"-"`
}

type AdminResult struct {
	EventID   string `json:"event_id"`
	OldStatus string `json:"old_status"`
	NewStatus string `json:"new_status"`
}

type TransitionError struct {
	EventID       string
	CurrentStatus string
	Message       string
}

func (transition *TransitionError) Error() string {
	return transition.Message
}

type BatchCoordinator struct {
	mu                     sync.Mutex
	pending                chan struct{}
	backend                batchIngestBackend
	store                  pendingEventStore
	indexRebuilder         WikiIndexRebuilder
	wikiMu                 *sync.Mutex
	wikiDir                string
	progressPath           string
	cfg                    CoordinatorConfig
	limits                 BatchLimits
	lastResult             *FlushResult
	lastFlushAt            time.Time
	lastError              *BatchError
	indexStale             bool
	writeFailCounts        map[string]int
	deferredSet            map[string]bool
	unknownStreakStop      *UnknownStreakStop
	consecutiveOuterErrors int
	suspended              bool
	currentBatch           *ActiveBatchState
	stopCh                 chan struct{}
	stopOnce               sync.Once
	cancelBatch            context.CancelFunc
	done                   chan struct{}
	started                bool
	hardStopTimeout        time.Duration
}

func NewBatchCoordinator(backend *ACPBackend, store *PendingEventStore, indexRebuilder WikiIndexRebuilder, wikiMu *sync.Mutex, wikiDir string, cfg CoordinatorConfig) *BatchCoordinator {
	return newBatchCoordinator(backend, store, indexRebuilder, wikiMu, wikiDir, cfg)
}

func newBatchCoordinator(backend batchIngestBackend, store pendingEventStore, indexRebuilder WikiIndexRebuilder, wikiMu *sync.Mutex, wikiDir string, cfg CoordinatorConfig) *BatchCoordinator {
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = DefaultFlushInterval
	}
	if cfg.FlushThreshold <= 0 {
		cfg.FlushThreshold = DefaultFlushThreshold
	}
	if cfg.EnvelopeTimeout <= 0 {
		cfg.EnvelopeTimeout = DefaultEnvelopeTimeout
	}
	if cfg.ResumeDelay <= 0 {
		cfg.ResumeDelay = DefaultResumeDelay
	}
	return &BatchCoordinator{
		pending:         make(chan struct{}, 1),
		backend:         backend,
		store:           store,
		indexRebuilder:  indexRebuilder,
		wikiMu:          wikiMu,
		wikiDir:         wikiDir,
		progressPath:    filepath.Join(filepath.Dir(wikiDir), "batch_ingest.log"),
		cfg:             cfg,
		limits:          DefaultBatchLimits,
		writeFailCounts: make(map[string]int),
		deferredSet:     make(map[string]bool),
		stopCh:          make(chan struct{}),
		done:            make(chan struct{}),
		hardStopTimeout: coordinatorHardStop,
	}
}

func (coordinator *BatchCoordinator) NotifyNewEvent(eventID string) {
	coordinator.store.NotifyAppended(eventID)
	coordinator.mu.Lock()
	coordinator.suspended = false
	coordinator.mu.Unlock()
	coordinator.signalPending()
}

func (coordinator *BatchCoordinator) signalPending() {
	select {
	case coordinator.pending <- struct{}{}:
	default:
	}
}

func (coordinator *BatchCoordinator) ResetEvent(eventID string) (AdminResult, error) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	status, err := coordinator.store.GetEventStatus(eventID)
	if err != nil {
		return AdminResult{}, err
	}
	switch status.Status {
	case "unknown", "failed":
	case "pending":
		return AdminResult{}, &TransitionError{EventID: eventID, CurrentStatus: status.Status, Message: "event is already pending"}
	case "integrated", "skipped":
		return AdminResult{}, &TransitionError{EventID: eventID, CurrentStatus: status.Status, Message: fmt.Sprintf("cannot reset %s event", status.Status)}
	default:
		return AdminResult{}, fmt.Errorf("unknown event status %q", status.Status)
	}
	if err := coordinator.store.WriteTombstone(eventID); err != nil {
		return AdminResult{}, err
	}
	coordinator.store.NotifyAppended(eventID)
	coordinator.suspended = false
	coordinator.signalPending()
	return AdminResult{EventID: eventID, OldStatus: status.Status, NewStatus: "pending"}, nil
}

func (coordinator *BatchCoordinator) ConfirmEvent(eventID string) (AdminResult, error) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	status, err := coordinator.store.GetEventStatus(eventID)
	if err != nil {
		return AdminResult{}, err
	}
	if status.Status != "unknown" {
		return AdminResult{}, &TransitionError{
			EventID: eventID, CurrentStatus: status.Status,
			Message: fmt.Sprintf("can only confirm unknown events, got %s", status.Status),
		}
	}
	if err := coordinator.store.WriteStatus(StatusEntry{EventID: eventID, Status: "integrated", Reason: "operator_confirmed"}); err != nil {
		return AdminResult{}, err
	}
	return AdminResult{EventID: eventID, OldStatus: "unknown", NewStatus: "integrated"}, nil
}

func (coordinator *BatchCoordinator) Start(startupTimeout time.Duration) error {
	if startupTimeout <= 0 {
		return fmt.Errorf("coordinator startup timeout must be positive")
	}
	coordinator.mu.Lock()
	if coordinator.started {
		coordinator.mu.Unlock()
		return nil
	}
	coordinator.started = true
	coordinator.mu.Unlock()
	ready := make(chan struct{})
	go coordinator.run(ready)
	select {
	case <-ready:
		return nil
	case <-time.After(startupTimeout):
		return fmt.Errorf("coordinator startup timed out after %s (recovery/rebuild still running)", startupTimeout)
	}
}

func (coordinator *BatchCoordinator) run(ready chan struct{}) {
	defer close(coordinator.done)
	timer := time.NewTimer(coordinator.cfg.FlushInterval)
	defer timer.Stop()

	coordinator.wikiMu.Lock()
	recoveryErr := coordinator.store.RecoverFromCrash(coordinator.wikiDir)
	if recoveryErr != nil {
		log.Printf("[batch-ingest] crash recovery failed: %v", recoveryErr)
	}
	rebuildErr := coordinator.indexRebuilder.RebuildWikiIndex()
	if rebuildErr != nil {
		log.Printf("[batch-ingest] startup index rebuild failed: %v", rebuildErr)
	}
	coordinator.wikiMu.Unlock()
	if recoveryErr != nil || rebuildErr != nil {
		coordinator.mu.Lock()
		coordinator.indexStale = rebuildErr != nil
		coordinator.lastError = &BatchError{ErrorClass: "internal", Message: firstError(recoveryErr, rebuildErr).Error(), Timestamp: time.Now().UTC()}
		coordinator.mu.Unlock()
	}
	close(ready)

	if hasPending, err := coordinator.store.HasPending(); err == nil && hasPending {
		resetTimer(timer, startupWarmupDelay)
	}
	for {
		select {
		case <-coordinator.stopCh:
			return
		case <-coordinator.pending:
			count, err := coordinator.store.PendingCount()
			if err != nil {
				log.Printf("[batch-ingest] pending count failed: %v", err)
				resetTimer(timer, coordinator.cfg.FlushInterval)
				continue
			}
			if count >= coordinator.cfg.FlushThreshold {
				coordinator.scheduleNext(coordinator.flush(), timer)
			} else {
				resetTimer(timer, coordinator.cfg.FlushInterval)
			}
		case <-timer.C:
			coordinator.scheduleNext(coordinator.flush(), timer)
		}
	}
}

func firstError(errorsToCheck ...error) error {
	for _, err := range errorsToCheck {
		if err != nil {
			return err
		}
	}
	return nil
}

func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
}

func (coordinator *BatchCoordinator) scheduleNext(result FlushResult, timer *time.Timer) {
	coordinator.mu.Lock()
	resultCopy := result
	coordinator.lastResult = &resultCopy
	coordinator.lastFlushAt = time.Now().UTC()
	deferredCount := len(coordinator.deferredSet)
	if result.StoppedReason == StopReasonInternalErrorLimit || result.StoppedReason == StopReasonStoreError || result.StoppedReason == StopReasonUnknownStreak {
		coordinator.suspended = true
	}
	suspended := coordinator.suspended
	coordinator.mu.Unlock()
	if suspended {
		timer.Stop()
		return
	}

	pendingCount, err := coordinator.store.PendingCount()
	if err != nil {
		log.Printf("[batch-ingest] pending count after flush failed: %v", err)
		resetTimer(timer, coordinator.cfg.ResumeDelay)
		return
	}
	if deferredCount > pendingCount {
		deferredCount = pendingCount
	}
	if pendingCount-deferredCount > 0 {
		resetTimer(timer, coordinator.cfg.ResumeDelay)
	} else {
		timer.Stop()
	}
}

func (coordinator *BatchCoordinator) flush() FlushResult {
	deadline := time.Now().Add(coordinator.cfg.EnvelopeTimeout)
	coordinator.mu.Lock()
	coordinator.suspended = false
	coordinator.unknownStreakStop = nil
	coordinator.mu.Unlock()

	events, err := coordinator.store.ListPendingEvents()
	if err != nil {
		coordinator.mu.Lock()
		coordinator.lastError = &BatchError{ErrorClass: "store_error", Message: err.Error(), Timestamp: time.Now().UTC()}
		coordinator.mu.Unlock()
		return FlushResult{StoppedReason: StopReasonStoreError, EventsRemaining: -1}
	}
	pendingSet := make(map[string]struct{}, len(events))
	for _, event := range events {
		pendingSet[event.ID] = struct{}{}
	}
	coordinator.mu.Lock()
	for eventID := range coordinator.deferredSet {
		if _, ok := pendingSet[eventID]; !ok {
			delete(coordinator.deferredSet, eventID)
			delete(coordinator.writeFailCounts, eventID)
		}
	}
	filtered := make([]PendingEvent, 0, len(events))
	for _, event := range events {
		if !coordinator.deferredSet[event.ID] {
			filtered = append(filtered, event)
		}
	}
	coordinator.mu.Unlock()
	if len(filtered) == 0 {
		return FlushResult{EventsRemaining: len(events)}
	}

	batches := FormBatches(filtered, coordinator.limits)
	result := FlushResult{}
	unknownStreak := 0
	streakReasons := make(map[string]int)
	var streakTranscripts []string
	for index := 0; index < len(batches); index++ {
		batch := batches[index]
		if time.Now().After(deadline) {
			result.StoppedReason = StopReasonEnvelopeTimeout
			result.EventsRemaining = coordinator.safeRemainingCount()
			return result
		}
		if coordinator.isStopped() {
			result.StoppedReason = StopReasonStopped
			result.EventsRemaining = coordinator.safeRemainingCount()
			return result
		}

		batchTimeout := coordinator.batchTimeout(batch)
		batchContext, batchCancel := context.WithTimeout(context.Background(), batchTimeout)
		coordinator.setBatchCancel(batchCancel)
		coordinator.setCurrentBatch(batch, index+1, len(batches))
		coordinator.emitProgress(batch, BatchResult{}, index+1, len(batches), "started")
		batchResult, outerErr := coordinator.backend.RunBatchIngest(
			batchContext, batch, coordinator.wikiMu,
			func() error { return coordinator.indexRebuilder.RebuildWikiIndex() },
		)
		batchCancel()
		coordinator.clearBatchCancel()
		coordinator.clearCurrentBatch()
		result.BatchesAttempted++

		if outerErr != nil {
			result.BatchesFailed++
			if coordinator.recordOuterError(batch.ID, outerErr) >= 3 {
				result.StoppedReason = StopReasonInternalErrorLimit
				result.EventsRemaining = coordinator.safeRemainingCount()
				coordinator.emitProgress(batch, BatchResult{BatchID: batch.ID, Status: "failed", Error: outerErr}, index+1, len(batches), "failed")
				return result
			}
			coordinator.emitProgress(batch, BatchResult{BatchID: batch.ID, Status: "failed", Error: outerErr}, index+1, len(batches), "failed")
			continue
		}
		coordinator.mu.Lock()
		coordinator.consecutiveOuterErrors = 0
		coordinator.mu.Unlock()

		switch batchResult.Status {
		case "success":
			result.BatchesSucceeded++
			coordinator.recordIndexState(batchResult.IndexStale)
			if batchResult.IndexStale {
				result.IndexStale = true
			}
			result.StatusWriteErrors += coordinator.persistEventStatuses(batchResult)
			adjudicated := 0
			batchUnknown := 0
			for _, eventResult := range batchResult.EventResults {
				switch eventResult.Status {
				case "integrated":
					result.EventsIntegrated++
					adjudicated++
				case "skipped":
					result.EventsSkipped++
					adjudicated++
				case "failed_by_agent":
					result.EventsFailed++
					adjudicated++
				case "unknown":
					result.EventsUnknown++
					batchUnknown++
				}
			}
			coordinator.emitProgress(batch, batchResult, index+1, len(batches), "completed")
			if adjudicated > 0 {
				unknownStreak = 0
				streakReasons = make(map[string]int)
				streakTranscripts = nil
			} else if batchUnknown > 0 {
				unknownStreak++
				for _, eventResult := range batchResult.EventResults {
					if eventResult.Status == "unknown" {
						streakReasons[eventResult.Reason]++
					}
				}
				if len(streakTranscripts) < unknownStreakLimit {
					for _, eventResult := range batchResult.EventResults {
						if eventResult.Status == "unknown" && eventResult.TranscriptPath != "" {
							streakTranscripts = append(streakTranscripts, eventResult.TranscriptPath)
							break
						}
					}
				}
				if unknownStreak >= unknownStreakLimit {
					// Unknown-streak breaker: the agent is completing turns
					// without adjudicating any event. Suspend (invariant #17
					// semantics) instead of burning more provider turns; the
					// next /remember or an admin retry clears it.
					stop := &UnknownStreakStop{
						Batches: unknownStreak, ReasonCounts: streakReasons, TranscriptPaths: streakTranscripts,
					}
					coordinator.mu.Lock()
					coordinator.suspended = true
					coordinator.unknownStreakStop = stop
					coordinator.lastError = &BatchError{
						BatchID: batch.ID, ErrorClass: StopReasonUnknownStreak,
						Message:   fmt.Sprintf("%d consecutive all-unknown batches; see unknown_streak in status", unknownStreak),
						Timestamp: time.Now().UTC(),
					}
					coordinator.mu.Unlock()
					result.StoppedReason = StopReasonUnknownStreak
					result.EventsRemaining = coordinator.safeRemainingCount()
					return result
				}
			}
		case "failed":
			result.BatchesFailed++
			if batchResult.IsShutdownCancel() {
				result.StoppedReason = StopReasonStopped
				result.EventsRemaining = coordinator.safeRemainingCount()
				coordinator.emitProgress(batch, batchResult, index+1, len(batches), "failed")
				return result
			}
			retryBatches := coordinator.classifyAndSplit(batch, batchResult)
			if len(retryBatches) > 0 {
				batches = insertBatches(batches, index+1, retryBatches)
				coordinator.emitProgress(batch, batchResult, index+1, len(batches), "retrying")
			} else {
				reason := terminalFailureReason(batchResult)
				for _, event := range batch.Events {
					if err := coordinator.store.WriteStatus(StatusEntry{EventID: event.ID, Status: "failed", BatchID: batch.ID, Reason: reason}); err != nil {
						result.StatusWriteErrors++
						coordinator.recordStatusWriteFailure(event.ID)
					} else {
						coordinator.clearStatusWriteFailure(event.ID)
					}
				}
				result.EventsFailed += len(batch.Events)
				coordinator.emitProgress(batch, batchResult, index+1, len(batches), "failed")
			}
			coordinator.mu.Lock()
			coordinator.lastError = &BatchError{
				BatchID: batch.ID, ErrorClass: batchResult.errorClass(), Message: batchResult.failureReason(), Timestamp: time.Now().UTC(),
			}
			coordinator.mu.Unlock()
		default:
			impossible := fmt.Errorf("invalid batch result status %q", batchResult.Status)
			result.BatchesFailed++
			if coordinator.recordOuterError(batch.ID, impossible) >= 3 {
				result.StoppedReason = StopReasonInternalErrorLimit
				result.EventsRemaining = coordinator.safeRemainingCount()
				return result
			}
		}
	}
	result.EventsRemaining = coordinator.safeRemainingCount()
	return result
}

func (coordinator *BatchCoordinator) batchTimeout(batch Batch) time.Duration {
	timeout := coordinator.backend.TurnTimeout()
	batchSizedTimeout := time.Duration(len(batch.Events)) * 90 * time.Second
	if batchSizedTimeout > timeout {
		timeout = batchSizedTimeout
	}
	if timeout > 10*time.Minute {
		timeout = 10 * time.Minute
	}
	if timeout <= 0 {
		timeout = DefaultACPTurnTimeout
	}
	return timeout
}

func (coordinator *BatchCoordinator) setCurrentBatch(batch Batch, index, total int) {
	eventIDs := make([]string, len(batch.Events))
	for eventIndex, event := range batch.Events {
		eventIDs[eventIndex] = event.ID
	}
	coordinator.mu.Lock()
	coordinator.currentBatch = &ActiveBatchState{
		BatchID: batch.ID, EventIDs: eventIDs, StartedAt: time.Now().UTC(), BatchIndex: index, TotalBatches: total,
	}
	coordinator.mu.Unlock()
}

func (coordinator *BatchCoordinator) clearCurrentBatch() {
	coordinator.mu.Lock()
	coordinator.currentBatch = nil
	coordinator.mu.Unlock()
}

func (coordinator *BatchCoordinator) recordOuterError(batchID string, err error) int {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.consecutiveOuterErrors++
	coordinator.lastError = &BatchError{BatchID: batchID, ErrorClass: "internal", Message: err.Error(), Timestamp: time.Now().UTC()}
	return coordinator.consecutiveOuterErrors
}

func (coordinator *BatchCoordinator) recordIndexState(stale bool) {
	coordinator.mu.Lock()
	coordinator.indexStale = stale
	coordinator.mu.Unlock()
}

func (coordinator *BatchCoordinator) persistEventStatuses(result BatchResult) int {
	failures := 0
	for _, eventResult := range result.EventResults {
		entry := StatusEntry{
			EventID: eventResult.EventID, BatchID: result.BatchID, Reason: eventResult.Reason,
			Pages: eventResult.Pages, TranscriptPath: eventResult.TranscriptPath,
		}
		switch eventResult.Status {
		case "integrated":
			entry.Status = "integrated"
		case "skipped":
			entry.Status = "skipped"
		case "failed_by_agent":
			entry.Status = "failed"
		case "unknown":
			entry.Status = "unknown"
		default:
			failures++
			coordinator.recordStatusWriteFailure(eventResult.EventID)
			continue
		}
		if err := coordinator.store.WriteStatus(entry); err != nil {
			failures++
			coordinator.recordStatusWriteFailure(eventResult.EventID)
		} else {
			coordinator.clearStatusWriteFailure(eventResult.EventID)
		}
	}
	return failures
}

func (coordinator *BatchCoordinator) recordStatusWriteFailure(eventID string) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.writeFailCounts[eventID]++
	if coordinator.writeFailCounts[eventID] >= 3 {
		coordinator.deferredSet[eventID] = true
	}
}

func (coordinator *BatchCoordinator) clearStatusWriteFailure(eventID string) {
	coordinator.mu.Lock()
	delete(coordinator.writeFailCounts, eventID)
	delete(coordinator.deferredSet, eventID)
	coordinator.mu.Unlock()
}

func (coordinator *BatchCoordinator) classifyAndSplit(batch Batch, result BatchResult) []Batch {
	if batch.Depth >= 2 || len(batch.Events) <= 5 {
		return nil
	}
	if len(result.Violations) > 0 {
		if split := splitByViolationPaths(batch, result.Violations, result.Summary); len(split) > 0 {
			return split
		}
	}
	return blindBisect(batch)
}

func splitByViolationPaths(batch Batch, violations []Violation, summary string) []Batch {
	violationPaths := make(map[string]struct{})
	for _, violation := range violations {
		if violation.Path != "" {
			violationPaths[filepath.ToSlash(filepath.Clean(violation.Path))] = struct{}{}
		}
	}
	if len(violationPaths) == 0 {
		return nil
	}
	parsedResults := parseEventResults(batch, summary, "")
	pagesByEvent := make(map[string][]string)
	for _, result := range parsedResults {
		if result.Status == "integrated" && len(result.Pages) > 0 {
			pagesByEvent[result.EventID] = result.Pages
		}
	}
	var clean []PendingEvent
	var suspect []PendingEvent
	for _, event := range batch.Events {
		isSuspect := false
		for _, page := range pagesByEvent[event.ID] {
			if _, ok := violationPaths[filepath.ToSlash(filepath.Clean(page))]; ok {
				isSuspect = true
				break
			}
		}
		if isSuspect {
			suspect = append(suspect, event)
		} else {
			clean = append(clean, event)
		}
	}
	if len(clean) == 0 || len(suspect) == 0 {
		return nil
	}
	return []Batch{makeBatch(clean, batch.Depth+1), makeBatch(suspect, batch.Depth+1)}
}

func blindBisect(batch Batch) []Batch {
	if len(batch.Events) < 2 {
		return nil
	}
	middle := len(batch.Events) / 2
	return []Batch{
		makeBatch(batch.Events[:middle], batch.Depth+1),
		makeBatch(batch.Events[middle:], batch.Depth+1),
	}
}

func insertBatches(batches []Batch, index int, inserted []Batch) []Batch {
	result := make([]Batch, 0, len(batches)+len(inserted))
	result = append(result, batches[:index]...)
	result = append(result, inserted...)
	result = append(result, batches[index:]...)
	return result
}

func terminalFailureReason(result BatchResult) string {
	switch {
	case errors.Is(result.Error, context.DeadlineExceeded):
		return "batch_timeout_at_max_depth"
	case len(result.Violations) > 0:
		return "collateral: bisect terminal with poison event"
	case result.Error != nil:
		return "batch_crash_at_max_depth: " + result.Error.Error()
	default:
		return "batch_failed_at_max_depth"
	}
}

func (coordinator *BatchCoordinator) safeRemainingCount() int {
	count, err := coordinator.store.PendingCount()
	if err != nil {
		return -1
	}
	return count
}

func (coordinator *BatchCoordinator) setBatchCancel(cancel context.CancelFunc) {
	coordinator.mu.Lock()
	coordinator.cancelBatch = cancel
	coordinator.mu.Unlock()
}

func (coordinator *BatchCoordinator) clearBatchCancel() {
	coordinator.mu.Lock()
	coordinator.cancelBatch = nil
	coordinator.mu.Unlock()
}

func (coordinator *BatchCoordinator) isStopped() bool {
	select {
	case <-coordinator.stopCh:
		return true
	default:
		return false
	}
}

func (coordinator *BatchCoordinator) Stop(shutdownContext context.Context) {
	coordinator.mu.Lock()
	started := coordinator.started
	coordinator.mu.Unlock()
	if !started {
		return
	}
	coordinator.stopOnce.Do(func() { close(coordinator.stopCh) })
	select {
	case <-coordinator.done:
		return
	case <-shutdownContext.Done():
		coordinator.mu.Lock()
		cancel := coordinator.cancelBatch
		coordinator.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		select {
		case <-coordinator.done:
		case <-time.After(coordinator.hardStopTimeout):
			log.Printf("[batch-ingest] coordinator cleanup timed out after cancel")
		}
	}
}

func (coordinator *BatchCoordinator) Status() CoordinatorStatus {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	counts := coordinator.store.Counts()
	inProgress := 0
	var currentBatch *CurrentBatchStatus
	if coordinator.currentBatch != nil {
		inProgress = len(coordinator.currentBatch.EventIDs)
		currentBatch = &CurrentBatchStatus{
			ID: coordinator.currentBatch.BatchID, EventIDs: append([]string(nil), coordinator.currentBatch.EventIDs...),
			Index: coordinator.currentBatch.BatchIndex, TotalBatches: coordinator.currentBatch.TotalBatches,
			EventsInBatch: len(coordinator.currentBatch.EventIDs), StartedAt: coordinator.currentBatch.StartedAt,
			ElapsedMs: time.Since(coordinator.currentBatch.StartedAt).Milliseconds(),
		}
	}
	displayPending := counts.Pending - inProgress
	if displayPending < 0 {
		displayPending = 0
	}
	deferredIDs := make([]string, 0, len(coordinator.deferredSet))
	for eventID := range coordinator.deferredSet {
		deferredIDs = append(deferredIDs, eventID)
	}
	sort.Strings(deferredIDs)
	status := CoordinatorStatus{
		TotalEvents: counts.Total, Pending: displayPending, InProgress: inProgress,
		Integrated: counts.Integrated, Skipped: counts.Skipped, Failed: counts.Failed, Unknown: counts.Unknown,
		ActionableFailures: counts.Failed + counts.Unknown, DeferredEvents: len(deferredIDs), DeferredIDs: deferredIDs,
		IndexStale: coordinator.indexStale, CurrentBatch: currentBatch, Suspended: coordinator.suspended,
		UnknownByReason: coordinator.store.UnknownByReason(), UnknownStreak: coordinator.unknownStreakStop,
	}
	if coordinator.lastResult != nil {
		status.StatusWriteErrors = coordinator.lastResult.StatusWriteErrors
		status.EventsRemaining = coordinator.lastResult.EventsRemaining
		status.LastFlush = &LastFlushStatus{
			CompletedAt: coordinator.lastFlushAt, BatchesSucceeded: coordinator.lastResult.BatchesSucceeded,
			BatchesFailed: coordinator.lastResult.BatchesFailed, StoppedReason: coordinator.lastResult.StoppedReason,
		}
	}
	if coordinator.lastError != nil {
		lastError := *coordinator.lastError
		status.LastError = &lastError
	}
	status.GeneratedAt = time.Now().UTC()
	status.Health = deriveCoordinatorHealth(status)
	return status
}

// wallClockStuckMultiple bounds how long a single batch may stay current
// before health reports "stuck" regardless of InProgress. With the batch cap
// enforced correctly this arm is unreachable; if it ever fires it indicates a
// cap-enforcement bug (e.g. a hung ACP subprocess ignoring context cancel)
// and MUST surface as stuck, not in_progress (defense-in-depth; incident
// 2026-07-26, task #66).
const wallClockStuckMultiple = 2

// maxBatchCap mirrors the hard ceiling in batchTimeout.
const maxBatchCap = 10 * time.Minute

func deriveCoordinatorHealth(status CoordinatorStatus) string {
	effectivePending := status.Pending
	if status.EventsRemaining == -1 {
		effectivePending = 1
	}
	actionablePending := effectivePending - status.DeferredEvents
	if actionablePending < 0 {
		actionablePending = 0
	}
	if status.Suspended && status.InProgress == 0 {
		return "suspended"
	}
	if actionablePending > 0 && status.InProgress == 0 && status.LastFlush != nil && status.LastFlush.BatchesSucceeded == 0 {
		return "stuck"
	}
	if status.CurrentBatch != nil && status.GeneratedAt.Sub(status.CurrentBatch.StartedAt) > wallClockStuckMultiple*maxBatchCap {
		return "stuck"
	}
	if status.InProgress > 0 || actionablePending > 0 {
		return "in_progress"
	}
	if status.ActionableFailures > 0 || status.IndexStale || status.StatusWriteErrors > 0 || status.DeferredEvents > 0 {
		return "degraded"
	}
	return "healthy"
}

func (coordinator *BatchCoordinator) emitProgress(batch Batch, result BatchResult, index, total int, status string) {
	progress := BatchProgressEvent{
		BatchID: batch.ID, BatchIndex: index, TotalBatches: total, EventsInBatch: len(batch.Events),
		Status: status, ElapsedMs: result.DurationMs, EventResults: result.EventResults,
	}
	data, err := json.Marshal(progress)
	if err != nil {
		return
	}
	log.Printf("[batch-ingest] %s", data)
	if err := os.MkdirAll(filepath.Dir(coordinator.progressPath), 0o755); err != nil {
		return
	}
	file, err := os.OpenFile(coordinator.progressPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = file.Write(append(data, '\n'))
}
