package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeCoordinatorStore struct {
	mu              sync.Mutex
	pending         map[string]PendingEvent
	statuses        map[string]EventStatus
	notifyCalls     []string
	writeErrorsLeft map[string]int
	listErr         error
	pendingErr      error
	recoverErr      error
	recoverCalls    int
}

func newFakeCoordinatorStore(events ...PendingEvent) *fakeCoordinatorStore {
	store := &fakeCoordinatorStore{
		pending: make(map[string]PendingEvent), statuses: make(map[string]EventStatus), writeErrorsLeft: make(map[string]int),
	}
	for _, event := range events {
		store.pending[event.ID] = event
	}
	return store
}

func (store *fakeCoordinatorStore) ListPendingEvents() ([]PendingEvent, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.listErr != nil {
		return nil, store.listErr
	}
	result := make([]PendingEvent, 0, len(store.pending))
	for _, event := range store.pending {
		result = append(result, event)
	}
	return result, nil
}

func (store *fakeCoordinatorStore) PendingCount() (int, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.pendingErr != nil {
		return 0, store.pendingErr
	}
	return len(store.pending), nil
}

func (store *fakeCoordinatorStore) HasPending() (bool, error) {
	count, err := store.PendingCount()
	return count > 0, err
}

func (store *fakeCoordinatorStore) NotifyAppended(eventID string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.notifyCalls = append(store.notifyCalls, eventID)
	if _, ok := store.pending[eventID]; !ok {
		store.pending[eventID] = PendingEvent{ID: eventID}
	}
}

func (store *fakeCoordinatorStore) WriteStatus(entry StatusEntry) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.writeErrorsLeft[entry.EventID] > 0 {
		store.writeErrorsLeft[entry.EventID]--
		return errors.New("status disk failed")
	}
	store.statuses[entry.EventID] = EventStatus{Status: entry.Status, BatchID: entry.BatchID, Reason: entry.Reason, Pages: entry.Pages}
	delete(store.pending, entry.EventID)
	return nil
}

func (store *fakeCoordinatorStore) WriteTombstone(eventID string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.statuses, eventID)
	store.pending[eventID] = PendingEvent{ID: eventID}
	return nil
}

func (store *fakeCoordinatorStore) UnknownByReason() map[string]int {
	store.mu.Lock()
	defer store.mu.Unlock()
	counts := make(map[string]int)
	for _, status := range store.statuses {
		if status.Status != "unknown" {
			continue
		}
		reason := status.Reason
		if reason == "" {
			reason = UnknownReasonUnclassified
		}
		counts[reason]++
	}
	return counts
}

func (store *fakeCoordinatorStore) GetEventStatus(eventID string) (EventStatus, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if status, ok := store.statuses[eventID]; ok {
		return status, nil
	}
	if _, ok := store.pending[eventID]; ok {
		return EventStatus{Status: "pending"}, nil
	}
	return EventStatus{}, ErrEventNotFound
}

func (store *fakeCoordinatorStore) RecoverFromCrash(string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.recoverCalls++
	return store.recoverErr
}

func (store *fakeCoordinatorStore) Counts() StatusCounts {
	store.mu.Lock()
	defer store.mu.Unlock()
	counts := StatusCounts{Pending: len(store.pending)}
	counts.Total = counts.Pending + len(store.statuses)
	for _, status := range store.statuses {
		switch status.Status {
		case "integrated":
			counts.Integrated++
		case "skipped":
			counts.Skipped++
		case "failed":
			counts.Failed++
		case "unknown":
			counts.Unknown++
		}
	}
	return counts
}

type fakeBatchBackend struct {
	mu      sync.Mutex
	calls   []Batch
	timeout time.Duration
	run     func(context.Context, Batch) (BatchResult, error)
}

func (backend *fakeBatchBackend) RunBatchIngest(ctx context.Context, batch Batch, _ *sync.Mutex, _ func() error) (BatchResult, error) {
	backend.mu.Lock()
	backend.calls = append(backend.calls, batch)
	backend.mu.Unlock()
	return backend.run(ctx, batch)
}

func (backend *fakeBatchBackend) TurnTimeout() time.Duration {
	return backend.timeout
}

func (backend *fakeBatchBackend) callCount() int {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return len(backend.calls)
}

type fakeRebuilder struct {
	mu    sync.Mutex
	err   error
	calls int
	run   func() error
}

func (rebuilder *fakeRebuilder) RebuildWikiIndex() error {
	rebuilder.mu.Lock()
	rebuilder.calls++
	rebuilder.mu.Unlock()
	if rebuilder.run != nil {
		return rebuilder.run()
	}
	return rebuilder.err
}

func TestNewBatchCoordinatorAppliesDefaults(t *testing.T) {
	coordinator := newTestCoordinator(t, newFakeCoordinatorStore(), &fakeBatchBackend{run: successfulBatchResult}, CoordinatorConfig{})
	require.Equal(t, DefaultFlushInterval, coordinator.cfg.FlushInterval)
	require.Equal(t, DefaultFlushThreshold, coordinator.cfg.FlushThreshold)
	require.Equal(t, DefaultEnvelopeTimeout, coordinator.cfg.EnvelopeTimeout)
	require.Equal(t, DefaultResumeDelay, coordinator.cfg.ResumeDelay)
}

func TestBatchCoordinatorNotifyCoalescesAndClearsSuspension(t *testing.T) {
	store := newFakeCoordinatorStore()
	coordinator := newTestCoordinator(t, store, &fakeBatchBackend{run: successfulBatchResult}, CoordinatorConfig{})
	coordinator.suspended = true
	coordinator.NotifyNewEvent("a")
	coordinator.NotifyNewEvent("b")

	require.Equal(t, []string{"a", "b"}, store.notifyCalls)
	require.Len(t, coordinator.pending, 1)
	require.False(t, coordinator.suspended)
}

func TestBatchCoordinatorFlushPersistsSuccessfulResults(t *testing.T) {
	events := makePendingEvents(3)
	store := newFakeCoordinatorStore(events...)
	backend := &fakeBatchBackend{run: func(_ context.Context, batch Batch) (BatchResult, error) {
		return BatchResult{
			BatchID: batch.ID, Status: "success", IndexStale: true,
			EventResults: []EventResult{
				{EventID: events[0].ID, Status: "integrated", Pages: []string{"semantic/a.md"}},
				{EventID: events[1].ID, Status: "skipped", Reason: "duplicate"},
				{EventID: events[2].ID, Status: "unknown", Reason: "not reported by agent"},
			},
		}, nil
	}}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})

	result := coordinator.flush(flushExplicit)
	require.Equal(t, 1, result.BatchesAttempted)
	require.Equal(t, 1, result.BatchesSucceeded)
	require.Equal(t, 1, result.EventsIntegrated)
	require.Equal(t, 1, result.EventsSkipped)
	require.Equal(t, 1, result.EventsUnknown)
	require.Equal(t, 0, result.EventsRemaining)
	require.True(t, result.IndexStale)
	require.Equal(t, "integrated", store.statuses[events[0].ID].Status)
	require.Equal(t, "skipped", store.statuses[events[1].ID].Status)
	require.Equal(t, "unknown", store.statuses[events[2].ID].Status)
	require.True(t, coordinator.indexStale)
	data, err := os.ReadFile(coordinator.progressPath)
	require.NoError(t, err)
	require.Contains(t, string(data), `"status":"started"`)
	require.Contains(t, string(data), `"status":"completed"`)
}

func TestBatchCoordinatorDrains186EventsAndIsIdempotent(t *testing.T) {
	events := makePendingEvents(186)
	store := newFakeCoordinatorStore(events...)
	backend := &fakeBatchBackend{run: successfulBatchResult}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})

	result := coordinator.flush(flushExplicit)
	require.Equal(t, 10, result.BatchesAttempted)
	require.Equal(t, 10, result.BatchesSucceeded)
	require.Equal(t, 186, result.EventsIntegrated)
	require.Equal(t, 0, result.EventsFailed)
	require.Equal(t, 0, result.EventsUnknown)
	require.Equal(t, 0, result.EventsRemaining)
	require.Equal(t, 10, backend.callCount())

	second := coordinator.flush(flushExplicit)
	require.Zero(t, second.BatchesAttempted)
	require.Equal(t, 10, backend.callCount(), "terminal events must not be reprocessed")
}

func TestBatchCoordinatorPoisonEventsHaveBoundedCollateral(t *testing.T) {
	events := makePendingEvents(186)
	poison := map[string]bool{}
	for _, index := range []int{0, 40, 80, 120, 160} {
		poison[events[index].ID] = true
	}
	store := newFakeCoordinatorStore(events...)
	backend := &fakeBatchBackend{run: func(_ context.Context, batch Batch) (BatchResult, error) {
		for _, event := range batch.Events {
			if poison[event.ID] {
				return BatchResult{BatchID: batch.ID, Status: "failed", Error: errors.New("poison event")}, nil
			}
		}
		return integratedResult(batch), nil
	}}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})

	result := coordinator.flush(flushExplicit)
	require.GreaterOrEqual(t, result.EventsIntegrated+result.EventsSkipped, 161)
	require.LessOrEqual(t, result.EventsFailed, 25)
	require.Zero(t, result.EventsUnknown)
	require.Zero(t, result.EventsRemaining)

	var collateralID string
	for eventID, status := range store.statuses {
		if status.Status == "failed" && !poison[eventID] {
			collateralID = eventID
			break
		}
	}
	require.NotEmpty(t, collateralID)
	reset, err := coordinator.ResetEvent(collateralID)
	require.NoError(t, err)
	require.Equal(t, "pending", reset.NewStatus)
	require.Equal(t, 1, coordinator.flush(flushExplicit).EventsIntegrated)
}

func TestBatchCoordinatorFlushUsesInsertedRetryBatches(t *testing.T) {
	events := makePendingEvents(12)
	store := newFakeCoordinatorStore(events...)
	backend := &fakeBatchBackend{}
	backend.run = func(_ context.Context, batch Batch) (BatchResult, error) {
		if batch.Depth == 0 {
			return BatchResult{BatchID: batch.ID, Status: "failed", Error: errors.New("acpmux crashed")}, nil
		}
		return integratedResult(batch), nil
	}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})
	coordinator.limits = BatchLimits{MaxEventsPerBatch: 20, MaxTokensPerBatch: 1_000_000, MaxBytesPerBatch: 1_000_000}

	result := coordinator.flush(flushExplicit)
	require.Equal(t, 3, result.BatchesAttempted)
	require.Equal(t, 2, result.BatchesSucceeded)
	require.Equal(t, 1, result.BatchesFailed)
	require.Equal(t, 12, result.EventsIntegrated)
	require.Equal(t, 0, result.EventsRemaining)
	require.Equal(t, 3, backend.callCount())
}

func TestBatchCoordinatorShutdownCancellationStaysPending(t *testing.T) {
	store := newFakeCoordinatorStore(PendingEvent{ID: "a"})
	backend := &fakeBatchBackend{run: func(_ context.Context, batch Batch) (BatchResult, error) {
		return BatchResult{BatchID: batch.ID, Status: "failed", Error: context.Canceled}, nil
	}}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})
	result := coordinator.flush(flushExplicit)
	require.Equal(t, "stopped", result.StoppedReason)
	require.Equal(t, 1, result.EventsRemaining)
	require.Empty(t, store.statuses)
	require.Equal(t, 1, backend.callCount())
	require.Nil(t, coordinator.lastError)
}

func TestBatchCoordinatorTerminalTimeoutWritesExactReason(t *testing.T) {
	store := newFakeCoordinatorStore(PendingEvent{ID: "a"})
	backend := &fakeBatchBackend{run: func(_ context.Context, batch Batch) (BatchResult, error) {
		return BatchResult{BatchID: batch.ID, Status: "failed", Error: context.DeadlineExceeded}, nil
	}}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})
	result := coordinator.flush(flushExplicit)
	require.Equal(t, 1, result.EventsFailed)
	require.Equal(t, "batch_timeout_at_max_depth", store.statuses["a"].Reason)
}

func TestBatchCoordinatorOuterErrorsPersistAcrossFlushes(t *testing.T) {
	store := newFakeCoordinatorStore(PendingEvent{ID: "a"})
	outerFailure := true
	backend := &fakeBatchBackend{run: func(_ context.Context, batch Batch) (BatchResult, error) {
		if outerFailure {
			return BatchResult{}, errors.New("impossible infrastructure error")
		}
		return integratedResult(batch), nil
	}}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})
	require.Empty(t, coordinator.flush(flushExplicit).StoppedReason)
	require.Empty(t, coordinator.flush(flushExplicit).StoppedReason)
	require.Equal(t, "internal_error_limit", coordinator.flush(flushExplicit).StoppedReason)
	require.Equal(t, 3, coordinator.consecutiveOuterErrors)

	outerFailure = false
	result := coordinator.flush(flushExplicit)
	require.Equal(t, 1, result.BatchesSucceeded)
	require.Equal(t, 0, coordinator.consecutiveOuterErrors)
}

func TestBatchCoordinatorBoundsOuterErrorsAcrossMultipleBatches(t *testing.T) {
	store := newFakeCoordinatorStore(makePendingEvents(80)...)
	backend := &fakeBatchBackend{run: func(context.Context, Batch) (BatchResult, error) {
		return BatchResult{}, errors.New("infrastructure failed")
	}}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})
	result := coordinator.flush(flushExplicit)
	require.Equal(t, "internal_error_limit", result.StoppedReason)
	require.Equal(t, 3, result.BatchesAttempted)
	require.Equal(t, 3, result.BatchesFailed)
	require.Equal(t, 80, result.EventsRemaining)
	require.Equal(t, 3, backend.callCount())

	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	coordinator.scheduleNext(result, timer)
	require.Equal(t, "suspended", coordinator.Status().Health)
	select {
	case <-timer.C:
		t.Fatal("suspended coordinator must not auto-resume")
	case <-time.After(5 * time.Millisecond):
	}

	coordinator.NotifyNewEvent("new-event")
	coordinator.setCurrentBatch(makeBatch([]PendingEvent{{ID: "new-event"}}, 0), 1, 1)
	require.Equal(t, "in_progress", coordinator.Status().Health)
}

func TestBatchCoordinatorOuterErrorCounterResetsWithinFlush(t *testing.T) {
	store := newFakeCoordinatorStore(makePendingEvents(100)...)
	call := 0
	backend := &fakeBatchBackend{run: func(_ context.Context, batch Batch) (BatchResult, error) {
		call++
		if call <= 2 {
			return BatchResult{}, errors.New("transient infrastructure failure")
		}
		return integratedResult(batch), nil
	}}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})
	result := coordinator.flush(flushExplicit)
	require.Equal(t, 5, result.BatchesAttempted)
	require.Equal(t, 3, result.BatchesSucceeded)
	require.Equal(t, 2, result.BatchesFailed)
	require.Empty(t, result.StoppedReason)
	require.Zero(t, coordinator.consecutiveOuterErrors)
}

func TestBatchCoordinatorMixedSuccessThenOuterErrorLimitSuspends(t *testing.T) {
	store := newFakeCoordinatorStore(makePendingEvents(100)...)
	call := 0
	backend := &fakeBatchBackend{run: func(_ context.Context, batch Batch) (BatchResult, error) {
		call++
		if call <= 2 {
			return integratedResult(batch), nil
		}
		return BatchResult{}, errors.New("persistent infrastructure failure")
	}}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})
	result := coordinator.flush(flushExplicit)
	require.Equal(t, "internal_error_limit", result.StoppedReason)
	require.Equal(t, 5, result.BatchesAttempted)
	require.Equal(t, 2, result.BatchesSucceeded)
	require.Equal(t, 3, result.BatchesFailed)
	require.Equal(t, 60, result.EventsRemaining)

	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	coordinator.scheduleNext(result, timer)
	require.Equal(t, "suspended", coordinator.Status().Health)
}

func TestBatchCoordinatorStoreErrorNeverReportsFalseZero(t *testing.T) {
	store := newFakeCoordinatorStore(PendingEvent{ID: "a"})
	store.listErr = errors.New("raw log unavailable")
	coordinator := newTestCoordinator(t, store, &fakeBatchBackend{run: successfulBatchResult}, CoordinatorConfig{})
	result := coordinator.flush(flushExplicit)
	require.Equal(t, "store_error", result.StoppedReason)
	require.Equal(t, -1, result.EventsRemaining)
	status := coordinator.Status()
	require.NotNil(t, status.LastError)
	require.Equal(t, "store_error", status.LastError.ErrorClass)
}

func TestBatchCoordinatorStatusPopulatesFrozenFailureFields(t *testing.T) {
	store := newFakeCoordinatorStore(PendingEvent{ID: "a"})
	backend := &fakeBatchBackend{run: func(_ context.Context, batch Batch) (BatchResult, error) {
		return BatchResult{BatchID: batch.ID, Status: "failed", Error: errors.New("acpmux crashed")}, nil
	}}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})
	result := coordinator.flush(flushExplicit)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	coordinator.scheduleNext(result, timer)

	status := coordinator.Status()
	require.NotNil(t, status.LastFlush)
	require.False(t, status.LastFlush.CompletedAt.IsZero())
	require.Equal(t, 1, status.LastFlush.BatchesFailed)
	require.Empty(t, status.LastFlush.StoppedReason)
	require.NotNil(t, status.LastError)
	require.NotEmpty(t, status.LastError.BatchID)
	require.Equal(t, "crash", status.LastError.ErrorClass)
	require.Equal(t, "acpmux crashed", status.LastError.Message)
	require.False(t, status.LastError.Timestamp.IsZero())
}

// task #38: the coordinator's batch capacity is the single source of truth an
// event producer (autopilot wiki ingest) must derive its per-event budget from.
// Status() must surface it so producers don't hard-code an independent constant.
func TestBatchCoordinatorStatusExposesBatchLimits(t *testing.T) {
	store := newFakeCoordinatorStore(PendingEvent{ID: "a"})
	coordinator := newTestCoordinator(t, store, &fakeBatchBackend{run: successfulBatchResult}, CoordinatorConfig{})

	status := coordinator.Status()
	require.Equal(t, DefaultMaxTokensPerBatch, status.MaxTokensPerBatch)
	require.Equal(t, DefaultMaxEventsPerBatch, status.MaxEventsPerBatch)
	require.Equal(t, DefaultMaxBytesPerBatch, status.MaxBytesPerBatch)
	require.Greater(t, status.MaxTokensPerBatch, 0, "producers divide by this; must be positive")
}

func TestBatchCoordinatorCountsEachStatusWriteFailure(t *testing.T) {
	events := makePendingEvents(20)
	store := newFakeCoordinatorStore(events...)
	for _, event := range events[:3] {
		store.writeErrorsLeft[event.ID] = 1
	}
	coordinator := newTestCoordinator(t, store, &fakeBatchBackend{run: successfulBatchResult}, CoordinatorConfig{})

	result := coordinator.flush(flushExplicit)
	require.Equal(t, 3, result.StatusWriteErrors)
	require.Equal(t, 3, result.EventsRemaining)
	require.Len(t, store.statuses, 17)
}

func TestBatchCoordinatorDefersAfterThreeStatusWriteFailures(t *testing.T) {
	store := newFakeCoordinatorStore(PendingEvent{ID: "a"})
	store.writeErrorsLeft["a"] = 3
	backend := &fakeBatchBackend{run: successfulBatchResult}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})

	for range 3 {
		result := coordinator.flush(flushExplicit)
		require.Equal(t, 1, result.StatusWriteErrors)
	}
	require.True(t, coordinator.deferredSet["a"])
	require.Equal(t, 3, backend.callCount())
	result := coordinator.flush(flushExplicit)
	require.Equal(t, 1, result.EventsRemaining)
	require.Equal(t, 3, backend.callCount())

	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	coordinator.scheduleNext(result, timer)
	status := coordinator.Status()
	require.Equal(t, "degraded", status.Health)
	require.Equal(t, []string{"a"}, status.DeferredIDs)
}

func TestBatchCoordinatorDefersTerminalFailureStatusWrites(t *testing.T) {
	store := newFakeCoordinatorStore(PendingEvent{ID: "a"})
	store.writeErrorsLeft["a"] = 3
	backend := &fakeBatchBackend{run: func(_ context.Context, batch Batch) (BatchResult, error) {
		return BatchResult{BatchID: batch.ID, Status: "failed", Error: errors.New("acpmux crashed")}, nil
	}}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})
	for range 3 {
		result := coordinator.flush(flushExplicit)
		require.Equal(t, 1, result.StatusWriteErrors)
		require.Equal(t, 1, result.EventsFailed)
	}
	require.True(t, coordinator.deferredSet["a"])
	require.Empty(t, store.statuses)
	require.Len(t, store.pending, 1)
}

func TestBatchCoordinatorStatusWriteSuccessClearsFailureCounter(t *testing.T) {
	store := newFakeCoordinatorStore(PendingEvent{ID: "a"})
	store.writeErrorsLeft["a"] = 2
	coordinator := newTestCoordinator(t, store, &fakeBatchBackend{run: successfulBatchResult}, CoordinatorConfig{})
	coordinator.flush(flushExplicit)
	coordinator.flush(flushExplicit)
	result := coordinator.flush(flushExplicit)
	require.Equal(t, 0, result.StatusWriteErrors)
	require.Empty(t, coordinator.writeFailCounts)
	require.Empty(t, coordinator.deferredSet)
}

func TestBatchCoordinatorDeferredEventsRetryWithFreshCoordinator(t *testing.T) {
	store := newFakeCoordinatorStore(PendingEvent{ID: "a"})
	store.writeErrorsLeft["a"] = 3
	first := newTestCoordinator(t, store, &fakeBatchBackend{run: successfulBatchResult}, CoordinatorConfig{})
	for range 3 {
		first.flush(flushExplicit)
	}
	require.True(t, first.deferredSet["a"])

	secondBackend := &fakeBatchBackend{run: successfulBatchResult}
	second := newTestCoordinator(t, store, secondBackend, CoordinatorConfig{})
	result := second.flush(flushExplicit)
	require.Equal(t, 1, result.EventsIntegrated)
	require.Zero(t, result.EventsRemaining)
	require.Equal(t, 1, secondBackend.callCount())
}

func TestBatchCoordinatorTargetedAndBlindSplit(t *testing.T) {
	coordinator := newTestCoordinator(t, newFakeCoordinatorStore(), &fakeBatchBackend{run: successfulBatchResult}, CoordinatorConfig{})
	batch := makeBatch(makePendingEvents(8), 0)
	summary := "EVENT event-00 INTEGRATED pages: semantic/bad.md\nEVENT event-01 INTEGRATED pages: semantic/good.md"
	targeted := coordinator.classifyAndSplit(batch, BatchResult{Violations: []Violation{{Path: "semantic/bad.md"}}, Summary: summary})
	require.Len(t, targeted, 2)
	require.Len(t, targeted[0].Events, 7)
	require.Equal(t, "event-00", targeted[1].Events[0].ID)
	require.Equal(t, 1, targeted[0].Depth)
	require.NotEmpty(t, targeted[0].ID)

	blind := coordinator.classifyAndSplit(batch, BatchResult{Violations: []Violation{{Message: "diff budget"}}})
	require.Len(t, blind, 2)
	require.Len(t, blind[0].Events, 4)
	require.Len(t, blind[1].Events, 4)
	require.Nil(t, coordinator.classifyAndSplit(makeBatch(makePendingEvents(5), 0), BatchResult{Error: errors.New("crash")}))
	require.Nil(t, coordinator.classifyAndSplit(makeBatch(makePendingEvents(8), 2), BatchResult{Error: errors.New("crash")}))
}

func TestBatchCoordinatorStatusHealthPrecedenceAndDisjointCounts(t *testing.T) {
	store := newFakeCoordinatorStore(makePendingEvents(3)...)
	store.statuses["failed"] = EventStatus{Status: "failed"}
	coordinator := newTestCoordinator(t, store, &fakeBatchBackend{run: successfulBatchResult}, CoordinatorConfig{})
	coordinator.currentBatch = &ActiveBatchState{BatchID: "active", EventIDs: []string{"event-00", "event-01"}, StartedAt: time.Now(), BatchIndex: 1, TotalBatches: 2}
	coordinator.lastResult = &FlushResult{BatchesSucceeded: 0, EventsRemaining: 3}
	coordinator.lastFlushAt = time.Now()

	status := coordinator.Status()
	require.Equal(t, 1, status.Pending)
	require.Equal(t, 2, status.InProgress)
	require.Equal(t, 4, status.TotalEvents)
	require.Equal(t, "in_progress", status.Health, "active recovery must not report stuck")
	require.Equal(t, "active", status.CurrentBatch.ID)
	require.Equal(t, []string{"event-00", "event-01"}, status.CurrentBatch.EventIDs)
	require.Equal(t, 1, status.CurrentBatch.Index)
	require.Equal(t, 2, status.CurrentBatch.TotalBatches)
	require.Equal(t, 2, status.CurrentBatch.EventsInBatch)
	require.False(t, status.CurrentBatch.StartedAt.IsZero())
	require.GreaterOrEqual(t, status.CurrentBatch.ElapsedMs, int64(0))
	require.NotNil(t, status.LastFlush)
	require.False(t, status.LastFlush.CompletedAt.IsZero())
	require.Zero(t, status.LastFlush.BatchesSucceeded)

	coordinator.currentBatch = nil
	status = coordinator.Status()
	require.Equal(t, "stuck", status.Health)
	coordinator.suspended = true
	status = coordinator.Status()
	require.Equal(t, "suspended", status.Health)
}

func TestStatusWallClockStuckArm(t *testing.T) {
	store := newFakeCoordinatorStore(makePendingEvents(2)...)
	coordinator := newTestCoordinator(t, store, &fakeBatchBackend{run: successfulBatchResult}, CoordinatorConfig{})

	// Fresh in-progress batch: the wall-clock arm must NOT fire.
	coordinator.currentBatch = &ActiveBatchState{
		BatchID: "fresh", EventIDs: []string{"event-00"},
		StartedAt: time.Now().UTC(), BatchIndex: 1, TotalBatches: 1,
	}
	status := coordinator.Status()
	require.Equal(t, "in_progress", status.Health, "fresh batch must stay in_progress")

	// Batch current for longer than 2× the hard batch cap with InProgress > 0:
	// health must report stuck regardless of the InProgress==0 guard on the
	// zero-success arm (defense-in-depth vs cap-enforcement bugs).
	coordinator.currentBatch = &ActiveBatchState{
		BatchID: "hung", EventIDs: []string{"event-00"},
		StartedAt:  time.Now().UTC().Add(-wallClockStuckMultiple*MaxBatchCap - time.Second),
		BatchIndex: 1, TotalBatches: 1,
	}
	status = coordinator.Status()
	require.Equal(t, 1, status.InProgress)
	require.Equal(t, "stuck", status.Health, "wall-clock arm must fire despite InProgress > 0")
}

func TestStatusGeneratedAtSingleAnchor(t *testing.T) {
	store := newFakeCoordinatorStore(makePendingEvents(1)...)
	coordinator := newTestCoordinator(t, store, &fakeBatchBackend{run: successfulBatchResult}, CoordinatorConfig{})
	startedAt := time.Now().UTC().Add(-90 * time.Second)
	coordinator.currentBatch = &ActiveBatchState{
		BatchID: "anchored", EventIDs: []string{"event-00"},
		StartedAt: startedAt, BatchIndex: 1, TotalBatches: 1,
	}

	before := time.Now().UTC()
	status := coordinator.Status()
	after := time.Now().UTC()

	require.False(t, status.GeneratedAt.IsZero())
	require.False(t, status.GeneratedAt.Before(before))
	require.False(t, status.GeneratedAt.After(after))
	// elapsed_ms must be derived from the SAME captured anchor as
	// generated_at — exact equality, zero tolerance (InDelta was blind to
	// the dual-time.Now() defect; adversary-2 B3).
	derived := status.GeneratedAt.Sub(status.CurrentBatch.StartedAt).Milliseconds()
	require.Equal(t, derived, status.CurrentBatch.ElapsedMs)
}

func allUnknownBatchResult(_ context.Context, batch Batch) (BatchResult, error) {
	results := make([]EventResult, len(batch.Events))
	for index, event := range batch.Events {
		results[index] = EventResult{
			EventID: event.ID, Status: "unknown",
			Reason: UnknownReasonNoPerEventVerdict, TranscriptPath: "transcripts/" + batch.ID + ".log",
		}
	}
	return BatchResult{BatchID: batch.ID, Status: "success", EventResults: results}, nil
}

func singleEventLimits() BatchLimits {
	limits := DefaultBatchLimits
	limits.MaxEventsPerBatch = 1
	return limits
}

func TestUnknownStreakSuspendsFlush(t *testing.T) {
	store := newFakeCoordinatorStore(makePendingEvents(6)...)
	backend := &fakeBatchBackend{run: allUnknownBatchResult}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})
	coordinator.limits = singleEventLimits()

	result := coordinator.flush(flushExplicit)

	require.Equal(t, StopReasonUnknownStreak, result.StoppedReason)
	require.Len(t, backend.calls, unknownStreakLimit, "flush must stop after the streak limit, not burn further turns")

	status := coordinator.Status()
	require.True(t, status.Suspended)
	require.Equal(t, "suspended", status.Health)
	require.NotNil(t, status.UnknownStreak)
	require.Equal(t, unknownStreakLimit, status.UnknownStreak.Batches)
	require.Equal(t, unknownStreakLimit, status.UnknownStreak.ReasonCounts[UnknownReasonNoPerEventVerdict])
	require.NotEmpty(t, status.UnknownStreak.TranscriptPaths)
	require.LessOrEqual(t, len(status.UnknownStreak.TranscriptPaths), unknownStreakLimit)
	for _, path := range status.UnknownStreak.TranscriptPaths {
		require.NotEmpty(t, path)
	}
	require.Equal(t, unknownStreakLimit, status.UnknownByReason[UnknownReasonNoPerEventVerdict])
	require.NotNil(t, status.LastError)
	require.Equal(t, StopReasonUnknownStreak, status.LastError.ErrorClass)
}

func TestUnknownStreakResetsOnAdjudicatedOutcome(t *testing.T) {
	// 5 single-event batches; batch 3 integrates. Streak: 1,2,reset,1,2 —
	// never reaches the limit, so the flush must run to completion.
	store := newFakeCoordinatorStore(makePendingEvents(5)...)
	call := 0
	backend := &fakeBatchBackend{run: func(ctx context.Context, batch Batch) (BatchResult, error) {
		call++
		if call == unknownStreakLimit { // an integrated outcome inside the would-be streak
			results := make([]EventResult, len(batch.Events))
			for index, event := range batch.Events {
				results[index] = EventResult{EventID: event.ID, Status: "integrated", Pages: []string{"wiki/p.md"}}
			}
			return BatchResult{BatchID: batch.ID, Status: "success", EventResults: results}, nil
		}
		return allUnknownBatchResult(ctx, batch)
	}}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})
	coordinator.limits = singleEventLimits()

	result := coordinator.flush(flushExplicit)

	require.Empty(t, result.StoppedReason, "adjudicated outcome must reset the streak and let the flush finish")
	require.Len(t, backend.calls, 5, "all batches must run")
	status := coordinator.Status()
	require.False(t, status.Suspended)
	require.Nil(t, status.UnknownStreak)
}

func TestUnknownStreakClearedByNextFlush(t *testing.T) {
	store := newFakeCoordinatorStore(makePendingEvents(3)...)
	backend := &fakeBatchBackend{run: allUnknownBatchResult}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})
	coordinator.limits = singleEventLimits()

	first := coordinator.flush(flushExplicit)
	require.Equal(t, StopReasonUnknownStreak, first.StoppedReason)
	require.True(t, coordinator.Status().Suspended)

	// The next explicit flush (triggered by /remember or admin retry) clears
	// the suspension and the streak record before running.
	second := coordinator.flush(flushExplicit)
	require.NotEqual(t, StopReasonUnknownStreak, second.StoppedReason, "no pending unknown-eligible events remain")
	status := coordinator.Status()
	require.False(t, status.Suspended)
	require.Nil(t, status.UnknownStreak)
}

func TestUnknownStreakProbeRetripAfterRecovery(t *testing.T) {
	store := newFakeCoordinatorStore(makePendingEvents(4)...)
	backend := &fakeBatchBackend{run: allUnknownBatchResult}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})
	coordinator.limits = singleEventLimits()

	first := coordinator.flush(flushExplicit)
	require.Equal(t, StopReasonUnknownStreak, first.StoppedReason)
	require.Len(t, backend.calls, unknownStreakLimit)

	// Probe semantics: recovery clears the suspension but NOT the counter.
	// With the agent still silently failing, the very next all-unknown batch
	// re-trips — each recovery attempt costs one probe turn, not N.
	second := coordinator.flush(flushExplicit)
	require.Equal(t, StopReasonUnknownStreak, second.StoppedReason)
	require.Len(t, backend.calls, unknownStreakLimit+1, "re-trip must cost exactly one probe batch")
	status := coordinator.Status()
	require.True(t, status.Suspended)
	require.NotNil(t, status.UnknownStreak)
	require.Equal(t, unknownStreakLimit+1, status.UnknownStreak.Batches)
}

func TestUnknownStreakAutoFlushRetainsEvidence(t *testing.T) {
	store := newFakeCoordinatorStore(makePendingEvents(4)...)
	backend := &fakeBatchBackend{run: allUnknownBatchResult}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})
	coordinator.limits = singleEventLimits()

	first := coordinator.flush(flushExplicit)
	require.Equal(t, StopReasonUnknownStreak, first.StoppedReason)

	// An auto (timer/threshold) flush while suspended is a no-op: it burns no
	// turns and the unknown_streak stop record survives for the operator.
	calls := len(backend.calls)
	auto := coordinator.flush(flushAuto)
	require.Equal(t, StopReasonStopped, auto.StoppedReason)
	require.Len(t, backend.calls, calls, "auto flush while suspended must not run batches")
	status := coordinator.Status()
	require.True(t, status.Suspended)
	require.NotNil(t, status.UnknownStreak, "stop record must survive auto flushes")
}

func TestSubThresholdRememberClearsSuspensionAndRecordAtomically(t *testing.T) {
	store := newFakeCoordinatorStore(makePendingEvents(3)...)
	backend := &fakeBatchBackend{run: allUnknownBatchResult}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})
	coordinator.limits = singleEventLimits()

	first := coordinator.flush(flushExplicit)
	require.Equal(t, StopReasonUnknownStreak, first.StoppedReason)
	require.True(t, coordinator.Status().Suspended)
	require.NotNil(t, coordinator.Status().UnknownStreak)

	// Sub-threshold /remember: NotifyNewEvent is the recovery point — it must
	// clear the suspension AND the stop record atomically, because the flush
	// that follows arrives via the timer (flushAuto) and never runs the
	// explicit-flush clearing path.
	coordinator.NotifyNewEvent("event-probe")
	status := coordinator.Status()
	require.False(t, status.Suspended)
	require.Nil(t, status.UnknownStreak, "stop record must clear with the suspension, not linger")

	// The subsequent timer flush probes normally (no suspended no-op), and
	// with the agent still silently failing it re-trips with a NEW record —
	// the streak counter survived the recovery (probe semantics).
	auto := coordinator.flush(flushAuto)
	require.Equal(t, StopReasonUnknownStreak, auto.StoppedReason)
	status = coordinator.Status()
	require.True(t, status.Suspended)
	require.NotNil(t, status.UnknownStreak)
	require.Equal(t, unknownStreakLimit+1, status.UnknownStreak.Batches, "probe re-trip must cost one batch and produce a fresh record")
}

func TestStatusElapsedNeverNegativeUnderConcurrentBatchStart(t *testing.T) {
	store := newFakeCoordinatorStore(makePendingEvents(1)...)
	coordinator := newTestCoordinator(t, store, &fakeBatchBackend{run: successfulBatchResult}, CoordinatorConfig{})

	// generatedAt is captured inside the same critical section that snapshots
	// currentBatch, so a concurrent setCurrentBatch can never yield
	// generated_at < started_at (negative elapsed_ms).
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			coordinator.setCurrentBatch(Batch{ID: "race", Events: makePendingEvents(1)}, 1, 1)
		}
	}()
	for i := 0; i < 200; i++ {
		status := coordinator.Status()
		if status.CurrentBatch != nil {
			require.GreaterOrEqual(t, status.CurrentBatch.ElapsedMs, int64(0))
			require.False(t, status.GeneratedAt.Before(status.CurrentBatch.StartedAt),
				"generated_at must never precede started_at")
		}
	}
	<-done
}

func TestFlushStopReasonsDocumented(t *testing.T) {
	source, err := os.ReadFile("coordinator.go")
	require.NoError(t, err)
	text := string(source)
	commentStart := strings.Index(text, "// Flush stop reasons.")
	require.GreaterOrEqual(t, commentStart, 0, "stop-reason doc comment missing")
	commentEnd := strings.Index(text[commentStart:], "const (")
	require.Greater(t, commentEnd, 0)
	comment := text[commentStart : commentStart+commentEnd]
	for _, reason := range []string{
		StopReasonEnvelopeTimeout, StopReasonStopped, StopReasonInternalErrorLimit,
		StopReasonStoreError, StopReasonUnknownStreak,
	} {
		require.Contains(t, comment, "\""+reason+"\"", "stop reason %q missing from doc comment", reason)
	}
}

func TestUnknownOutcomeAlwaysHasReasonAndTranscript(t *testing.T) {
	batch := Batch{ID: "batch-x", Events: makePendingEvents(1)}
	// Unclassifiable output with no transcript persisted: reason must fall
	// back to the closed enum and transcript to a non-empty marker.
	results := parseEventResults(batch, "unstructured", "")
	require.Equal(t, "unknown", results[0].Status)
	require.Equal(t, UnknownReasonNoPerEventVerdict, results[0].Reason)
	require.NotEmpty(t, results[0].TranscriptPath)
}

func TestBatchCoordinatorAdminTransitions(t *testing.T) {
	store := newFakeCoordinatorStore()
	store.statuses["unknown"] = EventStatus{Status: "unknown"}
	store.statuses["failed"] = EventStatus{Status: "failed"}
	store.statuses["failed-confirm"] = EventStatus{Status: "failed"}
	store.statuses["integrated"] = EventStatus{Status: "integrated"}
	store.statuses["skipped"] = EventStatus{Status: "skipped"}
	store.statuses["unknown-confirmed"] = EventStatus{Status: "unknown"}
	store.pending["pending"] = PendingEvent{ID: "pending"}
	coordinator := newTestCoordinator(t, store, &fakeBatchBackend{run: successfulBatchResult}, CoordinatorConfig{})

	result, err := coordinator.ResetEvent("failed")
	require.NoError(t, err)
	require.Equal(t, AdminResult{EventID: "failed", OldStatus: "failed", NewStatus: "pending"}, result)
	result, err = coordinator.ConfirmEvent("unknown")
	require.NoError(t, err)
	require.Equal(t, AdminResult{EventID: "unknown", OldStatus: "unknown", NewStatus: "integrated"}, result)

	_, err = coordinator.ResetEvent("integrated")
	var transition *TransitionError
	require.ErrorAs(t, err, &transition)
	require.Equal(t, "integrated", transition.CurrentStatus)
	for _, eventID := range []string{"skipped", "pending"} {
		_, err = coordinator.ResetEvent(eventID)
		require.ErrorAs(t, err, &transition)
	}
	for _, eventID := range []string{"pending", "failed-confirm", "skipped", "integrated"} {
		_, err = coordinator.ConfirmEvent(eventID)
		require.ErrorAs(t, err, &transition)
	}
	_, err = coordinator.ConfirmEvent("unknown-confirmed")
	require.NoError(t, err)
	_, err = coordinator.ConfirmEvent("unknown-confirmed")
	require.ErrorAs(t, err, &transition)
	_, err = coordinator.ResetEvent("missing")
	require.ErrorIs(t, err, ErrEventNotFound)
}

func TestBatchCoordinatorSerializesConcurrentAdminReset(t *testing.T) {
	store := newFakeCoordinatorStore()
	store.statuses["failed"] = EventStatus{Status: "failed"}
	coordinator := newTestCoordinator(t, store, &fakeBatchBackend{run: successfulBatchResult}, CoordinatorConfig{})

	results := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := coordinator.ResetEvent("failed")
			results <- err
		}()
	}
	wait.Wait()
	close(results)
	succeeded := 0
	conflicted := 0
	for err := range results {
		if err == nil {
			succeeded++
			continue
		}
		var transition *TransitionError
		require.ErrorAs(t, err, &transition)
		conflicted++
	}
	require.Equal(t, 1, succeeded)
	require.Equal(t, 1, conflicted)
	require.Equal(t, 1, len(store.pending))
}

func TestBatchCoordinatorAdminAndFlushUseLastEntryWins(t *testing.T) {
	store := newFakeCoordinatorStore(PendingEvent{ID: "event"})
	store.statuses["event"] = EventStatus{Status: "failed"}
	started := make(chan struct{})
	release := make(chan struct{})
	backend := &fakeBatchBackend{run: func(_ context.Context, batch Batch) (BatchResult, error) {
		close(started)
		<-release
		return integratedResult(batch), nil
	}}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{})
	flushDone := make(chan FlushResult, 1)
	go func() { flushDone <- coordinator.flush(flushExplicit) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("batch did not start")
	}
	reset, err := coordinator.ResetEvent("event")
	require.NoError(t, err)
	require.Equal(t, "failed", reset.OldStatus)
	close(release)
	result := <-flushDone
	require.Equal(t, 1, result.EventsIntegrated)
	require.Equal(t, "integrated", store.statuses["event"].Status)
	pending, err := store.ListPendingEvents()
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestBatchCoordinatorStartReadyBarrierAndStop(t *testing.T) {
	store := newFakeCoordinatorStore()
	wikiMu := &sync.Mutex{}
	rebuilder := &fakeRebuilder{}
	rebuilder.run = func() error {
		require.False(t, wikiMu.TryLock(), "startup rebuild must hold wikiMu")
		return nil
	}
	coordinator := newTestCoordinatorWithDeps(t, store, &fakeBatchBackend{run: successfulBatchResult}, rebuilder, wikiMu, CoordinatorConfig{})
	require.NoError(t, coordinator.Start(time.Second))
	require.Equal(t, 1, store.recoverCalls)
	require.Equal(t, 1, rebuilder.calls)
	coordinator.Stop(context.Background())
	select {
	case <-coordinator.done:
	default:
		t.Fatal("coordinator did not stop")
	}
}

func TestBatchCoordinatorAutoResumesAfterEnvelopeTimeout(t *testing.T) {
	store := newFakeCoordinatorStore(makePendingEvents(40)...)
	backend := &fakeBatchBackend{run: func(_ context.Context, batch Batch) (BatchResult, error) {
		time.Sleep(5 * time.Millisecond)
		return integratedResult(batch), nil
	}}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{
		FlushThreshold: 1, FlushInterval: time.Hour, EnvelopeTimeout: 3 * time.Millisecond, ResumeDelay: 5 * time.Millisecond,
	})
	require.NoError(t, coordinator.Start(time.Second))
	coordinator.NotifyNewEvent(makePendingEvents(1)[0].ID)
	require.Eventually(t, func() bool {
		count, _ := store.PendingCount()
		return count == 0
	}, time.Second, 5*time.Millisecond)
	require.Equal(t, 2, backend.callCount())
	coordinator.Stop(context.Background())
}

func TestBatchCoordinatorStopBetweenBatchesDoesNotStartNextBatch(t *testing.T) {
	store := newFakeCoordinatorStore(makePendingEvents(40)...)
	started := make(chan struct{})
	release := make(chan struct{})
	var firstBatch sync.Once
	backend := &fakeBatchBackend{run: func(_ context.Context, batch Batch) (BatchResult, error) {
		firstBatch.Do(func() {
			close(started)
			<-release
		})
		return integratedResult(batch), nil
	}}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, coordinator.Start(time.Second))
	coordinator.NotifyNewEvent(makePendingEvents(1)[0].ID)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first batch did not start")
	}
	stopDone := make(chan struct{})
	go func() {
		coordinator.Stop(context.Background())
		close(stopDone)
	}()
	require.Eventually(t, coordinator.isStopped, time.Second, time.Millisecond)
	close(release)
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not stop")
	}
	require.Equal(t, 1, backend.callCount())
	require.NotNil(t, coordinator.lastResult)
	require.Equal(t, "stopped", coordinator.lastResult.StoppedReason)
}

func TestBatchCoordinatorStopCancelsInFlightBatch(t *testing.T) {
	store := newFakeCoordinatorStore(PendingEvent{ID: "a"})
	started := make(chan struct{})
	backend := &fakeBatchBackend{run: func(ctx context.Context, batch Batch) (BatchResult, error) {
		close(started)
		<-ctx.Done()
		return BatchResult{BatchID: batch.ID, Status: "failed", Error: ctx.Err()}, nil
	}}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, coordinator.Start(time.Second))
	coordinator.NotifyNewEvent("a")
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("batch did not start")
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	coordinator.Stop(shutdownContext)
	require.Equal(t, 1, len(store.pending))
	require.Empty(t, store.statuses)
}

func TestBatchCoordinatorStopReturnsAfterHardTimeoutWhenCleanupBlocks(t *testing.T) {
	store := newFakeCoordinatorStore(PendingEvent{ID: "a"})
	rebuildStarted := make(chan struct{})
	releaseRebuild := make(chan struct{})
	var rebuildCalls atomic.Int64
	rebuilder := &fakeRebuilder{run: func() error {
		if rebuildCalls.Add(1) == 1 {
			return nil
		}
		close(rebuildStarted)
		<-releaseRebuild
		return nil
	}}
	backend := &fakeBatchBackend{run: func(_ context.Context, batch Batch) (BatchResult, error) {
		rebuildErr := rebuilder.RebuildWikiIndex()
		result := integratedResult(batch)
		result.IndexStale = rebuildErr != nil
		return result, nil
	}}
	coordinator := newTestCoordinatorWithDeps(
		t, store, backend, rebuilder, &sync.Mutex{},
		CoordinatorConfig{FlushThreshold: 1, FlushInterval: time.Hour},
	)
	coordinator.hardStopTimeout = 20 * time.Millisecond
	require.NoError(t, coordinator.Start(time.Second))
	coordinator.NotifyNewEvent("a")
	select {
	case <-rebuildStarted:
	case <-time.After(time.Second):
		t.Fatal("rebuild did not start")
	}

	shutdownContext, cancel := context.WithCancel(context.Background())
	cancel()
	startedAt := time.Now()
	coordinator.Stop(shutdownContext)
	elapsed := time.Since(startedAt)
	require.GreaterOrEqual(t, elapsed, coordinator.hardStopTimeout)
	require.Less(t, elapsed, 200*time.Millisecond)

	close(releaseRebuild)
	select {
	case <-coordinator.done:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not finish after rebuild was released")
	}
	require.Equal(t, "integrated", store.statuses["a"].Status)
}

func TestBatchCoordinatorShutdownCancellationAtRetryDepthStaysPending(t *testing.T) {
	events := makePendingEvents(24)
	store := newFakeCoordinatorStore(events...)
	depthTwoStarted := make(chan struct{})
	backend := &fakeBatchBackend{run: func(ctx context.Context, batch Batch) (BatchResult, error) {
		if batch.Depth < 2 {
			return BatchResult{BatchID: batch.ID, Status: "failed", Error: errors.New("retry")}, nil
		}
		close(depthTwoStarted)
		<-ctx.Done()
		return BatchResult{BatchID: batch.ID, Status: "failed", Error: ctx.Err()}, nil
	}}
	coordinator := newTestCoordinator(t, store, backend, CoordinatorConfig{FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, coordinator.Start(time.Second))
	coordinator.NotifyNewEvent(events[0].ID)
	select {
	case <-depthTwoStarted:
	case <-time.After(time.Second):
		t.Fatal("depth-two retry did not start")
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	coordinator.Stop(shutdownContext)
	require.Empty(t, store.statuses)
	require.Len(t, store.pending, 24)
}

func TestBatchCoordinatorStartupFailureIsDegradedNotFatal(t *testing.T) {
	store := newFakeCoordinatorStore()
	rebuilder := &fakeRebuilder{err: errors.New("rebuild failed")}
	coordinator := newTestCoordinatorWithDeps(t, store, &fakeBatchBackend{run: successfulBatchResult}, rebuilder, &sync.Mutex{}, CoordinatorConfig{})
	require.NoError(t, coordinator.Start(time.Second))
	require.Equal(t, "degraded", coordinator.Status().Health)
	coordinator.Stop(context.Background())
}

func TestBatchCoordinatorSuccessfulBatchClearsStartupIndexStale(t *testing.T) {
	store := newFakeCoordinatorStore(PendingEvent{ID: "a"})
	var rebuildCalls atomic.Int64
	rebuilder := &fakeRebuilder{run: func() error {
		if rebuildCalls.Add(1) == 1 {
			return errors.New("startup rebuild failed")
		}
		return nil
	}}
	coordinator := newTestCoordinatorWithDeps(
		t, store, &fakeBatchBackend{run: successfulBatchResult}, rebuilder, &sync.Mutex{},
		CoordinatorConfig{FlushThreshold: 1, FlushInterval: time.Hour},
	)
	require.NoError(t, coordinator.Start(time.Second))
	require.True(t, coordinator.Status().IndexStale)
	coordinator.NotifyNewEvent("a")
	require.Eventually(t, func() bool {
		return coordinator.Status().Pending == 0 && !coordinator.Status().IndexStale
	}, time.Second, 10*time.Millisecond)
	coordinator.Stop(context.Background())
}

func TestBatchCoordinatorRecoveryErrorDoesNotFalselyMarkIndexStale(t *testing.T) {
	store := newFakeCoordinatorStore()
	store.recoverErr = errors.New("recovery failed")
	coordinator := newTestCoordinatorWithDeps(t, store, &fakeBatchBackend{run: successfulBatchResult}, &fakeRebuilder{}, &sync.Mutex{}, CoordinatorConfig{})
	require.NoError(t, coordinator.Start(time.Second))
	require.False(t, coordinator.Status().IndexStale)
	require.NotNil(t, coordinator.Status().LastError)
	coordinator.Stop(context.Background())
}

func newTestCoordinator(t *testing.T, store pendingEventStore, backend batchIngestBackend, cfg CoordinatorConfig) *BatchCoordinator {
	t.Helper()
	return newTestCoordinatorWithDeps(t, store, backend, &fakeRebuilder{}, &sync.Mutex{}, cfg)
}

func newTestCoordinatorWithDeps(t *testing.T, store pendingEventStore, backend batchIngestBackend, rebuilder WikiIndexRebuilder, wikiMu *sync.Mutex, cfg CoordinatorConfig) *BatchCoordinator {
	t.Helper()
	dataDir := t.TempDir()
	wikiDir := filepath.Join(dataDir, "wiki")
	require.NoError(t, os.MkdirAll(wikiDir, 0o755))
	return newBatchCoordinator(backend, store, rebuilder, wikiMu, wikiDir, cfg)
}

func makePendingEvents(count int) []PendingEvent {
	events := make([]PendingEvent, count)
	for index := range events {
		events[index] = PendingEvent{ID: formatEventID(index), Text: "payload", CreatedAt: time.Unix(int64(index), 0)}
	}
	return events
}

func formatEventID(index int) string {
	return "event-" + string(rune('0'+index/10)) + string(rune('0'+index%10))
}

func integratedResult(batch Batch) BatchResult {
	results := make([]EventResult, len(batch.Events))
	for index, event := range batch.Events {
		results[index] = EventResult{EventID: event.ID, Status: "integrated"}
	}
	return BatchResult{BatchID: batch.ID, Status: "success", EventResults: results}
}

func successfulBatchResult(_ context.Context, batch Batch) (BatchResult, error) {
	return integratedResult(batch), nil
}
