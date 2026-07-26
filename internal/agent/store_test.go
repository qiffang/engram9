package agent

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/qiffang/engram9/internal/storage"
	"github.com/stretchr/testify/require"
)

type memoryEventSource struct {
	mu              sync.Mutex
	events          []storage.Event
	listErr         error
	getErr          error
	blockNextList   bool
	listStarted     chan struct{}
	releaseNextList chan struct{}
}

func (source *memoryEventSource) ListEvents() ([]storage.Event, error) {
	source.mu.Lock()
	if source.listErr != nil {
		source.mu.Unlock()
		return nil, source.listErr
	}
	events := append([]storage.Event(nil), source.events...)
	block := source.blockNextList
	source.blockNextList = false
	started := source.listStarted
	release := source.releaseNextList
	source.mu.Unlock()
	if block {
		close(started)
		<-release
	}
	return events, nil
}

func (source *memoryEventSource) GetEvent(id string) (storage.Event, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.getErr != nil {
		return storage.Event{}, source.getErr
	}
	for _, event := range source.events {
		if event.ID == id {
			return event, nil
		}
	}
	return storage.Event{}, ErrEventNotFound
}

func (source *memoryEventSource) append(event storage.Event) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.events = append(source.events, event)
}

func TestPendingEventStoreRequiresExplicitBootstrapEpoch(t *testing.T) {
	source := &memoryEventSource{events: []storage.Event{{ID: "old"}}}
	_, err := NewPendingEventStore(t.TempDir(), source, StoreConfig{})
	require.ErrorContains(t, err, "BATCH_INGEST_EPOCH")
}

func TestPendingEventStoreBootstrapBoundary(t *testing.T) {
	epoch := "2026-07-19T12:00:00Z"
	source := &memoryEventSource{events: []storage.Event{
		{ID: "before", Timestamp: "2026-07-19T11:59:59Z"},
		{ID: "equal", Timestamp: epoch},
		{ID: "after", Timestamp: "2026-07-19T12:00:01Z"},
		{ID: "invalid", Timestamp: "not-a-time"},
	}}
	dataDir := t.TempDir()
	store, err := NewPendingEventStore(dataDir, source, StoreConfig{BootstrapEpoch: epoch})
	require.NoError(t, err)

	pending, err := store.ListPendingEvents()
	require.NoError(t, err)
	require.Equal(t, []string{"equal", "after"}, pendingIDs(pending))
	require.Equal(t, EventStatus{Status: "integrated", Reason: "pre_epoch_bootstrap"}, mustEventStatus(t, store, "invalid"))

	data, err := os.ReadFile(filepath.Join(dataDir, integrationStatusFilename))
	require.NoError(t, err)
	require.Contains(t, string(data), `"type":"bootstrap"`)
	require.Contains(t, string(data), `"reason":"explicit_operator_epoch"`)
	require.FileExists(t, filepath.Join(dataDir, integrationStatusFilename))
	require.NoFileExists(t, filepath.Join(dataDir, integrationStatusFilename+".bootstrap"))
}

func TestPendingEventStoreFutureEpochDoesNotFilterLaterAppends(t *testing.T) {
	source := &memoryEventSource{events: []storage.Event{{ID: "existing", Timestamp: "2026-01-01T00:00:00Z"}}}
	store, err := NewPendingEventStore(t.TempDir(), source, StoreConfig{BootstrapEpoch: "2030-01-01T00:00:00Z"})
	require.NoError(t, err)
	require.Equal(t, EventStatus{Status: "integrated", Reason: "pre_epoch_bootstrap"}, mustEventStatus(t, store, "existing"))

	source.append(storage.Event{ID: "later-append", Timestamp: "2026-01-02T00:00:00Z", Content: "new work"})
	store.NotifyAppended("later-append")
	pending, err := store.ListPendingEvents()
	require.NoError(t, err)
	require.Equal(t, []string{"later-append"}, pendingIDs(pending))
}

func TestPendingEventStoreReconcilePreservesConcurrentNotify(t *testing.T) {
	source := &memoryEventSource{}
	store, err := NewPendingEventStore(t.TempDir(), source, StoreConfig{})
	require.NoError(t, err)

	started := make(chan struct{})
	release := make(chan struct{})
	source.mu.Lock()
	source.blockNextList = true
	source.listStarted = started
	source.releaseNextList = release
	source.mu.Unlock()

	listDone := make(chan error, 1)
	go func() {
		_, listErr := store.ListPendingEvents()
		listDone <- listErr
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("list did not capture its snapshot")
	}
	source.append(storage.Event{ID: "new", Content: "payload"})
	store.NotifyAppended("new")
	close(release)
	require.NoError(t, <-listDone)

	count, err := store.PendingCount()
	require.NoError(t, err)
	require.Equal(t, 1, count)
	pending, err := store.ListPendingEvents()
	require.NoError(t, err)
	require.Equal(t, []string{"new"}, pendingIDs(pending))
}

func TestPendingEventStoreRetriesBootstrapAfterStaleTempFile(t *testing.T) {
	dataDir := t.TempDir()
	tempPath := filepath.Join(dataDir, integrationStatusFilename+".bootstrap")
	require.NoError(t, os.WriteFile(tempPath, []byte(`{"type":"bootstrap"`), 0o600))
	source := &memoryEventSource{events: []storage.Event{{ID: "event", Timestamp: "2026-01-01T00:00:00Z"}}}
	store, err := NewPendingEventStore(dataDir, source, StoreConfig{BootstrapEpoch: "0001-01-01T00:00:00Z"})
	require.NoError(t, err)
	require.Equal(t, 1, store.Counts().Pending)
	require.NoFileExists(t, tempPath)
}

func TestPendingEventStoreZeroEpochReplaysAllEvents(t *testing.T) {
	source := &memoryEventSource{events: []storage.Event{{ID: "invalid"}, {ID: "valid", Timestamp: "2020-01-01T00:00:00Z"}}}
	store, err := NewPendingEventStore(t.TempDir(), source, StoreConfig{BootstrapEpoch: "0001-01-01T00:00:00Z"})
	require.NoError(t, err)
	count, err := store.PendingCount()
	require.NoError(t, err)
	require.Equal(t, 2, count)
}

func TestPendingEventStoreEmptyLogBootstrapsNow(t *testing.T) {
	dataDir := t.TempDir()
	store, err := NewPendingEventStore(dataDir, &memoryEventSource{}, StoreConfig{})
	require.NoError(t, err)
	require.Equal(t, StatusCounts{}, store.Counts())
	data, err := os.ReadFile(filepath.Join(dataDir, integrationStatusFilename))
	require.NoError(t, err)
	require.Contains(t, string(data), `"reason":"empty_event_log"`)
}

func TestPendingEventStoreRejectsInvalidEpochAndExistingEmptyStatus(t *testing.T) {
	source := &memoryEventSource{events: []storage.Event{{ID: "event"}}}
	_, err := NewPendingEventStore(t.TempDir(), source, StoreConfig{BootstrapEpoch: "yesterday"})
	require.ErrorContains(t, err, "invalid BATCH_INGEST_EPOCH")

	dataDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, integrationStatusFilename), nil, 0o600))
	_, err = NewPendingEventStore(dataDir, source, StoreConfig{BootstrapEpoch: "0001-01-01T00:00:00Z"})
	require.ErrorContains(t, err, "status file is empty")
}

func TestPendingEventStoreStatusLastEntryWins(t *testing.T) {
	dataDir := t.TempDir()
	statusPath := filepath.Join(dataDir, integrationStatusFilename)
	content := strings.Join([]string{
		`{"type":"bootstrap","epoch":"0001-01-01T00:00:00Z"}`,
		`{"event_id":"a","status":"integrated"}`,
		`{"event_id":"a","type":"reset"}`,
		`{"event_id":"b","status":"unknown","reason":"not reported"}`,
		`{"event_id":"ignored","status":"pending"}`,
	}, "\n") + "\n"
	require.NoError(t, os.WriteFile(statusPath, []byte(content), 0o600))
	source := &memoryEventSource{events: []storage.Event{{ID: "a"}, {ID: "b"}}}
	store, err := NewPendingEventStore(dataDir, source, StoreConfig{})
	require.NoError(t, err)

	require.Equal(t, EventStatus{Status: "pending"}, mustEventStatus(t, store, "a"))
	require.Equal(t, EventStatus{Status: "unknown", Reason: "not reported"}, mustEventStatus(t, store, "b"))
	require.Equal(t, StatusCounts{Total: 2, Pending: 1, Unknown: 1}, store.Counts())
}

func TestPendingEventStoreCorruptionRules(t *testing.T) {
	t.Run("truncated final line is ignored", func(t *testing.T) {
		dataDir := t.TempDir()
		content := `{"type":"bootstrap","epoch":"0001-01-01T00:00:00Z"}` + "\n" + `{"event_id":"a","status":"integrated"}`
		require.NoError(t, os.WriteFile(filepath.Join(dataDir, integrationStatusFilename), []byte(content), 0o600))
		store, err := NewPendingEventStore(dataDir, &memoryEventSource{events: []storage.Event{{ID: "a"}}}, StoreConfig{})
		require.NoError(t, err)
		require.Equal(t, EventStatus{Status: "pending"}, mustEventStatus(t, store, "a"))
	})

	t.Run("entirely unparseable", func(t *testing.T) {
		dataDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dataDir, integrationStatusFilename), []byte("garbage\n"), 0o600))
		_, err := NewPendingEventStore(dataDir, &memoryEventSource{}, StoreConfig{})
		require.ErrorContains(t, err, "entirely unparseable")
	})

	t.Run("threshold exceeded", func(t *testing.T) {
		dataDir := t.TempDir()
		var lines []string
		for range 90 {
			lines = append(lines, `{"type":"bootstrap","epoch":"0001-01-01T00:00:00Z"}`)
		}
		for range 11 {
			lines = append(lines, "garbage")
		}
		require.NoError(t, os.WriteFile(filepath.Join(dataDir, integrationStatusFilename), []byte(strings.Join(lines, "\n")+"\n"), 0o600))
		_, err := NewPendingEventStore(dataDir, &memoryEventSource{}, StoreConfig{})
		require.ErrorContains(t, err, "corruption threshold exceeded")
	})

	t.Run("small corruption is tolerated", func(t *testing.T) {
		dataDir := t.TempDir()
		var lines []string
		for range 95 {
			lines = append(lines, `{"type":"bootstrap","epoch":"0001-01-01T00:00:00Z"}`)
		}
		for range 5 {
			lines = append(lines, "garbage")
		}
		require.NoError(t, os.WriteFile(filepath.Join(dataDir, integrationStatusFilename), []byte(strings.Join(lines, "\n")+"\n"), 0o600))
		_, err := NewPendingEventStore(dataDir, &memoryEventSource{}, StoreConfig{})
		require.NoError(t, err)
	})
}

func TestPendingEventStoreWriteResetAndConfirmState(t *testing.T) {
	source := &memoryEventSource{events: []storage.Event{{ID: "a"}, {ID: "b"}}}
	store, err := NewPendingEventStore(t.TempDir(), source, StoreConfig{BootstrapEpoch: "0001-01-01T00:00:00Z"})
	require.NoError(t, err)

	require.NoError(t, store.WriteStatuses([]StatusEntry{
		{EventID: "a", Status: "integrated", BatchID: "batch", Pages: []string{"semantic/a.md"}},
		{EventID: "b", Status: "failed", Reason: "poison"},
	}))
	require.Equal(t, StatusCounts{Total: 2, Integrated: 1, Failed: 1}, store.Counts())
	require.Equal(t, EventStatus{Status: "integrated", BatchID: "batch", Pages: []string{"semantic/a.md"}}, mustEventStatus(t, store, "a"))

	require.NoError(t, store.WriteTombstone("b"))
	require.Equal(t, EventStatus{Status: "pending"}, mustEventStatus(t, store, "b"))
	require.Equal(t, StatusCounts{Total: 2, Pending: 1, Integrated: 1}, store.Counts())

	require.Error(t, store.WriteStatus(StatusEntry{EventID: "b", Status: "pending"}))
}

func TestPendingEventStoreResetTombstoneSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	source := &memoryEventSource{events: []storage.Event{{ID: "event"}}}
	store, err := NewPendingEventStore(dataDir, source, StoreConfig{BootstrapEpoch: "0001-01-01T00:00:00Z"})
	require.NoError(t, err)
	require.NoError(t, store.WriteStatus(StatusEntry{EventID: "event", Status: "unknown"}))
	require.NoError(t, store.WriteTombstone("event"))

	restarted, err := NewPendingEventStore(dataDir, source, StoreConfig{})
	require.NoError(t, err)
	pending, err := restarted.ListPendingEvents()
	require.NoError(t, err)
	require.Equal(t, []string{"event"}, pendingIDs(pending))
}

func TestPendingEventStoreIncludesPostStartupEvents(t *testing.T) {
	source := &memoryEventSource{}
	store, err := NewPendingEventStore(t.TempDir(), source, StoreConfig{})
	require.NoError(t, err)
	source.append(storage.Event{
		ID: "new", Content: "payload", Timestamp: "2026-07-19T00:00:00Z", ContextJSON: `{"custom":"yes"}`,
	})
	store.NotifyAppended("new")

	pending, err := store.ListPendingEvents()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "payload", pending[0].Text)
	require.Equal(t, map[string]string{"custom": "yes"}, pending[0].Context)
}

func TestPendingEventStoreGetEventStatusDistinguishesErrors(t *testing.T) {
	source := &memoryEventSource{events: []storage.Event{{ID: "known"}}}
	store, err := NewPendingEventStore(t.TempDir(), source, StoreConfig{BootstrapEpoch: "0001-01-01T00:00:00Z"})
	require.NoError(t, err)

	_, err = store.GetEventStatus("missing")
	require.ErrorIs(t, err, ErrEventNotFound)

	source.getErr = errors.New("disk failed")
	_, err = store.GetEventStatus("known")
	require.ErrorContains(t, err, "raw event source")
	require.NotErrorIs(t, err, ErrEventNotFound)
}

func TestPendingEventStoreCrashRecoveryIsConservative(t *testing.T) {
	source := &memoryEventSource{events: []storage.Event{{ID: "a"}, {ID: "b"}, {ID: "terminal"}}}
	dataDir := t.TempDir()
	store, err := NewPendingEventStore(dataDir, source, StoreConfig{BootstrapEpoch: "0001-01-01T00:00:00Z"})
	require.NoError(t, err)
	require.NoError(t, store.WriteStatus(StatusEntry{EventID: "terminal", Status: "integrated"}))
	wikiDir := filepath.Join(dataDir, "wiki")
	require.NoError(t, os.MkdirAll(filepath.Join(wikiDir, "semantic"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(wikiDir, "semantic", "page.md"), []byte("<!-- compiled_from: a, terminal -->\n# Page\n"), 0o644))

	require.NoError(t, store.RecoverFromCrash(wikiDir))
	require.Equal(t, EventStatus{Status: "unknown", Reason: UnknownReasonRecoveredFromCrash, TranscriptPath: TranscriptUnavailableMarker}, mustEventStatus(t, store, "a"))
	require.Equal(t, EventStatus{Status: "pending"}, mustEventStatus(t, store, "b"))
	require.Equal(t, EventStatus{Status: "integrated"}, mustEventStatus(t, store, "terminal"))
}

func TestStoreEventSourceUsesStorageStore(t *testing.T) {
	filesystem, err := storage.NewFS(t.TempDir())
	require.NoError(t, err)
	id, err := filesystem.AppendEvent(storage.Event{ID: "event", Content: "payload"})
	require.NoError(t, err)
	require.Equal(t, "event", id)
	source := NewStoreEventSource(filesystem)

	events, err := source.ListEvents()
	require.NoError(t, err)
	require.Len(t, events, 1)
	event, err := source.GetEvent("event")
	require.NoError(t, err)
	require.Equal(t, "payload", event.Content)
	_, err = source.GetEvent("missing")
	require.ErrorIs(t, err, ErrEventNotFound)
}

func pendingIDs(events []PendingEvent) []string {
	ids := make([]string, len(events))
	for index, event := range events {
		ids[index] = event.ID
	}
	return ids
}

func mustEventStatus(t *testing.T, store *PendingEventStore, eventID string) EventStatus {
	t.Helper()
	status, err := store.GetEventStatus(eventID)
	require.NoError(t, err)
	return status
}

func TestParseBootstrapEpochAcceptsNanoTimestamp(t *testing.T) {
	got, err := parseBootstrapEpoch("2026-07-19T12:00:00.123456789Z")
	require.NoError(t, err)
	require.Equal(t, 123456789, got.Nanosecond())
}

func TestPendingEventStoreBootstrapMethodRefusesExistingStatus(t *testing.T) {
	store, err := NewPendingEventStore(t.TempDir(), &memoryEventSource{}, StoreConfig{})
	require.NoError(t, err)
	require.ErrorContains(t, store.Bootstrap(time.Time{}), "already exists")
}

func TestPendingEventStoreUnknownByReason(t *testing.T) {
	epoch := "2026-07-19T12:00:00Z"
	source := &memoryEventSource{events: []storage.Event{
		{ID: "ev-a", Timestamp: "2026-07-19T12:00:01Z"},
		{ID: "ev-b", Timestamp: "2026-07-19T12:00:02Z"},
		{ID: "ev-c", Timestamp: "2026-07-19T12:00:03Z"},
		{ID: "ev-d", Timestamp: "2026-07-19T12:00:04Z"},
	}}
	store, err := NewPendingEventStore(t.TempDir(), source, StoreConfig{BootstrapEpoch: epoch})
	require.NoError(t, err)
	require.NoError(t, store.WriteStatuses([]StatusEntry{
		{EventID: "ev-a", Status: "unknown", Reason: UnknownReasonNoPerEventVerdict, TranscriptPath: "transcripts/b1.log"},
		{EventID: "ev-b", Status: "unknown", Reason: UnknownReasonNoPerEventVerdict, TranscriptPath: "transcripts/b1.log"},
		{EventID: "ev-c", Status: "unknown"}, // legacy empty reason
		{EventID: "ev-d", Status: "integrated"},
	}))

	counts := store.UnknownByReason()
	require.Equal(t, 2, counts[UnknownReasonNoPerEventVerdict])
	require.Equal(t, 1, counts[UnknownReasonUnclassified], "legacy empty reason must group under unclassified")
	require.Len(t, counts, 2, "integrated events must not appear")

	status := mustEventStatus(t, store, "ev-a")
	require.Equal(t, "transcripts/b1.log", status.TranscriptPath, "transcript path must round-trip through the store")
}

func TestWriteStatusesNormalizesUnknownAtStoreBoundary(t *testing.T) {
	epoch := "2026-07-19T12:00:00Z"
	source := &memoryEventSource{events: []storage.Event{
		{ID: "ev-bogus", Timestamp: "2026-07-19T12:00:01Z"},
		{ID: "ev-empty", Timestamp: "2026-07-19T12:00:02Z"},
		{ID: "ev-valid", Timestamp: "2026-07-19T12:00:03Z"},
	}}
	store, err := NewPendingEventStore(t.TempDir(), source, StoreConfig{BootstrapEpoch: epoch})
	require.NoError(t, err)
	require.NoError(t, store.WriteStatuses([]StatusEntry{
		{EventID: "ev-bogus", Status: "unknown", Reason: "bogus-freeform", TranscriptPath: ""},
		{EventID: "ev-empty", Status: "unknown"},
		{EventID: "ev-valid", Status: "unknown", Reason: UnknownReasonMalformedVerdict, TranscriptPath: "transcripts/b.log"},
	}))

	bogus := mustEventStatus(t, store, "ev-bogus")
	require.Equal(t, UnknownReasonUnclassified, bogus.Reason, "foreign reason must normalize to unclassified")
	require.Equal(t, TranscriptUnavailableMarker, bogus.TranscriptPath, "empty transcript must normalize to the marker")
	empty := mustEventStatus(t, store, "ev-empty")
	require.Equal(t, UnknownReasonUnclassified, empty.Reason)
	require.Equal(t, TranscriptUnavailableMarker, empty.TranscriptPath)
	valid := mustEventStatus(t, store, "ev-valid")
	require.Equal(t, UnknownReasonMalformedVerdict, valid.Reason, "enum reasons pass through untouched")
	require.Equal(t, "transcripts/b.log", valid.TranscriptPath)

	for reason := range store.UnknownByReason() {
		require.True(t, IsValidUnknownReason(reason), "UnknownByReason keys must be enum members, got %q", reason)
	}
}

func TestRecoverFromCrashRowsUseClosedEnum(t *testing.T) {
	epoch := "2026-07-19T12:00:00Z"
	source := &memoryEventSource{events: []storage.Event{
		{ID: "evt-crash", Timestamp: "2026-07-19T12:00:01Z"},
	}}
	dataDir := t.TempDir()
	store, err := NewPendingEventStore(dataDir, source, StoreConfig{BootstrapEpoch: epoch})
	require.NoError(t, err)

	wikiDir := filepath.Join(dataDir, "wiki")
	require.NoError(t, os.MkdirAll(wikiDir, 0o755))
	page := "<!--\ncompiled_from: evt-crash\n-->\nbody\n"
	require.NoError(t, os.WriteFile(filepath.Join(wikiDir, "page.md"), []byte(page), 0o644))

	require.NoError(t, store.RecoverFromCrash(wikiDir))

	status := mustEventStatus(t, store, "evt-crash")
	require.Equal(t, "unknown", status.Status)
	require.Equal(t, UnknownReasonRecoveredFromCrash, status.Reason)
	require.Equal(t, TranscriptUnavailableMarker, status.TranscriptPath, "no fabricated path across a crash")
	require.Equal(t, 1, store.UnknownByReason()[UnknownReasonRecoveredFromCrash])
}
