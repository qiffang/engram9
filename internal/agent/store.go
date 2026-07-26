package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/qiffang/engram9/internal/storage"
)

const integrationStatusFilename = "wiki_integration_status.jsonl"

var ErrEventNotFound = errors.New("event not found")

type RawEventSource interface {
	ListEvents() ([]storage.Event, error)
	GetEvent(id string) (storage.Event, error)
}

type StoreEventSource struct {
	store storage.Store
}

func NewStoreEventSource(store storage.Store) *StoreEventSource {
	return &StoreEventSource{store: store}
}

func (source *StoreEventSource) ListEvents() ([]storage.Event, error) {
	page, err := source.store.ReadEventsSince(0)
	if err != nil {
		return nil, err
	}
	return page.Events, nil
}

func (source *StoreEventSource) GetEvent(id string) (storage.Event, error) {
	page, err := source.store.ReadEventsSince(0)
	if err != nil {
		return storage.Event{}, err
	}
	for _, event := range page.Events {
		if event.ID == id {
			return event, nil
		}
	}
	return storage.Event{}, ErrEventNotFound
}

type StoreConfig struct {
	BootstrapEpoch string
}

type StatusEntry struct {
	EventID        string    `json:"event_id"`
	Status         string    `json:"status"`
	BatchID        string    `json:"batch_id,omitempty"`
	Reason         string    `json:"reason,omitempty"`
	Pages          []string  `json:"pages,omitempty"`
	TranscriptPath string    `json:"transcript_path,omitempty"`
	Timestamp      time.Time `json:"timestamp"`
	Recovered      bool      `json:"recovered,omitempty"`
}

type EventStatus struct {
	Status         string
	BatchID        string
	Reason         string
	Pages          []string
	TranscriptPath string
}

type StatusCounts struct {
	Total      int `json:"total_events"`
	Pending    int `json:"pending"`
	Integrated int `json:"integrated"`
	Skipped    int `json:"skipped"`
	Failed     int `json:"failed"`
	Unknown    int `json:"unknown"`
}

type statusRecord struct {
	EventID        string    `json:"event_id,omitempty"`
	Status         string    `json:"status,omitempty"`
	Type           string    `json:"type,omitempty"`
	BatchID        string    `json:"batch_id,omitempty"`
	Reason         string    `json:"reason,omitempty"`
	Pages          []string  `json:"pages,omitempty"`
	TranscriptPath string    `json:"transcript_path,omitempty"`
	Timestamp      time.Time `json:"timestamp,omitempty"`
	Recovered      bool      `json:"recovered,omitempty"`
	OldStatus      string    `json:"old_status,omitempty"`
	Operator       string    `json:"operator,omitempty"`
	Epoch          string    `json:"epoch,omitempty"`
}

type PendingEventStore struct {
	mu          sync.Mutex
	statusPath  string
	eventSource RawEventSource
	knownIDs    map[string]struct{}
	pendingIDs  map[string]struct{}
	terminal    map[string]EventStatus
}

func NewPendingEventStore(dataDir string, eventSource RawEventSource, cfg StoreConfig) (*PendingEventStore, error) {
	if eventSource == nil {
		return nil, fmt.Errorf("raw event source is required")
	}
	events, err := eventSource.ListEvents()
	if err != nil {
		return nil, fmt.Errorf("list raw events: %w", err)
	}
	store := &PendingEventStore{
		statusPath:  filepath.Join(dataDir, integrationStatusFilename),
		eventSource: eventSource,
		knownIDs:    make(map[string]struct{}, len(events)),
		pendingIDs:  make(map[string]struct{}, len(events)),
		terminal:    make(map[string]EventStatus),
	}
	for _, event := range events {
		store.knownIDs[event.ID] = struct{}{}
		store.pendingIDs[event.ID] = struct{}{}
	}

	info, statErr := os.Stat(store.statusPath)
	switch {
	case os.IsNotExist(statErr):
		if cfg.BootstrapEpoch == "" {
			if len(events) > 0 {
				return nil, missingBootstrapEpochError(len(events))
			}
			if err := store.bootstrapWithEvents(time.Now().UTC(), "empty_event_log", events); err != nil {
				return nil, err
			}
			return store, nil
		}
		epoch, err := parseBootstrapEpoch(cfg.BootstrapEpoch)
		if err != nil {
			return nil, err
		}
		if err := store.bootstrapWithEvents(epoch, "explicit_operator_epoch", events); err != nil {
			return nil, err
		}
		return store, nil
	case statErr != nil:
		return nil, fmt.Errorf("stat integration status: %w", statErr)
	case info.Size() == 0:
		if len(events) > 0 {
			return nil, fmt.Errorf("batch_ingest: status file is empty but raw event log contains %d events; delete %s and set BATCH_INGEST_EPOCH to re-bootstrap", len(events), store.statusPath)
		}
		return store, nil
	}

	if err := store.loadStatusLog(); err != nil {
		return nil, err
	}
	store.reconcilePending(events)
	return store, nil
}

func missingBootstrapEpochError(eventCount int) error {
	return fmt.Errorf("batch_ingest: status file does not exist but raw event log contains %d events; set BATCH_INGEST_EPOCH to specify which events should be processed (use 0001-01-01T00:00:00Z to replay all history)", eventCount)
}

func parseBootstrapEpoch(value string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339, time.RFC3339Nano} {
		if epoch, err := time.Parse(layout, value); err == nil {
			return epoch, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid BATCH_INGEST_EPOCH %q: expected ISO 8601/RFC3339 timestamp", value)
}

func (store *PendingEventStore) Bootstrap(epoch time.Time) error {
	events, err := store.eventSource.ListEvents()
	if err != nil {
		return fmt.Errorf("list raw events for bootstrap: %w", err)
	}
	return store.bootstrapWithEvents(epoch, "explicit_operator_epoch", events)
}

func (store *PendingEventStore) bootstrapWithEvents(epoch time.Time, reason string, events []storage.Event) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, err := os.Stat(store.statusPath); err == nil {
		return fmt.Errorf("integration status file already exists")
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat integration status: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(store.statusPath), 0o755); err != nil {
		return fmt.Errorf("create integration status dir: %w", err)
	}
	tempPath := store.statusPath + ".bootstrap"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create bootstrap status: %w", err)
	}
	removeTemp := true
	defer func() {
		_ = file.Close()
		if removeTemp {
			_ = os.Remove(tempPath)
		}
	}()

	header := statusRecord{Type: "bootstrap", Epoch: epoch.Format(time.RFC3339Nano), Reason: reason}
	if err := writeJSONLine(file, header); err != nil {
		return fmt.Errorf("write bootstrap header: %w", err)
	}
	terminal := make(map[string]EventStatus)
	for _, event := range events {
		createdAt := NormalizeToPendingEvent(event).CreatedAt
		if !createdAt.Before(epoch) {
			continue
		}
		entry := StatusEntry{
			EventID: event.ID, Status: "integrated", Reason: "pre_epoch_bootstrap", Timestamp: time.Now().UTC(),
		}
		if err := writeJSONLine(file, entry); err != nil {
			return fmt.Errorf("write bootstrap event %s: %w", event.ID, err)
		}
		terminal[event.ID] = EventStatus{Status: "integrated", Reason: "pre_epoch_bootstrap"}
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync bootstrap status: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close bootstrap status: %w", err)
	}
	if err := os.Rename(tempPath, store.statusPath); err != nil {
		return fmt.Errorf("install bootstrap status: %w", err)
	}
	removeTemp = false

	store.knownIDs = make(map[string]struct{}, len(events))
	store.pendingIDs = make(map[string]struct{}, len(events))
	store.terminal = terminal
	for _, event := range events {
		store.knownIDs[event.ID] = struct{}{}
		if _, ok := terminal[event.ID]; !ok {
			store.pendingIDs[event.ID] = struct{}{}
		}
	}
	return nil
}

func (store *PendingEventStore) loadStatusLog() error {
	data, err := os.ReadFile(store.statusPath)
	if err != nil {
		return fmt.Errorf("read integration status: %w", err)
	}
	lines := bytes.Split(data, []byte{'\n'})
	hasTrailingNewline := len(data) > 0 && data[len(data)-1] == '\n'
	totalLines := 0
	skippedLines := 0
	validLines := 0
	terminal := make(map[string]EventStatus)
	for index, line := range lines {
		if index == len(lines)-1 && len(line) == 0 && hasTrailingNewline {
			continue
		}
		totalLines++
		if index == len(lines)-1 && !hasTrailingNewline {
			skippedLines++
			continue
		}
		var record statusRecord
		if err := json.Unmarshal(line, &record); err != nil || !validStatusRecord(record) {
			skippedLines++
			log.Printf("[batch-ingest] skipping invalid status line %d", index+1)
			continue
		}
		validLines++
		applyStatusRecord(terminal, record)
	}
	if totalLines > 0 && validLines == 0 {
		return fmt.Errorf("batch_ingest: integration status file is entirely unparseable")
	}
	if skippedLines > 10 && float64(skippedLines) > float64(totalLines)*0.1 {
		return fmt.Errorf("batch_ingest: integration status corruption threshold exceeded: skipped %d of %d lines", skippedLines, totalLines)
	}
	store.terminal = terminal
	return nil
}

func validStatusRecord(record statusRecord) bool {
	switch {
	case record.Type == "bootstrap":
		return record.Epoch != ""
	case record.Type == "reset":
		return record.EventID != ""
	case record.Type != "":
		return false
	default:
		return record.EventID != "" && validTerminalStatus(record.Status)
	}
}

func applyStatusRecord(terminal map[string]EventStatus, record statusRecord) {
	if record.Type == "reset" {
		delete(terminal, record.EventID)
		return
	}
	if record.Type == "bootstrap" {
		return
	}
	terminal[record.EventID] = EventStatus{
		Status: record.Status, BatchID: record.BatchID, Reason: record.Reason,
		Pages: append([]string(nil), record.Pages...), TranscriptPath: record.TranscriptPath,
	}
}

func validTerminalStatus(status string) bool {
	switch status {
	case "integrated", "skipped", "failed", "unknown":
		return true
	default:
		return false
	}
}

func (store *PendingEventStore) reconcilePending(events []storage.Event) {
	previousPending := store.pendingIDs
	store.knownIDs = make(map[string]struct{}, len(events))
	store.pendingIDs = make(map[string]struct{}, len(events))
	for _, event := range events {
		store.knownIDs[event.ID] = struct{}{}
		if _, terminal := store.terminal[event.ID]; !terminal {
			store.pendingIDs[event.ID] = struct{}{}
		}
	}
	for eventID := range previousPending {
		if _, presentInSnapshot := store.knownIDs[eventID]; presentInSnapshot {
			continue
		}
		if _, terminal := store.terminal[eventID]; terminal {
			continue
		}
		store.knownIDs[eventID] = struct{}{}
		store.pendingIDs[eventID] = struct{}{}
	}
}

func (store *PendingEventStore) ListPendingEvents() ([]PendingEvent, error) {
	events, err := store.eventSource.ListEvents()
	if err != nil {
		return nil, fmt.Errorf("list raw events: %w", err)
	}

	store.mu.Lock()
	store.reconcilePending(events)
	pending := make(map[string]struct{}, len(store.pendingIDs))
	for id := range store.pendingIDs {
		pending[id] = struct{}{}
	}
	store.mu.Unlock()

	result := make([]PendingEvent, 0, len(pending))
	for _, event := range events {
		if _, ok := pending[event.ID]; ok {
			result = append(result, NormalizeToPendingEvent(event))
		}
	}
	sort.SliceStable(result, func(left, right int) bool {
		if result[left].CreatedAt.Equal(result[right].CreatedAt) {
			return result[left].ID < result[right].ID
		}
		return result[left].CreatedAt.Before(result[right].CreatedAt)
	})
	return result, nil
}

func (store *PendingEventStore) PendingCount() (int, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return len(store.pendingIDs), nil
}

func (store *PendingEventStore) HasPending() (bool, error) {
	count, err := store.PendingCount()
	return count > 0, err
}

func (store *PendingEventStore) NotifyAppended(eventID string) {
	if eventID == "" {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.knownIDs[eventID] = struct{}{}
	delete(store.terminal, eventID)
	store.pendingIDs[eventID] = struct{}{}
}

func (store *PendingEventStore) WriteStatus(entry StatusEntry) error {
	return store.WriteStatuses([]StatusEntry{entry})
}

func (store *PendingEventStore) WriteStatuses(entries []StatusEntry) error {
	if len(entries) == 0 {
		return nil
	}
	normalized := make([]StatusEntry, len(entries))
	for index, entry := range entries {
		if entry.EventID == "" {
			return fmt.Errorf("status event_id is required")
		}
		if !validTerminalStatus(entry.Status) {
			return fmt.Errorf("invalid terminal status %q", entry.Status)
		}
		if entry.Timestamp.IsZero() {
			entry.Timestamp = time.Now().UTC()
		}
		normalized[index] = entry
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	records := make([]any, len(normalized))
	for index := range normalized {
		records[index] = normalized[index]
	}
	if err := store.appendRecordsLocked(records); err != nil {
		return err
	}
	for _, entry := range normalized {
		store.knownIDs[entry.EventID] = struct{}{}
		store.terminal[entry.EventID] = EventStatus{
			Status: entry.Status, BatchID: entry.BatchID, Reason: entry.Reason,
			Pages: append([]string(nil), entry.Pages...), TranscriptPath: entry.TranscriptPath,
		}
		delete(store.pendingIDs, entry.EventID)
	}
	return nil
}

func (store *PendingEventStore) WriteTombstone(eventID string) error {
	if eventID == "" {
		return fmt.Errorf("reset event_id is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	oldStatus := "pending"
	if status, ok := store.terminal[eventID]; ok {
		oldStatus = status.Status
	}
	record := statusRecord{
		EventID: eventID, Type: "reset", OldStatus: oldStatus, Timestamp: time.Now().UTC(), Operator: "admin",
	}
	if err := store.appendRecordsLocked([]any{record}); err != nil {
		return err
	}
	store.knownIDs[eventID] = struct{}{}
	delete(store.terminal, eventID)
	store.pendingIDs[eventID] = struct{}{}
	return nil
}

func (store *PendingEventStore) appendRecordsLocked(records []any) error {
	if err := os.MkdirAll(filepath.Dir(store.statusPath), 0o755); err != nil {
		return fmt.Errorf("create integration status dir: %w", err)
	}
	file, err := os.OpenFile(store.statusPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open integration status: %w", err)
	}
	defer file.Close()
	for _, record := range records {
		if err := writeJSONLine(file, record); err != nil {
			return fmt.Errorf("append integration status: %w", err)
		}
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync integration status: %w", err)
	}
	return nil
}

func writeJSONLine(file *os.File, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = file.Write(append(data, '\n'))
	return err
}

func (store *PendingEventStore) GetEventStatus(eventID string) (EventStatus, error) {
	if _, err := store.eventSource.GetEvent(eventID); err != nil {
		if errors.Is(err, ErrEventNotFound) {
			return EventStatus{}, ErrEventNotFound
		}
		return EventStatus{}, fmt.Errorf("raw event source: %w", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if status, ok := store.terminal[eventID]; ok {
		status.Pages = append([]string(nil), status.Pages...)
		return status, nil
	}
	return EventStatus{Status: "pending"}, nil
}

var compiledFromPattern = regexp.MustCompile(`compiled_from:\s*([^\n]*?)\s*-->`)

func (store *PendingEventStore) RecoverFromCrash(wikiDir string) error {
	store.mu.Lock()
	pending := make(map[string]struct{}, len(store.pendingIDs))
	for id := range store.pendingIDs {
		pending[id] = struct{}{}
	}
	store.mu.Unlock()
	if len(pending) == 0 {
		return nil
	}

	recovered := make(map[string]struct{})
	err := filepath.Walk(wikiDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() || filepath.Ext(path) != ".md" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, match := range compiledFromPattern.FindAllStringSubmatch(string(data), -1) {
			for _, eventID := range strings.Split(match[1], ",") {
				eventID = strings.TrimSpace(eventID)
				if _, ok := pending[eventID]; ok {
					recovered[eventID] = struct{}{}
				}
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("scan wiki recovery frontmatter: %w", err)
	}
	if len(recovered) == 0 {
		return nil
	}
	ids := make([]string, 0, len(recovered))
	for id := range recovered {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	entries := make([]StatusEntry, 0, len(ids))
	for _, id := range ids {
		entries = append(entries, StatusEntry{
			EventID: id, Status: "unknown", Reason: "recovered_from_frontmatter", Recovered: true,
		})
	}
	if err := store.WriteStatuses(entries); err != nil {
		return fmt.Errorf("write recovered statuses: %w", err)
	}
	log.Printf("[batch-ingest] recovered %d pending events from wiki frontmatter", len(entries))
	return nil
}

// UnknownByReason returns the count of terminal "unknown" events grouped by
// their closed-enum reason. Empty reasons (legacy records) group under
// "unclassified".
func (store *PendingEventStore) UnknownByReason() map[string]int {
	store.mu.Lock()
	defer store.mu.Unlock()
	counts := make(map[string]int)
	for id, status := range store.terminal {
		if _, exists := store.knownIDs[id]; !exists {
			continue
		}
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

func (store *PendingEventStore) Counts() StatusCounts {
	store.mu.Lock()
	defer store.mu.Unlock()
	counts := StatusCounts{Total: len(store.knownIDs), Pending: len(store.pendingIDs)}
	for id, status := range store.terminal {
		if _, exists := store.knownIDs[id]; !exists {
			continue
		}
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
