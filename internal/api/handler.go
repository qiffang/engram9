package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qiffang/engram9/internal/agent"
	"github.com/qiffang/engram9/internal/storage"
)

// Handler implements the engram9 HTTP API.
type Handler struct {
	store          storage.Store
	wikiBackend    agent.AgentBackend // ingest
	compileBackend agent.AgentBackend // compile (always LLM in Phase 1)
	queryBackend   agent.AgentBackend
	coordinator    *agent.BatchCoordinator

	// wikiMu serializes all wiki-mutating operations: compile turns, ACP
	// ingest turns, and query/recall turns (QueryTools includes write_wiki_page).
	// ACP ingest snapshots the entire data dir and replaces wiki/ atomically —
	// concurrent compile, query, or ACP ingest writes would be lost.
	// LLM ingest uses per-file writes via ToolExecutor and does not need this lock.
	wikiMu sync.Mutex

	// pendingIntegrations tracks the number of async wiki integrations in flight.
	pendingIntegrations atomic.Int64

	// ingestErrors tracks the number of failed async wiki integrations.
	ingestErrors atomic.Int64

	// wg tracks background goroutines for graceful shutdown / testing.
	wg sync.WaitGroup

	// ingestTimeout bounds async /remember wiki integration.
	ingestTimeout time.Duration

	// integrationSlots bounds concurrent async wiki integrations.
	integrationSlots          chan struct{}
	maxConcurrentIntegrations int

	maxToolLoops                 int
	maxRepeatedReadOnlyToolCalls int
	maxInvalidToolCalls          int

	llmRetryAttempts int
	llmRetryBackoff  time.Duration
	llmCallTimeout   time.Duration
	llmProvider      string
	llmModel         string
	llmBaseURL       string

	// Per-capability backend identifiers for /status.
	wikiBackendName    string // "llm" or "acp"
	compileBackendName string // "llm" or "acp"
	queryBackendName   string // "llm" or "acp"
	acpProvider        string // e.g. "claude" — set when any capability is "acp"
	adminToken         string
}

const (
	defaultIngestTimeout             = 5 * time.Minute
	defaultMaxConcurrentIntegrations = 4
	ingestTimeoutEnv                 = "ENGRAM9_INGEST_TIMEOUT"
	maxConcurrentIntegrationsEnv     = "ENGRAM9_MAX_CONCURRENT_INTEGRATIONS"
)

type Options struct {
	MaxToolLoops                 int
	MaxRepeatedReadOnlyToolCalls int
	MaxInvalidToolCalls          int
	IngestTimeout                time.Duration
	MaxConcurrentIntegrations    int
	LLMRetryAttempts             int
	LLMRetryBackoff              time.Duration
	LLMCallTimeout               time.Duration
	LLMProvider                  string
	LLMModel                     string
	LLMBaseURL                   string

	// WikiBackend selects the backend for ingest: "llm" (default) or "acp".
	WikiBackend string
	// CompileBackend selects the backend for compile: "llm" (default) or "acp".
	CompileBackend string
	// QueryBackend selects the backend for query: "llm" (default) or "acp".
	QueryBackend string
	// ACPConfig is required when any of WikiBackend/CompileBackend/QueryBackend == "acp".
	ACPConfig *agent.ACPBackendConfig
	// CoordinatorConfig controls ACP batch scheduling. Zero fields use defaults.
	CoordinatorConfig agent.CoordinatorConfig
}

// New creates a new API handler with all agents wired up.
func New(dataDir string, llm agent.LLM) (*Handler, error) {
	return NewWithOptions(dataDir, llm, Options{})
}

func NewWithOptions(dataDir string, llm agent.LLM, opts Options) (*Handler, error) {
	store, err := storage.NewFS(dataDir)
	if err != nil {
		return nil, err
	}

	maxToolLoops := opts.MaxToolLoops
	if maxToolLoops <= 0 {
		maxToolLoops = agent.DefaultMaxToolLoops
	}
	maxRepeatedReadOnlyToolCalls := opts.MaxRepeatedReadOnlyToolCalls
	if maxRepeatedReadOnlyToolCalls <= 0 {
		maxRepeatedReadOnlyToolCalls = agent.DefaultMaxRepeatedReadOnlyToolCalls
	}
	maxInvalidToolCalls := opts.MaxInvalidToolCalls
	if maxInvalidToolCalls <= 0 {
		maxInvalidToolCalls = agent.DefaultMaxInvalidToolCalls
	}
	runnerOpts := agent.RunnerOptions{
		MaxToolLoops:                 maxToolLoops,
		MaxRepeatedReadOnlyToolCalls: maxRepeatedReadOnlyToolCalls,
		MaxInvalidToolCalls:          maxInvalidToolCalls,
	}
	ingestTimeout := opts.IngestTimeout
	if ingestTimeout <= 0 {
		ingestTimeout = ingestTimeoutFromEnv()
	}
	maxConcurrentIntegrations := opts.MaxConcurrentIntegrations
	if maxConcurrentIntegrations <= 0 {
		maxConcurrentIntegrations = maxConcurrentIntegrationsFromEnv()
	}

	// Resolve wiki backend (ingest only in Phase 1).
	// Default to ACP (Claude Code / Codex) for every capability — the wiki flow
	// runs through an agent by default, never the apikey LLM path. LLM is used
	// only when a capability is EXPLICITLY "llm"; never a silent fallback.
	wikiBackendName := opts.WikiBackend
	if wikiBackendName == "" {
		wikiBackendName = "acp"
	}
	compileBackendName := opts.CompileBackend
	if compileBackendName == "" {
		compileBackendName = "acp"
	}
	queryBackendName := opts.QueryBackend
	if queryBackendName == "" {
		queryBackendName = "acp"
	}

	// A single ACP backend serves whichever capabilities are configured as acp;
	// it is constructed once here and shared. `acp` for ANY capability requires
	// ACP config, else fail fast at init (no silent LLM fallback — silent apikey
	// use is exactly what motivated Phase 2). I5: the LLM client is only
	// constructed by the caller when some capability is llm; a capability on acp
	// never touches an LLM client.
	var acpBackend *agent.ACPBackend
	var acpProvider string
	anyACP := wikiBackendName == "acp" || compileBackendName == "acp" || queryBackendName == "acp"
	if anyACP {
		if opts.ACPConfig == nil {
			return nil, fmt.Errorf("a capability is set to acp but ACP configuration is missing (ACP_PROVIDER, ACPMUX_COMMAND)")
		}
		acpProvider = opts.ACPConfig.Provider
		b, err := agent.NewACPBackend(dataDir, *opts.ACPConfig)
		if err != nil {
			return nil, fmt.Errorf("init ACP backend: %w", err)
		}
		acpBackend = b
	}

	// resolveBackend returns the LLM or ACP backend for a capability, failing
	// fast on an unknown name.
	llmExecutor := agent.NewToolExecutor(store)
	newLLM := func() agent.AgentBackend { return agent.NewLLMBackend(llm, agent.NewToolExecutor(store), runnerOpts) }
	resolveBackend := func(capability, name string) (agent.AgentBackend, error) {
		switch name {
		case "llm":
			return newLLM(), nil
		case "acp":
			return acpBackend, nil
		default:
			return nil, fmt.Errorf("unknown %s backend %q (use 'llm' or 'acp')", capability, name)
		}
	}

	wikiBackend, err := resolveBackend("WIKI_BACKEND", wikiBackendName)
	if err != nil {
		return nil, err
	}
	// The compile capability keeps the shared llmExecutor for the LLM path so
	// existing compile behavior is byte-identical when compile_backend=llm.
	var compileBackend agent.AgentBackend
	switch compileBackendName {
	case "llm":
		compileBackend = agent.NewLLMBackend(llm, llmExecutor, runnerOpts)
	case "acp":
		compileBackend = acpBackend
	default:
		return nil, fmt.Errorf("unknown COMPILE_BACKEND %q (use 'llm' or 'acp')", compileBackendName)
	}
	queryBackend, err := resolveBackend("QUERY_BACKEND", queryBackendName)
	if err != nil {
		return nil, err
	}

	handler := &Handler{
		store:                        store,
		wikiBackend:                  wikiBackend,
		compileBackend:               compileBackend,
		queryBackend:                 queryBackend,
		ingestTimeout:                ingestTimeout,
		integrationSlots:             make(chan struct{}, maxConcurrentIntegrations),
		maxConcurrentIntegrations:    maxConcurrentIntegrations,
		maxToolLoops:                 maxToolLoops,
		maxRepeatedReadOnlyToolCalls: maxRepeatedReadOnlyToolCalls,
		maxInvalidToolCalls:          maxInvalidToolCalls,
		llmRetryAttempts:             opts.LLMRetryAttempts,
		llmRetryBackoff:              opts.LLMRetryBackoff,
		llmCallTimeout:               opts.LLMCallTimeout,
		llmProvider:                  opts.LLMProvider,
		llmModel:                     opts.LLMModel,
		llmBaseURL:                   opts.LLMBaseURL,
		wikiBackendName:              wikiBackendName,
		compileBackendName:           compileBackendName,
		queryBackendName:             queryBackendName,
		acpProvider:                  acpProvider,
		adminToken:                   os.Getenv("ENGRAM9_ADMIN_TOKEN"),
	}
	if wikiBackendName == "acp" {
		// Batch ingest is ingest-specific; use the shared ACP backend directly.
		pendingStore, storeErr := agent.NewPendingEventStore(dataDir, agent.NewStoreEventSource(store), agent.StoreConfig{
			BootstrapEpoch: os.Getenv("BATCH_INGEST_EPOCH"),
		})
		if storeErr != nil {
			log.Printf("[batch-ingest] disabled: %v", storeErr)
		} else {
			indexRebuilder := agent.WikiIndexRebuilderFunc(func() error { return handler.store.RebuildIndex() })
			handler.coordinator = agent.NewBatchCoordinator(
				acpBackend, pendingStore, indexRebuilder, &handler.wikiMu, filepath.Join(dataDir, "wiki"), opts.CoordinatorConfig,
			)
		}
	}
	return handler, nil
}

// Routes returns an http.Handler with all API routes.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /remember", h.handleRemember)
	mux.HandleFunc("POST /recall", h.handleRecall)
	mux.HandleFunc("POST /compile", h.handleCompile)
	mux.HandleFunc("GET /status", h.handleStatus)
	mux.HandleFunc("POST /admin/events/{id}/reset", h.handleResetEvent)
	mux.HandleFunc("POST /admin/events/{id}/confirm", h.handleConfirmEvent)
	return mux
}

// RememberRequest is the request body for POST /remember.
type RememberRequest struct {
	Text         string            `json:"text"`
	Context      map[string]string `json:"context,omitempty"`
	SourceType   string            `json:"source_type,omitempty"`
	EvidenceKind string            `json:"evidence_kind,omitempty"`
	TrustTier    *int              `json:"trust_tier,omitempty"`
}

// RecallRequest is the request body for POST /recall.
type RecallRequest struct {
	Question string            `json:"question"`
	Context  map[string]string `json:"context,omitempty"`
}

// APIResponse is a generic response envelope.
type APIResponse struct {
	Result string `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

// RememberResponse is the response for POST /remember.
type RememberResponse struct {
	EventID string `json:"event_id"`
}

func (h *Handler) handleRemember(w http.ResponseWriter, r *http.Request) {
	var req RememberRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Error: "invalid request body"})
		return
	}
	if req.Text == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Error: "text is required"})
		return
	}

	log.Printf("[api] remember: %s", truncate(req.Text, 80))

	// Validate optional metadata against the canonical enum contract.
	sourceType := "user"
	if req.SourceType != "" {
		if !validSourceType(req.SourceType) {
			writeJSON(w, http.StatusBadRequest, APIResponse{Error: "invalid source_type"})
			return
		}
		sourceType = req.SourceType
	}
	evidenceKind := "user_statement"
	if req.EvidenceKind != "" {
		if !validEvidenceKind(req.EvidenceKind) {
			writeJSON(w, http.StatusBadRequest, APIResponse{Error: "invalid evidence_kind"})
			return
		}
		evidenceKind = req.EvidenceKind
	}
	trustTier := 1
	if req.TrustTier != nil {
		if *req.TrustTier < 1 || *req.TrustTier > 3 {
			writeJSON(w, http.StatusBadRequest, APIResponse{Error: "invalid trust_tier: must be 1, 2, or 3"})
			return
		}
		trustTier = *req.TrustTier
	}
	ev := storage.Event{
		Content:       req.Text,
		Actor:         req.Context["actor"],
		Source:        req.Context["source"],
		SessionID:     req.Context["session_id"],
		ActiveProject: req.Context["active_project"],
		ActiveTask:    req.Context["active_task"],
		Durability:    "long-term",
		Actionability: "informational",
		SourceType:    sourceType,
		EvidenceKind:  evidenceKind,
		TrustTier:     trustTier,
	}
	if len(req.Context) > 0 {
		contextJSON, err := json.Marshal(req.Context)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, APIResponse{Error: "invalid context"})
			return
		}
		ev.ContextJSON = string(contextJSON)
	}

	if h.wikiBackendName == "acp" && h.coordinator == nil {
		writeJSON(w, http.StatusServiceUnavailable, APIResponse{Error: "batch ingest not configured (set BATCH_INGEST_EPOCH)"})
		return
	}

	eventID, err := h.store.AppendEvent(ev)
	if err != nil {
		log.Printf("[api] remember append error: %v", err)
		writeJSON(w, http.StatusInternalServerError, APIResponse{Error: err.Error()})
		return
	}
	if h.wikiBackendName == "acp" {
		h.coordinator.NotifyNewEvent(eventID)
		writeJSON(w, http.StatusOK, RememberResponse{EventID: eventID})
		return
	}

	// Asynchronous: wiki integration in background.
	h.pendingIntegrations.Add(1)
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		defer h.pendingIntegrations.Add(-1)

		releaseSlot := h.acquireIntegrationSlot()
		defer releaseSlot()

		ctx, cancel := context.WithTimeout(context.Background(), h.effectiveIngestTimeout())
		defer cancel()

		// ACP ingest snapshots the entire data dir and replaces wiki/ atomically.
		// Hold wikiMu to prevent concurrent compile or other ACP turns from racing.
		// LLM ingest uses per-file writes and doesn't need this lock.
		if h.wikiBackendName == "acp" {
			h.wikiMu.Lock()
			defer h.wikiMu.Unlock()
		}

		if _, err := h.wikiBackend.RunIngest(ctx, eventID, req.Text, req.Context); err != nil {
			log.Printf("[api] integrate error (event %s): %v", eventID, err)
			h.ingestErrors.Add(1)
		} else {
			log.Printf("[api] integrate done: %s", eventID)
			if err := h.store.RebuildIndex(); err != nil {
				log.Printf("[api] rebuild index error: %v", err)
				h.ingestErrors.Add(1)
			}
		}
	}()

	writeJSON(w, http.StatusOK, RememberResponse{EventID: eventID})
}

func (h *Handler) handleRecall(w http.ResponseWriter, r *http.Request) {
	var req RecallRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, APIResponse{Error: "invalid request body"})
		return
	}
	if req.Question == "" {
		writeJSON(w, http.StatusBadRequest, APIResponse{Error: "question is required"})
		return
	}

	log.Printf("[api] recall: %s", truncate(req.Question, 80))

	// QueryTools includes write_wiki_page — if ACP ingest is active, a concurrent
	// query write would be lost when ACP replaces wiki/ atomically. Hold wikiMu.
	if h.wikiBackendName == "acp" {
		h.wikiMu.Lock()
		defer h.wikiMu.Unlock()
	}

	// Inject recent events so the LLM can answer even if wiki integration is still pending.
	var recentEvents []storage.Event
	if h.hasPendingIntegrations() {
		recentEvents, _ = h.store.ReadRecentEvents(10)
	}

	result, err := h.queryBackend.RunQuery(r.Context(), req.Question, req.Context, recentEvents)
	if err != nil {
		log.Printf("[api] recall error: %v", err)
		writeJSON(w, http.StatusInternalServerError, APIResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, APIResponse{Result: result.Answer})
}

func (h *Handler) handleCompile(w http.ResponseWriter, r *http.Request) {
	// Only one compile at a time.
	h.wikiMu.Lock()
	defer h.wikiMu.Unlock()

	// Read cursor from persistent storage — single source of truth.
	cursor := h.store.GetCompileCursor()

	log.Printf("[api] compile: cursor=%d", cursor)
	result, err := h.compileBackend.RunCompile(r.Context(), cursor)
	if err != nil {
		log.Printf("[api] compile error: %v", err)
		writeJSON(w, http.StatusInternalServerError, APIResponse{Error: err.Error()})
		return
	}

	// Persist new cursor only if progress was made.
	newCursor := result.NewCursor
	if newCursor > cursor {
		if err := h.store.SetCompileCursor(newCursor); err != nil {
			log.Printf("[api] persist cursor error: %v", err)
		}
	}

	log.Printf("[api] compile done: cursor %d -> %d", cursor, newCursor)
	writeJSON(w, http.StatusOK, APIResponse{Result: result.Summary})
}

// StatusResponse extends MemoryStats with runtime info.
type StatusResponse struct {
	storage.MemoryStats
	// GeneratedAt timestamps this snapshot. Consumers presenting a snapshot
	// as failure evidence MUST include it and MUST NOT present a snapshot
	// older than one refresh interval as current state (task #66: a 2-day-old
	// snapshot was mistaken for live state and misled diagnosis).
	GeneratedAt                  time.Time `json:"generated_at"`
	PendingIntegrations          int64     `json:"pending_integrations"`
	IngestErrorCount             int64     `json:"ingest_error_count"`
	IngestTimeout                string    `json:"ingest_timeout"`
	MaxConcurrentIntegrations    int       `json:"max_concurrent_integrations"`
	MaxToolLoops                 int       `json:"max_tool_loops"`
	MaxRepeatedReadOnlyToolCalls int       `json:"max_repeated_read_only_tool_calls"`
	MaxInvalidToolCalls          int       `json:"max_invalid_tool_calls"`
	LLMRetryAttempts             int       `json:"llm_retry_attempts"`
	LLMRetryBackoff              string    `json:"llm_retry_backoff"`
	LLMCallTimeout               string    `json:"llm_call_timeout"`
	LLMProvider                  string    `json:"llm_provider,omitempty"`
	LLMModel                     string    `json:"llm_model,omitempty"`
	LLMBaseURL                   string    `json:"llm_base_url,omitempty"`
	// Per-capability backend identifiers.
	IngestBackend  string                   `json:"ingest_backend"`
	CompileBackend string                   `json:"compile_backend"`
	QueryBackend   string                   `json:"query_backend"`
	ACPProvider    string                   `json:"acp_provider,omitempty"`
	BatchIngest    *agent.CoordinatorStatus `json:"batch_ingest,omitempty"`
}

func (h *Handler) handleStatus(w http.ResponseWriter, r *http.Request) {
	stats, err := h.store.GetMemoryStats()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, APIResponse{Error: err.Error()})
		return
	}

	pendingIntegrations := h.pendingIntegrations.Load()
	ingestErrorCount := h.ingestErrors.Load()
	var batchIngest *agent.CoordinatorStatus
	if h.coordinator != nil {
		status := h.coordinator.Status()
		batchIngest = &status
		pendingIntegrations = int64(status.Pending + status.InProgress)
		ingestErrorCount = int64(status.ActionableFailures)
	}
	writeJSON(w, http.StatusOK, StatusResponse{
		MemoryStats:                  *stats,
		GeneratedAt:                  time.Now().UTC(),
		PendingIntegrations:          pendingIntegrations,
		IngestErrorCount:             ingestErrorCount,
		IngestTimeout:                h.effectiveIngestTimeout().String(),
		MaxConcurrentIntegrations:    h.maxConcurrentIntegrations,
		MaxToolLoops:                 h.maxToolLoops,
		MaxRepeatedReadOnlyToolCalls: h.maxRepeatedReadOnlyToolCalls,
		MaxInvalidToolCalls:          h.maxInvalidToolCalls,
		LLMRetryAttempts:             h.llmRetryAttempts,
		LLMRetryBackoff:              h.llmRetryBackoff.String(),
		LLMCallTimeout:               h.llmCallTimeout.String(),
		LLMProvider:                  h.llmProvider,
		LLMModel:                     h.llmModel,
		LLMBaseURL:                   h.llmBaseURL,
		IngestBackend:                h.wikiBackendName,
		CompileBackend:               h.compileBackendName,
		QueryBackend:                 h.queryBackendName,
		ACPProvider:                  h.acpProvider,
		BatchIngest:                  batchIngest,
	})
}

func (h *Handler) handleResetEvent(w http.ResponseWriter, r *http.Request) {
	h.handleAdminTransition(w, r, h.coordinatorReset)
}

func (h *Handler) handleConfirmEvent(w http.ResponseWriter, r *http.Request) {
	h.handleAdminTransition(w, r, h.coordinatorConfirm)
}

func (h *Handler) handleAdminTransition(w http.ResponseWriter, r *http.Request, transition func(string) (agent.AdminResult, error)) {
	if !h.adminAuthorized(r.Header.Get("X-Admin-Token")) {
		writeJSON(w, http.StatusForbidden, APIResponse{Error: "forbidden"})
		return
	}
	if h.coordinator == nil {
		writeJSON(w, http.StatusServiceUnavailable, APIResponse{Error: "batch ingest not configured"})
		return
	}
	result, err := transition(r.PathValue("id"))
	if err == nil {
		writeJSON(w, http.StatusOK, result)
		return
	}
	if errors.Is(err, agent.ErrEventNotFound) {
		writeJSON(w, http.StatusNotFound, APIResponse{Error: "event not found"})
		return
	}
	var transitionError *agent.TransitionError
	if errors.As(err, &transitionError) {
		writeJSON(w, http.StatusConflict, struct {
			Error         string `json:"error"`
			CurrentStatus string `json:"current_status"`
		}{Error: transitionError.Message, CurrentStatus: transitionError.CurrentStatus})
		return
	}
	log.Printf("[batch-ingest] admin transition failed: %v", err)
	writeJSON(w, http.StatusInternalServerError, APIResponse{Error: "internal error"})
}

func (h *Handler) coordinatorReset(eventID string) (agent.AdminResult, error) {
	return h.coordinator.ResetEvent(eventID)
}

func (h *Handler) coordinatorConfirm(eventID string) (agent.AdminResult, error) {
	return h.coordinator.ConfirmEvent(eventID)
}

func (h *Handler) adminAuthorized(provided string) bool {
	if h.adminToken == "" || len(provided) != len(h.adminToken) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(provided), []byte(h.adminToken)) == 1
}

// Wait blocks until all background integrations finish. Used for testing and graceful shutdown.
func (h *Handler) Wait() { h.wg.Wait() }

func (h *Handler) StartBatchCoordinator(timeout time.Duration) error {
	if h.coordinator == nil {
		return nil
	}
	return h.coordinator.Start(timeout)
}

func (h *Handler) StopBatchCoordinator(ctx context.Context) {
	if h.coordinator != nil {
		h.coordinator.Stop(ctx)
	}
}

func (h *Handler) EffectiveIngestTimeout() time.Duration {
	return h.effectiveIngestTimeout()
}

func (h *Handler) MaxConcurrentIntegrations() int {
	return h.maxConcurrentIntegrations
}

func (h *Handler) MaxToolLoops() int {
	return h.maxToolLoops
}

func (h *Handler) MaxRepeatedReadOnlyToolCalls() int {
	return h.maxRepeatedReadOnlyToolCalls
}

func (h *Handler) MaxInvalidToolCalls() int {
	return h.maxInvalidToolCalls
}

func (h *Handler) acquireIntegrationSlot() func() {
	if h.integrationSlots == nil {
		return func() {}
	}
	h.integrationSlots <- struct{}{}
	return func() { <-h.integrationSlots }
}

func (h *Handler) effectiveIngestTimeout() time.Duration {
	if h.ingestTimeout > 0 {
		return h.ingestTimeout
	}
	return defaultIngestTimeout
}

func (h *Handler) hasPendingIntegrations() bool {
	if h.coordinator != nil {
		status := h.coordinator.Status()
		return status.Pending+status.InProgress > 0
	}
	return h.pendingIntegrations.Load() > 0
}

func ingestTimeoutFromEnv() time.Duration {
	raw := os.Getenv(ingestTimeoutEnv)
	if raw == "" {
		return defaultIngestTimeout
	}

	timeout, err := time.ParseDuration(raw)
	if err != nil || timeout <= 0 {
		log.Printf("[api] invalid %s=%q, using default %s", ingestTimeoutEnv, raw, defaultIngestTimeout)
		return defaultIngestTimeout
	}
	return timeout
}

func maxConcurrentIntegrationsFromEnv() int {
	raw := os.Getenv(maxConcurrentIntegrationsEnv)
	if raw == "" {
		return defaultMaxConcurrentIntegrations
	}

	maxConcurrent, err := strconv.Atoi(raw)
	if err != nil || maxConcurrent <= 0 {
		log.Printf("[api] invalid %s=%q, using default %d", maxConcurrentIntegrationsEnv, raw, defaultMaxConcurrentIntegrations)
		return defaultMaxConcurrentIntegrations
	}
	return maxConcurrent
}

// runCompile executes a single compile cycle. Safe for concurrent use (serialized by wikiMu).
func (h *Handler) runCompile(ctx context.Context) {
	h.wikiMu.Lock()
	defer h.wikiMu.Unlock()

	cursor := h.store.GetCompileCursor()
	stats, _ := h.store.GetMemoryStats()
	if stats != nil && stats.UncompiledCount == 0 {
		return // nothing to compile
	}

	log.Printf("[auto-compile] starting: cursor=%d, uncompiled=%d", cursor, stats.UncompiledCount)
	result, err := h.compileBackend.RunCompile(ctx, cursor)
	if err != nil {
		log.Printf("[auto-compile] error: %v", err)
		return
	}

	newCursor := result.NewCursor
	if newCursor > cursor {
		if err := h.store.SetCompileCursor(newCursor); err != nil {
			log.Printf("[auto-compile] persist cursor error: %v", err)
		}
	}
	log.Printf("[auto-compile] done: cursor %d -> %d", cursor, newCursor)
}

// StartAutoCompile runs compile cycles periodically in the background.
// It stops when ctx is cancelled.
func (h *Handler) StartAutoCompile(ctx context.Context, interval time.Duration) {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		log.Printf("[auto-compile] enabled: every %s", interval)
		for {
			select {
			case <-ctx.Done():
				log.Print("[auto-compile] stopped")
				return
			case <-ticker.C:
				h.runCompile(ctx)
			}
		}
	}()
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// Validation helpers matching the canonical enum contract in tooldef.go.

func validSourceType(s string) bool {
	switch s {
	case "user", "assistant", "tool", "system", "compiler":
		return true
	}
	return false
}

func validEvidenceKind(s string) bool {
	switch s {
	case "direct_observation", "user_statement", "inferred", "compiler_synthesis":
		return true
	}
	return false
}
