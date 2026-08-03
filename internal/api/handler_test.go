package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qiffang/engram9/internal/agent"
	"github.com/qiffang/engram9/internal/storage"
	"github.com/stretchr/testify/require"
)

// mockLLM returns a canned response for testing without real API calls.
type mockLLM struct {
	response string
}

func (m *mockLLM) Call(_ context.Context, req agent.LLMRequest) (*agent.LLMResponse, error) {
	return &agent.LLMResponse{
		Content: []agent.ContentBlock{
			{Type: "text", Text: m.response},
		},
		StopReason: "end_turn",
	}, nil
}

func newTestHandler(t *testing.T) *Handler {
	t.Helper()
	llm := &mockLLM{response: "Memory stored successfully."}
	h, err := NewWithOptions(t.TempDir(), llm, Options{
		WikiBackend:               "llm",
		CompileBackend:            "llm",
		QueryBackend:              "llm",
		IngestTimeout:             time.Second,
		MaxConcurrentIntegrations: defaultMaxConcurrentIntegrations,
		LLMRetryAttempts:          agent.DefaultLLMRetryAttempts,
		LLMRetryBackoff:           agent.DefaultLLMRetryBackoff,
		LLMCallTimeout:            agent.DefaultLLMCallTimeout,
		LLMProvider:               "test",
		LLMModel:                  "mock-model",
		LLMBaseURL:                "https://example.invalid/v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func newACPTestHandler(t *testing.T, dataDir string) *Handler {
	t.Helper()
	scriptPath := filepath.Join(t.TempDir(), "acpmux")
	require.NoError(t, os.WriteFile(scriptPath, []byte("#!/bin/sh\nexit 0\n"), 0o755))
	handler, err := NewWithOptions(dataDir, &mockLLM{response: "ok"}, Options{
		WikiBackend:  "acp",
		QueryBackend: "llm",
		ACPConfig: &agent.ACPBackendConfig{
			Provider: "claude", AcpmuxCommand: scriptPath, Engram9McpCommand: scriptPath, TurnTimeout: time.Second,
		},
	})
	require.NoError(t, err)
	return handler
}

func TestIngestTimeoutFromEnv(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  time.Duration
	}{
		{name: "default", value: "", want: defaultIngestTimeout},
		{name: "configured", value: "10m", want: 10 * time.Minute},
		{name: "invalid", value: "slow", want: defaultIngestTimeout},
		{name: "zero", value: "0s", want: defaultIngestTimeout},
		{name: "negative", value: "-1s", want: defaultIngestTimeout},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(ingestTimeoutEnv, tt.value)
			if got := ingestTimeoutFromEnv(); got != tt.want {
				t.Fatalf("ingestTimeoutFromEnv()=%s, want %s", got, tt.want)
			}
		})
	}
}

func TestMaxConcurrentIntegrationsFromEnv(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  int
	}{
		{name: "default", value: "", want: defaultMaxConcurrentIntegrations},
		{name: "configured", value: "2", want: 2},
		{name: "invalid", value: "many", want: defaultMaxConcurrentIntegrations},
		{name: "zero", value: "0", want: defaultMaxConcurrentIntegrations},
		{name: "negative", value: "-1", want: defaultMaxConcurrentIntegrations},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(maxConcurrentIntegrationsEnv, tt.value)
			if got := maxConcurrentIntegrationsFromEnv(); got != tt.want {
				t.Fatalf("maxConcurrentIntegrationsFromEnv()=%d, want %d", got, tt.want)
			}
		})
	}
}

func TestRememberEndpoint(t *testing.T) {
	h := newTestHandler(t)
	srv := httptest.NewServer(h.Routes())
	defer srv.Close()
	defer h.Wait() // wait for background goroutines before TempDir cleanup

	resp, err := http.Post(srv.URL+"/remember",
		"application/json",
		strings.NewReader(`{"text": "Alice suggests partition tables"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}

	var result RememberResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.EventID == "" {
		t.Error("expected non-empty event_id")
	}
	if !strings.HasPrefix(result.EventID, "evt_") {
		t.Errorf("event_id=%q, want evt_ prefix", result.EventID)
	}
}

func TestRememberPersistsFullContextJSON(t *testing.T) {
	h := newTestHandler(t)
	request := httptest.NewRequest(http.MethodPost, "/remember", strings.NewReader(`{
        "text":"full context",
        "context":{"actor":"alice","custom_key":"custom_value"}
    }`))
	response := httptest.NewRecorder()
	h.Routes().ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	h.Wait()

	page, err := h.store.ReadEventsSince(0)
	require.NoError(t, err)
	require.Len(t, page.Events, 1)
	require.JSONEq(t, `{"actor":"alice","custom_key":"custom_value"}`, page.Events[0].ContextJSON)
}

func TestRememberOmitsContextJSONForEmptyContext(t *testing.T) {
	h := newTestHandler(t)
	request := httptest.NewRequest(http.MethodPost, "/remember", strings.NewReader(`{"text":"no context","context":{}}`))
	response := httptest.NewRecorder()
	h.Routes().ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	h.Wait()

	page, err := h.store.ReadEventsSince(0)
	require.NoError(t, err)
	require.Empty(t, page.Events[0].ContextJSON)
}

func TestACPRememberQueuesWithoutPerEventGoroutine(t *testing.T) {
	t.Setenv("BATCH_INGEST_EPOCH", "")
	h := newACPTestHandler(t, t.TempDir())
	request := httptest.NewRequest(http.MethodPost, "/remember", strings.NewReader(`{"text":"batch me","context":{"custom":"value"}}`))
	response := httptest.NewRecorder()
	h.Routes().ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, int64(0), h.pendingIntegrations.Load())

	statusResponse := httptest.NewRecorder()
	h.Routes().ServeHTTP(statusResponse, httptest.NewRequest(http.MethodGet, "/status", nil))
	require.Equal(t, http.StatusOK, statusResponse.Code)
	var status StatusResponse
	require.NoError(t, json.NewDecoder(statusResponse.Body).Decode(&status))
	require.NotNil(t, status.BatchIngest)
	require.Equal(t, 1, status.BatchIngest.Pending)
	require.Equal(t, int64(1), status.PendingIntegrations)
	require.Equal(t, "in_progress", status.BatchIngest.Health)
}

func TestACPRememberNilAndOmittedContextRemainNilInBatchPrompt(t *testing.T) {
	t.Setenv("BATCH_INGEST_EPOCH", "")
	h := newACPTestHandler(t, t.TempDir())
	for _, body := range []string{
		`{"text":"null context","context":null}`,
		`{"text":"omitted context"}`,
	} {
		response := httptest.NewRecorder()
		h.Routes().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/remember", strings.NewReader(body)))
		require.Equal(t, http.StatusOK, response.Code)
	}
	page, err := h.store.ReadEventsSince(0)
	require.NoError(t, err)
	require.Len(t, page.Events, 2)
	for _, event := range page.Events {
		pending := agent.NormalizeToPendingEvent(event)
		require.Nil(t, pending.Context)
		require.Contains(t, agent.BuildBatchPrompt(agent.Batch{Events: []agent.PendingEvent{pending}}), "Context: none")
	}
}

func TestACPRememberAfterStartAppearsInNextBatchPrompt(t *testing.T) {
	t.Setenv("BATCH_INGEST_EPOCH", "")
	dataDir := t.TempDir()
	capturePath := filepath.Join(t.TempDir(), "prompt.json")
	scriptPath := filepath.Join(t.TempDir(), "acpmux")
	script := fmt.Sprintf(`#!/bin/sh
set -eu
IFS= read -r initialize
printf '%%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}'
IFS= read -r initialized
IFS= read -r session
printf '%%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"session"}}'
IFS= read -r prompt
printf '%%s\n' "$prompt" > %q
printf '%%s\n' '{"jsonrpc":"2.0","id":3,"result":{"text":"processed"}}'
`, capturePath)
	require.NoError(t, os.WriteFile(scriptPath, []byte(script), 0o755))

	handler, err := NewWithOptions(dataDir, &mockLLM{response: "ok"}, Options{
		WikiBackend:  "acp",
		QueryBackend: "llm",
		ACPConfig: &agent.ACPBackendConfig{
			Provider: "claude", AcpmuxCommand: scriptPath, Engram9McpCommand: scriptPath, TurnTimeout: time.Second,
		},
		CoordinatorConfig: agent.CoordinatorConfig{FlushThreshold: 1, FlushInterval: time.Hour},
	})
	require.NoError(t, err)
	require.NoError(t, handler.StartBatchCoordinator(time.Second))
	defer handler.StopBatchCoordinator(context.Background())

	response := httptest.NewRecorder()
	handler.Routes().ServeHTTP(response, httptest.NewRequest(
		http.MethodPost,
		"/remember",
		strings.NewReader(`{"text":"deploy the release","context":{"topic":"deploy","env":"prod"}}`),
	))
	require.Equal(t, http.StatusOK, response.Code)
	rawEvents, err := handler.store.ReadEventsSince(0)
	require.NoError(t, err)
	require.Len(t, rawEvents.Events, 1)

	require.Eventually(t, func() bool {
		_, statErr := os.Stat(capturePath)
		return statErr == nil
	}, 5*time.Second, 10*time.Millisecond)
	captured, err := os.ReadFile(capturePath)
	require.NoError(t, err)
	var request struct {
		Params struct {
			Prompt string `json:"prompt"`
		} `json:"params"`
	}
	require.NoError(t, json.Unmarshal(captured, &request))
	require.Contains(t, request.Params.Prompt, "deploy the release")
	require.Contains(t, request.Params.Prompt, "Context: env=prod; topic=deploy")
	require.Contains(t, request.Params.Prompt, "Timestamp: "+rawEvents.Events[0].Timestamp)
}

func TestACPRememberReturns503WithoutDurableAppendWhenBootstrapRefused(t *testing.T) {
	t.Setenv("BATCH_INGEST_EPOCH", "")
	dataDir := t.TempDir()
	filesystem, err := storage.NewFS(dataDir)
	require.NoError(t, err)
	_, err = filesystem.AppendEvent(storage.Event{ID: "existing", Content: "backlog"})
	require.NoError(t, err)
	h := newACPTestHandler(t, dataDir)
	require.Nil(t, h.coordinator)

	response := httptest.NewRecorder()
	h.Routes().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/remember", strings.NewReader(`{"text":"must not append"}`)))
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	page, err := h.store.ReadEventsSince(0)
	require.NoError(t, err)
	require.Len(t, page.Events, 1)

	statusResponse := httptest.NewRecorder()
	h.Routes().ServeHTTP(statusResponse, httptest.NewRequest(http.MethodGet, "/status", nil))
	require.Equal(t, http.StatusOK, statusResponse.Code)
	var status StatusResponse
	require.NoError(t, json.NewDecoder(statusResponse.Body).Decode(&status))
	require.Nil(t, status.BatchIngest)
}

func TestActiveRememberCompletesAfterHTTPShutdownTimeout(t *testing.T) {
	t.Setenv("BATCH_INGEST_EPOCH", "")
	h := newACPTestHandler(t, t.TempDir())
	requestStarted := make(chan struct{})
	var startOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		startOnce.Do(func() { close(requestStarted) })
		h.Routes().ServeHTTP(w, request)
	}))
	defer server.Close()

	reader, writer := io.Pipe()
	request, err := http.NewRequest(http.MethodPost, server.URL+"/remember", reader)
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	responseDone := make(chan *http.Response, 1)
	errorDone := make(chan error, 1)
	go func() {
		response, requestErr := http.DefaultClient.Do(request)
		if requestErr != nil {
			errorDone <- requestErr
			return
		}
		responseDone <- response
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, server.Config.Shutdown(shutdownContext), context.DeadlineExceeded)
	_, err = io.WriteString(writer, `{"text":"durable during shutdown"}`)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	select {
	case response := <-responseDone:
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.NoError(t, response.Body.Close())
	case requestErr := <-errorDone:
		t.Fatal(requestErr)
	case <-time.After(time.Second):
		t.Fatal("request did not finish")
	}
	page, err := h.store.ReadEventsSince(0)
	require.NoError(t, err)
	require.Len(t, page.Events, 1)
	require.Equal(t, 1, h.coordinator.Status().Pending)
}

func TestAdminEventRoutes(t *testing.T) {
	t.Setenv("ENGRAM9_ADMIN_TOKEN", "secret")
	t.Setenv("BATCH_INGEST_EPOCH", "")
	dataDir := t.TempDir()
	filesystem, err := storage.NewFS(dataDir)
	require.NoError(t, err)
	_, err = filesystem.AppendEvent(storage.Event{ID: "unknown", Content: "uncertain"})
	require.NoError(t, err)
	_, err = filesystem.AppendEvent(storage.Event{ID: "failed", Content: "retry me"})
	require.NoError(t, err)
	statusLog := strings.Join([]string{
		`{"type":"bootstrap","epoch":"0001-01-01T00:00:00Z"}`,
		`{"event_id":"unknown","status":"unknown"}`,
		`{"event_id":"failed","status":"failed"}`,
	}, "\n") + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "wiki_integration_status.jsonl"), []byte(statusLog), 0o600))
	h := newACPTestHandler(t, dataDir)

	unauthorized := httptest.NewRecorder()
	h.Routes().ServeHTTP(unauthorized, httptest.NewRequest(http.MethodPost, "/admin/events/unknown/confirm", nil))
	require.Equal(t, http.StatusForbidden, unauthorized.Code)

	confirmRequest := httptest.NewRequest(http.MethodPost, "/admin/events/unknown/confirm", nil)
	confirmRequest.Header.Set("X-Admin-Token", "secret")
	confirmResponse := httptest.NewRecorder()
	h.Routes().ServeHTTP(confirmResponse, confirmRequest)
	require.Equal(t, http.StatusOK, confirmResponse.Code)
	var confirmResult agent.AdminResult
	require.NoError(t, json.NewDecoder(confirmResponse.Body).Decode(&confirmResult))
	require.Equal(t, agent.AdminResult{EventID: "unknown", OldStatus: "unknown", NewStatus: "integrated"}, confirmResult)

	conflictRequest := httptest.NewRequest(http.MethodPost, "/admin/events/unknown/confirm", nil)
	conflictRequest.Header.Set("X-Admin-Token", "secret")
	conflictResponse := httptest.NewRecorder()
	h.Routes().ServeHTTP(conflictResponse, conflictRequest)
	require.Equal(t, http.StatusConflict, conflictResponse.Code)
	require.Contains(t, conflictResponse.Body.String(), `"current_status":"integrated"`)

	resetRequest := httptest.NewRequest(http.MethodPost, "/admin/events/failed/reset", nil)
	resetRequest.Header.Set("X-Admin-Token", "secret")
	resetResponse := httptest.NewRecorder()
	h.Routes().ServeHTTP(resetResponse, resetRequest)
	require.Equal(t, http.StatusOK, resetResponse.Code)
	require.Contains(t, resetResponse.Body.String(), `"new_status":"pending"`)

	missingRequest := httptest.NewRequest(http.MethodPost, "/admin/events/missing/reset", nil)
	missingRequest.Header.Set("X-Admin-Token", "secret")
	missingResponse := httptest.NewRecorder()
	h.Routes().ServeHTTP(missingResponse, missingRequest)
	require.Equal(t, http.StatusNotFound, missingResponse.Code)
}

func TestAdminRoutesFailClosedWhenTokenUnset(t *testing.T) {
	t.Setenv("ENGRAM9_ADMIN_TOKEN", "")
	h := newTestHandler(t)
	request := httptest.NewRequest(http.MethodPost, "/admin/events/event/reset", nil)
	response := httptest.NewRecorder()
	h.Routes().ServeHTTP(response, request)
	require.Equal(t, http.StatusForbidden, response.Code)
}

func TestAdminTransitionMapsStoreIOErrorTo500(t *testing.T) {
	h := newTestHandler(t)
	h.adminToken = "secret"
	h.coordinator = agent.NewBatchCoordinator(nil, nil, nil, nil, "", agent.CoordinatorConfig{})
	request := httptest.NewRequest(http.MethodPost, "/admin/events/event/reset", nil)
	request.Header.Set("X-Admin-Token", "secret")
	response := httptest.NewRecorder()
	h.handleAdminTransition(response, request, func(string) (agent.AdminResult, error) {
		return agent.AdminResult{}, fmt.Errorf("raw event source: disk failed")
	})
	require.Equal(t, http.StatusInternalServerError, response.Code)
	require.JSONEq(t, `{"error":"internal error"}`, response.Body.String())
}

func TestRememberEmptyText(t *testing.T) {
	h := newTestHandler(t)
	srv := httptest.NewServer(h.Routes())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/remember",
		"application/json",
		strings.NewReader(`{"text": ""}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400", resp.StatusCode)
	}
}

func TestRecallEndpoint(t *testing.T) {
	h := newTestHandler(t)
	srv := httptest.NewServer(h.Routes())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/recall",
		"application/json",
		strings.NewReader(`{"question": "What did Alice say about db9?"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}
}

func TestRecallEmptyQuestion(t *testing.T) {
	h := newTestHandler(t)
	srv := httptest.NewServer(h.Routes())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/recall",
		"application/json",
		strings.NewReader(`{"question": ""}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400", resp.StatusCode)
	}
}

func TestStatusEndpoint(t *testing.T) {
	h := newTestHandler(t)
	srv := httptest.NewServer(h.Routes())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}

	var stats StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatal(err)
	}
	if stats.IngestTimeout != h.EffectiveIngestTimeout().String() {
		t.Fatalf("ingest_timeout=%q, want %q", stats.IngestTimeout, h.EffectiveIngestTimeout().String())
	}
	if stats.GeneratedAt.IsZero() {
		t.Fatal("generated_at must be set on every /status snapshot")
	}
	if time.Since(stats.GeneratedAt) > time.Minute {
		t.Fatalf("generated_at=%s is not fresh", stats.GeneratedAt)
	}
	if stats.MaxConcurrentIntegrations != h.MaxConcurrentIntegrations() {
		t.Fatalf("max_concurrent_integrations=%d, want %d", stats.MaxConcurrentIntegrations, h.MaxConcurrentIntegrations())
	}
	if stats.MaxToolLoops != h.MaxToolLoops() {
		t.Fatalf("max_tool_loops=%d, want %d", stats.MaxToolLoops, h.MaxToolLoops())
	}
	if stats.MaxRepeatedReadOnlyToolCalls != h.MaxRepeatedReadOnlyToolCalls() {
		t.Fatalf("max_repeated_read_only_tool_calls=%d, want %d", stats.MaxRepeatedReadOnlyToolCalls, h.MaxRepeatedReadOnlyToolCalls())
	}
	if stats.MaxInvalidToolCalls != h.MaxInvalidToolCalls() {
		t.Fatalf("max_invalid_tool_calls=%d, want %d", stats.MaxInvalidToolCalls, h.MaxInvalidToolCalls())
	}
	if stats.LLMRetryAttempts != agent.DefaultLLMRetryAttempts {
		t.Fatalf("llm_retry_attempts=%d, want %d", stats.LLMRetryAttempts, agent.DefaultLLMRetryAttempts)
	}
	if stats.LLMRetryBackoff != agent.DefaultLLMRetryBackoff.String() {
		t.Fatalf("llm_retry_backoff=%q, want %q", stats.LLMRetryBackoff, agent.DefaultLLMRetryBackoff.String())
	}
	if stats.LLMCallTimeout != agent.DefaultLLMCallTimeout.String() {
		t.Fatalf("llm_call_timeout=%q, want %q", stats.LLMCallTimeout, agent.DefaultLLMCallTimeout.String())
	}
	if stats.LLMProvider != "test" {
		t.Fatalf("llm_provider=%q, want test", stats.LLMProvider)
	}
	if stats.LLMModel != "mock-model" {
		t.Fatalf("llm_model=%q, want mock-model", stats.LLMModel)
	}
	if stats.LLMBaseURL != "https://example.invalid/v1" {
		t.Fatalf("llm_base_url=%q, want https://example.invalid/v1", stats.LLMBaseURL)
	}
	// Per-capability backend fields (adversary-1 gate).
	if stats.IngestBackend != "llm" {
		t.Fatalf("ingest_backend=%q, want llm", stats.IngestBackend)
	}
	if stats.CompileBackend != "llm" {
		t.Fatalf("compile_backend=%q, want llm", stats.CompileBackend)
	}
	if stats.QueryBackend != "llm" {
		t.Fatalf("query_backend=%q, want llm", stats.QueryBackend)
	}
	if stats.ACPProvider != "" {
		t.Fatalf("acp_provider=%q, want empty for llm backend", stats.ACPProvider)
	}
}

func TestCompileEndpoint(t *testing.T) {
	h := newTestHandler(t)
	srv := httptest.NewServer(h.Routes())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/compile",
		"application/json",
		strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}
}

func TestRememberCustomMetadata(t *testing.T) {
	h := newTestHandler(t)
	srv := httptest.NewServer(h.Routes())
	defer srv.Close()
	defer h.Wait()

	resp, err := http.Post(srv.URL+"/remember",
		"application/json",
		strings.NewReader(`{"text":"repo scan fact","source_type":"tool","evidence_kind":"direct_observation","trust_tier":2}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}

	var result RememberResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.EventID == "" {
		t.Error("expected non-empty event_id")
	}

	// Verify the stored event has the custom metadata.
	events, err := h.store.ReadRecentEvents(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 {
		t.Fatal("expected at least one event")
	}
	ev := events[len(events)-1]
	if ev.SourceType != "tool" {
		t.Errorf("source_type=%q, want tool", ev.SourceType)
	}
	if ev.EvidenceKind != "direct_observation" {
		t.Errorf("evidence_kind=%q, want direct_observation", ev.EvidenceKind)
	}
	if ev.TrustTier != 2 {
		t.Errorf("trust_tier=%d, want 2", ev.TrustTier)
	}
}

func TestRememberDefaultMetadata(t *testing.T) {
	h := newTestHandler(t)
	srv := httptest.NewServer(h.Routes())
	defer srv.Close()
	defer h.Wait()

	resp, err := http.Post(srv.URL+"/remember",
		"application/json",
		strings.NewReader(`{"text":"plain remember"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}

	events, err := h.store.ReadRecentEvents(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 {
		t.Fatal("expected at least one event")
	}
	ev := events[len(events)-1]
	if ev.SourceType != "user" {
		t.Errorf("default source_type=%q, want user", ev.SourceType)
	}
	if ev.EvidenceKind != "user_statement" {
		t.Errorf("default evidence_kind=%q, want user_statement", ev.EvidenceKind)
	}
	if ev.TrustTier != 1 {
		t.Errorf("default trust_tier=%d, want 1", ev.TrustTier)
	}
}

func TestStatusIncludesIngestErrorCount(t *testing.T) {
	h := newTestHandler(t)
	srv := httptest.NewServer(h.Routes())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}

	var stats StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatal(err)
	}
	if stats.IngestErrorCount != 0 {
		t.Errorf("initial ingest_error_count=%d, want 0", stats.IngestErrorCount)
	}
}

func TestRememberInvalidSourceType(t *testing.T) {
	h := newTestHandler(t)
	srv := httptest.NewServer(h.Routes())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/remember",
		"application/json",
		strings.NewReader(`{"text":"test","source_type":"repo_scan"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400", resp.StatusCode)
	}
}

func TestRememberInvalidEvidenceKind(t *testing.T) {
	h := newTestHandler(t)
	srv := httptest.NewServer(h.Routes())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/remember",
		"application/json",
		strings.NewReader(`{"text":"test","evidence_kind":"guess"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400", resp.StatusCode)
	}
}

func TestRememberInvalidTrustTier(t *testing.T) {
	h := newTestHandler(t)
	srv := httptest.NewServer(h.Routes())
	defer srv.Close()

	for _, body := range []string{
		`{"text":"test","trust_tier":0}`,
		`{"text":"test","trust_tier":4}`,
		`{"text":"test","trust_tier":-1}`,
	} {
		resp, err := http.Post(srv.URL+"/remember",
			"application/json",
			strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("body=%s status=%d, want 400", body, resp.StatusCode)
		}
	}
}

// failingLLM always returns an error, used to test async ingest error counting.
type failingLLM struct{}

func (f *failingLLM) Call(_ context.Context, _ agent.LLMRequest) (*agent.LLMResponse, error) {
	return nil, fmt.Errorf("simulated LLM failure")
}

func TestIngestErrorCountIncrementsOnFailure(t *testing.T) {
	h, err := NewWithOptions(t.TempDir(), &failingLLM{}, Options{
		WikiBackend:               "llm",
		CompileBackend:            "llm",
		QueryBackend:              "llm",
		IngestTimeout:             time.Second,
		MaxConcurrentIntegrations: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h.Routes())
	defer srv.Close()
	defer h.Wait()

	resp, err := http.Post(srv.URL+"/remember",
		"application/json",
		strings.NewReader(`{"text":"trigger failure"}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}

	// Wait for async integration to complete.
	h.Wait()

	if got := h.ingestErrors.Load(); got != 1 {
		t.Fatalf("ingest_error_count=%d, want 1", got)
	}
}

type activeCountingLLM struct {
	delay     time.Duration
	active    atomic.Int64
	maxActive atomic.Int64
}

func (m *activeCountingLLM) Call(ctx context.Context, _ agent.LLMRequest) (*agent.LLMResponse, error) {
	active := m.active.Add(1)
	m.recordMaxActive(active)
	defer m.active.Add(-1)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(m.delay):
	}

	return &agent.LLMResponse{
		Content: []agent.ContentBlock{
			{Type: "text", Text: "Memory stored successfully."},
		},
		StopReason: "end_turn",
	}, nil
}

func (m *activeCountingLLM) recordMaxActive(active int64) {
	for {
		maxActive := m.maxActive.Load()
		if active <= maxActive || m.maxActive.CompareAndSwap(maxActive, active) {
			return
		}
	}
}

func TestRememberLimitsConcurrentIntegrations(t *testing.T) {
	llm := &activeCountingLLM{delay: 20 * time.Millisecond}
	h, err := NewWithOptions(t.TempDir(), llm, Options{
		WikiBackend:               "llm",
		CompileBackend:            "llm",
		QueryBackend:              "llm",
		IngestTimeout:             time.Second,
		MaxConcurrentIntegrations: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h.Routes())
	defer srv.Close()
	defer h.Wait()

	for i := 0; i < 3; i++ {
		resp, err := http.Post(srv.URL+"/remember",
			"application/json",
			strings.NewReader(fmt.Sprintf(`{"text":"event %d"}`, i)))
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status=%d, want 200", resp.StatusCode)
		}
	}

	h.Wait()

	if got := llm.maxActive.Load(); got != 1 {
		t.Fatalf("max active integrations=%d, want 1", got)
	}
	if got := h.ingestErrors.Load(); got != 0 {
		t.Fatalf("ingest_error_count=%d, want 0", got)
	}
}

func TestRememberMethodNotAllowed(t *testing.T) {
	h := newTestHandler(t)
	srv := httptest.NewServer(h.Routes())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/remember")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d, want 405", resp.StatusCode)
	}
}

// I5 zero-apikey completeness: with every capability on acp, the handler
// constructs with a NIL LLM client (no LLM client needed, no apikey read).
// If any capability secretly required an LLM client, this would panic/deref nil.
func TestNewWithOptionsAllACPNoLLMClient(t *testing.T) {
	scriptPath := filepath.Join(t.TempDir(), "acpmux")
	require.NoError(t, os.WriteFile(scriptPath, []byte("#!/bin/sh\nexit 0\n"), 0o755))
	// llm is nil — an all-acp deployment must not construct or use an LLM client.
	h, err := NewWithOptions(t.TempDir(), nil, Options{
		WikiBackend:    "acp",
		CompileBackend: "acp",
		QueryBackend:   "acp",
		ACPConfig: &agent.ACPBackendConfig{
			Provider: "claude", AcpmuxCommand: scriptPath, Engram9McpCommand: scriptPath, TurnTimeout: time.Second,
		},
	})
	require.NoError(t, err)
	require.Equal(t, "acp", h.wikiBackendName)
	require.Equal(t, "acp", h.compileBackendName)
	require.Equal(t, "acp", h.queryBackendName)

	// /status must report every capability as acp.
	statusResp := httptest.NewRecorder()
	h.Routes().ServeHTTP(statusResp, httptest.NewRequest(http.MethodGet, "/status", nil))
	require.Equal(t, http.StatusOK, statusResp.Code)
	var status StatusResponse
	require.NoError(t, json.NewDecoder(statusResp.Body).Decode(&status))
	require.Equal(t, "acp", status.IngestBackend)
	require.Equal(t, "acp", status.CompileBackend)
	require.Equal(t, "acp", status.QueryBackend)
	require.Equal(t, "claude", status.ACPProvider)
}

// Defaults: an empty Options must default every capability to acp (the wiki
// flow runs through an agent by default, never the apikey LLM path).
func TestNewWithOptionsDefaultsToACP(t *testing.T) {
	scriptPath := filepath.Join(t.TempDir(), "acpmux")
	require.NoError(t, os.WriteFile(scriptPath, []byte("#!/bin/sh\nexit 0\n"), 0o755))
	h, err := NewWithOptions(t.TempDir(), nil, Options{
		ACPConfig: &agent.ACPBackendConfig{
			Provider: "claude", AcpmuxCommand: scriptPath, Engram9McpCommand: scriptPath, TurnTimeout: time.Second,
		},
	})
	require.NoError(t, err)
	require.Equal(t, "acp", h.wikiBackendName)
	require.Equal(t, "acp", h.compileBackendName)
	require.Equal(t, "acp", h.queryBackendName)
}

// QUERY_BACKEND=acp is now supported (canon9-ai #41); without ACP config it
// must fail fast (no silent LLM fallback), not be rejected as unsupported.
func TestNewWithOptionsQueryBackendACPRequiresConfig(t *testing.T) {
	llm := &mockLLM{response: "ok"}
	_, err := NewWithOptions(t.TempDir(), llm, Options{
		WikiBackend:    "llm",
		CompileBackend: "llm",
		QueryBackend:   "acp",
	})
	if err == nil {
		t.Fatal("expected error for QUERY_BACKEND=acp without ACP config")
	}
	if !strings.Contains(err.Error(), "ACP configuration is missing") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewWithOptionsRejectsUnknownWikiBackend(t *testing.T) {
	llm := &mockLLM{response: "ok"}
	_, err := NewWithOptions(t.TempDir(), llm, Options{
		WikiBackend:    "magic",
		CompileBackend: "llm",
		QueryBackend:   "llm",
	})
	if err == nil {
		t.Fatal("expected error for unknown WIKI_BACKEND")
	}
	if !strings.Contains(err.Error(), "unknown WIKI_BACKEND") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewWithOptionsRejectsUnknownQueryBackend(t *testing.T) {
	llm := &mockLLM{response: "ok"}
	_, err := NewWithOptions(t.TempDir(), llm, Options{
		WikiBackend:    "llm",
		CompileBackend: "llm",
		QueryBackend:   "magic",
	})
	if err == nil {
		t.Fatal("expected error for unknown QUERY_BACKEND")
	}
	if !strings.Contains(err.Error(), "unknown QUERY_BACKEND") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewWithOptionsRejectsACPWithoutConfig(t *testing.T) {
	llm := &mockLLM{response: "ok"}
	_, err := NewWithOptions(t.TempDir(), llm, Options{
		WikiBackend: "acp",
	})
	if err == nil {
		t.Fatal("expected error for WIKI_BACKEND=acp without ACPConfig")
	}
	if !strings.Contains(err.Error(), "ACP configuration") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestWikiMuSerializesCompileRecallInACPMode verifies that compile and recall
// remain serialized while batch ACP ingest owns wikiMu through the coordinator.
func TestWikiMuSerializesCompileRecallInACPMode(t *testing.T) {
	llm := &activeCountingLLM{delay: 20 * time.Millisecond}
	h, err := NewWithOptions(t.TempDir(), llm, Options{
		WikiBackend:               "llm",
		CompileBackend:            "llm",
		QueryBackend:              "llm",
		IngestTimeout:             2 * time.Second,
		MaxConcurrentIntegrations: 4,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Simulate ACP backend by setting wikiBackendName to "acp".
	// The actual backends remain LLM-based for testability — what matters
	// is that the handler's lock paths are activated.
	h.wikiBackendName = "acp"

	srv := httptest.NewServer(h.Routes())
	defer srv.Close()
	defer h.Wait()

	var wg sync.WaitGroup

	// Fire concurrent compile requests.
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(srv.URL+"/compile",
				"application/json",
				strings.NewReader(`{}`))
			if err != nil {
				t.Errorf("compile: %v", err)
				return
			}
			_ = resp.Body.Close()
		}()
	}

	// Fire concurrent recall requests.
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(srv.URL+"/recall",
				"application/json",
				strings.NewReader(`{"question":"test?"}`))
			if err != nil {
				t.Errorf("recall: %v", err)
				return
			}
			_ = resp.Body.Close()
		}()
	}

	// Wait for synchronous requests to finish.
	wg.Wait()
	h.Wait()

	// With ACP mode, all wiki-mutating operations should serialize.
	// The mock LLM's maxActive should be 1 (no concurrent wiki mutations).
	if got := llm.maxActive.Load(); got != 1 {
		t.Fatalf("max concurrent wiki-mutating operations=%d, want 1 (wikiMu not serializing)", got)
	}
}
