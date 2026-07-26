package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/qiffang/engram9/internal/storage"
)

const (
	DefaultACPTurnTimeout  = 10 * time.Minute
	DefaultACPMaxDiffBytes = 1 << 20 // 1 MB
)

// ACPBackendConfig holds configuration for the ACP backend.
type ACPBackendConfig struct {
	Provider       string        // "claude" (Phase 1; codex pending MCP injection support)
	AcpmuxCommand  string        // path to acpmux binary (default: "acpmux")
	TurnTimeout    time.Duration // per-turn timeout (default: 10m)
	MaxDiffBytes   int64         // max bytes changed per turn (default: 1MB)
	AdditionalDirs string        // rejected if set (Phase 1)
}

// ACPBackend runs wiki agents via acpmux ACP protocol.
// Callers must serialize ACP turns externally (e.g. via Handler.wikiMu)
// to prevent concurrent snapshot-replace from overwriting wiki changes.
type ACPBackend struct {
	cfg       ACPBackendConfig
	dataDir   string
	validator *WikiValidator
}

type TurnResult struct {
	Summary    string
	Violations []Violation
	Merged     bool
}

type BatchResult struct {
	BatchID      string        `json:"batch_id"`
	Status       string        `json:"status"`
	EventResults []EventResult `json:"event_results,omitempty"`
	Summary      string        `json:"summary,omitempty"`
	Violations   []Violation   `json:"violations,omitempty"`
	IndexStale   bool          `json:"index_stale"`
	Error        error         `json:"-"`
	DurationMs   int64         `json:"duration_ms"`
}

func (result BatchResult) IsShutdownCancel() bool {
	return result.Error != nil && errors.Is(result.Error, context.Canceled)
}

func (result BatchResult) errorClass() string {
	switch {
	case len(result.Violations) > 0:
		return "validation"
	case errors.Is(result.Error, context.DeadlineExceeded):
		return "timeout"
	default:
		return "crash"
	}
}

func (result BatchResult) failureReason() string {
	if len(result.Violations) > 0 {
		messages := make([]string, len(result.Violations))
		for index, violation := range result.Violations {
			messages[index] = violation.String()
		}
		return "validation failed: " + strings.Join(messages, "; ")
	}
	if result.Error != nil {
		return result.Error.Error()
	}
	return "batch failed"
}

// NewACPBackend creates an ACPBackend. It validates the config at construction time.
func NewACPBackend(dataDir string, cfg ACPBackendConfig) (*ACPBackend, error) {
	if cfg.Provider == "" {
		return nil, fmt.Errorf("ACP_PROVIDER is required")
	}
	// Phase 1: only Claude adapter supports MCP server injection via acpmux.
	// Codex adapter ignores MCPServers today.
	if cfg.Provider != "claude" {
		return nil, fmt.Errorf("ACP_PROVIDER=%q is not supported in Phase 1 (only 'claude' has MCP injection)", cfg.Provider)
	}
	if cfg.AcpmuxCommand == "" {
		cfg.AcpmuxCommand = "acpmux"
	}
	if cfg.TurnTimeout <= 0 {
		cfg.TurnTimeout = DefaultACPTurnTimeout
	}
	if cfg.MaxDiffBytes <= 0 {
		cfg.MaxDiffBytes = DefaultACPMaxDiffBytes
	}
	if cfg.AdditionalDirs != "" {
		return nil, fmt.Errorf("ACP_ADDITIONAL_DIRS is disabled in Phase 1; remove it from config")
	}

	// Verify acpmux binary exists.
	if _, err := exec.LookPath(cfg.AcpmuxCommand); err != nil {
		return nil, fmt.Errorf("acpmux binary not found: %w", err)
	}

	maxDiff := cfg.MaxDiffBytes
	if maxDiff <= 0 {
		maxDiff = DefaultACPMaxDiffBytes
	}

	return &ACPBackend{
		cfg:       cfg,
		dataDir:   dataDir,
		validator: NewWikiValidator(maxDiff),
	}, nil
}

func (b *ACPBackend) RunIngest(ctx context.Context, eventID string, text string, ctxInfo map[string]string) (IngestResult, error) {
	prompt := fmt.Sprintf(`You are the Ingest Agent. Event %s has been recorded with this content:

%s`, eventID, text)
	if len(ctxInfo) > 0 {
		ctxJSON, _ := json.Marshal(ctxInfo)
		prompt += fmt.Sprintf("\n\nContext: %s", string(ctxJSON))
	}
	prompt += "\n\n" + integrateSystemPrompt

	summary, err := b.runACPTurn(ctx, prompt, ValidateOptions{AllowDelete: false})
	if err != nil {
		return IngestResult{}, err
	}
	return IngestResult{Summary: summary}, nil
}

func (b *ACPBackend) TurnTimeout() time.Duration {
	return b.cfg.TurnTimeout
}

func (b *ACPBackend) RunBatchIngest(ctx context.Context, batch Batch, wikiMu *sync.Mutex, rebuildIndex func() error) (BatchResult, error) {
	startedAt := time.Now()
	result := BatchResult{BatchID: batch.ID, Status: "failed"}
	finish := func() BatchResult {
		result.DurationMs = time.Since(startedAt).Milliseconds()
		return result
	}
	if wikiMu == nil {
		return finish(), fmt.Errorf("wiki mutex is required")
	}
	if rebuildIndex == nil {
		return finish(), fmt.Errorf("wiki index rebuild function is required")
	}

	locked := make(chan struct{})
	go func() {
		wikiMu.Lock()
		close(locked)
	}()
	select {
	case <-locked:
	case <-ctx.Done():
		go func() {
			<-locked
			wikiMu.Unlock()
		}()
		result.Error = ctx.Err()
		return finish(), nil
	}

	turnResult, err := b.runACPTurnFull(ctx, BuildBatchPrompt(batch), ValidateOptions{AllowDelete: false})
	if err != nil {
		wikiMu.Unlock()
		if errors.Is(ctx.Err(), context.Canceled) {
			result.Error = context.Canceled
		} else {
			result.Error = err
		}
		return finish(), nil
	}
	result.Summary = turnResult.Summary
	if len(turnResult.Violations) > 0 {
		wikiMu.Unlock()
		result.Violations = turnResult.Violations
		return finish(), nil
	}

	indexErr := rebuildIndex()
	wikiMu.Unlock()
	result.Status = "success"
	result.EventResults = parseEventResults(batch, turnResult.Summary, b.persistTranscript(batch.ID, turnResult.Summary))
	result.IndexStale = indexErr != nil
	return finish(), nil
}

// persistTranscript writes the agent turn output for a batch under
// <dataDir>/transcripts so unknown outcomes always reference reviewable
// evidence. Best-effort: on failure it returns a non-empty marker instead of
// a path (the unknown contract forbids empty transcript references).
func (b *ACPBackend) persistTranscript(batchID, summary string) string {
	dir := filepath.Join(b.dataDir, "transcripts")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "transcript-write-failed"
	}
	path := filepath.Join(dir, batchID+".log")
	if err := os.WriteFile(path, []byte(summary), 0o644); err != nil {
		return "transcript-write-failed"
	}
	return path
}

func (b *ACPBackend) RunCompile(_ context.Context, _ uint64) (CompileResult, error) {
	// ACP compile is not yet supported: MCP agent mode only exposes wiki tools
	// (read_wiki_index, read_wiki_page, write_wiki_page, search_wiki) but compile
	// requires read_events_since, archive_wiki_page, and rebuild_index.
	// Until a compile-mode MCP tool set is implemented, compile stays on LLM.
	return CompileResult{}, ErrNotImplemented
}

func (b *ACPBackend) RunQuery(_ context.Context, _ string, _ map[string]string, _ []storage.Event) (QueryResult, error) {
	return QueryResult{}, ErrNotImplemented
}

func (b *ACPBackend) Close() error {
	return nil
}

// runACPTurn executes a single ACP agent turn:
// 1. Copy data dir to staging
// 2. Spawn acpmux with MCP config pointing to staging
// 3. Send initialize + session/new + session/prompt
// 4. Wait for completion
// 5. Validate staging wiki
// 6. Merge staging -> production
func (b *ACPBackend) runACPTurn(ctx context.Context, prompt string, valOpts ValidateOptions) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, b.cfg.TurnTimeout)
	defer cancel()
	result, err := b.runACPTurnFull(ctx, prompt, valOpts)
	if err != nil {
		return "", err
	}
	if len(result.Violations) > 0 {
		messages := make([]string, len(result.Violations))
		for index, violation := range result.Violations {
			messages[index] = violation.String()
		}
		return "", fmt.Errorf("validation failed: %s", strings.Join(messages, "; "))
	}
	return result.Summary, nil
}

func (b *ACPBackend) runACPTurnFull(ctx context.Context, prompt string, valOpts ValidateOptions) (TurnResult, error) {

	// 1. Create staging directory and copy data.
	stagingDir, err := os.MkdirTemp("", "engram9-acp-staging-*")
	if err != nil {
		return TurnResult{}, fmt.Errorf("create staging dir: %w", err)
	}
	defer os.RemoveAll(stagingDir)

	if err := copyDir(b.dataDir, stagingDir); err != nil {
		return TurnResult{}, fmt.Errorf("copy to staging: %w", err)
	}

	// 2. Spawn acpmux process.
	// Claude needs ToolSearch to discover MCP tools at runtime, plus Glob/Grep
	// for read-only file access. Write isolation is enforced by --allowedTools
	// restricting to engram9 MCP tools only (no Write/Edit/Bash).
	cmd := exec.CommandContext(ctx, b.cfg.AcpmuxCommand, acpmuxArgs(b.cfg.Provider)...)
	cmd.Stderr = os.Stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return TurnResult{}, fmt.Errorf("stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return TurnResult{}, fmt.Errorf("stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return TurnResult{}, fmt.Errorf("start acpmux: %w", err)
	}

	// Ensure process is killed on exit.
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4<<20), 4<<20)

	// 3. Send initialize.
	initReq := acpRequest{
		JSONRPC: "2.0",
		ID:      json.RawMessage(`1`),
		Method:  "initialize",
		Params: mustMarshal(map[string]any{
			"protocolVersion": 1,
			"clientInfo":      map[string]string{"name": "engram9", "version": "0.1.0"},
		}),
	}
	if err := sendACPRequest(stdin, initReq); err != nil {
		return TurnResult{}, fmt.Errorf("send initialize: %w", err)
	}
	initResp, err := readACPResponseForID(scanner, "1")
	if err != nil {
		return TurnResult{}, fmt.Errorf("initialize response: %w", err)
	}
	if initResp.Error != nil {
		return TurnResult{}, fmt.Errorf("initialize failed: ACP error %d: %s", initResp.Error.Code, initResp.Error.Message)
	}

	// Send initialized notification.
	initNotif := acpRequest{
		JSONRPC: "2.0",
		Method:  "notifications/initialized",
	}
	if err := sendACPRequest(stdin, initNotif); err != nil {
		return TurnResult{}, fmt.Errorf("send initialized notification: %w", err)
	}

	// 4. Send session/new with MCP config.
	// acpmux expects MCP server fields at top level (type, name, command, args),
	// NOT nested under a "transport" key.
	sessionReq := newACPSessionRequest(stagingDir)
	if err := sendACPRequest(stdin, sessionReq); err != nil {
		return TurnResult{}, fmt.Errorf("send session/new: %w", err)
	}
	sessionResp, err := readACPResponseForID(scanner, "2")
	if err != nil {
		return TurnResult{}, fmt.Errorf("session/new response: %w", err)
	}
	if sessionResp.Error != nil {
		return TurnResult{}, fmt.Errorf("session/new failed: ACP error %d: %s", sessionResp.Error.Code, sessionResp.Error.Message)
	}

	// Extract session ID.
	var sessionResult struct {
		SessionID string `json:"sessionId"`
	}
	if sessionResp.Result != nil {
		_ = json.Unmarshal(sessionResp.Result, &sessionResult)
	}

	// 5. Send session/prompt.
	promptReq := acpRequest{
		JSONRPC: "2.0",
		ID:      json.RawMessage(`3`),
		Method:  "session/prompt",
		Params: mustMarshal(map[string]any{
			"sessionId": sessionResult.SessionID,
			"prompt":    prompt,
		}),
	}
	if err := sendACPRequest(stdin, promptReq); err != nil {
		return TurnResult{}, fmt.Errorf("send session/prompt: %w", err)
	}

	// 6. Stream events until completion.
	var summary string
	promptCompleted := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var resp acpResponse
		if err := json.Unmarshal([]byte(line), &resp); err != nil {
			log.Printf("[acp] invalid response: %s", line)
			continue
		}

		// Check for errors.
		if resp.Error != nil {
			return TurnResult{Summary: summary}, fmt.Errorf("ACP error %d: %s", resp.Error.Code, resp.Error.Message)
		}

		// If this is a response to our prompt request (id=3), we're done.
		if string(resp.ID) == "3" {
			promptCompleted = true
			if resp.Result != nil {
				var promptResult struct {
					Text string `json:"text"`
				}
				_ = json.Unmarshal(resp.Result, &promptResult)
				summary = promptResult.Text
			}
			break
		}

		// Otherwise it's a notification/update — log and continue.
		if resp.Method != "" {
			log.Printf("[acp] notification: %s", resp.Method)
		}
	}
	if err := scanner.Err(); err != nil {
		return TurnResult{Summary: summary}, fmt.Errorf("read acp output: %w", err)
	}
	if !promptCompleted {
		return TurnResult{Summary: summary}, fmt.Errorf("read acp output: prompt response missing: %w", io.EOF)
	}

	// 7. Validate staging wiki.
	violations, err := b.validator.Validate(b.dataDir, stagingDir, valOpts)
	if err != nil {
		return TurnResult{Summary: summary}, fmt.Errorf("validate staging: %w", err)
	}
	if len(violations) > 0 {
		return TurnResult{Summary: summary, Violations: violations}, nil
	}

	// 8. Merge staging wiki -> production.
	if err := mergeWiki(stagingDir, b.dataDir); err != nil {
		return TurnResult{Summary: summary}, fmt.Errorf("merge staging: %w", err)
	}

	return TurnResult{Summary: summary, Merged: true}, nil
}

// --- ACP JSON-RPC types ---

type acpRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type acpResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *acpError       `json:"error,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type acpError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func acpmuxArgs(provider string) []string {
	return []string{
		"--provider", provider,
		"--provider-arg", "--tools",
		"--provider-arg", "ToolSearch,Glob,Grep",
		"--provider-arg", "--allowedTools",
		"--provider-arg", "mcp__engram9__read_wiki_index,mcp__engram9__read_wiki_page,mcp__engram9__write_wiki_page,mcp__engram9__search_wiki",
		"--provider-arg", "--permission-mode",
		"--provider-arg", "dontAsk",
		"--provider-arg", "--strict-mcp-config",
	}
}

func newACPSessionRequest(stagingDir string) acpRequest {
	return acpRequest{
		JSONRPC: "2.0",
		ID:      json.RawMessage(`2`),
		Method:  "session/new",
		Params: mustMarshal(map[string]any{
			"cwd": stagingDir,
			"mcpServers": []map[string]any{
				{
					"name":    "engram9",
					"type":    "stdio",
					"command": "engram9-mcp",
					"args":    []string{"-data", stagingDir, "-mode", "agent"},
				},
			},
		}),
	}
}

func sendACPRequest(w io.Writer, req acpRequest) error {
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "%s\n", data)
	return err
}

// readACPResponseForID reads lines until it finds a response matching the
// expected request ID. Notifications (no ID) and responses with non-matching
// IDs are logged and skipped. This prevents a stale response or notification
// from being mistaken for a handshake response.
func readACPResponseForID(scanner *bufio.Scanner, expectedID string) (*acpResponse, error) {
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var resp acpResponse
		if err := json.Unmarshal([]byte(line), &resp); err != nil {
			continue
		}
		// Skip notifications (no ID).
		if resp.ID == nil {
			if resp.Method != "" {
				log.Printf("[acp] notification during handshake: %s", resp.Method)
			}
			continue
		}
		// Check ID matches.
		respID := strings.Trim(string(resp.ID), `"`)
		if respID == expectedID {
			return &resp, nil
		}
		log.Printf("[acp] unexpected response id=%s (want %s), skipping", respID, expectedID)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

func readACPResponse(scanner *bufio.Scanner) (*acpResponse, error) {
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var resp acpResponse
		if err := json.Unmarshal([]byte(line), &resp); err != nil {
			continue
		}
		return &resp, nil
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

func mustMarshal(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// copyDir copies src directory to dst recursively.
func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(dstPath, data, info.Mode())
	})
}

// mergeWiki atomically replaces the production wiki/ with the staging wiki/.
// It copies staging wiki to a temp directory next to production, then renames
// the old wiki to a backup, renames the new wiki into place, and removes the
// backup. If any step fails after backup rename, the backup is restored.
func mergeWiki(stagingDir, prodDir string) error {
	stagingWiki := filepath.Join(stagingDir, "wiki")
	prodWiki := filepath.Join(prodDir, "wiki")

	// Check if staging wiki exists.
	if _, err := os.Stat(stagingWiki); os.IsNotExist(err) {
		return nil // nothing to merge
	}

	// Copy staging wiki to a temp dir adjacent to production (same filesystem for rename).
	tmpWiki := prodWiki + ".merging"
	if err := os.RemoveAll(tmpWiki); err != nil {
		return fmt.Errorf("clean merge temp: %w", err)
	}
	if err := copyDir(stagingWiki, tmpWiki); err != nil {
		os.RemoveAll(tmpWiki)
		return fmt.Errorf("copy staging to merge temp: %w", err)
	}

	// If production wiki exists, rename it to backup before swapping.
	backupWiki := prodWiki + ".backup"
	_ = os.RemoveAll(backupWiki)
	hadProd := false
	if _, err := os.Stat(prodWiki); err == nil {
		if err := os.Rename(prodWiki, backupWiki); err != nil {
			os.RemoveAll(tmpWiki)
			return fmt.Errorf("backup prod wiki: %w", err)
		}
		hadProd = true
	}

	// Rename new wiki into place.
	if err := os.Rename(tmpWiki, prodWiki); err != nil {
		// Restore backup if rename failed.
		if hadProd {
			_ = os.Rename(backupWiki, prodWiki)
		}
		return fmt.Errorf("rename merge temp to prod: %w", err)
	}

	// Clean up backup.
	if hadProd {
		_ = os.RemoveAll(backupWiki)
	}
	return nil
}
