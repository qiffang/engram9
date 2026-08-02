package agent

import (
	"bufio"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// RunCompile / RunQuery are now implemented (canon9-ai #41). Their deterministic
// logic (receipt validation, recall injection) is covered by the tests in
// backend_acp_compile_test.go and backend_acp_query_test.go; the full ACP turn
// requires a real acpmux+claude E2E per the #41 §10 gate.

func TestNewACPBackendRejectsNonClaudeProvider(t *testing.T) {
	_, err := NewACPBackend(t.TempDir(), ACPBackendConfig{Provider: "codex"})
	if err == nil {
		t.Fatal("expected error for ACP_PROVIDER=codex")
	}
}

// wantACPMuxArgs builds the expected acpmux args for a given allowedTools CSV.
func wantACPMuxArgs(allowedCSV string) []string {
	return []string{
		"--provider", "claude",
		"--provider-arg", "--tools",
		"--provider-arg", "ToolSearch,Glob,Grep",
		"--provider-arg", "--allowedTools",
		"--provider-arg", allowedCSV,
		"--provider-arg", "--permission-mode",
		"--provider-arg", "dontAsk",
		"--provider-arg", "--strict-mcp-config",
	}
}

func TestACPMuxArgsDefaultsToAgentTools(t *testing.T) {
	// Empty allowedTools ⇒ the agent (ingest) surface.
	want := wantACPMuxArgs("mcp__engram9__read_wiki_index,mcp__engram9__read_wiki_page,mcp__engram9__write_wiki_page,mcp__engram9__search_wiki")
	if got := acpmuxArgs("claude", nil); !reflect.DeepEqual(got, want) {
		t.Fatalf("acpmuxArgs(agent) = %#v, want %#v", got, want)
	}
}

// TestACPMuxArgsCompileWhitelistsCompileTools guards the bug the real E2E
// caught: compile turns need read_events_since / archive_wiki_page /
// rebuild_index in --allowedTools, or the agent cannot call read_events_since
// and the compile produces no receipt (unknown(no_valid_receipt)).
func TestACPMuxArgsCompileWhitelistsCompileTools(t *testing.T) {
	want := wantACPMuxArgs("mcp__engram9__read_wiki_index,mcp__engram9__read_wiki_page,mcp__engram9__write_wiki_page,mcp__engram9__search_wiki,mcp__engram9__read_events_since,mcp__engram9__archive_wiki_page,mcp__engram9__rebuild_index")
	if got := acpmuxArgs("claude", compileAllowedTools); !reflect.DeepEqual(got, want) {
		t.Fatalf("acpmuxArgs(compile) = %#v, want %#v", got, want)
	}
	// The compile whitelist MUST include read_events_since (receipt source).
	found := false
	for _, tool := range compileAllowedTools {
		if tool == mcpToolReadEventsSince {
			found = true
		}
	}
	if !found {
		t.Fatal("compileAllowedTools must include read_events_since")
	}
}

// TestACPMuxArgsQueryOmitsWriteTools enforces the read-only query surface: no
// write_wiki_page, no read_events_since, no archive, no rebuild.
func TestACPMuxArgsQueryOmitsWriteTools(t *testing.T) {
	want := wantACPMuxArgs("mcp__engram9__read_wiki_index,mcp__engram9__read_wiki_page,mcp__engram9__search_wiki")
	if got := acpmuxArgs("claude", queryAllowedTools); !reflect.DeepEqual(got, want) {
		t.Fatalf("acpmuxArgs(query) = %#v, want %#v", got, want)
	}
	forbidden := map[string]bool{
		mcpToolWriteWikiPage:   true,
		mcpToolReadEventsSince: true,
		mcpToolArchiveWikiPage: true,
		mcpToolRebuildIndex:    true,
	}
	for _, tool := range queryAllowedTools {
		if forbidden[tool] {
			t.Fatalf("queryAllowedTools must not include mutating tool %q", tool)
		}
	}
}

// TestAgentMessageChunkText verifies the answer-text extraction against the
// real acpmux session/update wire shapes. The Claude adapter streams the answer
// as agent_message_chunk notifications; the final prompt response is empty, so
// this parser is the only source of the query answer.
func TestAgentMessageChunkText(t *testing.T) {
	cases := []struct {
		name   string
		params string
		want   string
	}{
		{
			name:   "agent_message_chunk text",
			params: `{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hello world"}}}`,
			want:   "hello world",
		},
		{
			name:   "status update ignored",
			params: `{"sessionId":"s1","update":{"sessionUpdate":"status","status":"running","message":"claude turn started"}}`,
			want:   "",
		},
		{
			name:   "tool_call ignored",
			params: `{"sessionId":"s1","update":{"sessionUpdate":"tool_call","toolCallId":"t1","title":"ToolSearch","status":"in_progress"}}`,
			want:   "",
		},
		{
			name:   "tool_call_update text-content ignored (not the agent's message)",
			params: `{"sessionId":"s1","update":{"sessionUpdate":"tool_call_update","content":[{"type":"text","text":"tool result"}],"toolCallId":"t1","status":"completed"}}`,
			want:   "",
		},
		{
			name:   "non-text content ignored",
			params: `{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"image"}}}`,
			want:   "",
		},
		{name: "empty params", params: "", want: ""},
		{name: "malformed json", params: "{not json", want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := agentMessageChunkText(json.RawMessage(tc.params))
			if got != tc.want {
				t.Fatalf("agentMessageChunkText(%s) = %q, want %q", tc.params, got, tc.want)
			}
		})
	}
}

func TestACPSessionRequestUsesStagingCWD(t *testing.T) {
	req := newACPSessionRequest("/tmp/staging", "engram9-mcp")
	var params struct {
		CWD        string `json:"cwd"`
		MCPServers []struct {
			Command string   `json:"command"`
			Args    []string `json:"args"`
		} `json:"mcpServers"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		t.Fatalf("decode session params: %v", err)
	}
	if params.CWD != "/tmp/staging" {
		t.Fatalf("cwd = %q, want /tmp/staging", params.CWD)
	}
	if len(params.MCPServers) != 1 || params.MCPServers[0].Command != "engram9-mcp" {
		t.Fatalf("mcpServers = %#v, want engram9-mcp", params.MCPServers)
	}
	wantArgs := []string{"-data", "/tmp/staging", "-mode", "agent"}
	if !reflect.DeepEqual(params.MCPServers[0].Args, wantArgs) {
		t.Fatalf("mcp args = %#v, want %#v", params.MCPServers[0].Args, wantArgs)
	}
}

func TestReadACPResponseErrorField(t *testing.T) {
	// Simulate an ACP error response (e.g. initialize returns -32602).
	line := `{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid params"}}` + "\n"
	scanner := bufio.NewScanner(strings.NewReader(line))
	scanner.Buffer(make([]byte, 4<<20), 4<<20)

	resp, err := readACPResponse(scanner)
	if err != nil {
		t.Fatalf("readACPResponse returned error: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error field in response")
	}
	if resp.Error.Code != -32602 {
		t.Fatalf("error code=%d, want -32602", resp.Error.Code)
	}
}

func TestReadACPResponseEOF(t *testing.T) {
	// Empty input — readACPResponse should return io.EOF.
	scanner := bufio.NewScanner(strings.NewReader(""))
	_, err := readACPResponse(scanner)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestReadACPResponseSkipsMalformedLines(t *testing.T) {
	// First line is malformed, second is valid.
	input := "not json\n" + `{"jsonrpc":"2.0","id":1,"result":{"ok":true}}` + "\n"
	scanner := bufio.NewScanner(strings.NewReader(input))
	scanner.Buffer(make([]byte, 4<<20), 4<<20)

	resp, err := readACPResponse(scanner)
	if err != nil {
		t.Fatalf("readACPResponse returned error: %v", err)
	}
	if resp.Result == nil {
		t.Fatal("expected result in response")
	}
}

func TestReadACPResponseForIDMatchesCorrectID(t *testing.T) {
	// Response with id=1 should be returned when expecting "1".
	line := `{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}` + "\n"
	scanner := bufio.NewScanner(strings.NewReader(line))
	scanner.Buffer(make([]byte, 4<<20), 4<<20)

	resp, err := readACPResponseForID(scanner, "1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Result == nil {
		t.Fatal("expected result in response")
	}
}

func TestReadACPResponseForIDSkipsWrongID(t *testing.T) {
	// First line has id=99 (wrong), second has id=1 (correct).
	input := `{"jsonrpc":"2.0","id":99,"result":{"stale":true}}` + "\n" +
		`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}` + "\n"
	scanner := bufio.NewScanner(strings.NewReader(input))
	scanner.Buffer(make([]byte, 4<<20), 4<<20)

	resp, err := readACPResponseForID(scanner, "1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(string(resp.Result), "protocolVersion") {
		t.Fatalf("got wrong response: %s", string(resp.Result))
	}
}

func TestReadACPResponseForIDSkipsNotifications(t *testing.T) {
	// Notification (no id) followed by the expected response.
	input := `{"jsonrpc":"2.0","method":"some/notification","params":{}}` + "\n" +
		`{"jsonrpc":"2.0","id":2,"result":{"sessionId":"abc"}}` + "\n"
	scanner := bufio.NewScanner(strings.NewReader(input))
	scanner.Buffer(make([]byte, 4<<20), 4<<20)

	resp, err := readACPResponseForID(scanner, "2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(string(resp.Result), "sessionId") {
		t.Fatalf("got wrong response: %s", string(resp.Result))
	}
}

func TestReadACPResponseForIDEOFWithoutMatch(t *testing.T) {
	// Only wrong-id responses, never the expected one — should return EOF.
	input := `{"jsonrpc":"2.0","id":99,"result":{}}` + "\n"
	scanner := bufio.NewScanner(strings.NewReader(input))
	scanner.Buffer(make([]byte, 4<<20), 4<<20)

	_, err := readACPResponseForID(scanner, "1")
	if err == nil {
		t.Fatal("expected error when expected ID never appears")
	}
}

func TestACPProtocolVersionIsInteger(t *testing.T) {
	// Verify the initialize request uses integer protocolVersion, not string.
	params := mustMarshal(map[string]any{
		"protocolVersion": 1,
		"clientInfo":      map[string]string{"name": "engram9", "version": "0.1.0"},
	})
	// The JSON should contain "protocolVersion":1 (integer), not "protocolVersion":"..."
	s := string(params)
	if !strings.Contains(s, `"protocolVersion":1`) {
		t.Fatalf("protocolVersion should be integer 1, got: %s", s)
	}
}

// ─── Task #66 root 1: engram9-mcp companion fail-closed resolution ───────────

func TestNewACPBackendFailsClosedWhenEngram9McpMissing(t *testing.T) {
	// A non-resolvable engram9-mcp command must fail backend creation (fail
	// closed) rather than silently starting a tool-less session. No batch may
	// be consumed. Symmetric with the acpmux missing check.
	_, err := NewACPBackend(t.TempDir(), ACPBackendConfig{
		Provider:          "claude",
		AcpmuxCommand:     "/bin/sh", // resolvable stand-in so we reach the mcp check
		Engram9McpCommand: "/nonexistent/definitely-not-here-engram9-mcp",
	})
	if err == nil {
		t.Fatal("expected NewACPBackend to fail closed when engram9-mcp is missing")
	}
	if !strings.Contains(err.Error(), "engram9-mcp") {
		t.Fatalf("error must name the missing companion; got: %v", err)
	}
}

func TestNewACPBackendResolvesEngram9McpFromExplicitPath(t *testing.T) {
	// An explicit, existing, executable path is honored verbatim.
	_, err := NewACPBackend(t.TempDir(), ACPBackendConfig{
		Provider:          "claude",
		AcpmuxCommand:     "/bin/sh",
		Engram9McpCommand: "/bin/sh", // stand-in executable that resolves
	})
	if err != nil {
		t.Fatalf("expected resolution to succeed for an explicit executable path; got: %v", err)
	}
}

func TestResolveEngram9McpCommandRejectsDirectory(t *testing.T) {
	if _, err := resolveEngram9McpCommand(t.TempDir()); err == nil {
		t.Fatal("a directory path must not resolve as an executable")
	}
}

// ─── Task #66 root 2: chunk-boundary separator ───────────────────────────────

func TestAppendAgentMessageChunkInsertsSeparatorBetweenChunks(t *testing.T) {
	var b strings.Builder
	// Batch-13 shape: a prose block, then a verdict block. Pre-fix these were
	// concatenated into "...page.EVENT ..." on one line.
	appendAgentMessageChunk(&b, "Updated the quota page.")
	appendAgentMessageChunk(&b, "EVENT evt_1 INTEGRATED pages: a.md")
	got := b.String()
	if strings.Contains(got, "page.EVENT") {
		t.Fatalf("chunks must not run together across a boundary; got %q", got)
	}
	if got != "Updated the quota page.\nEVENT evt_1 INTEGRATED pages: a.md" {
		t.Fatalf("unexpected accumulation: %q", got)
	}
}

func TestAppendAgentMessageChunkSingleChunkUnchanged(t *testing.T) {
	var b strings.Builder
	appendAgentMessageChunk(&b, "only one chunk")
	if b.String() != "only one chunk" {
		t.Fatalf("single chunk must be unchanged; got %q", b.String())
	}
}

func TestAppendAgentMessageChunkEmptyChunkNoPhantomSeparator(t *testing.T) {
	var b strings.Builder
	appendAgentMessageChunk(&b, "first")
	appendAgentMessageChunk(&b, "") // must be skipped, no phantom "\n"
	appendAgentMessageChunk(&b, "second")
	if b.String() != "first\nsecond" {
		t.Fatalf("empty chunk must not add a phantom separator; got %q", b.String())
	}
}

func TestChunkBoundaryFixMakesVerdictParse(t *testing.T) {
	// End-to-end: the separator restores the line-start anchor so the strict
	// parser finds the verdict. This is the discriminating batch-13 regression:
	// the run-together (pre-fix) summary yields no_per_event_verdict; the
	// separated (post-fix) summary parses INTEGRATED.
	batch := Batch{ID: "b1", Events: []PendingEvent{{ID: "evt_1"}}}

	runTogether := "Updated the quota page.EVENT evt_1 INTEGRATED pages: a.md"
	pre := parseEventResults(batch, runTogether, "")
	if len(pre) != 1 || pre[0].Status != "unknown" || pre[0].Reason != UnknownReasonNoPerEventVerdict {
		t.Fatalf("run-together summary must yield no_per_event_verdict; got %+v", pre)
	}

	var b strings.Builder
	appendAgentMessageChunk(&b, "Updated the quota page.")
	appendAgentMessageChunk(&b, "EVENT evt_1 INTEGRATED pages: a.md")
	post := parseEventResults(batch, b.String(), "")
	if len(post) != 1 || post[0].Status != "integrated" {
		t.Fatalf("separated summary must parse the verdict as integrated; got %+v", post)
	}
}
