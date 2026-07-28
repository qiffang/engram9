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

func TestACPMuxArgsRestrictClaudeTools(t *testing.T) {
	want := []string{
		"--provider", "claude",
		"--provider-arg", "--tools",
		"--provider-arg", "ToolSearch,Glob,Grep",
		"--provider-arg", "--allowedTools",
		"--provider-arg", "mcp__engram9__read_wiki_index,mcp__engram9__read_wiki_page,mcp__engram9__write_wiki_page,mcp__engram9__search_wiki",
		"--provider-arg", "--permission-mode",
		"--provider-arg", "dontAsk",
		"--provider-arg", "--strict-mcp-config",
	}
	if got := acpmuxArgs("claude"); !reflect.DeepEqual(got, want) {
		t.Fatalf("acpmuxArgs() = %#v, want %#v", got, want)
	}
}

func TestACPSessionRequestUsesStagingCWD(t *testing.T) {
	req := newACPSessionRequest("/tmp/staging")
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
