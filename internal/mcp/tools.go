package mcp

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/qiffang/engram9/internal/agent"
	"github.com/qiffang/engram9/internal/storage"
)

// MCPTools defines the tools exposed to MCP clients.
// These are the consumption surface for OKF bundles:
//   - search_concepts: keyword search across wiki pages
//   - read_concept: read a specific wiki page with metadata
//   - neighbors: list related pages from the wiki index
//   - write_memory: store new information into the memory system
//   - memory_status: get system statistics
var MCPTools = []mcpTool{
	{
		Name:        "search_concepts",
		Description: "Search across all knowledge pages by keyword. Returns matching lines with file paths. Use this to find relevant concepts before reading full pages.",
		InputSchema: mustJSON(map[string]any{
			"type": "object",
			"properties": map[string]any{
				"query": map[string]any{
					"type":        "string",
					"description": "Search query (case-insensitive text match across all wiki pages)",
				},
			},
			"required": []string{"query"},
		}),
	},
	{
		Name:        "read_concept",
		Description: "Read a specific knowledge page by path. Returns the full Markdown content and metadata (memory type, trust tier, source events, access history). Paths are relative to wiki/ (e.g., 'semantic/projects/db9.md').",
		InputSchema: mustJSON(map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{
					"type":        "string",
					"description": "Relative path within the wiki (e.g., 'semantic/projects/db9.md')",
				},
			},
			"required": []string{"path"},
		}),
	},
	{
		Name:        "neighbors",
		Description: "List all knowledge pages in the wiki index, organized by memory type (semantic, episodic, procedural, prospective). Use this to discover available knowledge and find related concepts.",
		InputSchema: mustJSON(map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		}),
	},
	{
		Name:        "write_memory",
		Description: "Store new information into the memory system. The information is appended as a raw event and will be integrated into wiki pages by the ingest agent. Returns the event ID for tracking.",
		InputSchema: mustJSON(map[string]any{
			"type": "object",
			"properties": map[string]any{
				"text": map[string]any{
					"type":        "string",
					"description": "The information to remember",
				},
				"actor": map[string]any{
					"type":        "string",
					"description": "Who is providing this information (e.g., 'claude', 'codex', 'user')",
				},
				"source": map[string]any{
					"type":        "string",
					"description": "Where this came from (e.g., 'conversation', 'code_review', 'observation')",
				},
			},
			"required": []string{"text"},
		}),
	},
	{
		Name:        "memory_status",
		Description: "Get system statistics: total events, uncompiled events, active wiki pages, archived pages.",
		InputSchema: mustJSON(map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		}),
	},
}

// AgentMCPTools defines the tools exposed in agent mode.
// These mirror the IntegrateTools from internal/agent/tooldef.go.
var AgentMCPTools = []mcpTool{
	{
		Name:        "read_wiki_index",
		Description: "Read the wiki index (routing table) to understand the current knowledge structure. Always read this first before accessing specific pages.",
		InputSchema: mustJSON(map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		}),
	},
	{
		Name:        "read_wiki_page",
		Description: "Read a wiki page and its metadata. Automatically tracks access for memory strengthening.",
		InputSchema: mustJSON(map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{"type": "string", "description": "Relative path within wiki/ (e.g. semantic/projects/db9.md)"},
			},
			"required": []string{"path"},
		}),
	},
	{
		Name:        "write_wiki_page",
		Description: "Create or update a wiki page. Pass source_events and trust_tier to track provenance in sidecar metadata.",
		InputSchema: mustJSON(map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path":          map[string]any{"type": "string", "description": "Relative path within wiki/ (e.g. semantic/projects/db9.md)"},
				"content":       map[string]any{"type": "string", "description": "Full markdown content of the page"},
				"source_events": map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Event IDs that contributed to this page"},
				"trust_tier":    map[string]any{"type": "integer", "enum": []int{1, 2, 3}, "description": "Highest trust level among sources: 1=user direct, 2=tool/inferred, 3=second-hand"},
			},
			"required": []string{"path", "content"},
		}),
	},
	{
		Name:        "search_wiki",
		Description: "Search across all wiki pages by keyword. Returns matching lines with file paths.",
		InputSchema: mustJSON(map[string]any{
			"type": "object",
			"properties": map[string]any{
				"query": map[string]any{"type": "string", "description": "Search query (case-insensitive text match)"},
			},
			"required": []string{"query"},
		}),
	},
}

// readEventsSinceTool, archiveWikiPageTool, rebuildIndexTool are the extra
// tools the compile capability needs beyond the agent (ingest) surface.
var readEventsSinceTool = mcpTool{
	Name:        "read_events_since",
	Description: "Read unprocessed raw events after the given cursor. Returns the events and a new_cursor. Compile agents call this exactly once per cycle to fetch the batch to distill.",
	InputSchema: mustJSON(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"cursor": map[string]any{"type": "integer", "description": "Start cursor (exclusive lower bound); pass the compile cursor provided in the prompt"},
		},
		"required": []string{"cursor"},
	}),
}

var archiveWikiPageTool = mcpTool{
	Name:        "archive_wiki_page",
	Description: "Archive a wiki page (move it out of active wiki) with a reason. Use during sleep-pruning per memory-type rules.",
	InputSchema: mustJSON(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path":   map[string]any{"type": "string", "description": "Relative path within wiki/ (e.g. episodic/foo.md)"},
			"reason": map[string]any{"type": "string", "description": "Why the page is being archived"},
		},
		"required": []string{"path", "reason"},
	}),
}

var rebuildIndexTool = mcpTool{
	Name:        "rebuild_index",
	Description: "Regenerate the wiki index (routing table) after significant page changes. Call at the end of a compile cycle.",
	InputSchema: mustJSON(map[string]any{
		"type":       "object",
		"properties": map[string]any{},
	}),
}

// CompileMCPTools is the compile capability surface: the agent-mode wiki tools
// plus read_events_since, archive_wiki_page, and rebuild_index. The tool
// surface IS the capability boundary (invariant 1).
var CompileMCPTools = append(append([]mcpTool{}, AgentMCPTools...),
	readEventsSinceTool, archiveWikiPageTool, rebuildIndexTool)

// QueryMCPTools is the strictly read-only query surface: NO write tool is
// registered at all (invariant 1/12). read_wiki_page here maps to a read-only
// store read that writes no access telemetry.
var QueryMCPTools = []mcpTool{
	AgentMCPTools[0], // read_wiki_index
	AgentMCPTools[1], // read_wiki_page
	AgentMCPTools[3], // search_wiki
}

// executeTool dispatches an MCP tool call to the storage layer.
func (s *Server) executeTool(name string, args map[string]any) (string, error) {
	// Agent mode tools.
	if s.mode == ModeAgent {
		return s.executeAgentTool(name, args)
	}
	if s.mode == ModeCompile {
		return s.executeCompileTool(name, args)
	}
	if s.mode == ModeQuery {
		return s.executeQueryTool(name, args)
	}

	// Consumption mode tools.
	switch name {
	case "search_concepts":
		return s.execSearchConcepts(args)
	case "read_concept":
		return s.execReadConcept(args)
	case "neighbors":
		return s.execNeighbors()
	case "write_memory":
		return s.execWriteMemory(args)
	case "memory_status":
		return s.execMemoryStatus()
	default:
		return "", fmt.Errorf("unknown tool: %s", name)
	}
}

func (s *Server) executeAgentTool(name string, args map[string]any) (string, error) {
	switch name {
	case "read_wiki_index":
		return s.execAgentReadWikiIndex()
	case "read_wiki_page":
		return s.execAgentReadWikiPage(args)
	case "write_wiki_page":
		return s.execAgentWriteWikiPage(args)
	case "search_wiki":
		return s.execAgentSearchWiki(args)
	default:
		return "", fmt.Errorf("unknown tool: %s", name)
	}
}

func (s *Server) execAgentReadWikiIndex() (string, error) {
	content, err := s.store.ReadWikiIndex()
	if err != nil {
		return "", err
	}
	if content == "" {
		return "_No wiki index yet. Wiki is empty._", nil
	}
	return content, nil
}

func (s *Server) execAgentReadWikiPage(args map[string]any) (string, error) {
	path, ok := args["path"].(string)
	if !ok || path == "" {
		return "", fmt.Errorf("path is required")
	}
	if err := validatePath(path); err != nil {
		return "", err
	}
	page, err := s.store.ReadWikiPage(path)
	if err != nil {
		return "", err
	}
	data, _ := json.Marshal(page)
	return string(data), nil
}

func (s *Server) execAgentWriteWikiPage(args map[string]any) (string, error) {
	path, ok := args["path"].(string)
	if !ok || path == "" {
		return "", fmt.Errorf("path is required")
	}
	content, ok := args["content"].(string)
	if !ok || content == "" {
		return "", fmt.Errorf("content is required")
	}
	if err := validatePath(path); err != nil {
		return "", err
	}
	if !agent.IsValidWikiPath(path) {
		return "", fmt.Errorf("invalid wiki path %q: must start with semantic/, episodic/, procedural/, prospective/, or be index.md", path)
	}

	var sourceEvents []string
	if se, ok := args["source_events"].([]any); ok {
		for _, v := range se {
			if s, ok := v.(string); ok {
				sourceEvents = append(sourceEvents, s)
			}
		}
	}

	trustTier := 0
	if tt, ok := args["trust_tier"].(float64); ok {
		trustTier = int(tt)
	}

	content = agent.EnsureFrontmatter(content, sourceEvents, time.Now())
	if err := s.store.WriteWikiPageWithMeta(path, content, sourceEvents, trustTier); err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"status": "ok", "path": "%s"}`, path), nil
}

func (s *Server) execAgentSearchWiki(args map[string]any) (string, error) {
	query, ok := args["query"].(string)
	if !ok || query == "" {
		return "", fmt.Errorf("query is required")
	}
	results, err := s.store.SearchWiki(query)
	if err != nil {
		return "", err
	}
	if len(results) == 0 {
		return "No results found.", nil
	}
	data, _ := json.Marshal(results)
	return string(data), nil
}

// executeCompileTool dispatches compile-mode tool calls. It reuses the agent
// wiki tools (read/write/search) and adds read_events_since (snapshot-bounded +
// receipt-emitting), archive_wiki_page, and rebuild_index.
func (s *Server) executeCompileTool(name string, args map[string]any) (string, error) {
	switch name {
	case "read_wiki_index":
		return s.execAgentReadWikiIndex()
	case "read_wiki_page":
		return s.execAgentReadWikiPage(args)
	case "write_wiki_page":
		return s.execAgentWriteWikiPage(args)
	case "search_wiki":
		return s.execAgentSearchWiki(args)
	case "read_events_since":
		return s.execCompileReadEventsSince(args)
	case "archive_wiki_page":
		return s.execCompileArchiveWikiPage(args)
	case "rebuild_index":
		return s.execCompileRebuildIndex()
	default:
		return "", fmt.Errorf("unknown tool: %s", name)
	}
}

// executeQueryTool dispatches read-only query tool calls. read_wiki_page uses
// the mutation-free store read so a query turn writes zero store state,
// including .meta telemetry (invariant 12).
func (s *Server) executeQueryTool(name string, args map[string]any) (string, error) {
	switch name {
	case "read_wiki_index":
		return s.execAgentReadWikiIndex()
	case "read_wiki_page":
		return s.execQueryReadWikiPage(args)
	case "search_wiki":
		return s.execAgentSearchWiki(args)
	default:
		return "", fmt.Errorf("unknown tool: %s", name)
	}
}

// execQueryReadWikiPage reads a page with zero store mutation (no .meta
// access-telemetry writeback).
func (s *Server) execQueryReadWikiPage(args map[string]any) (string, error) {
	path, ok := args["path"].(string)
	if !ok || path == "" {
		return "", fmt.Errorf("path is required")
	}
	if err := validatePath(path); err != nil {
		return "", err
	}
	page, err := s.store.ReadWikiPageReadOnly(path)
	if err != nil {
		return "", err
	}
	data, _ := json.Marshal(page)
	return string(data), nil
}

func (s *Server) execSearchConcepts(args map[string]any) (string, error) {
	query, ok := args["query"].(string)
	if !ok || query == "" {
		return "", fmt.Errorf("query is required")
	}

	results, err := s.store.SearchWiki(query)
	if err != nil {
		return "", fmt.Errorf("search: %w", err)
	}
	if len(results) == 0 {
		return "No results found.", nil
	}

	// Format as readable text with paths and matching lines.
	var sb strings.Builder
	fmt.Fprintf(&sb, "Found %d match(es):\n\n", len(results))
	for _, r := range results {
		fmt.Fprintf(&sb, "- %s (line %d): %s\n", r.Path, r.Line, r.Content)
	}
	return sb.String(), nil
}

// validatePath rejects obviously malicious paths at the MCP layer before
// they reach the storage layer. Defense in depth — the store also validates.
func validatePath(path string) error {
	if path == "" {
		return fmt.Errorf("path is required")
	}
	if strings.HasPrefix(path, "/") {
		return fmt.Errorf("path must be relative, not absolute")
	}
	if strings.Contains(path, "..") {
		return fmt.Errorf("path traversal not allowed")
	}
	if strings.HasPrefix(path, ".meta/") {
		return fmt.Errorf("cannot access metadata directly")
	}
	return nil
}

func (s *Server) execReadConcept(args map[string]any) (string, error) {
	path, ok := args["path"].(string)
	if !ok || path == "" {
		return "", fmt.Errorf("path is required")
	}
	if err := validatePath(path); err != nil {
		return "", err
	}

	page, err := s.store.ReadWikiPage(path)
	if err != nil {
		return "", fmt.Errorf("read page: %w", err)
	}

	// Format as structured output.
	var sb strings.Builder
	fmt.Fprintf(&sb, "# %s\n\n", path)
	if page.Meta != nil {
		if page.Meta.MemoryType != "" {
			fmt.Fprintf(&sb, "Memory type: %s\n", page.Meta.MemoryType)
		}
		if page.Meta.TrustTierMax > 0 {
			fmt.Fprintf(&sb, "Trust tier: T%d\n", page.Meta.TrustTierMax)
		}
		if len(page.Meta.SourceEvents) > 0 {
			fmt.Fprintf(&sb, "Sources: %s\n", strings.Join(page.Meta.SourceEvents, ", "))
		}
		if page.Meta.LastAccessed != "" {
			fmt.Fprintf(&sb, "Last accessed: %s\n", page.Meta.LastAccessed)
		}
		sb.WriteString("\n---\n\n")
	}
	sb.WriteString(page.Content)
	return sb.String(), nil
}

func (s *Server) execNeighbors() (string, error) {
	content, err := s.store.ReadWikiIndex()
	if err != nil {
		return "", fmt.Errorf("read index: %w", err)
	}
	if content == "" {
		return "Wiki is empty. No knowledge pages yet.", nil
	}
	return content, nil
}

func (s *Server) execWriteMemory(args map[string]any) (string, error) {
	text, ok := args["text"].(string)
	if !ok || text == "" {
		return "", fmt.Errorf("text is required")
	}

	actor, _ := args["actor"].(string)
	source, _ := args["source"].(string)
	if actor == "" {
		actor = "mcp_client"
	}
	if source == "" {
		source = "mcp"
	}

	ev := storage.Event{
		Content:       text,
		Actor:         actor,
		Source:        source,
		Durability:    "long-term",
		Actionability: "informational",
		SourceType:    "assistant",
		EvidenceKind:  "direct_observation",
		TrustTier:     2,
	}

	eventID, err := s.store.AppendEvent(ev)
	if err != nil {
		return "", fmt.Errorf("append event: %w", err)
	}

	return fmt.Sprintf("Stored as %s. Will be integrated into wiki pages on next compile cycle.", eventID), nil
}

func (s *Server) execMemoryStatus() (string, error) {
	stats, err := s.store.GetMemoryStats()
	if err != nil {
		return "", fmt.Errorf("get stats: %w", err)
	}

	data, _ := json.MarshalIndent(stats, "", "  ")
	return string(data), nil
}

func mustJSON(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
