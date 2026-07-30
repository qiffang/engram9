package agent

import "encoding/json"

// Tool definitions with JSON schemas for the Anthropic tool-use API.

var ToolAppendEvent = Tool{
	Name:        "append_event",
	Description: "Write a new memory event to the raw append-only log. Use this to record new information before weaving it into wiki pages.",
	InputSchema: mustJSON(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"content":        map[string]any{"type": "string", "description": "The raw content/information to remember"},
			"actor":          map[string]any{"type": "string", "description": "Who provided this info: user, assistant, tool, system"},
			"source":         map[string]any{"type": "string", "description": "Where this came from: conversation, observation, tool_output"},
			"session_id":     map[string]any{"type": "string", "description": "Current session identifier"},
			"active_project": map[string]any{"type": "string", "description": "Current project context"},
			"active_task":    map[string]any{"type": "string", "description": "Current task context"},
			"durability":     map[string]any{"type": "string", "enum": []string{"ephemeral", "session", "long-term", "permanent"}, "description": "How long this memory should persist"},
			"actionability":  map[string]any{"type": "string", "enum": []string{"none", "informational", "actionable", "urgent"}, "description": "How actionable this information is"},
			"source_type":    map[string]any{"type": "string", "enum": []string{"user", "assistant", "tool", "system", "compiler"}, "description": "Type of information source"},
			"evidence_kind":  map[string]any{"type": "string", "enum": []string{"direct_observation", "user_statement", "inferred", "compiler_synthesis"}, "description": "Kind of evidence"},
			"trust_tier":     map[string]any{"type": "integer", "enum": []int{1, 2, 3}, "description": "Trust level: 1=high (user direct), 2=medium (tool/inferred), 3=low (second-hand)"},
		},
		"required": []string{"content", "durability", "source_type", "evidence_kind", "trust_tier"},
	}),
}

var ToolReadEventsSince = Tool{
	Name:        "read_events_since",
	Description: "Read raw events after a cursor position. Used by compile_agent to process uncompiled events.",
	ReadOnly:    true,
	InputSchema: mustJSON(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"cursor": map[string]any{"type": "integer", "description": "Read events after this position (0 = from beginning)"},
		},
		"required": []string{"cursor"},
	}),
}

var ToolReadWikiIndex = Tool{
	Name:        "read_wiki_index",
	Description: "Read the wiki index (routing table) to understand the current knowledge structure. Always read this first before accessing specific pages.",
	ReadOnly:    true,
	InputSchema: mustJSON(map[string]any{
		"type":       "object",
		"properties": map[string]any{},
	}),
}

var ToolReadWikiPage = Tool{
	Name:        "read_wiki_page",
	Description: "Read a wiki page and its metadata. Automatically tracks access for memory strengthening.",
	ReadOnly:    true,
	InputSchema: mustJSON(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path": map[string]any{"type": "string", "description": "Relative path within wiki/ (e.g. semantic/projects/db9.md)"},
		},
		"required": []string{"path"},
	}),
}

var ToolSearchWiki = Tool{
	Name:        "search_wiki",
	Description: "Search across all wiki pages (including archived) by keyword. Use when you need to find information not easily located via the index. Archived pages can be recovered if found useful.",
	ReadOnly:    true,
	InputSchema: mustJSON(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{"type": "string", "description": "Search query (case-insensitive text match)"},
		},
		"required": []string{"query"},
	}),
}

var ToolWriteWikiPage = Tool{
	Name:        "write_wiki_page",
	Description: "Create or update a wiki page. Path must start with semantic/, episodic/, procedural/, prospective/, or be index.md. Frontmatter (compiled_from, last_compiled) is auto-injected if missing. Pass source_events and trust_tier to track provenance.",
	InputSchema: mustJSON(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path":          map[string]any{"type": "string", "description": "Relative path: must start with semantic/, episodic/, procedural/, prospective/, or be index.md (e.g. semantic/projects/db9.md)"},
			"content":       map[string]any{"type": "string", "description": "Full markdown content of the page"},
			"source_events": map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Event IDs that contributed to this page (e.g. [\"evt_xxx\", \"evt_yyy\"])"},
			"trust_tier":    map[string]any{"type": "integer", "enum": []int{1, 2, 3}, "description": "Highest trust level among sources: 1=user direct, 2=tool/inferred, 3=second-hand"},
		},
		"required": []string{"path", "content"},
	}),
}

var ToolArchiveWikiPage = Tool{
	Name:        "archive_wiki_page",
	Description: "Move a wiki page to the archive. Used for forgetting: distilled episodic pages, superseded knowledge, completed intentions.",
	InputSchema: mustJSON(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path":   map[string]any{"type": "string", "description": "Path of the page to archive"},
			"reason": map[string]any{"type": "string", "description": "Why this page is being archived"},
		},
		"required": []string{"path", "reason"},
	}),
}

var ToolRebuildIndex = Tool{
	Name:        "rebuild_index",
	Description: "Rebuild the wiki index.md by scanning all active pages. Call after significant wiki changes.",
	InputSchema: mustJSON(map[string]any{
		"type":       "object",
		"properties": map[string]any{},
	}),
}

var ToolGetMemoryStats = Tool{
	Name:        "get_memory_stats",
	Description: "Get system statistics: event counts, wiki page counts, uncompiled event count.",
	ReadOnly:    true,
	InputSchema: mustJSON(map[string]any{
		"type":       "object",
		"properties": map[string]any{},
	}),
}

// Tool sets for each agent role.
var (
	IngestTools = []Tool{ToolAppendEvent, ToolReadWikiIndex, ToolReadWikiPage, ToolWriteWikiPage, ToolSearchWiki}
	// QueryTools is strictly read-only (invariant 1/12): the query agent must
	// not be able to mutate the wiki on any backend. WriteWikiPage was present
	// here historically (inventory ⚠ in #41) and is removed.
	QueryTools   = []Tool{ToolReadWikiIndex, ToolReadWikiPage, ToolSearchWiki}
	CompileTools = []Tool{ToolReadEventsSince, ToolReadWikiIndex, ToolReadWikiPage, ToolWriteWikiPage, ToolArchiveWikiPage, ToolRebuildIndex, ToolSearchWiki}
)

func mustJSON(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
