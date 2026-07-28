package agent

import (
	"strings"
	"testing"

	"github.com/qiffang/engram9/internal/storage"
)

// A4 parity: recent (pending-integration) events are injected into the query
// prompt verbatim in the LLM path's format, so an ACP query reflects
// not-yet-integrated information.
func TestBuildQueryPrompt_InjectsRecentEvents(t *testing.T) {
	events := []storage.Event{
		{ID: "evt_1", Content: "user prefers dark mode", ActiveProject: "webapp"},
		{ID: "evt_2", Content: "deadline moved to Friday", ActiveTask: "release"},
	}
	prompt := buildQueryPrompt("what does the user prefer?", nil, events)

	if !strings.Contains(prompt, "Recent events (may not be in wiki yet):") {
		t.Fatal("prompt missing recent-events header")
	}
	if !strings.Contains(prompt, "[evt_1] user prefers dark mode") {
		t.Fatal("prompt missing evt_1 content")
	}
	if !strings.Contains(prompt, "project=webapp") {
		t.Fatal("prompt missing evt_1 project")
	}
	if !strings.Contains(prompt, "[evt_2] deadline moved to Friday") {
		t.Fatal("prompt missing evt_2 content")
	}
	if !strings.Contains(prompt, "task=release") {
		t.Fatal("prompt missing evt_2 task")
	}
}

// With no recent events, the prompt has no recall block (parity with LLM path).
func TestBuildQueryPrompt_NoRecentEvents(t *testing.T) {
	prompt := buildQueryPrompt("q?", nil, nil)
	if strings.Contains(prompt, "Recent events") {
		t.Fatal("prompt should have no recall block when there are no recent events")
	}
	if !strings.Contains(prompt, "q?") {
		t.Fatal("prompt missing question")
	}
}

// Invariant 1/12: the LLM QueryTools set must be strictly read-only — no
// write_wiki_page (inventory ⚠ removed).
func TestQueryToolsAreReadOnly(t *testing.T) {
	for _, tool := range QueryTools {
		if tool.Name == "write_wiki_page" {
			t.Fatal("QueryTools must not include write_wiki_page (invariant 1/12)")
		}
	}
}
