package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/qiffang/engram9/internal/storage"
)

// RunQuery answers one question read-only through a query-mode engram9-mcp.
// The store is never mutated (Unit 1 registers no write tool and uses the
// mutation-free read path, including no .meta telemetry — invariant 12).
//
// Pending-event recall parity (adversary-1 A4): recentEvents are injected into
// the prompt exactly as the LLM path does (same data source, same format), so a
// query answered while integration is still pending reflects not-yet-integrated
// information without any new tool or mutable-store read path.
func (b *ACPBackend) RunQuery(ctx context.Context, question string, ctxInfo map[string]string, recentEvents []storage.Event) (QueryResult, error) {
	prompt := buildQueryPrompt(question, ctxInfo, recentEvents)

	opts := acpTurnOptions{
		allowedTools: queryAllowedTools,
		mcpArgs: func(stagingDir string) []string {
			return []string{"-data", stagingDir, "-mode", "query"}
		},
		skipMerge: true, // read-only: never write back to the live store.
	}

	result, err := b.runACPTurnFullOpts(ctx, prompt, ValidateOptions{AllowDelete: false}, opts)
	if err != nil {
		return QueryResult{}, err
	}
	return QueryResult{Answer: result.Summary}, nil
}

// buildQueryPrompt assembles the query user message, injecting recent events
// verbatim in the LLM path's format (query.go Recall) for parity.
func buildQueryPrompt(question string, ctxInfo map[string]string, recentEvents []storage.Event) string {
	userMsg := fmt.Sprintf("Recall/answer this question:\n\n%s", question)
	if len(ctxInfo) > 0 {
		ctxJSON, _ := json.Marshal(ctxInfo)
		userMsg += fmt.Sprintf("\n\nCurrent context: %s", string(ctxJSON))
	}
	if len(recentEvents) > 0 {
		userMsg += "\n\n---\nRecent events (may not be in wiki yet):\n"
		for _, ev := range recentEvents {
			var parts []string
			parts = append(parts, fmt.Sprintf("[%s] %s", ev.ID, ev.Content))
			if ev.ActiveProject != "" {
				parts = append(parts, fmt.Sprintf("  project=%s", ev.ActiveProject))
			}
			if ev.ActiveTask != "" {
				parts = append(parts, fmt.Sprintf("  task=%s", ev.ActiveTask))
			}
			userMsg += strings.Join(parts, "") + "\n"
		}
	}
	return userMsg + "\n\n" + querySystemPrompt
}
