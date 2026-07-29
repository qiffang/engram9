package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/qiffang/engram9/internal/agent"
	"github.com/qiffang/engram9/internal/api"
	"github.com/qiffang/engram9/internal/okf"
	"github.com/qiffang/engram9/internal/repo"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "validate":
			os.Exit(runValidate(os.Args[2:]))
		case "migrate-okf":
			os.Exit(runMigrateOKF(os.Args[2:]))
		case "repo":
			os.Exit(runRepo(os.Args[2:]))
		}
	}
	runServe(os.Args[1:])
}

func runServe(args []string) {
	flags := flag.NewFlagSet("engram9", flag.ExitOnError)
	addr := flags.String("addr", ":9090", "listen address")
	dataDir := flags.String("data", "./data", "data directory")
	model := flags.String("model", "", "LLM model name")
	compileInterval := flags.Duration("compile-interval", 30*time.Minute, "auto-compile interval (0 to disable)")
	maxToolLoops := flags.Int("max-tool-loops", agent.DefaultMaxToolLoops, "maximum LLM tool-use loop iterations per agent request")
	maxRepeatedReadOnlyToolCalls := flags.Int("max-repeated-read-only-tool-calls", agent.DefaultMaxRepeatedReadOnlyToolCalls, "maximum consecutive identical read-only tool calls per agent request")
	maxInvalidToolCalls := flags.Int("max-invalid-tool-calls", agent.DefaultMaxInvalidToolCalls, "maximum consecutive invalid tool calls per agent request")
	ingestTimeout := flags.Duration("ingest-timeout", 0, "maximum duration for each async /remember wiki integration (0 uses ENGRAM9_INGEST_TIMEOUT/default)")
	maxConcurrentIntegrations := flags.Int("max-concurrent-integrations", 0, "maximum concurrent async /remember wiki integrations (0 uses ENGRAM9_MAX_CONCURRENT_INTEGRATIONS/default)")
	llmRetryAttempts := flags.Int("llm-retry-attempts", agent.DefaultLLMRetryAttempts, "maximum attempts for retryable LLM calls")
	llmRetryBackoff := flags.Duration("llm-retry-backoff", agent.DefaultLLMRetryBackoff, "base exponential backoff for retryable LLM calls")
	llmCallTimeout := flags.Duration("llm-call-timeout", agent.DefaultLLMCallTimeout, "per-attempt timeout for each LLM API call (0 disables per-attempt timeout)")
	_ = flags.Parse(args)

	// Resolve backend configuration.
	// Default to ACP (Claude Code / Codex) for every capability: the wiki flow
	// must run through an agent by default, NOT the OpenAI-compatible apikey
	// path. LLM is only used when a capability is EXPLICITLY set to "llm"; it is
	// never a silent fallback.
	wikiBackend := os.Getenv("WIKI_BACKEND")
	if wikiBackend == "" {
		wikiBackend = "acp"
	}
	compileBackend := os.Getenv("COMPILE_BACKEND")
	if compileBackend == "" {
		compileBackend = "acp"
	}
	queryBackend := os.Getenv("QUERY_BACKEND")
	if queryBackend == "" {
		queryBackend = "acp"
	}

	// Determine if LLM is needed (I5: lazy LLM client construction).
	// LLM is required when ANY of ingest/compile/query is on the "llm" backend.
	// With all three on "acp", no LLM client is constructed and no LLM API keys
	// are read — the zero-apikey completeness invariant.
	needLLM := wikiBackend == "llm" || compileBackend == "llm" || queryBackend == "llm"

	var llm agent.LLM
	llmProvider := ""
	llmModel := ""
	llmBaseURL := ""

	if needLLM {
		switch os.Getenv("LLM_PROVIDER") {
		case "openai":
			if os.Getenv("OPENAI_API_KEY") == "" {
				if queryBackend == "llm" {
					fmt.Fprintf(os.Stderr, "error: OPENAI_API_KEY is required because QUERY_BACKEND=llm\n")
				} else {
					fmt.Fprintf(os.Stderr, "error: OPENAI_API_KEY is required because WIKI_BACKEND=llm\n")
				}
				os.Exit(1)
			}
			openAILLM := agent.NewOpenAILLM(*model)
			llm = openAILLM
			llmProvider = "openai"
			llmModel = openAILLM.Model
			llmBaseURL = openAILLM.BaseURL
			log.Printf("using OpenAI-compatible provider (base: %s, model: %s)", llmBaseURL, llmModel)
		default:
			if os.Getenv("ANTHROPIC_API_KEY") == "" {
				if queryBackend == "llm" {
					fmt.Fprintf(os.Stderr, "error: ANTHROPIC_API_KEY is required because QUERY_BACKEND=llm\n")
				} else {
					fmt.Fprintf(os.Stderr, "error: ANTHROPIC_API_KEY is required because WIKI_BACKEND=llm\n")
				}
				os.Exit(1)
			}
			anthropicLLM := agent.NewAnthropicLLM(*model)
			llm = anthropicLLM
			llmProvider = "anthropic"
			llmModel = anthropicLLM.Model
			log.Printf("using Anthropic provider (model: %s)", llmModel)
		}
		retryAttempts := *llmRetryAttempts
		if retryAttempts <= 0 {
			retryAttempts = 1
		}
		llm = agent.NewRetryLLM(llm, agent.RetryOptions{
			MaxAttempts:       retryAttempts,
			BaseDelay:         *llmRetryBackoff,
			PerAttemptTimeout: *llmCallTimeout,
		})
	}

	// Build ACP config if needed.
	// Build the ACP config when ANY capability is on the acp backend (ingest,
	// compile, or query — all default to acp).
	needACP := wikiBackend == "acp" || compileBackend == "acp" || queryBackend == "acp"
	var acpCfg *agent.ACPBackendConfig
	if needACP {
		// Default provider is Claude Code; Codex is opt-in via ACP_PROVIDER=codex
		// once its capability matrix / e2e passes (#41).
		acpProvider := os.Getenv("ACP_PROVIDER")
		if acpProvider == "" {
			acpProvider = "claude"
		}
		acpmuxCmd := os.Getenv("ACPMUX_COMMAND")
		if acpmuxCmd == "" {
			acpmuxCmd = "acpmux"
		}
		turnTimeout := agent.DefaultACPTurnTimeout
		if raw := os.Getenv("ACP_TURN_TIMEOUT"); raw != "" {
			if secs, err := strconv.Atoi(raw); err == nil && secs > 0 {
				turnTimeout = time.Duration(secs) * time.Second
			}
		}
		maxDiffBytes := int64(agent.DefaultACPMaxDiffBytes)
		if raw := os.Getenv("ACP_MAX_DIFF_BYTES"); raw != "" {
			if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v > 0 {
				maxDiffBytes = v
			}
		}
		acpCfg = &agent.ACPBackendConfig{
			Provider:       acpProvider,
			AcpmuxCommand:  acpmuxCmd,
			TurnTimeout:    turnTimeout,
			MaxDiffBytes:   maxDiffBytes,
			AdditionalDirs: os.Getenv("ACP_ADDITIONAL_DIRS"),
		}
		log.Printf("ACP backend (provider: %s, acpmux: %s, turn_timeout: %s) — ingest=%s compile=%s query=%s", acpProvider, acpmuxCmd, turnTimeout, wikiBackend, compileBackend, queryBackend)
	}

	retryAttempts := *llmRetryAttempts
	if retryAttempts <= 0 {
		retryAttempts = 1
	}

	handler, err := api.NewWithOptions(*dataDir, llm, api.Options{
		MaxToolLoops:                 *maxToolLoops,
		MaxRepeatedReadOnlyToolCalls: *maxRepeatedReadOnlyToolCalls,
		MaxInvalidToolCalls:          *maxInvalidToolCalls,
		IngestTimeout:                *ingestTimeout,
		MaxConcurrentIntegrations:    *maxConcurrentIntegrations,
		LLMRetryAttempts:             retryAttempts,
		LLMRetryBackoff:              *llmRetryBackoff,
		LLMCallTimeout:               *llmCallTimeout,
		LLMProvider:                  llmProvider,
		LLMModel:                     llmModel,
		LLMBaseURL:                   llmBaseURL,
		WikiBackend:                  wikiBackend,
		CompileBackend:               compileBackend,
		QueryBackend:                 queryBackend,
		ACPConfig:                    acpCfg,
	})
	if err != nil {
		log.Fatalf("init: %v", err)
	}

	if err := handler.StartBatchCoordinator(60 * time.Second); err != nil {
		log.Fatalf("batch coordinator start: %v", err)
	}
	compileContext, compileCancel := context.WithCancel(context.Background())
	if *compileInterval > 0 {
		handler.StartAutoCompile(compileContext, *compileInterval)
	}

	log.Printf("engram9 listening on %s (data: %s)", *addr, *dataDir)
	log.Printf("engram9 backends: ingest=%s compile=%s query=%s", wikiBackend, compileBackend, queryBackend)
	log.Printf("engram9 runtime: max_tool_loops=%d max_repeated_read_only_tool_calls=%d max_invalid_tool_calls=%d ingest_timeout=%s max_concurrent_integrations=%d",
		handler.MaxToolLoops(),
		handler.MaxRepeatedReadOnlyToolCalls(),
		handler.MaxInvalidToolCalls(),
		handler.EffectiveIngestTimeout(),
		handler.MaxConcurrentIntegrations(),
	)
	if needLLM {
		log.Printf("engram9 llm: provider=%s model=%s retry_attempts=%d retry_backoff=%s call_timeout=%s",
			llmProvider, llmModel, retryAttempts, *llmRetryBackoff, *llmCallTimeout)
	}
	server := &http.Server{Addr: *addr, Handler: handler.Routes()}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	signal.Stop(signals)

	httpContext, cancelHTTP := context.WithTimeout(context.Background(), 10*time.Second)
	if err := server.Shutdown(httpContext); err != nil {
		log.Printf("http drain incomplete: %v — proceeding with shutdown", err)
	}
	cancelHTTP()

	compileCancel()
	coordinatorContext, cancelCoordinator := context.WithTimeout(context.Background(), 45*time.Second)
	handler.StopBatchCoordinator(coordinatorContext)
	cancelCoordinator()

	waitDone := make(chan struct{})
	go func() {
		handler.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		log.Printf("handler.Wait() timed out — exiting with in-flight work")
	}
}

func runValidate(args []string) int {
	flags := flag.NewFlagSet("engram9 validate", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	strict := flags.Bool("strict", false, "treat warnings as validation failure")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: engram9 validate [--strict] <bundle-dir>")
		return 2
	}

	result, err := okf.ValidateBundle(flags.Arg(0), *strict)
	if err != nil {
		fmt.Fprintf(os.Stderr, "validate: %v\n", err)
		return 2
	}
	for _, issue := range result.Issues {
		fmt.Fprintf(os.Stderr, "%s %s: %s\n", issue.Severity, issue.Path, issue.Message)
	}

	errors := result.ErrorCount()
	warnings := result.WarningCount()
	if errors > 0 || (*strict && warnings > 0) {
		if *strict && warnings > 0 {
			fmt.Fprintf(os.Stderr, "OKF validation failed: %d file(s), %d error(s), %d warning(s); strict mode treats warnings as failure\n", result.FilesChecked, errors, warnings)
		} else {
			fmt.Fprintf(os.Stderr, "OKF validation failed: %d file(s), %d error(s), %d warning(s)\n", result.FilesChecked, errors, warnings)
		}
		return 1
	}
	fmt.Fprintf(os.Stdout, "OKF validation passed: %d file(s), %d warning(s)\n", result.FilesChecked, warnings)
	return 0
}

func runMigrateOKF(args []string) int {
	flags := flag.NewFlagSet("engram9 migrate-okf", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	write := flags.Bool("write", false, "rewrite files in place")
	check := flags.Bool("check", false, "exit 1 if migration would change files")
	backup := flags.Bool("backup", true, "create .bak files before rewriting when --write is set")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: engram9 migrate-okf [--write] [--check] [--backup=false] <bundle-dir>")
		return 2
	}
	if *write && *check {
		fmt.Fprintln(os.Stderr, "migrate-okf: --write and --check cannot be used together")
		return 2
	}

	result, err := okf.MigrateLegacyBundle(flags.Arg(0), okf.MigrationOptions{Write: *write, Backup: *backup})
	if err != nil {
		fmt.Fprintf(os.Stderr, "migrate-okf: %v\n", err)
		return 2
	}
	for _, change := range result.Changes {
		if *write {
			fmt.Fprintf(os.Stdout, "migrated %s\n", change.Path)
		} else {
			fmt.Fprintf(os.Stdout, "would migrate %s\n", change.Path)
		}
	}
	if *write {
		fmt.Fprintf(os.Stdout, "OKF migration wrote: %d file(s), %d change(s)\n", result.FilesChecked, result.ChangedCount())
	} else {
		fmt.Fprintf(os.Stdout, "OKF migration dry-run: %d file(s), %d change(s)\n", result.FilesChecked, result.ChangedCount())
	}
	if *check && result.ChangedCount() > 0 {
		return 1
	}
	return 0
}

func runRepo(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: engram9 repo <scan> [options]")
		return 2
	}
	switch args[0] {
	case "scan":
		return runRepoScan(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown repo subcommand: %s\n", args[0])
		return 2
	}
}

func runRepoScan(args []string) int {
	flags := flag.NewFlagSet("engram9 repo scan", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	repoPath := flags.String("path", ".", "git repository path")
	scope := flags.String("scope", ".", "repo-relative scope to scan")
	since := flags.String("since", "", "base commit for incremental scan")
	outDir := flags.String("out", "", "output directory; writes manifest.json, facts.jsonl, and snippets.jsonl (default: JSON to stdout)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: engram9 repo scan [--path <repo>] [--scope <repo-rel-path>] [--since <commit>] [--out <dir>]")
		return 2
	}
	bundle, err := repo.Scan(repo.ScanOptions{
		RepoPath: *repoPath,
		Scope:    *scope,
		Since:    *since,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "repo scan: %v\n", err)
		return 1
	}
	if err := repo.WriteBundle(bundle, *outDir); err != nil {
		fmt.Fprintf(os.Stderr, "repo scan: write output: %v\n", err)
		return 1
	}
	if *outDir != "" {
		fmt.Fprintf(os.Stdout, "repo scan wrote %d fact(s), %d file(s): %s\n", len(bundle.Facts), len(bundle.Manifest.Files), *outDir)
	}
	return 0
}
