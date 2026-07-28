// engram9-mcp is a Model Context Protocol (MCP) server for engram9.
//
// It exposes engram9 knowledge bundles as MCP tools that Claude, Codex, Pi,
// and other MCP-compatible agents can consume over stdio.
//
// Usage:
//
//	# Consume an OKF bundle directly (read-only)
//	engram9-mcp -bundle ./examples/repo-memory
//
//	# Consume the engram9 runtime data directory (read-write)
//	engram9-mcp -data ./data
//
// The server reads JSON-RPC 2.0 requests from stdin and writes responses to
// stdout, one JSON object per line. Diagnostic logs go to stderr.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/qiffang/engram9/internal/mcp"
	"github.com/qiffang/engram9/internal/storage"
)

func main() {
	dataDir := flag.String("data", "", "runtime data directory (same as engram9 HTTP server)")
	bundleDir := flag.String("bundle", "", "OKF bundle directory to consume (read-only)")
	mode := flag.String("mode", "consumption", "tool mode: consumption | agent | compile | query")
	turnID := flag.String("turn-id", "", "compile mode: per-turn freshness nonce stamped into each receipt entry")
	eventBound := flag.Uint64("event-bound", 0, "compile mode: pre-turn event count; read_events_since serves only events below this bound")
	receiptPath := flag.String("receipt", "", "compile mode: path where read_events_since appends receipt entries")
	flag.Parse()

	// Direct all log output to stderr so stdout is clean JSON-RPC.
	log.SetOutput(os.Stderr)

	if *dataDir == "" && *bundleDir == "" {
		fmt.Fprintln(os.Stderr, "error: specify either -data (runtime store) or -bundle (OKF bundle)")
		flag.Usage()
		os.Exit(1)
	}
	if *dataDir != "" && *bundleDir != "" {
		fmt.Fprintln(os.Stderr, "error: -data and -bundle are mutually exclusive")
		flag.Usage()
		os.Exit(1)
	}

	var store storage.Store
	var err error

	if *bundleDir != "" {
		store, err = storage.NewBundleFS(*bundleDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: init bundle store: %v\n", err)
			os.Exit(1)
		}
		log.Printf("engram9-mcp started in bundle mode (bundle: %s)", *bundleDir)
	} else {
		store, err = storage.NewFS(*dataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: init runtime store: %v\n", err)
			os.Exit(1)
		}
		log.Printf("engram9-mcp started in runtime mode (data: %s)", *dataDir)
	}

	var server *mcp.Server
	switch *mode {
	case "consumption", "":
		server = mcp.NewServerWithMode(store, mcp.ModeConsumption)
	case "agent":
		if *bundleDir != "" {
			fmt.Fprintln(os.Stderr, "error: -mode agent requires -data (read-write store); -bundle is read-only")
			os.Exit(1)
		}
		server = mcp.NewServerWithMode(store, mcp.ModeAgent)
	case "compile":
		if *bundleDir != "" {
			fmt.Fprintln(os.Stderr, "error: -mode compile requires -data (read-write store); -bundle is read-only")
			os.Exit(1)
		}
		if *turnID == "" || *receiptPath == "" {
			fmt.Fprintln(os.Stderr, "error: -mode compile requires -turn-id and -receipt")
			os.Exit(1)
		}
		server = mcp.NewCompileServer(store, *turnID, *eventBound, *receiptPath)
	case "query":
		// Query is strictly read-only; it works over either a runtime store or
		// a bundle. The query tool surface registers no write tool and uses the
		// mutation-free read path (invariant 12).
		server = mcp.NewServerWithMode(store, mcp.ModeQuery)
	default:
		fmt.Fprintf(os.Stderr, "error: unknown mode %q (use 'consumption', 'agent', 'compile', or 'query')\n", *mode)
		os.Exit(1)
	}

	log.Printf("engram9-mcp mode: %s", *mode)
	if err := server.Serve(os.Stdin, os.Stdout); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
