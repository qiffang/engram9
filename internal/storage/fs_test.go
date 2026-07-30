package storage

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func newTestFS(t *testing.T) *FS {
	t.Helper()
	dir := t.TempDir()
	fs, err := NewFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	return fs
}

func TestAppendAndReadEvents(t *testing.T) {
	fs := newTestFS(t)

	id, err := fs.AppendEvent(Event{
		Content:    "Alice suggests partition tables",
		Actor:      "user",
		Durability: "long-term",
		SourceType: "user",
		TrustTier:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("expected non-empty event ID")
	}

	page, err := fs.ReadEventsSince(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(page.Events))
	}
	if page.Events[0].Content != "Alice suggests partition tables" {
		t.Errorf("unexpected content: %s", page.Events[0].Content)
	}
	if page.NewCursor != 1 {
		t.Errorf("expected cursor=1, got %d", page.NewCursor)
	}

	// Reading from cursor=1 should return no events.
	page2, err := fs.ReadEventsSince(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Events) != 0 {
		t.Fatalf("expected 0 events from cursor=1, got %d", len(page2.Events))
	}
}

func TestWriteAndReadWikiPage(t *testing.T) {
	fs := newTestFS(t)

	err := fs.WriteWikiPage("semantic/projects/db9.md", "# DB9\n\nPartition tables.\n")
	if err != nil {
		t.Fatal(err)
	}

	page, err := fs.ReadWikiPage("semantic/projects/db9.md")
	if err != nil {
		t.Fatal(err)
	}
	if page.Content != "# DB9\n\nPartition tables.\n" {
		t.Errorf("unexpected content: %q", page.Content)
	}
	if page.Meta == nil {
		t.Fatal("expected non-nil meta")
	}
	if page.Meta.MemoryType != "semantic" {
		t.Errorf("expected memory_type=semantic, got %q", page.Meta.MemoryType)
	}
	if page.Meta.CreatedAt == "" {
		t.Error("expected non-empty created_at")
	}
	if len(page.Meta.AccessDates) != 1 {
		t.Errorf("expected 1 access date, got %d", len(page.Meta.AccessDates))
	}
}

func TestSearchWiki(t *testing.T) {
	fs := newTestFS(t)

	_ = fs.WriteWikiPage("semantic/projects/db9.md", "# DB9\n\nAlice suggests partition tables.\n")
	_ = fs.WriteWikiPage("semantic/people/alice.md", "# Alice\n\nSenior engineer.\n")

	results, err := fs.SearchWiki("alice")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestSearchWikiIncludesArchive(t *testing.T) {
	fs := newTestFS(t)

	_ = fs.WriteWikiPage("episodic/2026-04-12/meeting.md", "# Meeting with Alice\n\nDiscussed schema.\n")
	_ = fs.ArchiveWikiPage("episodic/2026-04-12/meeting.md", "distilled")

	// Archive pages should be searchable.
	results, err := fs.SearchWiki("Alice")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatal("expected search to find archived page")
	}
	found := false
	for _, r := range results {
		if strings.HasPrefix(r.Path, "archive/") {
			found = true
		}
	}
	if !found {
		t.Error("expected result from archive/ path")
	}
}

func TestArchiveWikiPage(t *testing.T) {
	fs := newTestFS(t)

	_ = fs.WriteWikiPage("episodic/2026-04-12/meeting.md", "# Meeting\n\nDiscussed schema.\n")

	err := fs.ArchiveWikiPage("episodic/2026-04-12/meeting.md", "distilled to semantic")
	if err != nil {
		t.Fatal(err)
	}

	// Original should not exist.
	_, err = fs.ReadWikiPage("episodic/2026-04-12/meeting.md")
	if err == nil {
		t.Fatal("expected error reading archived page")
	}

	// Archive should exist.
	archivePath := filepath.Join(fs.wikiDir(), "archive", "episodic", "2026-04-12", "meeting.md")
	if _, err := os.Stat(archivePath); err != nil {
		t.Fatalf("archive file should exist: %v", err)
	}
}

func TestRebuildIndex(t *testing.T) {
	fs := newTestFS(t)

	_ = fs.WriteWikiPage("semantic/projects/db9.md", "# DB9\n\nPartition tables are great.\n")
	_ = fs.WriteWikiPage("procedural/deploy-drive9.md", "# Deploy Drive9\n\nStep 1: build.\n")

	err := fs.RebuildIndex()
	if err != nil {
		t.Fatal(err)
	}

	idx, err := fs.ReadWikiIndex()
	if err != nil {
		t.Fatal(err)
	}
	if idx == "" {
		t.Fatal("expected non-empty index")
	}

	if !strings.Contains(idx, "db9.md") {
		t.Error("index should contain db9.md")
	}
	if !strings.Contains(idx, "deploy-drive9.md") {
		t.Error("index should contain deploy-drive9.md")
	}
}

func TestRebuildIndexGeneratesCategorySubIndexes(t *testing.T) {
	fs := newTestFS(t)

	_ = fs.WriteWikiPage("prospective/notify-alice.md", "# Notify Alice\n\nWhen drive9 releases v1.0\n")
	_ = fs.WriteWikiPage("semantic/projects/db9.md", "# DB9\n\nPartition tables.\n")

	if err := fs.RebuildIndex(); err != nil {
		t.Fatal(err)
	}

	// prospective/index.md should exist and list the page.
	data, err := os.ReadFile(filepath.Join(fs.wikiDir(), "prospective", "index.md"))
	if err != nil {
		t.Fatalf("prospective/index.md should exist: %v", err)
	}
	if !strings.Contains(string(data), "notify-alice.md") {
		t.Error("prospective/index.md should list notify-alice.md")
	}

	// semantic/index.md should exist and use category-relative paths.
	data2, err := os.ReadFile(filepath.Join(fs.wikiDir(), "semantic", "index.md"))
	if err != nil {
		t.Fatalf("semantic/index.md should exist: %v", err)
	}
	if !strings.Contains(string(data2), "db9.md") {
		t.Error("semantic/index.md should list db9.md")
	}
	// Category sub-index links must be relative to the category dir,
	// not the wiki root, so validate --strict resolves them correctly.
	if strings.Contains(string(data2), "semantic/projects/db9.md") {
		t.Error("semantic/index.md should use category-relative path (projects/db9.md), not wiki-root-relative (semantic/projects/db9.md)")
	}
	if !strings.Contains(string(data2), "(projects/db9.md)") {
		t.Error("semantic/index.md should contain link to (projects/db9.md)")
	}

	// Empty categories should still get an index.
	data3, err := os.ReadFile(filepath.Join(fs.wikiDir(), "episodic", "index.md"))
	if err != nil {
		t.Fatalf("episodic/index.md should exist: %v", err)
	}
	if !strings.Contains(string(data3), "No pages yet") {
		t.Error("empty category index should say 'No pages yet'")
	}

	for _, relPath := range []string{
		"index.md",
		"semantic/index.md",
		"episodic/index.md",
		"procedural/index.md",
		"prospective/index.md",
	} {
		content, err := os.ReadFile(filepath.Join(fs.wikiDir(), relPath))
		if err != nil {
			t.Fatalf("read generated structural index %s: %v", relPath, err)
		}
		if strings.Contains(string(content), "compiled_from:") || strings.Contains(string(content), "last_compiled:") {
			t.Errorf("generated structural index %s must not contain page frontmatter", relPath)
		}
	}
}

func TestGetMemoryStats(t *testing.T) {
	fs := newTestFS(t)

	_, _ = fs.AppendEvent(Event{Content: "test1", Durability: "long-term", SourceType: "user", TrustTier: 1})
	_, _ = fs.AppendEvent(Event{Content: "test2", Durability: "long-term", SourceType: "user", TrustTier: 1})
	_ = fs.WriteWikiPage("semantic/test.md", "# Test\n")

	stats, err := fs.GetMemoryStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.EventCount != 2 {
		t.Errorf("expected 2 events, got %d", stats.EventCount)
	}
	if stats.UncompiledCount != 2 {
		t.Errorf("expected 2 uncompiled, got %d", stats.UncompiledCount)
	}
	if stats.WikiPageCount != 1 {
		t.Errorf("expected 1 wiki page, got %d", stats.WikiPageCount)
	}

	// After setting cursor, uncompiled count should update.
	_ = fs.SetCompileCursor(1)
	stats2, _ := fs.GetMemoryStats()
	if stats2.UncompiledCount != 1 {
		t.Errorf("expected 1 uncompiled after cursor=1, got %d", stats2.UncompiledCount)
	}
}

func TestValidateWikiPath(t *testing.T) {
	tests := []struct {
		path    string
		wantErr bool
	}{
		{"semantic/db9.md", false},
		{"", true},
		{"../etc/passwd", true},
		{"/absolute/path.md", true},
		{".meta/secret.json", true},
	}
	for _, tt := range tests {
		err := validateWikiPath(tt.path)
		if (err != nil) != tt.wantErr {
			t.Errorf("validateWikiPath(%q) err=%v, wantErr=%v", tt.path, err, tt.wantErr)
		}
	}
}

func TestEventPersistence(t *testing.T) {
	dir := t.TempDir()

	// Write events.
	fs1, err := NewFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fs1.AppendEvent(Event{Content: "persisted", Durability: "long-term", SourceType: "user", TrustTier: 1})

	// Reload from disk.
	fs2, err := NewFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	page, err := fs2.ReadEventsSince(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Events[0].Content != "persisted" {
		t.Fatalf("expected persisted event, got %+v", page.Events)
	}
}

func TestCompileCursorPersistence(t *testing.T) {
	dir := t.TempDir()

	fs1, err := NewFS(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := fs1.SetCompileCursor(42); err != nil {
		t.Fatal(err)
	}
	if v := fs1.GetCompileCursor(); v != 42 {
		t.Fatalf("expected cursor=42, got %d", v)
	}

	// Reload from disk — cursor should persist.
	fs2, err := NewFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	if v := fs2.GetCompileCursor(); v != 42 {
		t.Fatalf("expected persisted cursor=42, got %d", v)
	}
}

func TestWriteWikiPageWithMeta(t *testing.T) {
	fs := newTestFS(t)

	err := fs.WriteWikiPageWithMeta(
		"semantic/projects/db9.md",
		"# DB9\n\nPartition tables [evt_042 T1]\n",
		[]string{"evt_042"},
		1,
	)
	if err != nil {
		t.Fatal(err)
	}

	page, err := fs.ReadWikiPage("semantic/projects/db9.md")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Meta.SourceEvents) != 1 || page.Meta.SourceEvents[0] != "evt_042" {
		t.Errorf("expected source_events=[evt_042], got %v", page.Meta.SourceEvents)
	}
	if page.Meta.TrustTierMax != 1 {
		t.Errorf("expected trust_tier_max=1, got %d", page.Meta.TrustTierMax)
	}

	// Write again with additional source event — should be deduplicated.
	err = fs.WriteWikiPageWithMeta(
		"semantic/projects/db9.md",
		"# DB9\n\nPartition tables [evt_042 T1] [evt_055 T2]\n",
		[]string{"evt_042", "evt_055"},
		2,
	)
	if err != nil {
		t.Fatal(err)
	}

	page2, err := fs.ReadWikiPage("semantic/projects/db9.md")
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Meta.SourceEvents) != 2 {
		t.Errorf("expected 2 source events (deduplicated), got %v", page2.Meta.SourceEvents)
	}
	// Trust tier should stay at 1 (more trusted) not downgrade to 2.
	if page2.Meta.TrustTierMax != 1 {
		t.Errorf("expected trust_tier_max=1 (kept higher trust), got %d", page2.Meta.TrustTierMax)
	}
}

func TestConcurrentPageWrites(t *testing.T) {
	fs := newTestFS(t)

	// Pre-create page.
	_ = fs.WriteWikiPage("semantic/test.md", "initial")

	// Concurrent writes to the same page should not lose data (page-level lock).
	var wg sync.WaitGroup
	n := 20
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			// Each writer reads then writes — the lock ensures serialization.
			_ = fs.WriteWikiPage("semantic/test.md", strings.Repeat("x", i+1))
		}(i)
	}
	wg.Wait()

	// Page should exist and be readable (no corruption from concurrent writes).
	page, err := fs.ReadWikiPage("semantic/test.md")
	if err != nil {
		t.Fatal(err)
	}
	if page.Content == "" {
		t.Error("expected non-empty content after concurrent writes")
	}
}

// TestReadWikiPageReadOnly_NoMetaMutation is the deterministic guard for
// invariant 12 (query zero store mutation): ReadWikiPageReadOnly must not write
// access telemetry back to the .meta sidecar. The contrast group — ordinary
// ReadWikiPage — DOES update the sidecar, which both proves the test is
// discriminating (it would catch a ReadWikiPageReadOnly that leaked a writeback)
// and documents the intended difference between the two read paths.
func TestReadWikiPageReadOnly_NoMetaMutation(t *testing.T) {
	fs := newTestFS(t)
	const page = "semantic/projects/db9.md"
	if err := fs.WriteWikiPageWithMeta(page, "# DB9\n\nPartition tables.\n", []string{"evt_1"}, 1); err != nil {
		t.Fatalf("write: %v", err)
	}

	sidecar := fs.sidecarPath(page)
	before, err := os.ReadFile(sidecar)
	if err != nil {
		t.Fatalf("read sidecar: %v", err)
	}

	// Read-only path: two reads must leave the sidecar byte-for-byte unchanged.
	for i := 0; i < 2; i++ {
		if _, err := fs.ReadWikiPageReadOnly(page); err != nil {
			t.Fatalf("ReadWikiPageReadOnly: %v", err)
		}
	}
	afterReadOnly, err := os.ReadFile(sidecar)
	if err != nil {
		t.Fatalf("read sidecar: %v", err)
	}
	if string(before) != string(afterReadOnly) {
		t.Fatalf("ReadWikiPageReadOnly mutated the .meta sidecar (invariant 12 violation):\nbefore=%s\nafter=%s", before, afterReadOnly)
	}

	// Contrast group: ordinary ReadWikiPage DOES write access telemetry, so the
	// sidecar must change — proving the read-only assertion above is discriminating.
	if _, err := fs.ReadWikiPage(page); err != nil {
		t.Fatalf("ReadWikiPage: %v", err)
	}
	afterMutating, err := os.ReadFile(sidecar)
	if err != nil {
		t.Fatalf("read sidecar: %v", err)
	}
	if string(afterMutating) == string(afterReadOnly) {
		t.Fatal("ReadWikiPage should update the .meta sidecar (contrast group); if it does not, the read-only test is not discriminating")
	}
}
