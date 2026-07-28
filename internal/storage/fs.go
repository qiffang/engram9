package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FS implements Store using local filesystem.
// raw/ → JSONL file, wiki/ → markdown files, .meta/ → sidecar JSON.
type FS struct {
	dataDir string

	mu     sync.Mutex
	events []Event // in-memory cache loaded from JSONL

	// pageMu provides per-page locking to prevent concurrent read/write/archive
	// from clobbering each other's sidecar metadata.
	pageMu    sync.Mutex
	pageLocks map[string]*sync.Mutex

	// indexMu serializes RebuildIndex calls.
	indexMu sync.Mutex
}

// NewFS creates a filesystem-backed Store rooted at dataDir.
// It initializes the directory structure and loads existing events.
func NewFS(dataDir string) (*FS, error) {
	dirs := []string{
		filepath.Join(dataDir, "raw"),
		filepath.Join(dataDir, "wiki"),
		filepath.Join(dataDir, "wiki", ".meta"),
		filepath.Join(dataDir, "wiki", "semantic"),
		filepath.Join(dataDir, "wiki", "episodic"),
		filepath.Join(dataDir, "wiki", "procedural"),
		filepath.Join(dataDir, "wiki", "prospective"),
		filepath.Join(dataDir, "wiki", "archive"),
		filepath.Join(dataDir, "wiki", "archive", "semantic"),
		filepath.Join(dataDir, "wiki", "archive", "episodic"),
		filepath.Join(dataDir, "wiki", "archive", "procedural"),
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0755); err != nil {
			return nil, fmt.Errorf("create dir %s: %w", d, err)
		}
	}

	fs := &FS{
		dataDir:   dataDir,
		pageLocks: make(map[string]*sync.Mutex),
	}
	if err := fs.loadEvents(); err != nil {
		return nil, err
	}
	return fs, nil
}

func (f *FS) rawPath() string    { return filepath.Join(f.dataDir, "raw", "events.jsonl") }
func (f *FS) wikiDir() string    { return filepath.Join(f.dataDir, "wiki") }
func (f *FS) metaDir() string    { return filepath.Join(f.dataDir, "wiki", ".meta") }
func (f *FS) indexPath() string  { return filepath.Join(f.dataDir, "wiki", "index.md") }
func (f *FS) cursorPath() string { return filepath.Join(f.dataDir, "raw", "cursor") }

// lockPage returns a per-page mutex. Concurrent writes to different pages
// proceed in parallel; writes to the same page are serialized.
func (f *FS) lockPage(path string) *sync.Mutex {
	f.pageMu.Lock()
	defer f.pageMu.Unlock()
	m, ok := f.pageLocks[path]
	if !ok {
		m = &sync.Mutex{}
		f.pageLocks[path] = m
	}
	return m
}

func (f *FS) loadEvents() error {
	path := f.rawPath()
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open events: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1<<20), 1<<20) // 1MB line buffer
	for scanner.Scan() {
		var ev Event
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			continue // skip malformed lines
		}
		f.events = append(f.events, ev)
	}
	return scanner.Err()
}

// AppendEvent writes a new event to the raw log.
func (f *FS) AppendEvent(ev Event) (string, error) {
	if ev.ID == "" {
		ev.ID = GenerateEventID()
	}
	if ev.Timestamp == "" {
		ev.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}

	data, err := json.Marshal(ev)
	if err != nil {
		return "", fmt.Errorf("marshal event: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	file, err := os.OpenFile(f.rawPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return "", fmt.Errorf("open raw log: %w", err)
	}
	defer file.Close()

	if _, err := file.Write(append(data, '\n')); err != nil {
		return "", fmt.Errorf("write event: %w", err)
	}

	f.events = append(f.events, ev)
	return ev.ID, nil
}

// ReadEventsSince returns events after the cursor position (0-indexed).
func (f *FS) ReadEventsSince(cursor uint64) (*EventsPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if cursor >= uint64(len(f.events)) {
		return &EventsPage{NewCursor: uint64(len(f.events))}, nil
	}

	events := make([]Event, len(f.events)-int(cursor))
	copy(events, f.events[cursor:])
	return &EventsPage{
		Events:    events,
		NewCursor: uint64(len(f.events)),
	}, nil
}

// ReadRecentEvents returns the last n events from the log.
func (f *FS) ReadRecentEvents(n int) ([]Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	total := len(f.events)
	if n <= 0 || total == 0 {
		return nil, nil
	}
	if n > total {
		n = total
	}
	out := make([]Event, n)
	copy(out, f.events[total-n:])
	return out, nil
}

// ReadWikiIndex returns the contents of wiki/index.md.
func (f *FS) ReadWikiIndex() (string, error) {
	data, err := os.ReadFile(f.indexPath())
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read index: %w", err)
	}
	return string(data), nil
}

// ReadWikiPage reads a wiki page and its sidecar.
// The sidecar writeback (access telemetry update) is done under the same
// per-page lock as WriteWikiPage/ArchiveWikiPage to prevent read/write races
// from clobbering provenance metadata.
func (f *FS) ReadWikiPage(path string) (*WikiPage, error) {
	if err := validateWikiPath(path); err != nil {
		return nil, err
	}

	// Acquire per-page lock — same lock domain as Write/Archive.
	mu := f.lockPage(path)
	mu.Lock()
	defer mu.Unlock()

	fullPath := filepath.Join(f.wikiDir(), path)
	content, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("read page %s: %w", path, err)
	}

	meta := f.readSidecar(path)

	// Update access telemetry.
	now := time.Now().UTC()
	today := now.Format("2006-01-02")
	meta.LastAccessed = now.Format(time.RFC3339)
	if len(meta.AccessDates) == 0 || meta.AccessDates[len(meta.AccessDates)-1] != today {
		meta.AccessDates = append(meta.AccessDates, today)
	}
	_ = f.writeSidecar(path, meta)

	return &WikiPage{
		Path:    path,
		Content: string(content),
		Meta:    meta,
	}, nil
}

// ReadWikiPageReadOnly reads a wiki page and its sidecar with NO store
// mutation: it never writes access telemetry back to the sidecar. The query
// capability requires zero store mutation (invariant 12 / adversary-1 A3), so
// it uses this path instead of ReadWikiPage. The per-page lock is still taken
// (read lock domain) to avoid reading a torn write, but nothing is written.
func (f *FS) ReadWikiPageReadOnly(path string) (*WikiPage, error) {
	if err := validateWikiPath(path); err != nil {
		return nil, err
	}

	mu := f.lockPage(path)
	mu.Lock()
	defer mu.Unlock()

	fullPath := filepath.Join(f.wikiDir(), path)
	content, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("read page %s: %w", path, err)
	}

	meta := f.readSidecar(path)
	// Intentionally NO telemetry writeback: zero store mutation.

	return &WikiPage{
		Path:    path,
		Content: string(content),
		Meta:    meta,
	}, nil
}

// SearchWiki does a case-insensitive text search across wiki pages,
// including archived pages (per design: archive is searchable for recovery).
func (f *FS) SearchWiki(query string) ([]SearchResult, error) {
	var results []SearchResult
	queryLower := strings.ToLower(query)

	err := filepath.Walk(f.wikiDir(), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // skip errors
		}
		if info.IsDir() {
			if info.Name() == ".meta" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".md") {
			return nil
		}

		relPath, _ := filepath.Rel(f.wikiDir(), path)
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}

		lines := strings.Split(string(data), "\n")
		for i, line := range lines {
			if strings.Contains(strings.ToLower(line), queryLower) {
				results = append(results, SearchResult{
					Path:    relPath,
					Line:    i + 1,
					Content: line,
				})
			}
		}
		return nil
	})

	return results, err
}

// WriteWikiPage creates or updates a wiki page and its sidecar.
// Page-level locking prevents concurrent writes from clobbering each other.
func (f *FS) WriteWikiPage(path string, content string) error {
	return f.WriteWikiPageWithMeta(path, content, nil, 0)
}

// WriteWikiPageWithMeta creates or updates a wiki page with optional source event tracking.
// sourceEvents are appended to the sidecar's SourceEvents (deduplicated).
// trustTier updates TrustTierMax if it's higher (lower number = more trusted).
func (f *FS) WriteWikiPageWithMeta(path string, content string, sourceEvents []string, trustTier int) error {
	if err := validateWikiPath(path); err != nil {
		return err
	}

	// Acquire per-page lock to serialize writes to the same page.
	mu := f.lockPage(path)
	mu.Lock()
	defer mu.Unlock()

	fullPath := filepath.Join(f.wikiDir(), path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return fmt.Errorf("create parent dirs: %w", err)
	}

	if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
		return fmt.Errorf("write page %s: %w", path, err)
	}

	// Create or update sidecar.
	meta := f.readSidecar(path)
	now := time.Now().UTC().Format(time.RFC3339)
	if meta.CreatedAt == "" {
		meta.CreatedAt = now
		meta.LastAccessed = now
		meta.MemoryType = inferMemoryType(path)
	}

	// Append source events (deduplicated).
	if len(sourceEvents) > 0 {
		existing := make(map[string]bool, len(meta.SourceEvents))
		for _, e := range meta.SourceEvents {
			existing[e] = true
		}
		for _, e := range sourceEvents {
			if !existing[e] {
				meta.SourceEvents = append(meta.SourceEvents, e)
				existing[e] = true
			}
		}
	}

	// Update trust tier max (lower number = more trusted).
	if trustTier > 0 && (meta.TrustTierMax == 0 || trustTier < meta.TrustTierMax) {
		meta.TrustTierMax = trustTier
	}

	_ = f.writeSidecar(path, meta)
	return nil
}

// ArchiveWikiPage moves a page to archive/ and updates its sidecar.
func (f *FS) ArchiveWikiPage(path string, reason string) error {
	if err := validateWikiPath(path); err != nil {
		return err
	}

	// Acquire per-page lock.
	mu := f.lockPage(path)
	mu.Lock()
	defer mu.Unlock()

	srcPath := filepath.Join(f.wikiDir(), path)
	if _, err := os.Stat(srcPath); os.IsNotExist(err) {
		return fmt.Errorf("page %s does not exist", path)
	}

	// Determine archive destination: wiki/archive/{original_path}
	dstPath := filepath.Join(f.wikiDir(), "archive", path)
	if err := os.MkdirAll(filepath.Dir(dstPath), 0755); err != nil {
		return fmt.Errorf("create archive dir: %w", err)
	}

	if err := os.Rename(srcPath, dstPath); err != nil {
		return fmt.Errorf("move to archive: %w", err)
	}

	// Update sidecar.
	meta := f.readSidecar(path)
	now := time.Now().UTC().Format(time.RFC3339)
	meta.ArchivedAt = now
	meta.ArchiveReason = reason

	// Move sidecar to archive location too.
	archiveSidecarPath := "archive/" + path
	_ = f.writeSidecar(archiveSidecarPath, meta)
	_ = f.deleteSidecar(path)

	return nil
}

// RebuildIndex scans all active wiki pages and regenerates both:
// - wiki/index.md (root routing table)
// - wiki/{category}/index.md (per-category sub-indexes)
//
// Uses a snapshot approach: first collect all page paths and descriptions,
// then write all index files at once. This avoids interleaving with concurrent
// wiki mutations that could produce an inconsistent index.
func (f *FS) RebuildIndex() error {
	// indexMu serializes concurrent RebuildIndex calls.
	f.indexMu.Lock()
	defer f.indexMu.Unlock()

	// Phase 1: Snapshot — collect page info without holding wiki locks.
	type pageInfo struct {
		relPath string
		desc    string
	}
	categories := []string{"semantic", "episodic", "procedural", "prospective"}
	catPages := make(map[string][]pageInfo)

	for _, cat := range categories {
		catDir := filepath.Join(f.wikiDir(), cat)
		_ = filepath.Walk(catDir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(path, ".md") {
				return nil
			}
			relPath, _ := filepath.Rel(f.wikiDir(), path)
			if relPath == cat+"/index.md" {
				return nil
			}
			desc := extractPageDescription(path)
			catPages[cat] = append(catPages[cat], pageInfo{relPath: relPath, desc: desc})
			return nil
		})
	}

	// Phase 2: Write all index files from the snapshot.
	var sections []string
	for _, cat := range categories {
		pages := catPages[cat]
		// Root index uses wiki-root-relative paths; category sub-index uses
		// paths relative to the category directory.
		var rootLines []string
		var catLines []string
		for _, p := range pages {
			rootLines = append(rootLines, fmt.Sprintf("- [%s](%s) — %s", filepath.Base(p.relPath), p.relPath, p.desc))
			catRelPath := strings.TrimPrefix(p.relPath, cat+"/")
			catLines = append(catLines, fmt.Sprintf("- [%s](%s) — %s", filepath.Base(p.relPath), catRelPath, p.desc))
		}

		catIndexPath := filepath.Join(f.wikiDir(), cat, "index.md")
		catContent := fmt.Sprintf("# %s\n\n", capitalize(cat))
		if len(catLines) == 0 {
			catContent += "_No pages yet._\n"
		} else {
			catContent += strings.Join(catLines, "\n") + "\n"
		}
		_ = os.WriteFile(catIndexPath, []byte(catContent), 0644)

		if len(rootLines) > 0 {
			sections = append(sections, fmt.Sprintf("## %s\n\n%s", cat, strings.Join(rootLines, "\n")))
		}
	}

	content := "# Wiki Index\n\n"
	if len(sections) == 0 {
		content += "_No pages yet._\n"
	} else {
		content += strings.Join(sections, "\n\n") + "\n"
	}

	return os.WriteFile(f.indexPath(), []byte(content), 0644)
}

// GetMemoryStats returns system-level statistics.
func (f *FS) GetMemoryStats() (*MemoryStats, error) {
	f.mu.Lock()
	eventCount := len(f.events)
	f.mu.Unlock()

	compileCursor := f.GetCompileCursor()

	wikiCount := 0
	archiveCount := 0

	_ = filepath.Walk(f.wikiDir(), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".md") {
			return nil
		}
		relPath, _ := filepath.Rel(f.wikiDir(), path)
		if relPath == "index.md" {
			return nil
		}
		if strings.HasPrefix(relPath, "archive/") {
			archiveCount++
		} else if !strings.HasPrefix(relPath, ".meta/") {
			wikiCount++
		}
		return nil
	})

	return &MemoryStats{
		EventCount:        eventCount,
		UncompiledCount:   eventCount - int(compileCursor),
		WikiPageCount:     wikiCount,
		ArchivedPageCount: archiveCount,
	}, nil
}

// SetCompileCursor persists the compile cursor to disk and updates in-memory state.
func (f *FS) SetCompileCursor(cursor uint64) error {
	return os.WriteFile(f.cursorPath(), []byte(strconv.FormatUint(cursor, 10)), 0644)
}

// GetCompileCursor reads the persisted compile cursor from disk.
func (f *FS) GetCompileCursor() uint64 {
	data, err := os.ReadFile(f.cursorPath())
	if err != nil {
		return 0
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// --- Sidecar helpers ---

func (f *FS) sidecarPath(wikiPath string) string {
	// wiki/semantic/projects/db9.md → wiki/.meta/semantic/projects/db9.json
	jsonPath := strings.TrimSuffix(wikiPath, ".md") + ".json"
	return filepath.Join(f.metaDir(), jsonPath)
}

func (f *FS) readSidecar(wikiPath string) *Sidecar {
	path := f.sidecarPath(wikiPath)
	data, err := os.ReadFile(path)
	if err != nil {
		return &Sidecar{}
	}
	var meta Sidecar
	if err := json.Unmarshal(data, &meta); err != nil {
		return &Sidecar{}
	}
	return &meta
}

func (f *FS) writeSidecar(wikiPath string, meta *Sidecar) error {
	path := f.sidecarPath(wikiPath)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (f *FS) deleteSidecar(wikiPath string) error {
	return os.Remove(f.sidecarPath(wikiPath))
}

// --- Helpers ---

func validateWikiPath(path string) error {
	if path == "" {
		return fmt.Errorf("empty wiki path")
	}
	if strings.Contains(path, "..") {
		return fmt.Errorf("path traversal not allowed")
	}
	if strings.HasPrefix(path, "/") {
		return fmt.Errorf("wiki path must be relative")
	}
	if strings.HasPrefix(path, ".meta/") {
		return fmt.Errorf("cannot directly access .meta/")
	}
	return nil
}

func inferMemoryType(path string) string {
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 0 {
		return ""
	}
	switch parts[0] {
	case "semantic", "episodic", "procedural", "prospective":
		return parts[0]
	}
	return ""
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

func extractPageDescription(path string) string {
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "<!--") || strings.HasPrefix(line, "#") {
			continue
		}
		if len(line) > 100 {
			return line[:100] + "..."
		}
		return line
	}
	return ""
}
