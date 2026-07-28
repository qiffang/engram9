package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// BundleFS implements a read-only Store over an OKF bundle directory.
//
// Unlike FS (which expects a runtime data directory with raw/ and wiki/
// subdirectories), BundleFS treats the given directory as the wiki root
// directly. This allows the MCP server to consume OKF bundles produced
// by engram9 or any other OKF-compatible tool.
//
// Write operations return errors — bundles are read-only through this store.
type BundleFS struct {
	bundleDir string
}

// NewBundleFS creates a read-only store rooted at bundleDir.
// Unlike NewFS, it does NOT create any directories — the bundle must exist.
func NewBundleFS(bundleDir string) (*BundleFS, error) {
	info, err := os.Stat(bundleDir)
	if err != nil {
		return nil, fmt.Errorf("bundle dir: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("bundle path is not a directory: %s", bundleDir)
	}
	return &BundleFS{bundleDir: bundleDir}, nil
}

func (b *BundleFS) AppendEvent(_ Event) (string, error) {
	return "", fmt.Errorf("bundle store is read-only")
}

func (b *BundleFS) ReadEventsSince(_ uint64) (*EventsPage, error) {
	return &EventsPage{}, nil
}

func (b *BundleFS) ReadRecentEvents(_ int) ([]Event, error) {
	return nil, nil
}

func (b *BundleFS) ReadWikiIndex() (string, error) {
	data, err := os.ReadFile(filepath.Join(b.bundleDir, "index.md"))
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read index: %w", err)
	}
	return string(data), nil
}

func (b *BundleFS) ReadWikiPage(path string) (*WikiPage, error) {
	if err := validateWikiPath(path); err != nil {
		return nil, err
	}
	fullPath := filepath.Join(b.bundleDir, path)
	content, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("read page %s: %w", path, err)
	}
	memType := inferMemoryType(path)
	return &WikiPage{
		Path:    path,
		Content: string(content),
		Meta:    &Sidecar{MemoryType: memType},
	}, nil
}

// ReadWikiPageReadOnly is identical to ReadWikiPage for a bundle: the bundle
// store never writes access telemetry, so the read path is already mutation-free.
func (b *BundleFS) ReadWikiPageReadOnly(path string) (*WikiPage, error) {
	return b.ReadWikiPage(path)
}

func (b *BundleFS) SearchWiki(query string) ([]SearchResult, error) {
	var results []SearchResult
	queryLower := strings.ToLower(query)

	err := filepath.Walk(b.bundleDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".md") {
			return nil
		}
		relPath, _ := filepath.Rel(b.bundleDir, path)
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

func (b *BundleFS) WriteWikiPage(_ string, _ string) error {
	return fmt.Errorf("bundle store is read-only")
}

func (b *BundleFS) WriteWikiPageWithMeta(_ string, _ string, _ []string, _ int) error {
	return fmt.Errorf("bundle store is read-only")
}

func (b *BundleFS) ArchiveWikiPage(_ string, _ string) error {
	return fmt.Errorf("bundle store is read-only")
}

func (b *BundleFS) RebuildIndex() error {
	return fmt.Errorf("bundle store is read-only")
}

func (b *BundleFS) SetCompileCursor(_ uint64) error {
	return fmt.Errorf("bundle store is read-only")
}

func (b *BundleFS) GetCompileCursor() uint64 { return 0 }

func (b *BundleFS) GetMemoryStats() (*MemoryStats, error) {
	pageCount := 0
	archiveCount := 0
	_ = filepath.Walk(b.bundleDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".md") {
			return nil
		}
		relPath, _ := filepath.Rel(b.bundleDir, path)
		if relPath == "index.md" {
			return nil
		}
		if strings.HasPrefix(relPath, "archive/") {
			archiveCount++
		} else {
			pageCount++
		}
		return nil
	})
	return &MemoryStats{WikiPageCount: pageCount, ArchivedPageCount: archiveCount}, nil
}
