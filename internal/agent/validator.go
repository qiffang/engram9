package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Violation describes a single validation issue in the staging wiki.
type Violation struct {
	Path    string
	Message string
}

func (v Violation) String() string {
	if v.Path != "" {
		return fmt.Sprintf("%s: %s", v.Path, v.Message)
	}
	return v.Message
}

// WikiValidator validates staging wiki before merge to production.
type WikiValidator struct {
	maxDiffBytes int64
}

// NewWikiValidator creates a validator with the given diff budget.
func NewWikiValidator(maxDiffBytes int64) *WikiValidator {
	if maxDiffBytes <= 0 {
		maxDiffBytes = DefaultACPMaxDiffBytes
	}
	return &WikiValidator{maxDiffBytes: maxDiffBytes}
}

// ValidateOptions controls validation behavior per turn type.
type ValidateOptions struct {
	// AllowDelete permits UNPAIRED page deletion in staging. This grants
	// destruction capability and is normally false. Ingest sets it false; the
	// compile/prune path uses AllowPairedArchiveMove instead.
	AllowDelete bool
	// AllowPairedArchiveMove permits sleep-pruning's archive: an active page P
	// may disappear from the active tree IFF a byte-identical copy exists at
	// archive/P in staging (a MOVE, not a destroy). An active page removed with
	// no matching archive copy is still rejected. This is strictly weaker than
	// AllowDelete: it cannot destroy knowledge, only relocate it to archive/.
	AllowPairedArchiveMove bool
}

// Validate compares staging wiki against production wiki and returns violations.
// Returns nil if validation passes.
func (v *WikiValidator) Validate(prodDir, stagingDir string, opts ...ValidateOptions) ([]Violation, error) {
	var opt ValidateOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	stagingWiki := filepath.Join(stagingDir, "wiki")
	prodWiki := filepath.Join(prodDir, "wiki")

	if _, err := os.Stat(stagingWiki); os.IsNotExist(err) {
		return nil, nil // no wiki changes
	}

	var violations []Violation
	var totalDiffBytes int64

	// Walk staging wiki and check each file.
	err := filepath.Walk(stagingWiki, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(stagingWiki, path)
		if err != nil {
			return err
		}

		// Skip store-owned sidecar metadata (e.g. .meta/*.json).
		if strings.HasPrefix(relPath, ".meta/") || strings.HasPrefix(relPath, ".meta\\") {
			return nil
		}

		// archive/: sleep-pruning moves a stale active page into archive/ via a
		// store-managed rename (ArchiveWikiPage). The taxonomy/frontmatter checks
		// (which target freshly written ACTIVE pages) are exempt for archive/ ONLY
		// when the archived page is a faithful move: archive/semantic/x.md must be
		// byte-identical to the former active semantic/x.md in production. That
		// prevents an agent from smuggling arbitrary content into archive/ to
		// bypass the taxonomy exemption. A mismatched or source-less archive page
		// is treated as ordinary content and validated normally (→ taxonomy
		// violation), so it cannot pass. Its bytes still count toward the diff
		// budget.
		if opt.AllowPairedArchiveMove && isArchivePath(relPath) {
			data, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			activeRel := strings.TrimPrefix(filepath.ToSlash(relPath), "archive/")
			prodActive, prodErr := os.ReadFile(filepath.Join(prodWiki, activeRel))
			if prodErr == nil && string(prodActive) == string(data) {
				// Faithful move: exempt from taxonomy/frontmatter. Count bytes if new.
				if _, perr := os.Stat(filepath.Join(prodWiki, relPath)); os.IsNotExist(perr) {
					totalDiffBytes += int64(len(data))
				}
				return nil
			}
			// Not a faithful move (no source or content differs): fall through and
			// validate as ordinary content, which will flag the non-taxonomy path.
		}

		// Only .md files are allowed in wiki content. Reject anything else.
		if !strings.HasSuffix(relPath, ".md") {
			violations = append(violations, Violation{
				Path:    relPath,
				Message: "non-Markdown file in wiki (only .md files are allowed)",
			})
			return nil
		}

		// Check valid taxonomy path.
		if !IsValidWikiPath(relPath) {
			violations = append(violations, Violation{
				Path:    relPath,
				Message: "path does not conform to wiki taxonomy (semantic/, episodic/, procedural/, prospective/)",
			})
		}

		// Read staging content.
		stagingContent, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		// Check frontmatter on content pages. RebuildIndex owns the root and
		// category indexes and intentionally generates them without frontmatter.
		if !isStructuralWikiIndex(relPath) && !hasFrontmatter(string(stagingContent)) {
			violations = append(violations, Violation{
				Path:    relPath,
				Message: "missing required frontmatter (compiled_from, last_compiled)",
			})
		}

		// Calculate diff size.
		prodPath := filepath.Join(prodWiki, relPath)
		prodContent, prodErr := os.ReadFile(prodPath)
		if prodErr != nil {
			// New file — entire content counts as diff.
			totalDiffBytes += int64(len(stagingContent))
		} else {
			// Modified file — diff is the absolute size difference plus changed bytes.
			totalDiffBytes += abs64(int64(len(stagingContent)) - int64(len(prodContent)))
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk staging wiki: %w", err)
	}

	// Check diff budget.
	if totalDiffBytes > v.maxDiffBytes {
		violations = append(violations, Violation{
			Message: fmt.Sprintf("diff budget exceeded: %d bytes > %d bytes max", totalDiffBytes, v.maxDiffBytes),
		})
	}

	// Check that no production pages were deleted in staging. A page missing from
	// staging is a violation UNLESS:
	//   - AllowDelete is set (blanket destruction — not used by compile), or
	//   - AllowPairedArchiveMove is set AND the missing active page P has a
	//     byte-identical copy at archive/P in staging (a MOVE, not a destroy).
	// An unpaired delete (page gone, no matching archive copy) is always rejected,
	// so compile/prune cannot destroy knowledge — only relocate it.
	if !opt.AllowDelete {
		if _, err := os.Stat(prodWiki); err == nil {
			err := filepath.Walk(prodWiki, func(path string, info os.FileInfo, err error) error {
				if err != nil || info.IsDir() {
					return err
				}
				if !strings.HasSuffix(path, ".md") {
					return nil
				}

				relPath, _ := filepath.Rel(prodWiki, path)
				stagingPath := filepath.Join(stagingWiki, relPath)

				// Existing archive/** history is APPEND-ONLY under the paired-move
				// contract: mergeWiki replaces the whole wiki/ subtree, so a staging
				// tree that omits an existing archived page would DELETE archive
				// history on merge. Reject any prod archive/** entry missing from
				// staging (deletion), regardless of AllowPairedArchiveMove. New
				// archive entries (a fresh move) are allowed by the staging walk;
				// modifying an existing one is rejected there (byte-identity).
				if isArchivePath(relPath) {
					if _, statErr := os.Stat(stagingPath); os.IsNotExist(statErr) {
						violations = append(violations, Violation{
							Path:    relPath,
							Message: "archived page deleted in staging (archive/ history is append-only)",
						})
					}
					return nil
				}
				if _, statErr := os.Stat(stagingPath); os.IsNotExist(statErr) {
					if opt.AllowPairedArchiveMove && isFaithfulArchiveMove(prodWiki, stagingWiki, relPath) {
						return nil // paired move: P moved to archive/P byte-identically
					}
					violations = append(violations, Violation{
						Path:    relPath,
						Message: "page deleted in staging without a byte-identical archive/ copy (compile may archive-move, not destroy)",
					})
				}
				return nil
			})
			if err != nil {
				return nil, fmt.Errorf("walk prod wiki: %w", err)
			}
		}
	}

	return violations, nil
}

// IsValidWikiPath checks if a path conforms to the wiki taxonomy.
func IsValidWikiPath(relPath string) bool {
	validPrefixes := []string{"semantic/", "episodic/", "procedural/", "prospective/"}
	// Also allow index.md at root.
	if relPath == "index.md" {
		return true
	}
	for _, prefix := range validPrefixes {
		if strings.HasPrefix(relPath, prefix) {
			return true
		}
	}
	return false
}

// isFaithfulArchiveMove reports whether an active page P (relPath, relative to
// wiki/) that disappeared from staging's active tree was moved faithfully to
// archive/P: staging/archive/<relPath> exists and is byte-identical to the
// former active production page prod/<relPath>. This is the paired-move
// predicate — it distinguishes a legitimate sleep-pruning archive from an
// arbitrary deletion or a rewrite disguised as an archive.
func isFaithfulArchiveMove(prodWiki, stagingWiki, relPath string) bool {
	prodActive, err := os.ReadFile(filepath.Join(prodWiki, relPath))
	if err != nil {
		return false
	}
	archived, err := os.ReadFile(filepath.Join(stagingWiki, "archive", relPath))
	if err != nil {
		return false
	}
	return string(prodActive) == string(archived)
}

// isArchivePath reports whether relPath (relative to wiki/) is under the
// store-managed archive/ subtree. Sleep-pruning moves stale active pages here
// via ArchiveWikiPage; those files are the prior active content, not
// agent-authored, so taxonomy/frontmatter checks do not apply to them.
func isArchivePath(relPath string) bool {
	slash := filepath.ToSlash(relPath)
	return slash == "archive" || strings.HasPrefix(slash, "archive/")
}

func isStructuralWikiIndex(relPath string) bool {
	switch filepath.ToSlash(relPath) {
	case "index.md",
		"semantic/index.md",
		"episodic/index.md",
		"procedural/index.md",
		"prospective/index.md":
		return true
	default:
		return false
	}
}

// hasFrontmatter checks if wiki content has required frontmatter comments.
func hasFrontmatter(content string) bool {
	return strings.Contains(content, "compiled_from:") &&
		strings.Contains(content, "last_compiled:")
}

func abs64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
