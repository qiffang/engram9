package agent

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTestFile(t *testing.T, root, rel, content string) {
	t.Helper()
	path := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

const validFrontmatter = `<!-- compiled_from: evt_001 -->
<!-- last_compiled: 2026-07-13T00:00:00Z -->
# Test Page

Content here.
`

func TestWikiValidatorPassesCleanStaging(t *testing.T) {
	prod := t.TempDir()
	staging := t.TempDir()

	writeTestFile(t, prod, "wiki/semantic/a.md", validFrontmatter)
	writeTestFile(t, staging, "wiki/semantic/a.md", validFrontmatter)

	v := NewWikiValidator(DefaultACPMaxDiffBytes)
	violations, err := v.Validate(prod, staging)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) > 0 {
		t.Fatalf("expected no violations, got %v", violations)
	}
}

func TestWikiValidatorNoStagingWiki(t *testing.T) {
	prod := t.TempDir()
	staging := t.TempDir()

	writeTestFile(t, prod, "wiki/semantic/a.md", validFrontmatter)
	// No staging wiki directory at all.

	v := NewWikiValidator(DefaultACPMaxDiffBytes)
	violations, err := v.Validate(prod, staging)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("expected 0 violations for missing staging wiki, got %v", violations)
	}
}

func TestWikiValidatorRejectsMissingFrontmatter(t *testing.T) {
	prod := t.TempDir()
	staging := t.TempDir()

	noFrontmatter := "# Page without frontmatter\n\nJust content.\n"
	writeTestFile(t, staging, "wiki/semantic/a.md", noFrontmatter)

	v := NewWikiValidator(DefaultACPMaxDiffBytes)
	violations, err := v.Validate(prod, staging)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, vi := range violations {
		if vi.Path == "semantic/a.md" && contains(vi.Message, "frontmatter") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected frontmatter violation for semantic/a.md, got %v", violations)
	}
}

func TestWikiValidatorRejectsInvalidTaxonomy(t *testing.T) {
	prod := t.TempDir()
	staging := t.TempDir()

	writeTestFile(t, staging, "wiki/random/a.md", validFrontmatter)

	v := NewWikiValidator(DefaultACPMaxDiffBytes)
	violations, err := v.Validate(prod, staging)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, vi := range violations {
		if vi.Path == "random/a.md" && contains(vi.Message, "taxonomy") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected taxonomy violation for random/a.md, got %v", violations)
	}
}

func TestWikiValidatorAllowsStructuralIndexesWithoutFrontmatter(t *testing.T) {
	prod := t.TempDir()
	staging := t.TempDir()

	for _, path := range []string{
		"index.md",
		"semantic/index.md",
		"episodic/index.md",
		"procedural/index.md",
		"prospective/index.md",
	} {
		writeTestFile(t, staging, filepath.Join("wiki", path), "# Generated Index\n")
	}

	v := NewWikiValidator(DefaultACPMaxDiffBytes)
	violations, err := v.Validate(prod, staging)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("structural indexes should be allowed without frontmatter, got %v", violations)
	}
}

func TestWikiValidatorRejectsNestedIndexWithoutFrontmatter(t *testing.T) {
	prod := t.TempDir()
	staging := t.TempDir()

	writeTestFile(t, staging, "wiki/semantic/projects/index.md", "# Projects\n")

	v := NewWikiValidator(DefaultACPMaxDiffBytes)
	violations, err := v.Validate(prod, staging)
	if err != nil {
		t.Fatal(err)
	}
	for _, vi := range violations {
		if vi.Path == "semantic/projects/index.md" && contains(vi.Message, "frontmatter") {
			return
		}
	}
	t.Fatalf("expected frontmatter violation for nested index page, got %v", violations)
}

func TestWikiValidatorRejectsDiffBudgetExceeded(t *testing.T) {
	prod := t.TempDir()
	staging := t.TempDir()

	// Create a staging file larger than the budget.
	bigContent := validFrontmatter + string(make([]byte, 100))
	writeTestFile(t, staging, "wiki/semantic/big.md", bigContent)

	v := NewWikiValidator(10) // very small budget
	violations, err := v.Validate(prod, staging)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, vi := range violations {
		if contains(vi.Message, "diff budget") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected diff budget violation, got %v", violations)
	}
}

func TestWikiValidatorRejectsDeletedPages(t *testing.T) {
	prod := t.TempDir()
	staging := t.TempDir()

	writeTestFile(t, prod, "wiki/semantic/a.md", validFrontmatter)
	writeTestFile(t, prod, "wiki/semantic/b.md", validFrontmatter)
	// Staging only has a.md — b.md is "deleted".
	writeTestFile(t, staging, "wiki/semantic/a.md", validFrontmatter)

	v := NewWikiValidator(DefaultACPMaxDiffBytes)
	violations, err := v.Validate(prod, staging)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, vi := range violations {
		if vi.Path == "semantic/b.md" && contains(vi.Message, "deleted") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected deleted page violation for semantic/b.md, got %v", violations)
	}
}

func TestWikiValidatorAllowsDeleteWhenEnabled(t *testing.T) {
	prod := t.TempDir()
	staging := t.TempDir()

	writeTestFile(t, prod, "wiki/semantic/a.md", validFrontmatter)
	writeTestFile(t, prod, "wiki/semantic/b.md", validFrontmatter)
	// Staging only has a.md — b.md is "deleted". With AllowDelete=true, no violation.
	writeTestFile(t, staging, "wiki/semantic/a.md", validFrontmatter)

	v := NewWikiValidator(DefaultACPMaxDiffBytes)
	violations, err := v.Validate(prod, staging, ValidateOptions{AllowDelete: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, vi := range violations {
		if contains(vi.Message, "deleted") {
			t.Fatalf("expected no deleted page violation with AllowDelete=true, got: %v", vi)
		}
	}
}

// TestWikiValidatorAllowsFaithfulArchiveMove (regression #1, positive) models
// sleep-pruning: the compile agent archives an active page — ArchiveWikiPage
// renames wiki/semantic/b.md -> wiki/archive/semantic/b.md byte-identically.
// Under AllowPairedArchiveMove the original's removal is fine and the archived
// copy is exempt from taxonomy/frontmatter checks. Before Blocker-1 fix this
// scenario produced three violations and every pruning compile was discarded.
func TestWikiValidatorAllowsFaithfulArchiveMove(t *testing.T) {
	prod := t.TempDir()
	staging := t.TempDir()

	const bContent = validFrontmatter // the exact former active bytes
	writeTestFile(t, prod, "wiki/semantic/a.md", validFrontmatter)
	writeTestFile(t, prod, "wiki/semantic/b.md", bContent)
	// Keep a.md active; move b.md into archive/ byte-identically.
	writeTestFile(t, staging, "wiki/semantic/a.md", validFrontmatter)
	writeTestFile(t, staging, "wiki/archive/semantic/b.md", bContent)

	v := NewWikiValidator(DefaultACPMaxDiffBytes)
	violations, err := v.Validate(prod, staging, ValidateOptions{AllowPairedArchiveMove: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("faithful archive move must pass validation, got: %v", violations)
	}
}

// TestWikiValidatorRejectsUnpairedDelete (regression #2, the DISCRIMINATING
// test) is what separates the paired-move contract from a blanket AllowDelete:
// an active page removed with NO archive/ copy must still be rejected even under
// AllowPairedArchiveMove. A blanket-true implementation would wrongly pass this,
// granting the compile agent destruction capability.
func TestWikiValidatorRejectsUnpairedDelete(t *testing.T) {
	prod := t.TempDir()
	staging := t.TempDir()

	writeTestFile(t, prod, "wiki/semantic/a.md", validFrontmatter)
	writeTestFile(t, prod, "wiki/semantic/b.md", validFrontmatter)
	// b.md deleted with NO archive/ copy — destruction, not a move.
	writeTestFile(t, staging, "wiki/semantic/a.md", validFrontmatter)

	v := NewWikiValidator(DefaultACPMaxDiffBytes)
	violations, err := v.Validate(prod, staging, ValidateOptions{AllowPairedArchiveMove: true})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, vi := range violations {
		if vi.Path == "semantic/b.md" && contains(vi.Message, "deleted") {
			found = true
		}
	}
	if !found {
		t.Fatalf("unpaired delete must be rejected even with AllowPairedArchiveMove, got: %v", violations)
	}
}

// TestWikiValidatorRejectsMismatchedArchive (regression #1, negative) guards the
// byte-identical precondition: if archive/P differs from the former active P,
// the taxonomy exemption must NOT apply — the agent cannot smuggle rewritten or
// arbitrary content into archive/ under the guise of an archive move.
func TestWikiValidatorRejectsMismatchedArchive(t *testing.T) {
	prod := t.TempDir()
	staging := t.TempDir()

	writeTestFile(t, prod, "wiki/semantic/a.md", validFrontmatter)
	writeTestFile(t, prod, "wiki/semantic/b.md", validFrontmatter)
	writeTestFile(t, staging, "wiki/semantic/a.md", validFrontmatter)
	// archive/b.md content does NOT match the former active b.md — a rewrite,
	// not a faithful move.
	writeTestFile(t, staging, "wiki/archive/semantic/b.md", "# smuggled\n\ntotally different content\n")

	v := NewWikiValidator(DefaultACPMaxDiffBytes)
	violations, err := v.Validate(prod, staging, ValidateOptions{AllowPairedArchiveMove: true})
	if err != nil {
		t.Fatal(err)
	}
	// Two independent rejections are acceptable; require at least one that ties
	// the failure to the mismatch (deleted-without-copy OR non-taxonomy archive).
	if len(violations) == 0 {
		t.Fatalf("mismatched archive must be rejected (deleted b.md has no faithful archive copy), got no violations")
	}
}

// TestWikiValidatorRejectsModifiedExistingArchive (regression: archive/** edit)
// guards that an already-archived page cannot be rewritten in place. The
// exemption is only for a fresh faithful move (archive/P byte-identical to the
// former ACTIVE prod P). An edit to a pre-existing archive/P has no matching
// active source, so it falls through to ordinary validation and is rejected on
// its non-taxonomy path — the agent cannot silently overwrite archive history.
func TestWikiValidatorRejectsModifiedExistingArchive(t *testing.T) {
	prod := t.TempDir()
	staging := t.TempDir()

	// A page that is ALREADY archived in production (no active source).
	writeTestFile(t, prod, "wiki/archive/semantic/old.md", "# old archived\n\noriginal archive content\n")
	writeTestFile(t, prod, "wiki/semantic/a.md", validFrontmatter)
	writeTestFile(t, staging, "wiki/semantic/a.md", validFrontmatter)
	// Staging REWRITES the existing archived page — must be rejected.
	writeTestFile(t, staging, "wiki/archive/semantic/old.md", "# tampered\n\nrewritten archive content\n")

	v := NewWikiValidator(DefaultACPMaxDiffBytes)
	violations, err := v.Validate(prod, staging, ValidateOptions{AllowPairedArchiveMove: true})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, vi := range violations {
		if vi.Path == "archive/semantic/old.md" {
			found = true
		}
	}
	if !found {
		t.Fatalf("modifying an existing archive/ page must be rejected, got: %v", violations)
	}
}

func TestWikiValidatorAllowsMetaSidecars(t *testing.T) {
	prod := t.TempDir()
	staging := t.TempDir()

	writeTestFile(t, staging, "wiki/.meta/index.json", `{"pages":[]}`)
	writeTestFile(t, staging, "wiki/semantic/a.md", validFrontmatter)

	v := NewWikiValidator(DefaultACPMaxDiffBytes)
	violations, err := v.Validate(prod, staging)
	if err != nil {
		t.Fatal(err)
	}
	for _, vi := range violations {
		if vi.Path == ".meta/index.json" {
			t.Fatalf("expected .meta/ sidecar to be allowed, got violation: %v", vi)
		}
	}
}

func TestWikiValidatorRejectsNonMarkdownFiles(t *testing.T) {
	prod := t.TempDir()
	staging := t.TempDir()

	writeTestFile(t, staging, "wiki/semantic/foo.txt", "not markdown")

	v := NewWikiValidator(DefaultACPMaxDiffBytes)
	violations, err := v.Validate(prod, staging)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, vi := range violations {
		if vi.Path == "semantic/foo.txt" && contains(vi.Message, "non-Markdown") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected non-Markdown violation for semantic/foo.txt, got %v", violations)
	}
}

func TestIsValidWikiPath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"semantic/a.md", true},
		{"episodic/b.md", true},
		{"procedural/c.md", true},
		{"prospective/d.md", true},
		{"index.md", true},
		{"random/e.md", false},
		{"a.md", false},
		{"semantic/nested/f.md", true},
	}
	for _, tt := range tests {
		if got := IsValidWikiPath(tt.path); got != tt.want {
			t.Errorf("IsValidWikiPath(%q)=%v, want %v", tt.path, got, tt.want)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
