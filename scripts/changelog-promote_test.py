#!/usr/bin/env python3
"""Tests for changelog-promote.py.

Run: python3 scripts/changelog-promote_test.py
"""

from __future__ import annotations

import importlib.util
import sys
import tempfile
from pathlib import Path

# Load changelog-promote.py as a module (hyphen in filename prevents normal import).
_spec = importlib.util.spec_from_file_location(
    "changelog_promote",
    Path(__file__).resolve().parent / "changelog-promote.py",
)
assert _spec is not None and _spec.loader is not None
cp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cp)


MINIMAL = """\
# Changelog

## [Unreleased]

### Added
- New feature X.

## [0.1.0] - 2026-01-01

### Fixed
- Initial bug.

[Unreleased]: https://github.com/bouine-cache/bouine/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.0
"""

EXPECTED = """\
# Changelog

## [Unreleased]

## [0.2.0] - 2026-06-15

### Added
- New feature X.

## [0.1.0] - 2026-01-01

### Fixed
- Initial bug.

[Unreleased]: https://github.com/bouine-cache/bouine/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/bouine-cache/bouine/releases/tag/v0.2.0
[0.1.0]: https://github.com/bouine-cache/bouine/releases/tag/v0.1.0
"""


def run_promote(tmp: Path, version: str, date: str) -> str:
    changelog = tmp / "CHANGELOG.md"
    changelog.write_text(MINIMAL)
    cp.CHANGELOG = changelog
    rc = cp.main([sys.argv[0], version, date])
    assert rc == 0, f"expected rc=0, got {rc}"
    return changelog.read_text()


def test_basic_promotion(tmp: Path) -> None:
    result = run_promote(tmp, "0.2.0", "2026-06-15")
    assert result == EXPECTED, f"result mismatch:\n{result}"


def test_version_already_exists(tmp: Path) -> None:
    changelog = tmp / "CHANGELOG.md"
    changelog.write_text(MINIMAL)
    cp.CHANGELOG = changelog
    rc = cp.main([sys.argv[0], "0.1.0", "2026-06-15"])
    assert rc != 0, "expected non-zero rc for duplicate version"


def test_v_prefix_stripped(tmp: Path) -> None:
    result = run_promote(tmp, "v0.3.0", "2026-07-01")
    assert "## [0.3.0] - 2026-07-01" in result
    assert "[0.3.0]: https://github.com/bouine-cache/bouine/releases/tag/v0.3.0" in result


def test_empty_unreleased(tmp: Path) -> None:
    empty = MINIMAL.replace("### Added\n- New feature X.\n\n## [0.1.0]", "## [0.1.0]")
    changelog = tmp / "CHANGELOG.md"
    changelog.write_text(empty)
    cp.CHANGELOG = changelog
    rc = cp.main([sys.argv[0], "0.2.0", "2026-06-15"])
    assert rc == 0


def test_missing_unreleased_section(tmp: Path) -> None:
    no_unreleased = MINIMAL.replace(
        "## [Unreleased]\n\n### Added\n- New feature X.\n\n", ""
    )
    changelog = tmp / "CHANGELOG.md"
    changelog.write_text(no_unreleased)
    cp.CHANGELOG = changelog
    rc = cp.main([sys.argv[0], "0.2.0", "2026-06-15"])
    assert rc != 0, "expected non-zero rc for missing [Unreleased]"


def test_missing_comparison_link(tmp: Path) -> None:
    no_link = MINIMAL.replace(
        "[Unreleased]: https://github.com/bouine-cache/bouine/compare/v0.1.0...HEAD\n",
        "",
    )
    changelog = tmp / "CHANGELOG.md"
    changelog.write_text(no_link)
    cp.CHANGELOG = changelog
    rc = cp.main([sys.argv[0], "0.2.0", "2026-06-15"])
    assert rc != 0, "expected non-zero rc for missing comparison link"


def main() -> int:
    tests = [
        test_basic_promotion,
        test_version_already_exists,
        test_v_prefix_stripped,
        test_empty_unreleased,
        test_missing_unreleased_section,
        test_missing_comparison_link,
    ]
    failures = 0
    for test in tests:
        with tempfile.TemporaryDirectory() as tmp:
            try:
                test(Path(tmp))
                print(f"  PASS  {test.__name__}")
            except AssertionError as e:
                print(f"  FAIL  {test.__name__}: {e}")
                failures += 1
            except Exception as e:
                print(f"  ERROR {test.__name__}: {type(e).__name__}: {e}")
                failures += 1
    print(f"\n{len(tests) - failures}/{len(tests)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
