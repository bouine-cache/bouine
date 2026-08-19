#!/usr/bin/env python3
"""changelog-promote -- cut the [Unreleased] section into a versioned section.

Usage:
    changelog-promote.py <version> [date]

    version: X.Y.Z (without 'v' prefix)
    date:    YYYY-MM-DD (defaults to today UTC)

Transforms CHANGELOG.md:
  1. Replaces the ``## [Unreleased]`` heading with ``## [version] - date``.
  2. Inserts a fresh, empty ``## [Unreleased]`` section above it.
  3. Updates the ``[Unreleased]`` comparison link to point at the new
     version tag and adds a ``[version]`` link entry.

Exits non-zero if the file is missing, the [Unreleased] section is
absent, or the version already exists.
"""

from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path


REPO = "bouine-cache/bouine"
CHANGELOG = Path(__file__).resolve().parent.parent / "CHANGELOG.md"


def main(argv: list[str]) -> int:
    if len(argv) < 2 or len(argv) > 3:
        print(f"usage: {argv[0]} <version> [date]", file=sys.stderr)
        return 2

    version = argv[1].removeprefix("v")
    date = argv[2] if len(argv) == 3 else datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        print(f"error: version '{version}' is not X.Y.Z", file=sys.stderr)
        return 1

    if not CHANGELOG.exists():
        print(f"error: {CHANGELOG} not found", file=sys.stderr)
        return 1

    content = CHANGELOG.read_text()

    # --- guards ---------------------------------------------------------
    if "## [Unreleased]" not in content:
        print("error: no [Unreleased] section in CHANGELOG.md", file=sys.stderr)
        return 1

    if f"## [{version}]" in content:
        print(f"error: version {version} already exists in CHANGELOG.md", file=sys.stderr)
        return 1

    # --- split into header / unreleased / rest --------------------------
    header, sep, rest = content.partition("## [Unreleased]")
    if not sep:
        print("error: could not locate [Unreleased] heading", file=sys.stderr)
        return 1

    # The [Unreleased] body is everything until the next version section.
    # Version sections start with "## [0." or "## [1." etc.
    next_match = re.search(r"\n## \[\d", rest)
    if next_match:
        unreleased_body = rest[: next_match.start()]
        tail = rest[next_match.start() :]
    else:
        unreleased_body = rest
        tail = ""

    # Warn if the Unreleased section is empty.
    stripped = unreleased_body.strip()
    if not stripped:
        print(
            "warning: [Unreleased] section is empty -- nothing to promote",
            file=sys.stderr,
        )

    # --- rebuild --------------------------------------------------------
    new_unreleased = "## [Unreleased]\n\n"
    versioned_heading = f"## [{version}] - {date}"
    new_content = header + new_unreleased + versioned_heading + unreleased_body + tail

    # --- update comparison links at the bottom --------------------------
    tag = f"v{version}"
    compare_base = f"https://github.com/{REPO}/compare"
    releases_base = f"https://github.com/{REPO}/releases/tag"

    # Replace the [Unreleased] link to point at the new tag.
    old_pattern = r"\[Unreleased\]: " + re.escape(compare_base) + r"/v[^\s]+"
    new_content, unreleased_sub_count = re.subn(
        old_pattern,
        f"[Unreleased]: {compare_base}/{tag}...HEAD",
        new_content,
    )
    if unreleased_sub_count == 0:
        print(
            "error: could not find [Unreleased] comparison link to update",
            file=sys.stderr,
        )
        return 1

    # Insert the new version link right after the [Unreleased] link.
    version_link = f"[{version}]: {releases_base}/{tag}\n"
    new_content, version_sub_count = re.subn(
        r"(\[Unreleased\]: [^\n]+\n)",
        r"\1" + version_link,
        new_content,
        count=1,
    )
    if version_sub_count == 0:
        print(
            "error: could not insert version link after [Unreleased] link",
            file=sys.stderr,
        )
        return 1

    CHANGELOG.write_text(new_content)
    print(f"promoted [Unreleased] to [{version}] - {date}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
