#!/usr/bin/env python3
"""Enforce strictly greater than 90% statement coverage for a Go profile."""
from pathlib import Path
import sys


def statement_counts(profile):
    lines = profile.splitlines()
    if not lines or lines[0] not in ('mode: set', 'mode: count', 'mode: atomic'):
        raise ValueError('missing or invalid Go coverage profile header')
    blocks = {}
    for line in lines[1:]:
        location, statements, hits = line.split()
        statements, hits = int(statements), int(hits)
        if statements < 0 or hits < 0:
            raise ValueError('negative coverage count')
        previous = blocks.get(location, (statements, False))
        if previous[0] != statements:
            raise ValueError('inconsistent statement count for repeated block')
        # -coverpkg emits the same block from multiple test binaries. Merge
        # executions exactly as Go's coverage tool does; count each block once.
        blocks[location] = (statements, previous[1] or hits > 0)
    total = sum(statements for statements, _ in blocks.values())
    covered = sum(statements for statements, hit in blocks.values() if hit)
    if not total:
        raise ValueError('profile contains no statements')
    return covered, total


def main():
    if len(sys.argv) != 2:
        raise SystemExit('Usage: check_coverage.py coverage.out')
    try:
        covered, total = statement_counts(Path(sys.argv[1]).read_text())
    except (OSError, ValueError) as error:
        raise SystemExit(f'Invalid coverage profile: {error}')
    print(f'Statement coverage: {100 * covered / total:.2f}% '
          f'({covered}/{total}); required: >90%')
    if covered * 100 <= total * 90:
        raise SystemExit('Coverage must be strictly greater than 90%.')


if __name__ == '__main__':
    main()
