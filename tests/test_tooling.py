"""Regression checks for portable smoke timestamps and coverage selection."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from smoke import parse_timestamp


class SmokeTimestampTests(unittest.TestCase):
    def test_all_api_fraction_lengths_preserve_microseconds(self):
        for fraction in ('', '1', '12', '123', '1234', '12345', '123456'):
            for suffix in ('Z', '+00:00'):
                with self.subTest(fraction=fraction, suffix=suffix):
                    value = '2026-11-09T13:48:59' + ('.' + fraction if fraction else '') + suffix
                    expected = datetime(2026, 11, 9, 13, 48, 59,
                                        int(fraction.ljust(6, '0')), tzinfo=timezone.utc)
                    self.assertEqual(parse_timestamp(value), expected)

    def test_offset_timestamps_represent_the_same_instant(self):
        expected = parse_timestamp('2026-11-09T13:48:59.12345Z')
        self.assertEqual(parse_timestamp('2026-11-09T19:18:59.123450+05:30'), expected)
        self.assertEqual(parse_timestamp('2026-11-09T09:48:59.12345-04:00'), expected)

    def test_ordering_uses_numeric_fractional_precision(self):
        values = ['2026-11-09T13:48:59.000001Z', '2026-11-09T13:48:59.00001Z',
                  '2026-11-09T13:48:59.12345Z', '2026-11-09T13:48:59.123451Z',
                  '2026-11-09T13:49:00Z']
        parsed = [parse_timestamp(value) for value in values]
        self.assertEqual(parsed, sorted(parsed))
        self.assertEqual(len(set(parsed)), len(values))

    def test_invalid_or_unsupported_timestamps_are_rejected(self):
        for value in ('2026-11-09T13:48:59.1234567Z', '2026-11-09T13:48:59.Z',
                      '2026-11-09T13:48:59.12345', '2026-02-30T13:48:59Z', 'invalid'):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    parse_timestamp(value)


class CoverageSelectionTests(unittest.TestCase):
    def run_selection(self, mode):
        # This PATH deliberately contains no rg. Stop the fake Go command at
        # `go test`, before reports or nested Python checks can run.
        with tempfile.TemporaryDirectory(prefix='coverage-selection-') as directory:
            directory = Path(directory)
            for command in ('sed', 'paste'):
                (directory / command).symlink_to(shutil.which(command))
            fake_go = directory / 'go'
            fake_go.write_text('#!' + sys.executable + '\n' + '''
import json, os, sys
with open('calls.jsonl', 'a') as calls:
    calls.write(json.dumps(sys.argv[1:]) + '\\n')
if sys.argv[1] == 'list':
    mode = os.environ['SELECTION_MODE']
    if mode == 'empty':
        sys.exit(0)
    if mode == 'helpers only':
        print('example/internal/testsupport')
        sys.exit(0)
    print('example/internal/queue')
    print('example/internal/testsupport')
    print('example/internal/testsupport_extra')
    print('example/migrations')
    if mode == 'failure':
        print('package discovery failed', file=sys.stderr)
        sys.exit(7)
    sys.exit(0)
if sys.argv[1] == 'test':
    sys.exit(77)
raise SystemExit('unexpected Go invocation')
''')
            fake_go.chmod(0o755)
            env = {**os.environ, 'PATH': str(directory), 'SELECTION_MODE': mode}
            result = subprocess.run([shutil.which('make'), '-f', str(ROOT / 'Makefile'), 'coverage'],
                                    cwd=directory, env=env, text=True, capture_output=True)
            calls = [json.loads(line) for line in (directory / 'calls.jsonl').read_text().splitlines()]
            return result, calls

    def test_selection_without_ripgrep_preserves_package_scope(self):
        result, calls = self.run_selection('valid')
        self.assertEqual(calls[0], ['list', './internal/...', './migrations/...'])
        self.assertEqual(len(calls), 2, result.stdout + result.stderr)
        self.assertIn('-coverpkg=example/internal/queue,example/internal/testsupport_extra,example/migrations', calls[1])
        self.assertNotIn('command not found', result.stderr)
        self.assertNotEqual(result.returncode, 0)  # Fake go test deliberately stops here.

    def test_failed_discovery_never_runs_tests_even_with_partial_output(self):
        result, calls = self.run_selection('failure')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('package discovery failed', result.stderr)
        self.assertEqual(len(calls), 1)

    def test_empty_selection_never_runs_tests(self):
        for mode in ('empty', 'helpers only'):
            with self.subTest(mode=mode):
                result, calls = self.run_selection(mode)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('No coverage packages found.', result.stderr)
                self.assertEqual(len(calls), 1)


if __name__ == '__main__':
    unittest.main()
