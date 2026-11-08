"""Regression checks for the strict coverage threshold and profile merging."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class CoverageGateTests(unittest.TestCase):
    def run_profile(self, profile):
        with tempfile.TemporaryDirectory(prefix='coverage-gate-') as directory:
            path = Path(directory) / 'coverage.out'
            path.write_text(profile)
            return subprocess.run([sys.executable, str(ROOT / 'tools/check_coverage.py'), str(path)],
                                  text=True, capture_output=True)

    def test_exactly_ninety_is_rejected(self):
        result = self.run_profile('mode: atomic\na.go:1.1,2.1 9 1\na.go:3.1,4.1 1 0\n')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('90.00%', result.stdout)

    def test_above_ninety_passes(self):
        result = self.run_profile('mode: atomic\na.go:1.1,2.1 91 1\na.go:3.1,4.1 9 0\n')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('91.00%', result.stdout)

    def test_repeated_package_blocks_are_merged(self):
        result = self.run_profile('mode: atomic\na.go:1.1,2.1 9 0\na.go:1.1,2.1 9 1\na.go:3.1,4.1 1 1\n')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('(10/10)', result.stdout)

    def test_empty_invalid_and_inconsistent_profiles_fail(self):
        for profile in ('', 'mode: atomic\n', 'mode: atomic\na.go:1.1,2.1 1 1\na.go:1.1,2.1 2 1\n'):
            with self.subTest(profile=profile):
                result = self.run_profile(profile)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('Invalid coverage profile', result.stderr)


if __name__ == '__main__':
    unittest.main()
