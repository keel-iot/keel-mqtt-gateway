#!/usr/bin/env python3
"""Freezes run_report.py's own reporting semantics — the report runner is
part of what makes the conformance result trustworthy, so its behavior
needs the same regression protection as any broker code. Run directly
(no pytest dependency, no network, no broker):

    python3 test/conformance/test_run_report.py -v

run.sh runs this first, before starting any real infrastructure, and
refuses to proceed if it fails — a broken report runner must never be
allowed to silently produce a misleading conformance result.
"""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run_report  # noqa: E402

VALID_EVIDENCE = """# Investigation record

## Requirement
Some MQTT requirement.

## Test
some_test_name

## Environment
Local.

## Expected behavior
X should happen.

## Observed behavior
X happened, confirmed by evidence below.

## Evidence
- some proof

## Result

HARNESS — not a Keel failure, see evidence above.
"""

MISSING_SECTIONS_EVIDENCE = """# Investigation record

## Requirement
Some MQTT requirement.

## Result

HARNESS
"""

WRONG_VERDICT_EVIDENCE = VALID_EVIDENCE.replace("HARNESS", "FAIL")


def _write(tmp_dir, rel_path, content):
    full = os.path.join(tmp_dir, rel_path)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w", encoding="utf-8") as f:
        f.write(content)
    return full


class EvidenceValidationTests(unittest.TestCase):
    def test_missing_file_is_invalid(self):
        self.assertFalse(run_report.evidence_is_valid("/nonexistent/path/evidence.md"))

    def test_valid_evidence_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write(tmp, "evidence/ok.md", VALID_EVIDENCE)
            self.assertTrue(run_report.evidence_is_valid(path))

    def test_missing_required_sections_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write(tmp, "evidence/incomplete.md", MISSING_SECTIONS_EVIDENCE)
            self.assertFalse(run_report.evidence_is_valid(path))

    def test_result_section_not_asserting_harness_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write(tmp, "evidence/wrong-verdict.md", WRONG_VERDICT_EVIDENCE)
            self.assertFalse(run_report.evidence_is_valid(path))


class ClassifyAndExitCodeTests(unittest.TestCase):
    """The matrix that matters: HARNESS must never affect the exit code,
    a real failure always must, and a "known" harness issue whose
    evidence doesn't check out must fall back to a real failure — never
    silently disappear."""

    def setUp(self):
        self._saved_known_issues = dict(run_report.KNOWN_HARNESS_ISSUES)
        self.tmp_ctx = tempfile.TemporaryDirectory()
        self.tmp = self.tmp_ctx.name
        _write(self.tmp, "evidence/known.md", VALID_EVIDENCE)

    def tearDown(self):
        run_report.KNOWN_HARNESS_ISSUES = self._saved_known_issues
        self.tmp_ctx.cleanup()

    def test_pass_plus_pass_exits_zero(self):
        failed, harness = run_report.classify(set(), base_dir=self.tmp)
        self.assertEqual(failed, [])
        self.assertEqual(harness, [])
        self.assertEqual(run_report.exit_code_for(failed), 0)

    def test_pass_plus_harness_exits_zero(self):
        run_report.KNOWN_HARNESS_ISSUES = {"test_known": "evidence/known.md"}
        failed, harness = run_report.classify({"test_known"}, base_dir=self.tmp)
        self.assertEqual(failed, [])
        self.assertEqual(harness, ["test_known"])
        self.assertEqual(run_report.exit_code_for(failed), 0)

    def test_pass_plus_fail_exits_nonzero(self):
        run_report.KNOWN_HARNESS_ISSUES = {}
        failed, harness = run_report.classify({"test_unlisted_failure"}, base_dir=self.tmp)
        self.assertEqual(failed, ["test_unlisted_failure"])
        self.assertEqual(harness, [])
        self.assertNotEqual(run_report.exit_code_for(failed), 0)

    def test_harness_without_valid_evidence_exits_nonzero(self):
        # Listed as "known", but the evidence file doesn't exist — must
        # NOT be silently treated as harness just because it's in the map.
        run_report.KNOWN_HARNESS_ISSUES = {"test_known": "evidence/does-not-exist.md"}
        failed, harness = run_report.classify({"test_known"}, base_dir=self.tmp)
        self.assertEqual(failed, ["test_known"])
        self.assertEqual(harness, [])
        self.assertNotEqual(run_report.exit_code_for(failed), 0)

    def test_harness_with_gutted_evidence_exits_nonzero(self):
        # File exists but no longer says HARNESS (e.g. someone edited it
        # without updating the verdict) — same fail-closed outcome.
        _write(self.tmp, "evidence/known.md", WRONG_VERDICT_EVIDENCE)
        run_report.KNOWN_HARNESS_ISSUES = {"test_known": "evidence/known.md"}
        failed, harness = run_report.classify({"test_known"}, base_dir=self.tmp)
        self.assertEqual(failed, ["test_known"])
        self.assertEqual(harness, [])
        self.assertNotEqual(run_report.exit_code_for(failed), 0)


if __name__ == "__main__":
    unittest.main()
