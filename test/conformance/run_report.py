#!/usr/bin/env python3
"""Runs an eclipse-paho/paho.mqtt.testing client_test module against a
running broker and prints/writes a JSON report — bypasses that module's
own buggy __main__ CLI parsing (its `if o in ("--help")` check is a
one-element-tuple-without-a-comma bug that matches "-h" as a substring
of "--help", and it never strips consumed argv before calling
unittest.main(), which then chokes on argparse's own arg validation) by
importing it as a module and driving unittest's API directly instead.

Report classification is four-valued, not a plain pass/fail count:
  passed  - behavior verified as expected
  failed  - Keel/mochi-mqtt behavior did not match the MQTT spec
  harness - the underlying test/client library itself is the demonstrated
            cause, not the broker
n/a is not emitted here (no test in this runner is currently considered
inapplicable), but is reserved in report consumers for that case.

HARNESS is an asserted classification, not a fallback result. A failing
test name only becomes "harness" — see classify() below — when BOTH:
  1. it's listed in KNOWN_HARNESS_ISSUES (a human decided this, it's
     never inferred from the failure itself), AND
  2. its evidence file exists and passes evidence_is_valid()'s
     structural check (the required sections are present and the
     document's own Result section explicitly says HARNESS).
Anything else — an unlisted failure, a listed one whose evidence file
went missing or was gutted, or one whose own Result section doesn't
actually assert HARNESS — fails closed back to a real "failed", never a
silent pass-through. This is deliberately unit-tested (test_run_report.py)
so the reporting semantics can't quietly regress into something that
lets a real bug hide as a harness issue to keep CI green.
"""
import argparse
import importlib
import json
import os
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))

# Test name -> evidence file (relative to this script's directory). Each
# entry must have a corresponding, structurally valid document under
# evidence/ — see evidence_is_valid(). Keep this list short; every entry
# is a deliberate, reviewed classification, not a place to dump
# inconvenient failures.
KNOWN_HARNESS_ISSUES = {
    "test_flow_control2": "evidence/test_flow_control2.md",
}

# Sections an evidence document must contain, verbatim, to back a HARNESS
# classification — mirrors test/conformance/evidence/test_flow_control2.md's
# structure. A machine-checkable proxy for CONTRIBUTING.md's full
# requirement list (documented evidence, reproducible behavior,
# identification of the failing external component, proof the broker's
# own protocol behavior is correct) — not a replacement for the human
# review that wrote the prose in the first place.
REQUIRED_EVIDENCE_SECTIONS = [
    "## Requirement",
    "## Test",
    "## Environment",
    "## Expected behavior",
    "## Observed behavior",
    "## Evidence",
    "## Result",
]


def evidence_is_valid(evidence_path):
    """True only if evidence_path exists, contains every required
    section, and its Result section explicitly asserts HARNESS."""
    if not os.path.isfile(evidence_path):
        return False
    with open(evidence_path, encoding="utf-8") as f:
        text = f.read()
    if not all(section in text for section in REQUIRED_EVIDENCE_SECTIONS):
        return False
    result_section = text.split("## Result", 1)[1]
    result_section = result_section.split("##", 1)[0]  # stop at the next header, if any
    return "HARNESS" in result_section


def classify(failing_names, base_dir=_HERE):
    """Splits a set/iterable of failing test method names into
    (failed, harness), both sorted lists. See module doc for the
    fail-closed rule governing what can become "harness"."""
    failed = []
    harness = []
    for name in sorted(failing_names):
        evidence_rel = KNOWN_HARNESS_ISSUES.get(name)
        if evidence_rel and evidence_is_valid(os.path.join(base_dir, evidence_rel)):
            harness.append(name)
        else:
            failed.append(name)
    return failed, harness


def exit_code_for(failed):
    """Non-zero if and only if there's at least one real (non-harness)
    failure — a known, evidence-backed harness issue must never affect
    the exit code."""
    return 0 if not failed else 1


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--suite-dir", required=True, help="path to paho.mqtt.testing/interoperability")
    p.add_argument("--module", required=True, choices=["client_test", "client_test5"])
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=1883)
    p.add_argument("--name", required=True, help="report key, e.g. mqtt_3_1_1")
    p.add_argument("--json-out", help="also write the report to this file")
    args = p.parse_args()

    sys.path.insert(0, args.suite_dir)
    mod = importlib.import_module(args.module)

    mod.host = args.host
    mod.port = args.port
    mod.nosubscribe_topics = ("test/nosubscribe",)
    if args.module == "client_test5":
        # Mirrors client_test5.py's own __main__ block exactly — its
        # test_shared_subscriptions references a module-level
        # `topic_prefix` global that only that block sets; omitting it
        # raises NameError (not a broker bug, a gap in this wrapper).
        mod.topic_prefix = "client_test5/"
        mod.topics = [mod.topic_prefix + t for t in ["TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA"]]
        mod.wildtopics = [mod.topic_prefix + t for t in ["TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#"]]
    else:
        mod.topics = ("TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA")
        mod.wildtopics = ("TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#")

    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(mod)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    failing_names = {test._testMethodName for test, _ in result.failures + result.errors}
    failed, harness = classify(failing_names)
    passed = result.testsRun - len(failing_names)

    report = {
        args.name: {
            "passed": passed,
            "failed": len(failed),
            "harness": len(harness),
            "failed_tests": failed,
            "harness_issues": [{"test": n, "evidence": KNOWN_HARNESS_ISSUES[n]} for n in harness],
        }
    }
    report_json = json.dumps(report, indent=2)
    print(report_json)
    if args.json_out:
        with open(args.json_out, "w") as f:
            f.write(report_json + "\n")

    return exit_code_for(failed)


if __name__ == "__main__":
    sys.exit(main())
