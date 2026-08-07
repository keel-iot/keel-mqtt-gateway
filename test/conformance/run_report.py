#!/usr/bin/env python3
"""Runs an eclipse-paho/paho.mqtt.testing client_test module against a
running broker and prints a JSON pass/fail report — bypasses that
module's own buggy __main__ CLI parsing (its `if o in ("--help")` check
is a one-element-tuple-without-a-comma bug that matches "-h" as a
substring of "--help", and it never strips consumed argv before calling
unittest.main(), which then chokes on argparse's own arg validation) by
importing it as a module and driving unittest's API directly instead.
"""
import argparse
import importlib
import json
import sys
import unittest


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--suite-dir", required=True, help="path to paho.mqtt.testing/interoperability")
    p.add_argument("--module", required=True, choices=["client_test", "client_test5"])
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=1883)
    p.add_argument("--name", required=True, help="report key, e.g. mqtt_3_1_1")
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

    failed = len(result.failures) + len(result.errors)
    passed = result.testsRun - failed
    report = {args.name: {"passed": passed, "failed": failed}}
    print(json.dumps(report))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
