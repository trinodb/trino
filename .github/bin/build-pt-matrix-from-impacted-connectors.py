#!/usr/bin/env python3

import argparse
import collections
import copy
import itertools
import yaml
import json
import logging
import subprocess
import sys
import tempfile
import unittest


def main():
    parser = argparse.ArgumentParser(description="Filter test matrix modules using list of impacted features.")
    parser.add_argument(
        "-m",
        "--matrix",
        type=argparse.FileType("r"),
        default=".github/test-pt-matrix.yaml",
        help="A YAML file with the PT matrix",
    )
    parser.add_argument(
        "-i",
        "--impacted-features",
        type=argparse.FileType("r"),
        dest="impacted_features",
        help="List of impacted features vs master, one per line",
    )
    parser.add_argument(
        "--impacted-features-prev",
        type=argparse.FileType("r"),
        dest="impacted_features_prev",
        default=None,
        help="List of impacted features vs the merge commit of the previous PR CI run. "
        "When provided together with --prev-non-success-jobs, matrix items whose features "
        "are not impacted vs the previous run AND that are not on the prev-non-success list "
        "are skipped.",
    )
    parser.add_argument(
        "--prev-non-success-jobs",
        type=argparse.FileType("r"),
        dest="prev_non_success_jobs",
        default=None,
        help="File containing rendered job names (one per line) for jobs from the "
        "previous PR CI run whose conclusion was NOT success. Treated as a deny list "
        "for the prev-run gate.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=argparse.FileType("w"),
        default=sys.stdout,
        help="Filename to write JSON output to",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_const",
        dest="loglevel",
        const=logging.DEBUG,
        default=logging.INFO,
        help="Print info level logs",
    )
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        help="test this script instead of executing it",
    )
    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel, format="%(asctime)s %(levelname)s %(message)s")
    if args.test:
        sys.argv = [sys.argv[0]]
        unittest.main()
        return
    build(
        args.matrix,
        args.impacted_features,
        args.output,
        "testing/bin/ptl",
        impacted_features_prev_file=args.impacted_features_prev,
        prev_non_success_jobs_file=args.prev_non_success_jobs,
    )


def render(item):
    """Compute the GHA job name for a PT matrix item.

    Must match the `pt` job's explicit `name:` declaration in ci.yml:
    `pt (${{ matrix.config }}, ${{ matrix.suite }}, ${{ matrix.jdk }})`.
    """
    return "pt ({}, {}, {})".format(item.get("config", ""), item.get("suite", ""), item.get("jdk", ""))


def excluded(item, excludes):
    result = any(exclude.items() <= item.items() for exclude in excludes)
    logging.debug("excluded(%s, %s) returns %s", item, excludes, result)
    return result


def expand_matrix(matrix):
    include = matrix.pop("include", [])
    exclude = matrix.pop("exclude", [])

    # for every key in the matrix dict, convert its values to tuples of key and value
    tuples = [[(k, v) for v in vals] for k, vals in matrix.items()]
    logging.debug("tuples: %s", tuples)
    # then calculate the product of such lists of tuples
    # and convert tuples back to a dict stored in a list
    combinations = list(map(dict, itertools.product(*tuples)))
    logging.debug("combinations: %s", combinations)
    # filter out excludes and add includes as the last step
    # so that excluded combinations can be explicitly added back
    return [item for item in combinations if not excluded(item, exclude)] + include


def load_available_features_for_config(config, suites, ptl_binary_path):
    cmd = [
        ptl_binary_path,
        "suite",
        "describe",
        "--suite",
        ",".join(suites),
        "--config",
        "config-" + config,
        "--format",
        "JSON",
    ]
    logging.debug("executing: %s", " ".join(cmd))
    process = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    logging.debug("ptl suite describe: %s", process)
    if process.returncode != 0:
        raise RuntimeError(f"ptl suite describe failed: {process}")
    for line in process.stdout.splitlines():
        if not line.startswith("{"):
            continue
        logging.debug("Parsing JSON: %s", line)
        ptl_output = json.loads(line)
        logging.debug("Handling JSON object: %s", ptl_output)
        config_features = {}
        for suite in ptl_output.get("suites", []):
            key = (config, suite.get("name"))
            value = set()
            for testRun in suite.get("testRuns", []):
                for features in testRun["environment"].get("features", []):
                    value.add(features)
            config_features[key] = value

        logging.debug("config_features: %s", config_features)
        return config_features
    logging.error("ptl suite describe hasn't returned any JSON line: %s", process)
    return {}


def load_available_features(configToSuiteMap, ptl_binary_path):
    available_features = {}
    for config, suites in configToSuiteMap.items():
        available_features.update(load_available_features_for_config(config, suites, ptl_binary_path))
    return available_features


def tested_features(available_features, config, suite):
    return available_features.get((config, suite), [])


def build(
    matrix_file,
    impacted_file,
    output_file,
    ptl_binary_path,
    impacted_features_prev_file=None,
    prev_non_success_jobs_file=None,
):
    matrix = yaml.load(matrix_file, Loader=yaml.Loader)
    logging.info("Read matrix: %s", matrix)
    if impacted_file is None:
        logging.info("No impacted_features, not applying any changes to matrix")
        json.dump(matrix, output_file)
        output_file.write("\n")
        return

    impacted_features = list(filter(None, [line.rstrip() for line in impacted_file.readlines()]))
    logging.info("Read impacted_features: %s", impacted_features)
    impacted_features_prev = None
    if impacted_features_prev_file is not None:
        impacted_features_prev = list(filter(None, [line.rstrip() for line in impacted_features_prev_file.readlines()]))
        logging.info("Read impacted_features_prev: %s", impacted_features_prev)
    prev_non_success = None
    if prev_non_success_jobs_file is not None:
        prev_non_success = set(filter(None, [line.rstrip() for line in prev_non_success_jobs_file.readlines()]))
        logging.info("Read prev_non_success (%d names)", len(prev_non_success))

    result = copy.copy(matrix)
    items = expand_matrix(matrix)
    logging.info("Expanded matrix: %s", items)

    configToSuiteMap = collections.defaultdict(list)
    for item in items:
        configToSuiteMap[item.get("config")].append(item.get("suite"))
    available_features = load_available_features(configToSuiteMap, ptl_binary_path)
    rendered_names = []
    if len(available_features) > 0:
        all_excluded = True
        for item in items:
            features = tested_features(available_features, item.get("config"), item.get("suite"))
            logging.debug("matrix item features: %s", features)
            if not _keep(item, features, impacted_features, impacted_features_prev, prev_non_success):
                if "include" in result and item in result["include"]:
                    logging.info("Removing from include list: %s", item)
                    result["include"].remove(item)
                else:
                    logging.info("Excluding matrix entry: %s", item)
                    result.setdefault("exclude", []).append(item)
            else:
                rendered_names.append(render(item))
        if not rendered_names:
            # if every single item in the matrix is excluded, write an empty matrix
            output_file.write("{}\n")
            return
    _assert_unique_names(rendered_names)
    json.dump(result, output_file)
    output_file.write("\n")


def _keep(item, features, impacted_master, impacted_prev, prev_non_success):
    """Apply both gates. Returns True if the matrix item should be kept."""
    # Gate 1: master-side. Skip if none of the item's features are impacted vs master.
    if not any(f in impacted_master for f in features):
        return False
    # Gate 2: prev-run-side. Skip when none of the item's features are impacted
    # vs the prev merge commit AND the item is NOT on the non-success deny list
    # (i.e., the prev run either succeeded for this item, or didn't run it at
    # all — both equivalent under the empty-diff invariant).
    if impacted_prev is not None and prev_non_success is not None:
        if render(item) not in prev_non_success and not any(f in impacted_prev for f in features):
            return False
    return True


def _assert_unique_names(names):
    if len(names) != len(set(names)):
        seen = {}
        dups = set()
        for n in names:
            if n in seen:
                dups.add(n)
            seen[n] = True
        raise AssertionError(
            "render() produced duplicate job names; matching against prev jobs "
            "API would be ambiguous. Duplicates: " + ", ".join(sorted(dups))
        )


class TestBuild(unittest.TestCase):
    def test_build(self):
        self.maxDiff = None
        # cases are tuples of: name, matrix, impacted, ptl_binary_path, expected
        cases = [
            (
                "impacted features empty, all items are excluded and an empty matrix is returned",
                {
                    "config": ["A", "B", "C"],
                    "suite": ["1", "2", "3"],
                },
                [],
                ".github/bin/fake-ptl",
                {},
            ),
            (
                "no impacted features, ptl is not called, no changes to matrix",
                {
                    "config": ["A", "B", "C"],
                    "suite": ["1", "2", "3"],
                },
                None,
                "invalid",
                {
                    "config": ["A", "B", "C"],
                    "suite": ["1", "2", "3"],
                },
            ),
            (
                "missing features get added to exclude list",
                {
                    "config": ["A", "B", "C"],
                    "suite": ["1", "2", "3"],
                },
                ["A:1", "B:2", "C:1", "C:3", "D:1"],
                ".github/bin/fake-ptl",
                {
                    "config": ["A", "B", "C"],
                    "suite": ["1", "2", "3"],
                    "exclude": [
                        {"config": "A", "suite": "2"},
                        {"config": "A", "suite": "3"},
                        {"config": "B", "suite": "1"},
                        {"config": "B", "suite": "3"},
                        {"config": "C", "suite": "2"},
                    ],
                },
            ),
            (
                "missing features get removed from include list",
                {
                    "config": ["A", "B", "C"],
                    "suite": ["1", "2", "3"],
                    "include": [
                        {"config": "A", "suite": "4"},
                        {"config": "D", "suite": "1"},
                        {"config": "D", "suite": "2"},
                    ],
                },
                ["A:1", "B:2", "C:1", "C:3", "D:1"],
                ".github/bin/fake-ptl",
                {
                    "config": ["A", "B", "C"],
                    "suite": ["1", "2", "3"],
                    "exclude": [
                        {"config": "A", "suite": "2"},
                        {"config": "A", "suite": "3"},
                        {"config": "B", "suite": "1"},
                        {"config": "B", "suite": "3"},
                        {"config": "C", "suite": "2"},
                    ],
                    "include": [
                        {"config": "D", "suite": "1"},
                    ],
                },
            ),
            (
                "missing features get added to exclude list and removed from include",
                {
                    "config": ["A", "B", "C"],
                    "suite": ["1", "2", "3"],
                    "exclude": [
                        {"config": "A", "suite": "1"},
                        {"config": "A", "suite": "2"},
                    ],
                    "include": [
                        {"config": "A", "suite": "1", "jdk": "17"},
                        {"config": "D", "suite": "1"},
                        {"config": "D", "suite": "2"},
                    ],
                },
                ["A:1", "B:2", "C:1", "C:3", "D:1"],
                ".github/bin/fake-ptl",
                {
                    "config": ["A", "B", "C"],
                    "suite": ["1", "2", "3"],
                    "exclude": [
                        {"config": "A", "suite": "1"},
                        {"config": "A", "suite": "2"},
                        {"config": "A", "suite": "3"},
                        {"config": "B", "suite": "1"},
                        {"config": "B", "suite": "3"},
                        {"config": "C", "suite": "2"},
                    ],
                    "include": [
                        {"config": "A", "suite": "1", "jdk": "17"},
                        {"config": "D", "suite": "1"},
                    ],
                },
            ),
            # integration test with real PTL
            (
                "input matrix",
                {
                    "config": ["default"],
                    "suite": ["suite-1", "suite-2", "suite-3", "suite-5"],
                    "jdk": ["11"],
                    "ignore exclusion if": [False],
                    "exclude": [
                        {"config": "default", "ignore exclusion if": False},
                    ],
                    "include": [
                        {
                            "config": "default",
                            "suite": "suite-6-non-generic",
                            "jdk": "11",
                        },
                        {
                            "config": "default",
                            "suite": "suite-7-non-generic",
                            "jdk": "11",
                        },
                        {
                            "config": "default",
                            "suite": "suite-hive-transactional",
                            "jdk": "11",
                        },
                        {"config": "default", "suite": "suite-tpcds", "jdk": "11"},
                        {"config": "default", "suite": "suite-oauth2", "jdk": "11"},
                        {"config": "default", "suite": "suite-ldap", "jdk": "11"},
                        {
                            "config": "default",
                            "suite": "suite-compatibility",
                            "jdk": "11",
                        },
                        {
                            "config": "default",
                            "suite": "suite-hms-only",
                            "jdk": "11",
                        },
                        {"config": "default", "suite": "suite-1", "jdk": "17"},
                        {"config": "default", "suite": "suite-2", "jdk": "17"},
                    ],
                },
                # impacted features
                ["connector:hive"],
                # ptl_binary_path
                "testing/bin/ptl",
                # expected matrix
                {
                    "config": ["default"],
                    "exclude": [
                        {"config": "default", "ignore exclusion if": False},
                    ],
                    "ignore exclusion if": [False],
                    "include": [
                        {
                            "config": "default",
                            "jdk": "11",
                            "suite": "suite-6-non-generic",
                        },
                        {
                            "config": "default",
                            "jdk": "11",
                            "suite": "suite-7-non-generic",
                        },
                        {
                            "config": "default",
                            "jdk": "11",
                            "suite": "suite-hive-transactional",
                        },
                        {"config": "default", "jdk": "11", "suite": "suite-tpcds"},
                        {"config": "default", "jdk": "11", "suite": "suite-ldap"},
                        {
                            "config": "default",
                            "jdk": "11",
                            "suite": "suite-compatibility",
                        },
                        {
                            "config": "default",
                            "jdk": "11",
                            "suite": "suite-hms-only",
                        },
                        {"config": "default", "jdk": "17", "suite": "suite-1"},
                        {"config": "default", "jdk": "17", "suite": "suite-2"},
                    ],
                    "jdk": ["11"],
                    "suite": ["suite-1", "suite-2", "suite-3", "suite-5"],
                },
            ),
        ]
        for name, matrix, impacted, ptl_binary_path, expected in cases:
            with self.subTest(name):
                with tempfile.TemporaryFile("w+") as matrix_file, tempfile.TemporaryFile(
                    "w+"
                ) as impacted_file, tempfile.TemporaryFile("w+") as output_file:
                    # given
                    yaml.dump(matrix, matrix_file)
                    matrix_file.seek(0)
                    if impacted is not None:
                        impacted_file.write("\n".join(impacted))
                        impacted_file.seek(0)
                        impacted_file_final = impacted_file
                    else:
                        impacted_file_final = None
                    # when
                    build(matrix_file, impacted_file_final, output_file, ptl_binary_path)
                    output_file.seek(0)
                    output = json.load(output_file)
                    # then
                    self.assertEqual(output, expected)

    def _run_prev(self, matrix, impacted, impacted_prev, prev_non_success, expected, ptl_binary_path=".github/bin/fake-ptl"):
        with tempfile.TemporaryFile("w+") as matrix_file, \
                tempfile.TemporaryFile("w+") as impacted_file, \
                tempfile.TemporaryFile("w+") as impacted_prev_file, \
                tempfile.TemporaryFile("w+") as prev_non_success_file, \
                tempfile.TemporaryFile("w+") as output_file:
            yaml.dump(matrix, matrix_file)
            matrix_file.seek(0)
            impacted_file.write("\n".join(impacted))
            impacted_file.seek(0)
            impacted_prev_file.write("\n".join(impacted_prev))
            impacted_prev_file.seek(0)
            prev_non_success_file.write("\n".join(prev_non_success))
            prev_non_success_file.seek(0)
            build(
                matrix_file,
                impacted_file,
                output_file,
                ptl_binary_path,
                impacted_features_prev_file=impacted_prev_file,
                prev_non_success_jobs_file=prev_non_success_file,
            )
            output_file.seek(0)
            output = json.load(output_file)
            self.assertEqual(output, expected)

    def test_prev_skip_when_not_on_deny_list_and_no_diff(self):
        # A:1 and B:2 are present (per fake-ptl mapping); neither failed prev,
        # no diff vs prev -> both skipped.
        self._run_prev(
            matrix={"config": ["A", "B"], "suite": ["1", "2"]},
            impacted=["A:1", "A:2", "B:1", "B:2"],
            impacted_prev=[],
            prev_non_success=[],  # nothing failed prev
            expected={},
        )

    def test_prev_runs_when_on_deny_list(self):
        # B:2 failed (timed_out) in prev -> runs even with no diff.
        self._run_prev(
            matrix={"config": ["A", "B"], "suite": ["1", "2"]},
            impacted=["A:1", "B:2"],
            impacted_prev=[],
            prev_non_success=["pt (B, 2, )"],  # rendered name for jdk=""
            expected={
                "config": ["A", "B"],
                "suite": ["1", "2"],
                "exclude": [
                    # A:1: master-impacted but no diff vs prev and not on deny -> prev-gate skips it
                    {"config": "A", "suite": "1"},
                    {"config": "A", "suite": "2"},  # not impacted vs master
                    {"config": "B", "suite": "1"},  # not impacted vs master
                    # B:2 kept: on deny list -> prev-gate doesn't fire -> runs
                ],
            },
        )

    def test_master_gate_dominates_over_prev(self):
        # A:2 not impacted vs master -> dropped regardless of prev state.
        self._run_prev(
            matrix={"config": ["A"], "suite": ["1", "2"]},
            impacted=["A:1"],  # only A:1 in master-impacted
            impacted_prev=["A:2"],  # diff present for A:2 but moot
            prev_non_success=["pt (A, 1, )", "pt (A, 2, )"],
            expected={
                "config": ["A"],
                "suite": ["1", "2"],
                "exclude": [
                    # A:1 runs (on deny list, master-impacted)
                    {"config": "A", "suite": "2"},  # master gate dropped it
                ],
            },
        )


if __name__ == "__main__":
    main()
