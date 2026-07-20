#!/usr/bin/env python3

import argparse
import json
import logging
import os
import sys
import tempfile
import unittest


def main():
    parser = argparse.ArgumentParser(
        description="Process a Scalpel impact analysis report: filter a test matrix, "
        "write a flat list of impacted modules, or build a Maven -pl value."
    )
    parser.add_argument(
        "-m",
        "--matrix",
        type=argparse.FileType("r"),
        help="A YAML file with the test matrix",
    )
    parser.add_argument(
        "-i",
        "--impacted",
        metavar="FILE",
        help="Write the list of impacted modules to this file, one per line, "
        "as paths, not artifact ids; the file is removed instead when "
        "everything is impacted",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=argparse.FileType("w"),
        default=sys.stdout,
        help="Filename to write impacted modules matrix JSON to",
    )
    parser.add_argument(
        "-r",
        "--report",
        default="target/scalpel-report.json",
        help="Scalpel JSON report produced with -Dscalpel.mode=report; a missing "
        "report or a full build trigger means everything is impacted",
    )
    parser.add_argument(
        "-e",
        "--exclude",
        nargs="*",
        default=[],
        metavar="PATH",
        help="Module paths to remove from the impacted modules list",
    )
    parser.add_argument(
        "-s",
        "--select",
        metavar="EXCLUSIONS",
        help="Print a Maven -pl value selecting the impacted modules that are not in "
        "the given -pl exclusions value (e.g. '!:trino-docs,!:trino-server'). Prints "
        "the exclusions themselves when everything is impacted, and nothing when no "
        "modules need to be built",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_const",
        dest="loglevel",
        const=logging.INFO,
        default=logging.WARNING,
        help="Print info level logs",
    )
    parser.add_argument(
        "-t",
        "--test",
        action='store_true',
        help="test this script instead of executing it",
    )

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    if args.test:
        sys.argv = [sys.argv[0]]
        unittest.main()
        return
    impacted = read_report(args.report, args.exclude)
    if args.impacted:
        write_impacted(impacted, args.impacted)
    if args.select is not None:
        print(select(impacted, args.select))
    if args.matrix:
        build(args.matrix, impacted, args.output)


def build(matrix_file, impacted, output_file):
    # imported here so that call sites which don't filter a matrix don't need PyYAML
    import yaml

    matrix = yaml.load(matrix_file, Loader=yaml.Loader)
    logging.info("Read matrix: %s", matrix)
    logging.info("Read impacted: %s", impacted)

    modules = []
    for item in matrix.get("modules", []):
        module = check_modules(item, impacted)
        if module is None:
            logging.info("Excluding matrix section: %s", item)
            continue
        modules.append(module)
    if "modules" in matrix:
        matrix["modules"] = modules

    include = []
    for item in matrix.get("include", []):
        modules = check_modules(item.get("modules", []), impacted)
        if modules is None:
            logging.info("Excluding matrix section: %s", item)
            continue
        item["modules"] = modules
        include.append(item)
    if "include" in matrix:
        if include:
            matrix["include"] = include
        else:
            del matrix["include"]

    json.dump(matrix, output_file)
    output_file.write("\n")


def check_modules(modules, impacted):
    if isinstance(modules, str):
        modules = [modules]
    # `impacted` can be empty when Scalpel detected no changes, and None when Scalpel
    # was not run at all or triggered a full build. The latter is the case on builds
    # on master branch. For these builds we want to run all tests, so we don't filter
    # out any modules.
    if impacted and not any(module in impacted for module in modules):
        return None
    # concatenate because matrix values should be primitives
    return ",".join(modules)


def read_report(report_file, excludes):
    """Returns a sorted list of impacted module paths: directly changed modules, modules
    affected by transitive dependency changes, and their downstream dependents. Modules
    included only as upstream dependencies are not impacted. Returns None when everything
    is impacted: the report is missing (Scalpel did not run, e.g. on master) or Scalpel
    triggered a full build."""
    if not os.path.exists(report_file):
        logging.info("No Scalpel report at %s, everything is impacted", report_file)
        return None
    with open(report_file) as f:
        report = json.load(f)
    if report.get("fullBuildTriggered"):
        logging.info(
            "Scalpel triggered a full build (trigger: %s), everything is impacted",
            report.get("triggerFile"),
        )
        return None
    # the root module path is empty in the report, represent it as "."
    impacted = sorted(
        {
            module["path"] or "."
            for module in report.get("affectedModules", [])
            if module.get("category") != "UPSTREAM"
        }
        - set(excludes)
    )
    logging.info("Impacted modules: %s", impacted)
    return impacted


def write_impacted(impacted, filename):
    if impacted is None:
        # a missing file tells consumers that everything is impacted
        if os.path.exists(filename):
            os.remove(filename)
        return
    with open(filename, "w") as output:
        output.writelines(module + "\n" for module in impacted)


def select(impacted, exclusions, module_paths=None):
    """Returns a Maven -pl value selecting the impacted modules that are not in the
    given -pl exclusions value (a comma-separated list of !:artifactId or !path
    selectors). When everything is impacted (Scalpel did not run, triggered a full
    build, or the root module is impacted), the exclusions themselves are returned
    so that Maven builds everything else. Returns an empty string when no modules
    need to be built."""
    excluded = [selector.strip() for selector in exclusions.split(",") if selector.strip()]
    if impacted is None or "." in impacted:
        return ",".join(excluded)
    if module_paths is None:
        module_paths = read_module_paths()
    excluded_paths = set()
    for selector in excluded:
        if not selector.startswith("!"):
            raise ValueError(f"Not an exclusion: {selector}")
        selector = selector[1:]
        if selector.startswith(":"):
            artifact_id = selector[1:]
            if artifact_id not in module_paths:
                raise ValueError(f"Unknown module: {artifact_id}")
            excluded_paths.add(module_paths[artifact_id])
        else:
            excluded_paths.add(selector)
    return ",".join(module for module in impacted if module not in excluded_paths)


def read_module_paths(root_pom="pom.xml"):
    """Returns a map from artifactId to module path for all modules of the root POM."""
    import xml.etree.ElementTree as ET

    ns = {"maven": "http://maven.apache.org/POM/4.0.0"}
    modules = {}
    for module in ET.parse(root_pom).getroot().findall("maven:modules/maven:module", ns):
        path = module.text.strip()
        pom = ET.parse(os.path.join(os.path.dirname(root_pom), path, "pom.xml"))
        modules[pom.getroot().find("maven:artifactId", ns).text.strip()] = path
    return modules


class TestBuild(unittest.TestCase):
    def test_build(self):
        import yaml

        cases = [
            # basic test
            (
                {
                    "modules": ["a", "b"],
                },
                ["a"],
                {
                    "modules": ["a"],
                },
            ),
            # everything is impacted
            (
                {
                    "modules": ["a", "b"],
                },
                None,
                {
                    "modules": ["a", "b"],
                },
            ),
            # include adds a new entry
            (
                {
                    "modules": ["a", "b"],
                    "include": [
                        {"modules": "c"},
                        {"modules": "d"},
                    ],
                },
                ["a", "d"],
                {
                    "modules": ["a"],
                    "include": [
                        {"modules": "d"},
                    ],
                },
            ),
            # include entry overwrites one in matrix
            (
                {
                    "modules": ["a", "b"],
                    "include": [
                        {"modules": "a", "options": "-Pprofile"},
                    ],
                },
                ["a"],
                {
                    "modules": ["a"],
                    "include": [
                        {"modules": "a", "options": "-Pprofile"},
                    ],
                },
            ),
            # with excludes
            (
                {
                    "modules": ["a", "b"],
                    "exclude": ["b"],
                },
                ["a"],
                {
                    "modules": ["a"],
                    "exclude": ["b"],
                },
            ),
        ]
        for matrix, impacted, expected in cases:
            with self.subTest():
                # given
                matrix_file = tempfile.TemporaryFile("w+")
                yaml.dump(matrix, matrix_file)
                matrix_file.seek(0)
                output_file = tempfile.TemporaryFile("w+")
                # when
                build(matrix_file, impacted, output_file)
                output_file.seek(0)
                output = json.load(output_file)
                # then
                self.assertEqual(output, expected)
                matrix_file.close()
                output_file.close()


class TestReadReport(unittest.TestCase):
    def read(self, report, excludes=()):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as report_file:
            json.dump(report, report_file)
        try:
            return read_report(report_file.name, list(excludes))
        finally:
            os.remove(report_file.name)

    def test_missing_report(self):
        self.assertIsNone(read_report("/nonexistent/scalpel-report.json", []))

    def test_full_build(self):
        report = {
            "fullBuildTriggered": True,
            "triggerFile": ".mvn/maven.config",
            "affectedModules": [],
        }
        self.assertIsNone(self.read(report))

    def test_affected_modules(self):
        report = {
            "fullBuildTriggered": False,
            "affectedModules": [
                {"path": "core/trino-main", "category": "DIRECT"},
                {"path": "plugin/trino-hive", "category": "DOWNSTREAM"},
                {"path": "core/trino-spi", "category": "UPSTREAM"},
                {"path": "", "category": "DIRECT"},
                {"path": "testing/trino-tests", "category": "DOWNSTREAM"},
            ],
        }
        self.assertEqual(
            self.read(report, excludes=["testing/trino-tests"]),
            [".", "core/trino-main", "plugin/trino-hive"],
        )

    def test_no_changes(self):
        self.assertEqual(self.read({"fullBuildTriggered": False, "affectedModules": []}), [])


class TestWriteImpacted(unittest.TestCase):
    def test_write(self):
        filename = tempfile.mktemp()
        write_impacted(["a", "b/c"], filename)
        try:
            with open(filename) as impacted_file:
                self.assertEqual(impacted_file.read(), "a\nb/c\n")
        finally:
            os.remove(filename)

    def test_everything_impacted_removes_file(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as stale:
            stale.write("stale\n")
        write_impacted(None, stale.name)
        self.assertFalse(os.path.exists(stale.name))


class TestSelect(unittest.TestCase):
    MODULE_PATHS = {"trino-d": "plugin/trino-d"}

    def test_impacted_modules(self):
        self.assertEqual(select(["a", "b/c"], "!:trino-d", self.MODULE_PATHS), "a,b/c")

    def test_excluded_modules(self):
        self.assertEqual(
            select(["a", "b/c", "plugin/trino-d"], "\n  !:trino-d,\n  !b/c", self.MODULE_PATHS),
            "a",
        )

    def test_nothing_impacted(self):
        self.assertEqual(select([], "!:trino-d", self.MODULE_PATHS), "")

    def test_everything_excluded(self):
        self.assertEqual(select(["plugin/trino-d"], "!:trino-d", self.MODULE_PATHS), "")

    def test_everything_impacted(self):
        self.assertEqual(select(None, "\n  !:trino-d,\n  !e", self.MODULE_PATHS), "!:trino-d,!e")

    def test_root_impacted(self):
        self.assertEqual(select([".", "a"], "!:trino-d", self.MODULE_PATHS), "!:trino-d")

    def test_unknown_module(self):
        with self.assertRaises(ValueError):
            select(["a"], "!:trino-nonexistent", self.MODULE_PATHS)


if __name__ == "__main__":
    main()
