#!/usr/bin/env python3

import argparse
import json
import logging
import sys
import tempfile
import unittest

import yaml


def main():
    parser = argparse.ArgumentParser(
        description="Filter test matrix modules using list of impacted modules."
    )
    parser.add_argument(
        "-m",
        "--matrix",
        type=argparse.FileType("r"),
        default=".github/test-matrix.yaml",
        help="A YAML file with the test matrix",
    )
    parser.add_argument(
        "-i",
        "--impacted",
        type=argparse.FileType("r"),
        default="gib-impacted.log",
        help="File containing list of impacted modules, one per line, "
        "as paths, not artifact ids",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=argparse.FileType("w"),
        default=sys.stdout,
        help="Filename to write impacted modules matrix JSON to",
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
    build(args.matrix, args.impacted, args.output)


def build(matrix_file, impacted_file, output_file):
    matrix = yaml.load(matrix_file, Loader=yaml.Loader)
    impacted = list(filter(None, [line.strip() for line in impacted_file.readlines()]))
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
        matrix["include"] = include

    json.dump(matrix, output_file)
    output_file.write("\n")


def check_modules(modules, impacted):
    if isinstance(modules, str):
        modules = [modules]
    if impacted and not any(module in impacted for module in modules):
        return None
    # concatenate because matrix values should be primitives
    return ",".join(modules)


class TestBuild(unittest.TestCase):
    def test_build(self):
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
                impacted_file = tempfile.TemporaryFile("w+")
                impacted_file.write("\n".join(impacted))
                impacted_file.seek(0)
                output_file = tempfile.TemporaryFile("w+")
                # when
                build(matrix_file, impacted_file, output_file)
                output_file.seek(0)
                output = json.load(output_file)
                # then
                self.assertEqual(output, expected)
                matrix_file.close()
                impacted_file.close()
                output_file.close()


if __name__ == "__main__":
    main()
