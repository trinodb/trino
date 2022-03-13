#!/usr/bin/env python3

import argparse
import itertools
import json
import logging
import sys

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

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
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


if __name__ == "__main__":
    main()
