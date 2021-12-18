#!/usr/bin/env python3

import argparse
import json
import logging
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Filter test matrix modules using list of impacted modules."
    )
    parser.add_argument(
        "-m",
        "--matrix",
        type=argparse.FileType("r"),
        default=".github/test-matrix.json",
        help="A JSON file with the test matrix",
    )
    parser.add_argument(
        "-i",
        "--impacted",
        type=argparse.FileType("r"),
        default="gib-impacted.log",
        help="List of impacted modules, one per line",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=argparse.FileType("w"),
        default=sys.stdout,
        help="Filename to write output to",
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_const",
        dest="loglevel",
        const=logging.DEBUG,
        default=logging.WARNING,
        help="Print lots of debugging statements",
    )

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    build(args.matrix, args.impacted, args.output)


def build(matrix_file, impacted_file, output_file):
    matrix = json.load(matrix_file)
    impacted = list(filter(None, [line.rstrip() for line in impacted_file.readlines()]))
    logging.debug("Read matrix: %s", matrix)
    logging.debug("Read impacted: %s", impacted)
    matrix["include"] = [
        {
            # concatenate because matrix values should be primitives
            "modules": ",".join(item["modules"]),
            "options": item.get("options", ""),
        }
        for item in matrix.get("include", [])
        if any(module in impacted for module in item.get("modules", []))
    ]
    if not matrix["include"]:
        del matrix["include"]
    json.dump(matrix, output_file)
    output_file.write("\n")


if __name__ == "__main__":
    main()
