#!/usr/bin/env python3

import argparse
import json
import logging
import sys
import tempfile
import unittest

import csv


def main():
    parser = argparse.ArgumentParser(
        description="Choose commits to compile using their CSV descriptions."
    )
    parser.add_argument(
        "-i",
        "--input",
        type=argparse.FileType("r"),
        default=sys.stdin,
        help="A CSV file in <commit hash>,<tree hash>,<commit subject>",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=argparse.FileType("w"),
        default=sys.stdout,
        help="Filename to write chosen commit hashes to",
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
    build(args.input, args.output)


def build(input_file, output_file):
    logging.info("input_file: %s", input_file)
    logging.info("output_file: %s", output_file)
    reader = csv.reader(input_file)
    entries = list(reader)
    logging.info("entries: %s", entries)
    for i, entry in enumerate(entries):
        commit_hash = entry[0]
        # TODO: add filtering based on GitHub Actions cache entries
        if not has_followup(entries, i, commit_hash, entry[2]):
            output_file.write(commit_hash)
            output_file.write("\n")


def has_followup(entries, i, commit_hash, subject):
    for later_entry in entries[i+1:]:
        later_subject = later_entry[2]
        if later_subject.startswith(('fixup! ', 'squash! ')) and (subject in later_subject or commit_hash in later_subject):
            return True
    return False


class TestBuild(unittest.TestCase):
    def test_build(self):
        cases = [
            # basic test
            (
                (
                    ("c1", "t1", "Hello World"),
                ),
                ["c1"]
            ),
            # add a new entry
            (
                (
                    ("c1", "t1", "Hello World"),
                    ("c2", "t2", "Quick brown fox"),
                ),
                ["c1", "c2"]
            ),
            # add a fixup
            (
                (
                    ("c1", "t1", "Hello World"),
                    ("c2", "t2", "Quick brown fox"),
                    ("c3", "t3", "fixup! Hello World - fixed a bug"),
                ),
                ["c2", "c3"]
            ),
            # add a squash
            (
                (
                    ("c1", "t1", "Hello World"),
                    ("c2", "t2", "Quick brown fox"),
                    ("c3", "t3", "fixup! Hello World - fixed a bug"),
                    ("c4", "t4", "squash! Quick brown fox - refactoring"),
                ),
                ["c3", "c4"]
            ),
            # add a fixup using commit hash
            (
                (
                    ("c1", "t1", "Hello World"),
                    ("c2", "t2", "Quick brown fox"),
                    ("c3", "t3", "fixup! Hello World - fixed a bug"),
                    ("c4", "t4", "squash! Quick brown fox - refactoring"),
                    ("c5", "t5", "fixup! fixup! c3"),
                ),
                ["c4", "c5"]
            ),
            # add a squash using commit hash
            (
                (
                    ("c1", "t1", "Hello World"),
                    ("c2", "t2", "Quick brown fox"),
                    ("c3", "t3", "fixup! Hello World - fixed a bug"),
                    ("c4", "t4", "squash! Quick brown fox - refactoring"),
                    ("c5", "t5", "fixup! c3"),
                    ("c6", "t6", "squash! c4"),
                ),
                ["c5", "c6"]
            ),
        ]
        for input_rows, expected in cases:
            with self.subTest(), tempfile.TemporaryFile("w+") as input_file, tempfile.TemporaryFile("w+") as output_file:
                # given
                writer = csv.writer(input_file)
                writer.writerows(input_rows)
                input_file.seek(0)
                # when
                build(input_file, output_file)
                output_file.seek(0)
                output = [line[:-1] for line in output_file.readlines()]    # Remove trailing newlines
                # then
                self.assertEqual(output, expected)


if __name__ == "__main__":
    main()
