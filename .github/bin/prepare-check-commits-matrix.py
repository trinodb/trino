#!/usr/bin/env python3

import argparse
import json
import logging
import sys
import tempfile
import unittest

import csv
import json


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
    commits = []
    for i, entry in enumerate(entries):
        try:
            commit_hash, _, subject = entry
            # TODO: add filtering based on GitHub Actions cache entries
            if not has_followup(entries, i, commit_hash, subject):
                commits.append(commit_hash)
        except ValueError:
            # ignore lines which don't match the expected CSV pattern
            pass
    if len(commits) > 0:
        json.dump({'include': [{'commit': commit} for commit in commits]}, output_file, separators=(',', ':'), sort_keys=True)


def has_followup(entries, i, commit_hash, subject):
    for later_entry in entries[i+1:]:
        _, _, later_subject = later_entry
        if later_subject.startswith(('fixup! ', 'squash! ', 'amend! ')) and (subject in later_subject or commit_hash in later_subject):
            return True
    return False


class TestBuild(unittest.TestCase):
    PERFORMANCE_TEST_SIZE = 1000

    def test_build(self):
        cases = [
            (
                "Empty test",
                (),
                []
            ),
            (
                "Malformed input test",
                (
                    "c1,t1\n"
                ),
                []
            ),
            (
                "Basic test",
                (
                    "c1,t1,Hello World\n",
                ),
                ['{"include":[{"commit":"c1"}]}']
            ),
            (
                "Add a new entry",
                (
                    "c1,t1,Hello World\n",
                    "c2,t2,Quick brown fox\n",
                ),
                ['{"include":[{"commit":"c1"},{"commit":"c2"}]}']
            ),
            (
                "Add a fixup",
                (
                    "c1,t1,Hello World\n",
                    "c2,t2,Quick brown fox\n",
                    "c3,t3,fixup! Hello World - fixed a bug\n",
                ),
                ['{"include":[{"commit":"c2"},{"commit":"c3"}]}']
            ),
            (
                "Add a floating fixup",
                (
                    "c1,t1,Hello World\n",
                    "c2,t2,Quick brown fox\n",
                    "c3,t3,fixup! Unknown commits are fun!\n",
                ),
                ['{"include":[{"commit":"c1"},{"commit":"c2"},{"commit":"c3"}]}']
            ),
            (
                "Add a squash",
                (
                    "c1,t1,Hello World\n",
                    "c2,t2,Quick brown fox\n",
                    "c3,t3,fixup! Hello World - fixed a bug\n",
                    "c4,t4,squash! Quick brown fox - refactoring\n",
                ),
                ['{"include":[{"commit":"c3"},{"commit":"c4"}]}']
            ),
            (
                "Add an amend",
                (
                    "c1,t1,Hello World\n",
                    "c2,t2,Quick brown fox\n",
                    "c3,t3,fixup! Hello World - fixed a bug\n",
                    "c4,t4,squash! Quick brown fox - refactoring\n",
                    "c5,t5,amend! fixup! Hello World - fixed a bug\n",
                ),
                ['{"include":[{"commit":"c4"},{"commit":"c5"}]}']
            ),
            (
                "Add a fixup using commit hash",
                (
                    "c1,t1,Hello World\n",
                    "c2,t2,Quick brown fox\n",
                    "c3,t3,fixup! Hello World - fixed a bug\n",
                    "c4,t4,squash! Quick brown fox - refactoring\n",
                    "c5,t5,amend! fixup! Hello World - fixed a bug\n",
                    "c6,t6,fixup! c3\n",
                ),
                ['{"include":[{"commit":"c4"},{"commit":"c5"},{"commit":"c6"}]}']
            ),
            (
                "Add a squash using commit hash",
                (
                    "c1,t1,Hello World\n",
                    "c2,t2,Quick brown fox\n",
                    "c3,t3,fixup! Hello World - fixed a bug\n",
                    "c4,t4,squash! Quick brown fox - refactoring\n",
                    "c5,t5,amend! fixup! Hello World - fixed a bug\n",
                    "c6,t6,fixup! c3\n",
                    "c7,t7,squash! c4\n",
                ),
                ['{"include":[{"commit":"c5"},{"commit":"c6"},{"commit":"c7"}]}']
            ),
            (
                "Add an amend using commit hash",
                (
                    "c1,t1,Hello World\n",
                    "c2,t2,Quick brown fox\n",
                    "c3,t3,fixup! Hello World - fixed a bug\n",
                    "c4,t4,squash! Quick brown fox - refactoring\n",
                    "c5,t5,amend! fixup! Hello World - fixed a bug\n",
                    "c6,t6,fixup! c3\n",
                    "c7,t7,squash! c4\n",
                    "c8,t8,amend! c5\n",
                ),
                ['{"include":[{"commit":"c6"},{"commit":"c7"},{"commit":"c8"}]}']
            ),
            (
                "Quoted newline test",
                (
                    'c1,t1,"Hello\nWorld"\n',
                    'c2,t2,"fixup! Hello\nWorld"\n'
                ),
                ['{"include":[{"commit":"c2"}]}']
            ),
            (
                "Quoted quote test",
                (
                    'c1,t1,""Hello"World"\n',
                    'c2,t2,"fixup! "Hello"World"\n'
                ),
                ['{"include":[{"commit":"c2"}]}']
            ),
            (
                "Performance test",
                # O(n^2) case for our algorithm is a list of commits where the last commit is the fixup to the first one, the second last to the second, etc.
                # Generate the commits that will be fixed later
                [f"c{i},t{i},Fix me\n" for i in range(self.PERFORMANCE_TEST_SIZE)] +
                    # Followed by fixups
                    [f"c{self.PERFORMANCE_TEST_SIZE + i},t{self.PERFORMANCE_TEST_SIZE + i},fixup! c{self.PERFORMANCE_TEST_SIZE - (i + 1)}\n" for i in range(self.PERFORMANCE_TEST_SIZE)],
                # Expect fixups to survive
                [f'{{"include":[{self.calculate_expected(range(self.PERFORMANCE_TEST_SIZE, 2 * self.PERFORMANCE_TEST_SIZE))}]}}']
            ),
        ]
        for test_name, input_lines, expected in cases:
            with self.subTest(test_name), tempfile.TemporaryFile("w+") as input_file, tempfile.TemporaryFile("w+") as output_file:
                # given
                input_file.writelines(input_lines)
                input_file.seek(0)
                # when
                build(input_file, output_file)
                output_file.seek(0)
                output = [line.rstrip() for line in output_file.readlines()]    # Remove trailing newlines
                # then
                self.assertEqual(output, expected)

    def calculate_expected(self, seq):
        return ",".join([f'{{"commit":"c{i}"}}' for i in seq])


if __name__ == "__main__":
    main()
