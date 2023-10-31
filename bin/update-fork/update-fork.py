#!/usr/bin/env python3

import argparse
import logging
import re
import subprocess

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import TypeAlias

CommitId: TypeAlias = str

TRINO_REMOTE_NAME = "upstream"
TRINO_BRANCH = "master"
FORK_BRANCH = TRINO_BRANCH

class Fork:
    def __init__(self) -> None:
        self.run_git("remote", "get-url", TRINO_REMOTE_NAME)

    def find_latest_cherry_pick(self) -> CommitId:
        latest_log = self.run_git(
            "log",
            "--extended-regexp",
            "--grep",
            "^\\(cherry picked from commit [[:xdigit:]]+\\)",
            "--max-count",
            "1")
        match = re.search(r"\(cherry picked from commit ([0-9a-f]+)\)", latest_log)
        assert match is not None
        return match.group(1)

    def list_trino_commits_after(self, commit: CommitId) -> list[CommitId]:
        return list(reversed(self.run_git(
            "log", "--pretty=format:%H", f"{commit}..{TRINO_REMOTE_NAME}/{TRINO_BRANCH}").splitlines()))

    def try_apply(self, commits: list[CommitId]) -> None:
        for index, commit in enumerate(commits):
            if self.is_release_commit(commit):
                logging.warning(f"Aborting found release commit: {commit}")
                return
            try:
                self.run_git("cherry-pick", "-x", "--keep-redundant-commits", commit)
            except subprocess.CalledProcessError:
                logging.warning("Failed to apply cherry-pick of commit %s", self.commit_description(commit))
                logging.info("Number of applied commits: %d", index)
                return
        logging.info("Number of applied commits: %d", len(commits))

    def strip_release_commits(self, commits: list[CommitId]) -> list[CommitId]:
        if self.is_release_commit(commits[0]):
            if not self.is_post_release_commit(commits[1]):
                logging.error("Found release commit %s, next commit %s should be a post-release commit",
                              commits[0], commits[1])
                raise RuntimeError("Post-release commit not found")
            logging.info("Skipping release commits %s and %s", commits[0], commits[1])
            return commits[2:]
        return commits

    def is_post_release_commit(self, commit: CommitId) -> bool:
        return self.commit_message_startswith(commit, "[maven-release-plugin] prepare for next development iteration")

    def is_release_commit(self, commit: CommitId) -> bool:
        return self.commit_message_startswith(commit, "[maven-release-plugin] prepare release")

    def commit_message_startswith(self, commit: CommitId, msg: str) -> bool:
        commit_msg = self.commit_description(commit).split(maxsplit=1)[1]
        return commit_msg.startswith(msg)

    def commit_description(self, commit: CommitId) -> str:
        return self.run_git("rev-list", "--format=oneline", "--max-count=1", commit)

    def run_git(self, *args: str) -> str:
        """
        Run git command with given arguments in the repository directory.
        Raise exception if command fails.
        :param args: git options/arguments
        :return: output of the command
        """
        return self.command_output("git", *args)

    def command_output(self, *args: str) -> str:
        """
        Run given command in the repository directory capturing output.
        Raise exception if command fails.
        :param args: command and arguments
        :return: output of the command
        """
        cmd_args = list(args)
        logging.debug("Running: %s", " ".join(cmd_args))
        output = subprocess.check_output(cmd_args, encoding="utf-8").strip()
        logging.debug("Got output: %s", output)
        return output

@dataclass
class Arguments:
    debug: bool
    start_from: str

def parse_args() -> Arguments:
    parser = argparse.ArgumentParser(description="Update Trino fork fork with cherry-picks from Trino")
    parser.add_argument("-v", "--debug", action="store_true",
                        help="enable verbose logging")
    parser.add_argument("-s", "--start-from", metavar="COMMIT_SHA",
                        help="start from given commit instead of searching for the last applied")

    parsed: dict[str, Any] = vars(parser.parse_args())
    args = Arguments(**parsed)
    return args


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)
    fork = Fork()
    if args.start_from:
        logging.info("Starting cherry-pick from: %s", fork.commit_description(args.start_from))
        to_apply = [args.start_from, ] + fork.list_trino_commits_after(args.start_from)
    else:
        latest_applied = fork.find_latest_cherry_pick()
        logging.info("Found latest applied commit: %s", fork.commit_description(latest_applied))
        to_apply = fork.list_trino_commits_after(latest_applied)
    if len(to_apply) == 0:
        logging.info("No commits found after latest cherry-pick")
        return
    to_apply = fork.strip_release_commits(to_apply)
    logging.info("List of commits to apply (latest on top): \n%s",
                 "\n".join(map(lambda commit: fork.commit_description(commit), reversed(to_apply))))
    fork.try_apply(to_apply)


if __name__ == "__main__":
    main()
