#!/usr/bin/env python3

"""Rewrite the HEAD commit message to satisfy the project commit message policy.

Dependabot generates its own commit messages and offers no configuration for
subject length. Most of them already comply, but some do not:

  * Long subjects, such as
    "Bump actions/setup-java from 6.0.0 to 6.0.1 in /.github/actions/setup"
    (69).
  * Long description lines, such as the "Bumps <artifact> from A to B." line
    that Dependabot writes without a Markdown link when it cannot resolve a
    repository URL for the package.
  * Long lines in the updated-dependencies metadata, such as
    "- dependency-name: io.github.gitflow-incremental-builder:gitflow-incremental-builder".

All of them break the rules that the check-commit-messages job in ci.yml
enforces. This script amends HEAD so those commits comply.

The rules mirror airlift/github-actions/check-commit-messages: subjects are at
most 60 characters and should be at most 50, and ordinary description lines are
at most 79 characters and should wrap at 72.

Subjects are shortened one step at a time, keeping the first result that fits,
so no more detail is dropped than necessary:

  Bump org.apache.commons:commons-configuration2 from 2.13.0 to 2.14.0  (68)
  Bump org.apache.commons:commons-configuration2 to 2.14.0              (56)

Nothing is lost in the process, because Dependabot repeats the full name and
both versions in the description and in its updated-dependencies metadata.

This amends HEAD in place and does not push, which keeps it runnable locally:

    git log -1 --pretty=%B
    .github/bin/reword-dependabot-commit.py
    git log -1 --pretty=%B

Use --dry-run to print the result without amending. When GITHUB_OUTPUT is set,
a "changed" output reports whether the message was rewritten.
"""

import argparse
import os
import re
import subprocess
import sys
import textwrap

MAX_SUBJECT_LENGTH = 60
WRAP_WIDTH = 72
MAX_DESCRIPTION_LINE_LENGTH = 79

URL_PATTERN = re.compile(r"(?:https?://|ssh://|git@|www\.)\S+")
TRAILER_PATTERN = re.compile(
    r"^(?:"
    r"Signed-off-by|Co-authored-by|Assisted-by|Reviewed-by|Acked-by|"
    r"Tested-by|Reported-by|Fixes|Refs|Relates-to|Change-Id"
    r"):\s+\S.+$",
    re.IGNORECASE,
)

# "Bump <name> from <old> to <new>[ <qualifier>]". The name and versions are
# matched non-greedily so a trailing qualifier such as " in /webapp" stays in
# the qualifier group rather than being absorbed into the new version.
BUMP_PATTERN = re.compile(
    r"^Bump (?P<name>\S+) from (?P<old>\S+) to (?P<new>\S+)(?P<qualifier>.*)$"
)
# Trailing scope Dependabot appends: " in /.github/actions/setup",
# " in the airlift group", " in the airlift group across 1 directory".
QUALIFIER_PATTERN = re.compile(
    r"\s+in\s+(?:/\S+|the\s+\S+\s+group)(?:\s+across\s+\d+\s+director(?:y|ies))?$"
)
# Directory count of a grouped update, which can also precede " with N updates",
# as in "Bump the airlift group across 1 directory with 2 updates".
ACROSS_PATTERN = re.compile(r"\s+across\s+\d+\s+director(?:y|ies)")
# Directory of a grouped update, as in "Bump the web-ui-dependencies group in
# /core/trino-web-ui/src/main/resources/webapp with 17 updates".
DIRECTORY_PATTERN = re.compile(r"\s+in\s+/\S+")
# A Maven coordinate, "group:artifact", where the group is a dotted namespace.
COORDINATE_PATTERN = re.compile(r"^[\w.-]+\.[\w-]+:(?P<artifact>[\w.-]+)$")

# Dependabot appends a YAML metadata block delimited by "---" and "...".
# Rewrapping it would corrupt the YAML, so the only change made there is moving
# an overlong value onto its own line, which YAML reads as the same value.
METADATA_START = "---"
METADATA_END = "..."
METADATA_ENTRY_PATTERN = re.compile(
    r"^(?P<prefix>\s*(?:-\s+)?)(?P<key>[\w-]+):\s+(?P<value>\S.*)$"
)


def run_git(arguments: list[str], input_text: str | None = None) -> str:
    result = subprocess.run(
        ["git", *arguments],
        check=True,
        input=input_text,
        stdout=subprocess.PIPE,
        text=True,
    )
    return result.stdout


def shorten_subject(subject: str) -> str:
    """Return the longest form of the subject that fits the length limit."""
    for candidate in subject_candidates(subject):
        if len(candidate) <= MAX_SUBJECT_LENGTH:
            return candidate

    # Nothing fit, so fall back to a hard truncation on a word boundary. This
    # is unreachable for the messages Dependabot generates today, but a silent
    # CI failure later would be worse than a blunt subject.
    return textwrap.shorten(subject, width=MAX_SUBJECT_LENGTH, placeholder="...")


def subject_candidates(subject: str):
    """Yield progressively shorter forms of a Dependabot subject."""
    yield subject

    match = BUMP_PATTERN.match(subject)
    if match is None:
        # Not a "Bump X from A to B" subject, for example
        # "Bump io.airlift:airbase in the airlift group across 1 directory" or
        # "Bump the airlift group across 1 directory with 2 updates". Only the
        # scope can be trimmed, and the group id of a Maven coordinate dropped.
        yield ACROSS_PATTERN.sub("", subject)
        yield DIRECTORY_PATTERN.sub("", ACROSS_PATTERN.sub("", subject))
        trimmed = QUALIFIER_PATTERN.sub("", subject)
        yield trimmed
        words = trimmed.split()
        if len(words) == 2:
            coordinate = COORDINATE_PATTERN.match(words[1])
            if coordinate is not None:
                yield f"Bump {coordinate['artifact']}"
        return

    name = match["name"]
    new = match["new"]
    qualifier = match["qualifier"]

    # Drop the old version; the diff and the description still record it.
    yield f"Bump {name} to {new}{qualifier}"
    # Drop the directory or group scope.
    yield f"Bump {name} to {new}{ACROSS_PATTERN.sub('', qualifier)}"
    yield f"Bump {name} to {new}"

    # Drop the group id from a Maven coordinate; the artifact id identifies the
    # dependency well enough and the description keeps the full coordinate.
    coordinate = COORDINATE_PATTERN.match(name)
    if coordinate is not None:
        yield f"Bump {coordinate['artifact']} to {new}"

    # Last resort: name the dependency without a version, which is what
    # Dependabot itself does for coordinates that are too long to fit.
    yield f"Bump {name}"
    if coordinate is not None:
        yield f"Bump {coordinate['artifact']}"


def is_wrapping_exempt(line: str, in_code_block: bool) -> bool:
    """Mirror the checker's exemptions so only flagged lines are rewrapped."""
    stripped = line.strip()

    if in_code_block or not stripped:
        return True
    if stripped.startswith(">"):
        return True
    if TRAILER_PATTERN.fullmatch(stripped):
        return True

    # A line holding a URL or another token too long to break is exempt only
    # when what remains around that token already fits.
    tokens = stripped.split()
    if any(URL_PATTERN.search(token) for token in tokens):
        return is_wrapped_without_unwrappable_tokens(tokens)
    if any(len(token) > MAX_DESCRIPTION_LINE_LENGTH for token in tokens):
        return is_wrapped_without_unwrappable_tokens(tokens)

    return False


def is_wrapped_without_unwrappable_tokens(tokens: list[str]) -> bool:
    wrappable = [
        token
        for token in tokens
        if not URL_PATTERN.search(token)
        and len(token) <= MAX_DESCRIPTION_LINE_LENGTH
    ]
    return len(" ".join(wrappable)) <= MAX_DESCRIPTION_LINE_LENGTH


def rewrap_description(lines: list[str]) -> list[str]:
    """Rewrap description lines that the checker would reject."""
    rewrapped = []
    in_code_block = False
    in_metadata = False

    for line in lines:
        stripped = line.strip()

        if stripped.startswith("```") or stripped.startswith("~~~"):
            in_code_block = not in_code_block
            rewrapped.append(line)
            continue

        # Leave Dependabot's YAML metadata block alone.
        if not in_metadata and stripped == METADATA_START:
            in_metadata = True
            rewrapped.append(line)
            continue
        if in_metadata:
            if stripped == METADATA_END:
                in_metadata = False
            rewrapped.extend(fold_metadata_line(line))
            continue

        if len(line) <= MAX_DESCRIPTION_LINE_LENGTH or is_wrapping_exempt(
            line, in_code_block
        ):
            rewrapped.append(line)
            continue

        rewrapped.extend(
            textwrap.wrap(
                line,
                width=WRAP_WIDTH,
                break_long_words=False,
                break_on_hyphens=False,
            )
        )

    return rewrapped


def fold_metadata_line(line: str) -> list[str]:
    """Move an overlong metadata value onto its own, further indented line.

    "- dependency-name: <value>" becomes "- dependency-name:" followed by the
    value indented past the key, which YAML parses to the same mapping.
    """
    if len(line) <= MAX_DESCRIPTION_LINE_LENGTH:
        return [line]
    entry = METADATA_ENTRY_PATTERN.match(line)
    if entry is None:
        return [line]
    indent = " " * (len(entry["prefix"]) + 2)
    return [f"{entry['prefix']}{entry['key']}:", f"{indent}{entry['value']}"]


def reword(message: str) -> str:
    lines = message.splitlines()
    if not lines:
        return message

    new_subject = shorten_subject(lines[0])
    description = rewrap_description(lines[1:])

    body = "\n".join(description).strip("\n")
    if body:
        return f"{new_subject}\n\n{body}\n"
    return f"{new_subject}\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reword HEAD to satisfy the commit message policy."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the reworded message without amending the commit.",
    )
    args = parser.parse_args()

    original = run_git(["show", "-s", "--format=%B", "HEAD"]).strip("\n")
    reworded = reword(original).strip("\n")

    if reworded == original:
        print("Commit message already complies, nothing to do.")
        report_changed(False)
        return 0

    if args.dry_run:
        print(reworded)
        report_changed(True)
        return 0

    run_git(["commit", "--amend", "--allow-empty", "--file=-"], input_text=reworded)

    print("Reworded commit message:")
    print(f"  from: {original.splitlines()[0]}")
    print(f"  to:   {reworded.splitlines()[0]}")
    report_changed(True)
    return 0


def report_changed(changed: bool) -> None:
    github_output = os.environ.get("GITHUB_OUTPUT")
    if not github_output:
        return
    with open(github_output, "a", encoding="utf-8") as output:
        output.write(f"changed={str(changed).lower()}\n")


if __name__ == "__main__":
    sys.exit(main())
