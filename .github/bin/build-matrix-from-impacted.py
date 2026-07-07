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
        "--impacted-prev",
        type=argparse.FileType("r"),
        default=None,
        help="File containing list of impacted modules vs the merge commit of the "
        "previous PR CI run. When provided together with --prev-non-success-jobs, "
        "matrix items whose modules are not impacted vs the previous run AND that "
        "are not on the prev-non-success list are skipped.",
    )
    parser.add_argument(
        "--prev-non-success-jobs",
        type=argparse.FileType("r"),
        default=None,
        help="File containing rendered job names (one per line) for jobs from the "
        "previous PR CI run whose conclusion was NOT success. Treated as a deny "
        "list for the prev-run gate: a job whose name is on this list is never "
        "carried as 'previously validated'.",
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
    build(args.matrix, args.impacted, args.output, args.impacted_prev, args.prev_non_success_jobs)


def render(item):
    """Compute the GHA job name for a matrix item.

    Must match GitHub Actions' default matrix-job naming for the `test` job
    (`<job_id> (<comma-joined-values-of-present-keys>)`), so this is what the
    next run's `resolve-prev-run` will see in the jobs API as `name`.

    The item must have its `modules` field normalized to a comma-joined string.
    """
    parts = [item["modules"]]
    if item.get("profile"):
        parts.append(item["profile"])
    return f"test ({', '.join(parts)})"


def build(matrix_file, impacted_file, output_file, impacted_prev_file=None, prev_non_success_jobs_file=None):
    matrix = yaml.load(matrix_file, Loader=yaml.Loader)
    impacted = list(filter(None, [line.strip() for line in impacted_file.readlines()]))
    impacted_prev = None
    if impacted_prev_file is not None:
        impacted_prev = list(filter(None, [line.strip() for line in impacted_prev_file.readlines()]))
    prev_non_success = None
    if prev_non_success_jobs_file is not None:
        prev_non_success = set(filter(None, [line.strip() for line in prev_non_success_jobs_file.readlines()]))
    logging.info("Read matrix: %s", matrix)
    logging.info("Read impacted: %s", impacted)
    logging.info("Read impacted_prev: %s", impacted_prev)
    logging.info("Read prev_non_success (%d names)", len(prev_non_success) if prev_non_success is not None else 0)

    # We compute render(item) for every kept item and assert uniqueness, so the
    # next run's resolve-prev-run can unambiguously map a job name back to "did
    # this item run / pass". The workflow uses GHA's default job naming, which
    # matches render() for the test job; nothing is injected into the matrix.
    rendered_names = []

    modules = []
    for item in matrix.get("modules", []):
        normalized = _normalize_top_level(item)
        if not _keep(normalized, impacted, impacted_prev, prev_non_success):
            logging.info("Excluding matrix section: %s", item)
            continue
        modules.append(normalized["modules"])  # matrix value must remain a primitive
        rendered_names.append(render(normalized))
    if "modules" in matrix:
        matrix["modules"] = modules

    include = []
    for item in matrix.get("include", []):
        normalized = _normalize_include(item)
        if not _keep(normalized, impacted, impacted_prev, prev_non_success):
            logging.info("Excluding matrix section: %s", item)
            continue
        rendered_names.append(render(normalized))
        include.append(normalized)
    if "include" in matrix:
        if include:
            matrix["include"] = include
        else:
            del matrix["include"]

    _assert_unique_names(rendered_names)

    json.dump(matrix, output_file)
    output_file.write("\n")


def _normalize_top_level(modules):
    if isinstance(modules, str):
        modules = [modules]
    return {"modules": ",".join(modules)}


def _normalize_include(item):
    raw = item.get("modules", [])
    if isinstance(raw, str):
        raw = [raw]
    result = dict(item)
    result["modules"] = ",".join(raw)
    return result


def _keep(item, impacted, impacted_prev, prev_non_success):
    """Apply both gates. Returns True if the matrix item should be kept."""
    modules = item["modules"].split(",")
    # Gate 1: master-side. `impacted` empty means GIB was not run at all (master
    # builds), in which case keep everything. On PR builds GIB always runs.
    if impacted and not any(m in impacted for m in modules):
        return False
    # Gate 2: prev-run-side. We have prev info only when both inputs are present.
    # Skip when none of J's modules are impacted vs prev AND J is NOT on the
    # non-success deny list (i.e., J either succeeded in prev or wasn't in prev's
    # jobs API at all — both equivalent under the empty-diff invariant).
    if impacted_prev is not None and prev_non_success is not None:
        if render(item) not in prev_non_success and not any(m in impacted_prev for m in modules):
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
    def _run(self, matrix, impacted, expected, impacted_prev=None, prev_non_success=None):
        matrix_file = tempfile.TemporaryFile("w+")
        yaml.dump(matrix, matrix_file)
        matrix_file.seek(0)
        impacted_file = tempfile.TemporaryFile("w+")
        impacted_file.write("\n".join(impacted))
        impacted_file.seek(0)
        impacted_prev_file = None
        if impacted_prev is not None:
            impacted_prev_file = tempfile.TemporaryFile("w+")
            impacted_prev_file.write("\n".join(impacted_prev))
            impacted_prev_file.seek(0)
        prev_non_success_file = None
        if prev_non_success is not None:
            prev_non_success_file = tempfile.TemporaryFile("w+")
            prev_non_success_file.write("\n".join(prev_non_success))
            prev_non_success_file.seek(0)
        output_file = tempfile.TemporaryFile("w+")
        build(matrix_file, impacted_file, output_file, impacted_prev_file, prev_non_success_file)
        output_file.seek(0)
        output = json.load(output_file)
        self.assertEqual(output, expected)

    def test_legacy_basic(self):
        # No prev-run inputs -> classic behavior.
        self._run(
            {"include": [{"modules": "a"}, {"modules": "b"}]},
            ["a"],
            {"include": [{"modules": "a"}]},
        )

    def test_legacy_modules_list_joined(self):
        self._run(
            {"include": [{"modules": ["a", "b"]}]},
            ["a"],
            {"include": [{"modules": "a,b"}]},
        )

    def test_legacy_include_with_profile(self):
        self._run(
            {"include": [{"modules": "a", "profile": "p"}]},
            ["a"],
            {"include": [{"modules": "a", "profile": "p"}]},
        )

    def test_legacy_empty_impacted_keeps_all(self):
        # Master branch builds: GIB didn't run, impacted is empty, keep all.
        self._run(
            {"include": [{"modules": "a"}, {"modules": "b"}]},
            [],
            {"include": [{"modules": "a"}, {"modules": "b"}]},
        )

    def test_prev_skip_when_not_on_deny_list_and_no_diff(self):
        # J passed in prev (== not on deny list) AND no diff -> skip.
        self._run(
            matrix={"include": [{"modules": "a"}, {"modules": "b"}]},
            impacted=["a", "b"],
            impacted_prev=[],
            prev_non_success=[],
            expected={},  # both items skipped -> include empty -> removed
        )

    def test_prev_runs_when_diff_present(self):
        # J's module impacted vs prev -> can't skip even if not on deny list.
        self._run(
            matrix={"include": [{"modules": "a"}, {"modules": "b"}]},
            impacted=["a", "b"],
            impacted_prev=["a"],
            prev_non_success=[],
            expected={"include": [{"modules": "a"}]},
        )

    def test_prev_runs_when_on_deny_list(self):
        # J on prev_non_success (failed/cancelled/timed_out etc.) -> must run.
        self._run(
            matrix={"include": [{"modules": "a"}, {"modules": "b"}]},
            impacted=["a", "b"],
            impacted_prev=[],  # no diff vs prev
            prev_non_success=["test (a)"],  # a failed last time
            expected={"include": [{"modules": "a"}]},
        )

    def test_prev_absent_from_api_treated_as_validated(self):
        # If a job is in current matrix but wasn't in prev jobs API (absent),
        # the empty-diff invariant makes it safe to skip — same as if it passed.
        self._run(
            matrix={"include": [{"modules": "new-module"}]},
            impacted=["new-module"],
            impacted_prev=[],
            # prev_non_success doesn't mention "test (new-module)" at all
            prev_non_success=["test (something-else)"],
            expected={},
        )

    def test_master_gate_dominates(self):
        # Module not impacted vs master is dropped even if otherwise eligible.
        self._run(
            matrix={"include": [{"modules": "a"}, {"modules": "b"}]},
            impacted=["a"],
            impacted_prev=["a", "b"],
            prev_non_success=[],
            expected={"include": [{"modules": "a"}]},
        )

    def test_profile_disambiguates_name(self):
        # Two items differing only by profile render to distinct names, so the
        # deny list applies to them independently.
        self._run(
            matrix={"include": [{"modules": "a"}, {"modules": "a", "profile": "p"}]},
            impacted=["a"],
            impacted_prev=[],
            prev_non_success=["test (a)"],  # only the unprofiled failed prev
            expected={
                "include": [
                    {"modules": "a"},
                    # profiled item: not on deny list, no diff -> skipped
                ],
            },
        )

    def test_render_uniqueness_self_check(self):
        # Two distinct items rendering to the same name -> assertion error.
        with tempfile.TemporaryFile("w+") as mf, tempfile.TemporaryFile("w+") as imf, \
                tempfile.TemporaryFile("w+") as outf:
            # Two entries with identical modules+profile (somehow) -> same render -> conflict.
            yaml.dump({"include": [{"modules": "a"}, {"modules": "a"}]}, mf)
            mf.seek(0)
            imf.write("a")
            imf.seek(0)
            with self.assertRaisesRegex(AssertionError, "duplicate"):
                build(mf, imf, outf)


if __name__ == "__main__":
    main()
