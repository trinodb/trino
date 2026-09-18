#!/usr/bin/env python3

import argparse
from dataclasses import dataclass, field
from enum import Enum
import itertools
import json
import logging
from pathlib import Path
import re
import sys
import tempfile
import textwrap
from typing import Any, TypeVar
import unittest

import yaml


def main():
    parser = argparse.ArgumentParser(
        description="Build test matrix using list of impacted modules and scheduling config."
    )
    cmds = parser.add_subparsers(required=True)
    build_matrix_parser = cmds.add_parser(
        "build-matrix",
        aliases=["build"],
        help="Main program - build the scheduled test matrix",
        description="Build test matrix using list of impacted modules and scheduling config.",
    )
    build_matrix_parser.set_defaults(cmd=build_test_matrix)
    build_matrix_parser.add_argument(
        "-c",
        "--schedule-config",
        type=Path,
        default=".github/schedule-config.yml",
        help="A YAML file with the schedule config. Should match schema in "
        ".github/schedule-config-schema.yml",
    )
    build_matrix_parser.add_argument(
        "-i",
        "--impacted",
        type=Path,
        default="gib-impacted.log",
        help="File containing list of impacted modules, one per line, as paths, not artifact ids",
    )
    build_matrix_parser.add_argument(
        "-e",
        "--event-name",
        required=True,
        help="Name of the event which triggered the active workflow, i.e. ${{ github.event_name }}",
    )
    build_matrix_parser.add_argument(
        "-s",
        "--event-schedule",
        required=False,
        help="The schedule value of scheduled workflows, i.e. ${{ github.event.schedule }}",
    )
    build_matrix_parser.add_argument(
        "-L",
        "--pr-labels",
        required=False,
        type=json.loads,
        help="JSON string of PR labels from the workflow's context, i.e. "
        "${{ toJson(github.event.pull_request.labels) }}",
    )
    build_matrix_parser.add_argument(
        "-v",
        "--verbose",
        action="store_const",
        dest="loglevel",
        const=logging.INFO,
        default=logging.WARNING,
        required=False,
        help="Print info level logs",
    )
    test_parser = cmds.add_parser(
        "test", help="Run unit tests instead of executing the script"
    )
    test_parser.set_defaults(cmd=run_tests)

    args, leftover_args = parser.parse_known_args()
    args.cmd(args, leftover_args)


##########
### Config object POYOs
##########


class Schedule(str, Enum):
    nightly = "nightly"
    weekly = "weekly"
    never = "never"


@dataclass(frozen=True)
class ScheduleFilter:
    """
    Definition for when a particular set of tests should run.

    See .github/schedule-config-schema.yml for attribute descriptions.
    """

    pushes: bool = field(default=False, kw_only=True)
    dispatches: bool = field(default=True, kw_only=True)
    impacted: bool = field(default=True, kw_only=True)
    scheduled: Schedule = Schedule.nightly
    labeled: list[str] = field(default_factory=list)


@dataclass
class ScheduleConfigItem:
    """
    One item in the schedule config YAML, defines a set of tests that run in the same conditions.

    See .github/schedule-config-schema.yml for attribute descriptions
    """

    module_sets: list[list[str]]
    profiles: list[str] = field(default_factory=list)
    runners: list[str] = field(default_factory=list)
    notify_channels: list[str] = field(default_factory=list)
    when: ScheduleFilter | None = None
    matrix_properties: dict[str, Any] = field(default_factory=dict)

    def build_matrix_includes(self) -> list[dict[str, str | list[str]]]:
        """Build a list of matrix cells for all tests in this set."""
        return self._matrix_includes_from(self.module_sets)

    def build_impacted_matrix_includes(
        self, impacted: set[str]
    ) -> list[dict[str, str | list[str]]]:
        """Build a list of matrix cells for impacted tests in this set."""
        if self.when and not self.when.impacted:
            return []
        impacted_module_sets = []
        for module_set in self.module_sets:
            if has_impacted_module(module_set, impacted):
                impacted_module_sets.append(module_set)
            else:
                logging.info("Excluding non-impacted module set '%s'", module_set)
        return self._matrix_includes_from(impacted_module_sets)

    def _matrix_includes_from(
        self, module_sets: list[list[str]]
    ) -> list[dict[str, str | list[str]]]:
        includes = []
        for module_set, profile in itertools.product(
            module_sets, self.profiles or [None]
        ):
            include: dict[str, str | list[str]] = {"modules": ",".join(module_set)}
            if profile:
                include["profile"] = profile
            if self.runners:
                include["runners"] = self.runners
            if self.notify_channels:
                include["notify-channels"] = self.notify_channels
            if self.matrix_properties:
                include.update(self.matrix_properties)
            include["name"] = build_test_name(
                module_set, profile, self.matrix_properties
            )
            includes.append(include)
        return includes

    def matches_event(self, github_context: dict[str, Any]) -> bool:
        when = self.when or ScheduleFilter(pushes=True)
        return any(
            matches(github_context, when)
            for matches in [
                matches_push,
                matches_dispatch,
                matches_scheduled,
                matches_label,
            ]
        )


def has_impacted_module(module_set: list[str], impacted: set[str]) -> bool:
    return any(module in impacted for module in module_set)


def build_test_name(
    module_set: list[str], profile: str | None, additional_properties: dict[str, Any]
) -> str:
    values = list(module_set)
    if profile:
        values.append(profile)
    for key in sorted(additional_properties):
        values.append(f"{key}={additional_properties[key]}")
    return f"test ({', '.join(values)})"


def load_schedule_configs(stream) -> list[ScheduleConfigItem]:
    """Parses a YAML stream into ScheduleConfigItem objects"""
    config = _require_type(yaml.safe_load(stream), list, "config")
    logging.info("Read config: %s", config)
    configs = []
    for item in config:
        item = _require_type(item, dict, "config item")
        module_sets = _expect_str_lists(item, "modules")
        if not module_sets:
            raise ValueError("modules should have at least one element")
        profile = item.get("profile")
        if profile is not None:
            _require_type(profile, str, "profile")
        profiles = [profile] if profile is not None else []
        runners = _expect_str_list(item, "runners")
        notify_channels = _expect_str_list(item, "notify-channels")
        when = (
            load_when(_require_type(item["when"], dict, "when"))
            if "when" in item
            else None
        )
        matrix_properties = _require_type(
            item.get("matrix-properties", {}), dict, "matrix-properties"
        )
        check_matrix_properties(matrix_properties)

        configs.append(
            ScheduleConfigItem(
                module_sets,
                profiles=profiles,
                runners=runners,
                notify_channels=notify_channels,
                when=when,
                matrix_properties=matrix_properties,
            )
        )
    return configs


def check_matrix_properties(matrix_properties: dict[str, Any]) -> None:
    """Validate that the matrix-properties object doesn't override reserved matrix keys."""
    reserved = {"modules", "profile", "runners", "notify-channels", "name"}
    conflicts = reserved.intersection(matrix_properties.keys())
    if conflicts:
        raise ValueError(
            f"matrix-properties contains reserved keys: {sorted(conflicts)}"
        )


def load_when(item: dict[str, Any]) -> ScheduleFilter:
    return ScheduleFilter(
        pushes=_require_type(item.get("pushes", False), bool, "pushes"),
        dispatches=_require_type(item.get("dispatches", True), bool, "dispatches"),
        impacted=_require_type(item.get("impacted", True), bool, "impacted"),
        scheduled=Schedule(
            _require_type(item.get("scheduled", "nightly"), str, "scheduled")
        ),
        labeled=_expect_str_list(item, "labeled"),
    )


##########
### Event matchers
##########


def matches_push(
    github_context: dict[str, Any], schedule_filter: ScheduleFilter
) -> bool:
    return schedule_filter.pushes and github_context.get("event_name") in [
        "push",
        "merge_group",
    ]


def matches_dispatch(
    github_context: dict[str, Any], schedule_filter: ScheduleFilter
) -> bool:
    return schedule_filter.dispatches and github_context.get("event_name") in [
        "workflow_dispatch",
        "repository_dispatch",
    ]


_WEEKLY_CRON = re.compile(r"^\d+ \d+ \* \* [0-7]$")


def matches_scheduled(
    github_context: dict[str, Any], schedule_filter: ScheduleFilter
) -> bool:
    if (
        github_context.get("event_name") != "schedule"
        or schedule_filter.scheduled == Schedule.never
    ):
        return False
    run_schedule = (
        Schedule.weekly
        if _WEEKLY_CRON.match(github_context.get("event", {}).get("schedule", ""))
        else Schedule.nightly
    )
    return schedule_filter.scheduled == run_schedule


def matches_label(
    github_context: dict[str, Any], schedule_filter: ScheduleFilter
) -> bool:
    if github_context.get("event_name") != "pull_request":
        return False
    labels = github_context.get("event", {}).get("pull_request", {}).get("labels", [])
    labels = _require_type(labels, list, "pull_request.labels")
    labels = [label["name"] for label in labels if "name" in label]
    return any(label in schedule_filter.labeled for label in labels)


##########
### Helpers for arbitrary dicts and type hinting
##########


T = TypeVar("T")


def _require_type(obj: Any, t: type[T], name: str | None = None) -> T:
    if not isinstance(obj, t):
        raise ValueError(
            f"{name} should be a {t.__name__}, was {obj}"
            if name
            else f"unexpected value {obj}, expecting a {t.__name__}"
        )
    return obj


def _expect_str_list(d: dict[str, Any], key: str) -> list[str]:
    values = _require_type(d.get(key, []), list, key)
    return [_require_type(value, str) for value in values]


def _expect_str_lists(d: dict[str, Any], key: str) -> list[list[str]]:
    """
    Parses a schedule config's "modules" field into a list of module sets (one matrix job each).

    A single string, e.g. a, or a flat list of strings, e.g. [a, b], is shorthand for a single
    module set (respectively [[a]] or [[a, b]]) that runs all its modules in one job. Nest lists
    explicitly, e.g. [[a], [b, c]], to define multiple module sets that share the rest of the
    schedule item's config (profiles, when, etc).
    """
    raw = d.get(key, [])
    if isinstance(raw, str):
        return [[raw]]
    values = _require_type(raw, list, key)
    if all(isinstance(value, str) for value in values):
        return [values] if values else []
    sublists = []
    for value in values:
        if isinstance(value, str):
            sublists.append([value])
        elif isinstance(value, list):
            sublists.append([_require_type(v, str) for v in value])
        else:
            raise ValueError(
                f"{key} should be a list of (string | string list), but contained {value}"
            )
    return sublists


##########
### Main build matrix script
##########


def build_test_matrix(args: argparse.Namespace, _: list[str]):
    logging.basicConfig(level=args.loglevel)

    with open(args.schedule_config, "r") as config_file:
        configs = load_schedule_configs(config_file)

    try:
        with open(args.impacted, "r") as impacted_file:
            impacted = {
                line.strip()
                for line in impacted_file.readlines()
                if len(line.strip()) > 0
            }
        logging.info("Read impacted: %s", impacted)
    except FileNotFoundError:
        logging.warning("impacted file %s not found, using empty set", args.impacted)
        impacted = set()
    except OSError as e:
        raise RuntimeError(f"failed to read impacted file {args.impacted}: {e}") from e

    github_context = {
        "event_name": args.event_name,
        "event": {
            "schedule": args.event_schedule,
            "pull_request": {"labels": args.pr_labels or []},
        },
    }
    logging.info("Read (filtered) GitHub context: %s", github_context)

    print(json.dumps(build_matrix_json(configs, impacted, github_context), indent=2))


def build_matrix_json(
    configs: list[ScheduleConfigItem],
    impacted_modules: set[str],
    github_context: dict[str, Any],
) -> dict[str, Any]:
    includes = []
    for config_item in configs:
        when = config_item.when
        if config_item.matches_event(github_context):
            # Still filter to impacted when impact data exists, so we don't include jobs whose
            # own Maven Install step will see the same GIB data and skip installing the module.
            if impacted_modules and (when is None or when.impacted):
                includes.extend(
                    config_item.build_impacted_matrix_includes(impacted_modules)
                )
            else:
                includes.extend(config_item.build_matrix_includes())
        else:
            # Otherwise, filter to impacted
            includes.extend(
                config_item.build_impacted_matrix_includes(impacted_modules)
            )
    return {"include": includes} if includes else {}


##########
### Unit tests
##########


def run_tests(_, leftover_args: list[str]):
    sys.argv = [sys.argv[0], *leftover_args]
    unittest.main()


class TestBuild(unittest.TestCase):
    maxDiff = None

    def test_load_schedule_configs(self):
        configs = textwrap.dedent("""
                                 - { modules: [a], profile: b }
                                 - { modules: [a], profile: c }
                                 - modules: [[d, m, n], o]
                                 - modules: [e]
                                   profile: f
                                   when:
                                     scheduled: nightly
                                 - modules: [h]
                                   runners: [i, j, k]
                                   when:
                                     scheduled: weekly
                                     labeled: [l]
                                 - modules: [p]
                                   notify-channels: [foo]
                                   matrix-properties:
                                     bar: baz
                                 - modules: [q, r, s]
                                 - modules: t
                                """)
        with tempfile.TemporaryFile("w+") as config_file:
            config_file.write(configs)
            config_file.seek(0)
            configs = load_schedule_configs(config_file)

        self.assertEqual(
            configs,
            [
                ScheduleConfigItem([["a"]], profiles=["b"]),
                ScheduleConfigItem([["a"]], profiles=["c"]),
                ScheduleConfigItem([["d", "m", "n"], ["o"]]),
                ScheduleConfigItem(
                    [["e"]],
                    profiles=["f"],
                    when=ScheduleFilter(scheduled=Schedule.nightly),
                ),
                ScheduleConfigItem(
                    [["h"]],
                    runners=["i", "j", "k"],
                    when=ScheduleFilter(scheduled=Schedule.weekly, labeled=["l"]),
                ),
                ScheduleConfigItem(
                    [["p"]], notify_channels=["foo"], matrix_properties={"bar": "baz"}
                ),
                ScheduleConfigItem([["q", "r", "s"]]),
                ScheduleConfigItem([["t"]]),
            ],
        )

    def test_matches_scheduled(self):
        nightly_workflow = {
            "event_name": "schedule",
            "event": {"schedule": "0 0 * * *"},
        }
        weekly_workflows = [
            {"event_name": "schedule", "event": {"schedule": f"0 0 * * {d}"}}
            for d in range(8)
        ]

        nightly_test = ScheduleFilter(scheduled=Schedule.nightly)
        weekly_test = ScheduleFilter(scheduled=Schedule.weekly)
        as_needed_test = ScheduleFilter(scheduled=Schedule.never)
        self.assertTrue(
            matches_scheduled(nightly_workflow, nightly_test),
            "nightly test runs on nightly workflow",
        )
        self.assertFalse(
            matches_scheduled(nightly_workflow, weekly_test),
            "weekly test doesn't run on nightly workflow",
        )
        self.assertFalse(
            matches_scheduled(nightly_workflow, as_needed_test),
            "as-needed test doesn't run on nightly workflow",
        )
        for weekly_workflow in weekly_workflows:
            self.assertFalse(
                matches_scheduled(weekly_workflow, nightly_test),
                f"nightly test doesn't run on weekly workflow ({weekly_workflow})",
            )
            self.assertTrue(
                matches_scheduled(weekly_workflow, weekly_test),
                f"weekly test runs on weekly workflow ({weekly_workflow})",
            )
            self.assertFalse(
                matches_scheduled(weekly_workflow, as_needed_test),
                f"as-needed test doesn't run on weekly workflow ({weekly_workflow})",
            )

    def test_matches_label(self):
        labels = ["foo", "bar"]
        pr_workflow = {
            "event_name": "pull_request",
            "event": {
                "pull_request": {"labels": [{"name": label} for label in labels]}
            },
        }

        self.assertFalse(
            matches_label(pr_workflow, ScheduleFilter(labeled=[])),
            "test without labels doesn't run",
        )
        self.assertTrue(
            matches_label(pr_workflow, ScheduleFilter(labeled=["foo"])),
            "test with one matching label runs",
        )
        self.assertTrue(
            matches_label(pr_workflow, ScheduleFilter(labeled=["foo", "baz"])),
            "test with matching and non-matching labels runs",
        )
        self.assertFalse(
            matches_label(pr_workflow, ScheduleFilter(labeled=["baz"])),
            "test with non-matching labels doesn't run",
        )

    def test_build_matrix_module_sets(self):
        configs = [ScheduleConfigItem([["a"], ["b", "c"]])]
        self.assertEqual(
            build_matrix_json(configs, set(), {"event_name": "push"}),
            {
                "include": [
                    {"modules": "a", "name": "test (a)"},
                    {"modules": "b,c", "name": "test (b, c)"},
                ]
            },
        )
        self.assertEqual(
            build_matrix_json(configs, {"b"}, {"event_name": "pull_request"}),
            {"include": [{"modules": "b,c", "name": "test (b, c)"}]},
            "test runs if any module impacted",
        )
        self.assertEqual(
            build_matrix_json(configs, set(), {"event_name": "pull_request"}),
            {},
            "test doesn't run if modules aren't impacted",
        )

    def test_build_matrix_additional_properties(self):
        configs = [
            ScheduleConfigItem(
                [["a"]],
                profiles=["foo-profile"],
                notify_channels=["foo", "bar"],
                matrix_properties={"buildAll": True},
            )
        ]
        self.assertEqual(
            build_matrix_json(configs, set(), {"event_name": "push"}),
            {
                "include": [
                    {
                        "modules": "a",
                        "profile": "foo-profile",
                        "notify-channels": ["foo", "bar"],
                        "buildAll": True,
                        "name": "test (a, foo-profile, buildAll=True)",
                    }
                ]
            },
        )

    def test_build_matrix_default_when(self):
        configs = [ScheduleConfigItem([["a"]])]

        for event_name in [
            "push",
            "merge_group",
            "workflow_dispatch",
            "repository_dispatch",
        ]:
            context = {"event_name": event_name}
            self.assertEqual(
                build_matrix_json(configs, set(), context),
                {"include": [{"modules": "a", "name": "test (a)"}]},
                f"default runs on {event_name} without impact",
            )
            self.assertEqual(
                build_matrix_json(configs, {"a"}, context),
                {"include": [{"modules": "a", "name": "test (a)"}]},
                f"default runs on {event_name} with impact",
            )
            self.assertEqual(
                build_matrix_json(configs, {"b"}, context),
                {},
                f"default doesn't run on {event_name} when a different module is impacted",
            )

        context = {"event_name": "schedule", "event": {"schedule": "0 0 * * *"}}
        self.assertEqual(
            build_matrix_json(configs, set(), context),
            {"include": [{"modules": "a", "name": "test (a)"}]},
            "default runs with empty impact",
        )
        self.assertEqual(
            build_matrix_json(configs, {"a"}, context),
            {"include": [{"modules": "a", "name": "test (a)"}]},
            "default runs with impact",
        )

        context = {
            "event_name": "pull_request",
            "event": {"pull_request": {"labels": []}},
        }
        self.assertEqual(
            build_matrix_json(configs, set(), context),
            {},
            "default doesn't run on pull request without impact",
        )
        self.assertEqual(
            build_matrix_json(configs, {"a"}, context),
            {"include": [{"modules": "a", "name": "test (a)"}]},
            "default runs on pull request with impact",
        )

    def test_build_matrix_impacted(self):
        # Module 'a' runs only when explicitly impacted
        configs = [
            ScheduleConfigItem(
                [["a"]],
                when=ScheduleFilter(
                    scheduled=Schedule.never,
                    pushes=False,
                    dispatches=False,
                    impacted=True,
                ),
            )
        ]
        github_context = {"event_name": "push"}

        matrix_impacted = build_matrix_json(configs, {"a"}, github_context)
        matrix_other_impacted = build_matrix_json(configs, {"b"}, github_context)
        matrix_not_impacted = build_matrix_json(configs, set(), github_context)

        self.assertEqual(
            matrix_impacted,
            {"include": [{"modules": "a", "name": "test (a)"}]},
        )
        self.assertEqual(matrix_other_impacted, {})
        self.assertEqual(matrix_not_impacted, {})

    def test_build_matrix_ignore_impact(self):
        configs = [ScheduleConfigItem([["a"]], when=ScheduleFilter(impacted=False))]
        github_context = {"event_name": "pull_request"}

        matrix_impacted = build_matrix_json(configs, {"a"}, github_context)
        matrix_other_impacted = build_matrix_json(configs, {"b"}, github_context)
        matrix_not_impacted = build_matrix_json(configs, set(), github_context)
        matrix_dispatched = build_matrix_json(
            configs, set(), {"event_name": "workflow_dispatch"}
        )

        self.assertEqual(matrix_impacted, {})
        self.assertEqual(matrix_other_impacted, {})
        self.assertEqual(matrix_not_impacted, {})
        self.assertEqual(
            matrix_dispatched, {"include": [{"modules": "a", "name": "test (a)"}]}
        )


if __name__ == "__main__":
    main()
