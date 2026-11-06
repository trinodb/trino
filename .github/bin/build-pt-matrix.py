#!/usr/bin/env python3

import argparse
import json
import logging
import re
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

import yaml

# Declares the server plugins each suite uses
SUITES_FILE = Path("testing/trino-product-tests/src/test/resources/product-test-suites.yaml")
SUITE_DIR = Path("testing/trino-product-tests/src/test/java/io/trino/tests/product/suite")
SUITE_HELPERS = {"SuiteRunner", "SuiteTag"}
SERVER_TARBALL = "core/trino-server/target/trino-server-*.tar.gz"

# Changes to these run all suites. trino-root stands for any file outside a module, including
# the product-test Docker image. trino-cli is used by several suites and baked into the image.
FULL_RUN_ARTIFACTS = {"trino-root", "trino-product-tests", "trino-cli", "trino-server", "trino-server-core"}


def main():
    parser = argparse.ArgumentParser(
        description="Build the JUnit product-test matrix, optionally filtered by the changed Maven modules."
    )
    parser.add_argument(
        "-c",
        "--changed",
        type=Path,
        help="GIB GAV log of changed Maven modules; a missing file selects all suites",
    )
    parser.add_argument(
        "-s",
        "--server",
        type=Path,
        help=f"Server tarball, used to check the declared plugins and to locate the changed modules; defaults to the only {SERVER_TARBALL}",
    )
    parser.add_argument(
        "-p",
        "--classpath",
        type=Path,
        help="Product-test classpath.txt, used to locate the changed modules; required with --changed",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_const",
        dest="loglevel",
        const=logging.INFO,
        default=logging.WARNING,
        help="Print matrix filtering decisions",
    )
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        help="Test this script instead of executing it",
    )
    args = parser.parse_args()
    if args.changed is not None and args.classpath is None:
        parser.error("--changed requires --classpath")
    logging.basicConfig(level=args.loglevel, format="%(levelname)s: %(message)s")

    if args.test:
        sys.argv = [sys.argv[0]]
        unittest.main()
        return

    config = load_config(SUITES_FILE)
    with tarfile.open(args.server or server_tarball(Path())) as server:
        server_files = server.getnames()
    validate_config(config, {path.stem for path in SUITE_DIR.glob("Suite*.java")} - SUITE_HELPERS, server_plugins(server_files))
    if args.changed is None or not args.changed.is_file():
        logging.info("No changed modules reported; using the full product-test matrix")
        suites = all_suites(config)
    else:
        classpath = args.classpath.read_text().strip().split(":")
        locations = artifact_locations(server_files, classpath)
        validate_locations(server_plugins(server_files), locations)
        suites = select_suites(config, changed_artifacts(args.changed.read_text()), locations)
    print(json.dumps(build_matrix(suites)))


def server_tarball(root):
    tarballs = sorted(root.glob(SERVER_TARBALL))
    if len(tarballs) != 1:
        raise ValueError(f"Expected one server tarball {SERVER_TARBALL}, found {[str(tarball) for tarball in tarballs]}")
    return tarballs[0]


def changed_artifacts(gib_log):
    """Artifact ids from GIB's GAV log, e.g. io.trino:trino-mysql:485-SNAPSHOT."""
    return {line.split(":")[1] for line in gib_log.splitlines() if line}


def load_config(path):
    with path.open() as file:
        config = yaml.safe_load(file)
    for key in ("smoke-suite", "installed-by-default", "smoke-only", "suites", "disabled-suites"):
        if config.get(key) is None:
            raise ValueError(f"{path} must declare {key}")
    for suite, plugins in config["suites"].items():
        if plugins is None:
            raise ValueError(f"{path} must declare the plugins of {suite}, or [] when it uses none")
    return config


def enabled_suites(config):
    """The suites other than the smoke suite, in the declared order."""
    return [suite for suite in config["suites"] if suite not in config["disabled-suites"]]


def all_suites(config):
    """The smoke suite first, as it runs for every change, then the other suites."""
    return [config["smoke-suite"]] + enabled_suites(config)


def server_plugins(server_files):
    return {match.group(1) for path in server_files if (match := re.match(r"[^/]+/plugin/([^/]+)/", path))}


def validate_config(config, suite_classes, plugins):
    """Fail when a suite, or a server plugin, is added or removed without updating the suites file."""
    declared_suites = set(config["suites"]) | {config["smoke-suite"]}
    if suite_classes != declared_suites:
        raise ValueError(f"Suites missing from {SUITES_FILE}: {sorted(suite_classes - declared_suites)}, "
                         f"declared suites without a class: {sorted(declared_suites - suite_classes)}")
    declared_plugins = set(config["installed-by-default"]) | set(config["smoke-only"]) | set().union(*config["suites"].values())
    if plugins != declared_plugins:
        raise ValueError(f"Server plugins not used by any suite in {SUITES_FILE}, nor declared smoke-only: {sorted(plugins - declared_plugins)}, "
                         f"declared plugins missing from the server: {sorted(declared_plugins - plugins)}")


def validate_locations(plugins, locations):
    """Fail when the jars of a plugin are not recognized, as changes to them would not select the plugin's suites."""
    unlocated_plugins = plugins - set().union(*locations.values())
    if unlocated_plugins:
        raise ValueError(f"Server plugins without a recognized Trino jar: {sorted(unlocated_plugins)}")


def artifact_locations(server_files, classpath):
    """Map each io.trino artifactId to where its jar lands: "lib", "product-tests", or a plugin directory."""
    locations = {}
    for path in server_files:
        # trino-server-485/plugin/hive/io.trino_trino-parquet-485.jar, trino-server-485/plugin/hive/trino-hive-485.jar
        match = re.fullmatch(r"[^/]+/(lib|plugin/([^/]+))/(?:[^/]+/)*(?:io\.trino_)?(trino-[a-z0-9-]+?)-\d[^/]*\.jar", path)
        if match:
            locations.setdefault(match.group(3), set()).add(match.group(2) or "lib")
    for path in classpath:
        # lib/io.trino.trino-jdbc-485.jar
        match = re.fullmatch(r"lib/io\.trino\.(trino-[a-z0-9-]+?)-\d[^/]*\.jar", path)
        if match:
            locations.setdefault(match.group(1), set()).add("product-tests")
    return locations


def select_suites(config, changed_artifacts, locations):
    if not changed_artifacts:
        logging.info("No non-test changes; skipping product tests")
        return []
    logging.info("Changed modules: %s", ", ".join(sorted(changed_artifacts)))
    plugins = set()
    for artifact in sorted(changed_artifacts):
        where = locations.get(artifact, set())
        if not where:
            logging.info("%s is not part of the server nor of the product tests", artifact)
        if artifact in FULL_RUN_ARTIFACTS or {"lib", "product-tests", *config["installed-by-default"]} & where:
            logging.info("%s affects every product test; using the full product-test matrix", artifact)
            return all_suites(config)
        plugins |= where
    if not plugins:
        logging.info("No changes to the server nor to the product tests; skipping product tests")
        return []
    suites = [config["smoke-suite"]] + [suite for suite in enabled_suites(config) if plugins & set(config["suites"][suite])]
    logging.info("Selected product-test suites: %s", ", ".join(suites))
    return suites


def build_matrix(suites):
    include = [{"suite": suite} for suite in suites]
    return {"include": include} if include else {}


class TestBuildMatrix(unittest.TestCase):
    CONFIG = {
        "smoke-suite": "SuiteSmoke",
        "installed-by-default": ["tpch"],
        "smoke-only": ["opa"],
        "suites": {
            "SuiteMysql": ["mysql", "mariadb"],
            "SuiteRanger": ["mariadb", "ranger"],
            "SuiteHive": ["hive"],
            "SuiteDeltaLake": ["delta-lake", "hive"],
            "SuiteTls": [],
            "SuiteSnowflake": ["snowflake"],
        },
        "disabled-suites": ["SuiteSnowflake"],
    }
    ALL_SUITES = ["SuiteSmoke", "SuiteMysql", "SuiteRanger", "SuiteHive", "SuiteDeltaLake", "SuiteTls"]
    LOCATIONS = artifact_locations(
        [
            "trino-server-485/lib/io.trino_trino-main-485.jar",
            "trino-server-485/lib/io.trino_trino-plugin-toolkit-485.jar",
            "trino-server-485/plugin/mysql/trino-mysql-485.jar",
            "trino-server-485/plugin/mysql/trino-mysql-485-services.jar",
            "trino-server-485/plugin/mysql/io.trino_trino-base-jdbc-485.jar",
            "trino-server-485/plugin/mysql/io.trino_trino-plugin-toolkit-485.jar",
            "trino-server-485/plugin/mariadb/io.trino_trino-base-jdbc-485.jar",
            "trino-server-485/plugin/hive/io.trino_trino-hive-485.jar",
            "trino-server-485/plugin/hive/hdfs/io.trino_trino-hdfs-485.jar",
            "trino-server-485/plugin/delta-lake/io.trino_trino-hive-485.jar",
            "trino-server-485/plugin/opa/trino-opa-485.jar",
            "trino-server-485/plugin/tpch/trino-tpch-485.jar",
        ],
        [
            "lib/io.trino.trino-jdbc-485.jar",
            "lib/io.trino.trino-testing-containers-485.jar",
            "lib/org.testcontainers.testcontainers-trino-2.0.5.jar",
        ],
    )

    def select(self, *artifacts):
        return select_suites(self.CONFIG, set(artifacts), self.LOCATIONS)

    def test_artifact_locations(self):
        self.assertEqual(
            self.LOCATIONS,
            {
                "trino-main": {"lib"},
                "trino-plugin-toolkit": {"lib", "mysql"},
                "trino-mysql": {"mysql"},
                "trino-base-jdbc": {"mysql", "mariadb"},
                "trino-hive": {"hive", "delta-lake"},
                "trino-hdfs": {"hive"},
                "trino-opa": {"opa"},
                "trino-tpch": {"tpch"},
                "trino-jdbc": {"product-tests"},
                "trino-testing-containers": {"product-tests"},
            },
        )

    def test_changed_artifacts(self):
        self.assertEqual(changed_artifacts("io.trino:trino-mysql:485-SNAPSHOT\n\nio.trino:trino-root:485-SNAPSHOT\n"), {"trino-mysql", "trino-root"})
        self.assertEqual(changed_artifacts(""), set())

    def test_disabled_suite(self):
        self.assertEqual(select_suites(self.CONFIG, {"trino-snowflake"}, {"trino-snowflake": {"snowflake"}}), ["SuiteSmoke"])

    def test_validate_locations(self):
        validate_locations({"mysql", "opa"}, self.LOCATIONS)
        with self.assertRaisesRegex(ValueError, r"without a recognized Trino jar: \['shortened'\]"):
            validate_locations({"mysql", "shortened"}, self.LOCATIONS)

    def test_snapshot_version(self):
        self.assertEqual(
            artifact_locations(
                ["trino-server-485-SNAPSHOT/plugin/geospatial/trino-geospatial-485-SNAPSHOT.jar",
                 "trino-server-485-SNAPSHOT/plugin/geospatial/io.trino_trino-geospatial-toolkit-485-SNAPSHOT.jar"],
                ["lib/io.trino.trino-testing-services-485-SNAPSHOT.jar"]),
            {"trino-geospatial": {"geospatial"}, "trino-geospatial-toolkit": {"geospatial"}, "trino-testing-services": {"product-tests"}})

    def test_suites_file(self):
        config = load_config(SUITES_FILE)
        self.assertEqual(all_suites(config)[0], "SuiteAllConnectorsSmoke")

    def test_no_changes(self):
        self.assertEqual(self.select(), [])
        self.assertEqual(build_matrix([]), {})

    def test_plugin(self):
        self.assertEqual(self.select("trino-mysql"), ["SuiteSmoke", "SuiteMysql"])

    def test_plugin_used_by_smoke_suite_only(self):
        self.assertEqual(self.select("trino-opa"), ["SuiteSmoke"])

    def test_library_bundled_in_plugins(self):
        self.assertEqual(self.select("trino-base-jdbc"), ["SuiteSmoke", "SuiteMysql", "SuiteRanger"])
        self.assertEqual(self.select("trino-hdfs"), ["SuiteSmoke", "SuiteHive", "SuiteDeltaLake"])

    def test_multiple_plugins(self):
        self.assertEqual(self.select("trino-mysql", "trino-hive"), ["SuiteSmoke", "SuiteMysql", "SuiteHive", "SuiteDeltaLake"])

    def test_module_outside_server(self):
        self.assertEqual(self.select("trino-tests"), [])

    def test_plugin_installed_by_default(self):
        self.assertEqual(self.select("trino-tpch"), self.ALL_SUITES)

    def test_server_library(self):
        self.assertEqual(self.select("trino-mysql", "trino-plugin-toolkit"), self.ALL_SUITES)

    def test_product_tests_classpath(self):
        self.assertEqual(self.select("trino-testing-containers"), self.ALL_SUITES)

    def test_full_run_artifacts(self):
        for artifact in FULL_RUN_ARTIFACTS:
            self.assertEqual(self.select(artifact), self.ALL_SUITES)

    def test_full_matrix(self):
        self.assertEqual(build_matrix(all_suites(self.CONFIG)), {"include": [{"suite": suite} for suite in self.ALL_SUITES]})

    def test_server_tarball(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "core/trino-server/target"
            target.mkdir(parents=True)
            with self.assertRaisesRegex(ValueError, r"Expected one server tarball .*, found \[\]"):
                server_tarball(root)
            (target / "trino-server-485-SNAPSHOT.tar.gz").touch()
            self.assertEqual(server_tarball(root), target / "trino-server-485-SNAPSHOT.tar.gz")
            (target / "trino-server-486-SNAPSHOT.tar.gz").touch()
            with self.assertRaisesRegex(ValueError, r"Expected one server tarball .*, found \[.*485.*, .*486.*\]"):
                server_tarball(root)

    def test_server_plugins(self):
        self.assertEqual(server_plugins(["trino-server-485/plugin/hive/hdfs/a.jar", "trino-server-485/plugin/opa/", "trino-server-485/lib/a.jar"]), {"hive", "opa"})

    def test_validate_config(self):
        suites = {"SuiteSmoke", "SuiteMysql", "SuiteRanger", "SuiteHive", "SuiteDeltaLake", "SuiteTls", "SuiteSnowflake"}
        plugins = {"tpch", "opa", "mysql", "mariadb", "ranger", "hive", "delta-lake", "snowflake"}
        validate_config(self.CONFIG, suites, plugins)
        with self.assertRaisesRegex(ValueError, r"Suites missing from .*\['SuiteNew'\]"):
            validate_config(self.CONFIG, suites | {"SuiteNew"}, plugins)
        with self.assertRaisesRegex(ValueError, r"declared suites without a class: \['SuiteTls'\]"):
            validate_config(self.CONFIG, suites - {"SuiteTls"}, plugins)
        with self.assertRaisesRegex(ValueError, r"nor declared smoke-only: \['new'\]"):
            validate_config(self.CONFIG, suites, plugins | {"new"})
        with self.assertRaisesRegex(ValueError, r"missing from the server: \['opa'\]"):
            validate_config(self.CONFIG, suites, plugins - {"opa"})


if __name__ == "__main__":
    main()
