#!/usr/bin/env python3

import argparse
import json
import logging
import sys
import unittest
from pathlib import Path


SUITE_DIR = Path("testing/trino-product-tests/src/test/java/io/trino/tests/product/suite")
SUITE_HELPERS = {"SuiteRunner", "SuiteTag"}

SUITES = [
    "SuiteMysql",
    "SuitePostgresql",
    "SuiteSqlServer",
    "SuiteFunctions",
    "SuiteTpch",
    "SuiteTpcds",
    "SuiteExasol",
    "SuiteSnowflake",
    "SuiteCassandra",
    "SuiteClickhouse",
    "SuiteBlackHole",
    "SuiteAllConnectorsSmoke",
    "SuiteIgnite",
    "SuiteKafka",
    "SuiteLdap",
    "SuiteOauth2",
    "SuiteClients",
    "SuiteJdbcKerberos",
    "SuiteLoki",
    "SuiteRanger",
    "SuiteTls",
    "SuiteHiveBasic",
    "SuiteHmsOnly",
    "SuiteHiveStorageFormats",
    "SuiteSqlCancel",
    "SuiteHdfsImpersonation",
    "SuiteTwoHives",
    "SuiteHive4",
    "SuiteHudi",
    "SuiteHiveTransactional",
    "SuiteAuthorization",
    "SuiteFaultTolerant",
    "SuiteHiveSpark",
    "SuiteHiveAlluxioCaching",
    "SuiteStorageFormatsDetailed",
    "SuiteParquet",
    "SuiteIceberg",
    "SuiteIcebergVariants",
    "SuiteDeltaLakeFloci",
    "SuiteDeltaLakeHdfs",
    "SuiteDeltaLakeOss",
    "SuiteDeltaLakeAlluxioCaching",
    "SuiteCompatibility",
    "SuiteGcs",
    "SuiteAzure",
    "SuiteDeltaLakeDatabricks133",
    "SuiteDeltaLakeDatabricks143",
    "SuiteDeltaLakeDatabricks154",
    "SuiteDeltaLakeDatabricks164",
    "SuiteDeltaLakeDatabricks173",
]

ALL_SUITES = frozenset(SUITES)
ALL_CONNECTORS_SMOKE = frozenset({"SuiteAllConnectorsSmoke"})
DATABRICKS_SUITES = frozenset({
    "SuiteDeltaLakeDatabricks133",
    "SuiteDeltaLakeDatabricks143",
    "SuiteDeltaLakeDatabricks154",
    "SuiteDeltaLakeDatabricks164",
    "SuiteDeltaLakeDatabricks173",
})
DELTA_LAKE_SUITES = DATABRICKS_SUITES | {
    "SuiteAzure",
    "SuiteDeltaLakeAlluxioCaching",
    "SuiteDeltaLakeFloci",
    "SuiteDeltaLakeHdfs",
    "SuiteDeltaLakeOss",
    "SuiteGcs",
}
HIVE_SUITES = DATABRICKS_SUITES | {
    "SuiteAuthorization",
    "SuiteAzure",
    "SuiteClients",
    "SuiteCompatibility",
    "SuiteDeltaLakeFloci",
    "SuiteDeltaLakeHdfs",
    "SuiteDeltaLakeOss",
    "SuiteGcs",
    "SuiteHdfsImpersonation",
    "SuiteHive4",
    "SuiteHiveAlluxioCaching",
    "SuiteHiveBasic",
    "SuiteHiveSpark",
    "SuiteHiveStorageFormats",
    "SuiteHiveTransactional",
    "SuiteHmsOnly",
    "SuiteHudi",
    "SuiteIceberg",
    "SuiteIcebergVariants",
    "SuiteParquet",
    "SuiteSqlCancel",
    "SuiteStorageFormatsDetailed",
    "SuiteTpcds",
    "SuiteTpch",
    "SuiteTwoHives",
}
ICEBERG_SUITES = {
    "SuiteAzure",
    "SuiteCompatibility",
    "SuiteDeltaLakeFloci",
    "SuiteGcs",
    "SuiteHiveStorageFormats",
    "SuiteHmsOnly",
    "SuiteIceberg",
    "SuiteIcebergVariants",
    "SuiteStorageFormatsDetailed",
}
JDBC_CONNECTOR_SUITES = ALL_CONNECTORS_SMOKE | {
    "SuiteClickhouse",
    "SuiteClients",
    "SuiteExasol",
    "SuiteIgnite",
    "SuiteMysql",
    "SuitePostgresql",
    "SuiteRanger",
    "SuiteSnowflake",
    "SuiteSqlServer",
}

# Only modules in this map are eligible for matrix filtering. The suites include secondary
# connectors and services used by each environment, not only the connector named by the suite.
# Any impacted module absent from this map causes a full run.
MODULE_TO_SUITES = {
    # The old product-test impact analysis was connector-oriented. Keep this first pass at the
    # same level: shared libraries, clients, core, and other infrastructure cause a full run.
    "plugin/trino-base-jdbc": JDBC_CONNECTOR_SUITES,
    "plugin/trino-blob-cache-alluxio": {
        "SuiteDeltaLakeAlluxioCaching",
        "SuiteHiveAlluxioCaching",
        "SuiteIcebergVariants",
    },
    "plugin/trino-exchange-filesystem": {"SuiteFaultTolerant"},
    "plugin/trino-example-jdbc": set(),
    "plugin/trino-spooling-filesystem": {"SuitePostgresql"},

    # Connectors loaded by the all-connectors smoke environment, plus their focused suites.
    "plugin/trino-bigquery": ALL_CONNECTORS_SMOKE,
    "plugin/trino-blackhole": ALL_CONNECTORS_SMOKE | {"SuiteBlackHole"},
    "plugin/trino-cassandra": ALL_CONNECTORS_SMOKE | {"SuiteCassandra"},
    "plugin/trino-clickhouse": ALL_CONNECTORS_SMOKE | {"SuiteClickhouse"},
    "plugin/trino-delta-lake": ALL_CONNECTORS_SMOKE | DELTA_LAKE_SUITES,
    "plugin/trino-druid": ALL_CONNECTORS_SMOKE,
    "plugin/trino-duckdb": ALL_CONNECTORS_SMOKE,
    "plugin/trino-elasticsearch": ALL_CONNECTORS_SMOKE,
    "plugin/trino-exasol": {"SuiteExasol"},
    "plugin/trino-faker": ALL_CONNECTORS_SMOKE,
    "plugin/trino-google-sheets": ALL_CONNECTORS_SMOKE,
    "plugin/trino-hive": ALL_CONNECTORS_SMOKE | HIVE_SUITES,
    "plugin/trino-hudi": ALL_CONNECTORS_SMOKE | {"SuiteHudi"},
    "plugin/trino-iceberg": ALL_CONNECTORS_SMOKE | ICEBERG_SUITES,
    "plugin/trino-ignite": ALL_CONNECTORS_SMOKE | {"SuiteIgnite"},
    "plugin/trino-jmx": ALL_CONNECTORS_SMOKE | {
        "SuiteDeltaLakeAlluxioCaching",
        "SuiteHiveAlluxioCaching",
        "SuiteIcebergVariants",
    },
    "plugin/trino-kafka": ALL_CONNECTORS_SMOKE | {"SuiteKafka"},
    "plugin/trino-loki": ALL_CONNECTORS_SMOKE | {"SuiteLoki"},
    "plugin/trino-mariadb": ALL_CONNECTORS_SMOKE | {"SuiteMysql", "SuiteRanger"},
    "plugin/trino-memory": ALL_CONNECTORS_SMOKE | {"SuiteClients", "SuiteJdbcKerberos"},
    "plugin/trino-mongodb": ALL_CONNECTORS_SMOKE,
    "plugin/trino-mysql": ALL_CONNECTORS_SMOKE | {"SuiteMysql"},
    "plugin/trino-opensearch": ALL_CONNECTORS_SMOKE,
    "plugin/trino-oracle": ALL_CONNECTORS_SMOKE,
    "plugin/trino-pinot": ALL_CONNECTORS_SMOKE,
    "plugin/trino-postgresql": ALL_CONNECTORS_SMOKE | {"SuiteClients", "SuitePostgresql"},
    "plugin/trino-prometheus": ALL_CONNECTORS_SMOKE,
    "plugin/trino-redis": ALL_CONNECTORS_SMOKE,
    "plugin/trino-redshift": ALL_CONNECTORS_SMOKE,
    "plugin/trino-singlestore": ALL_CONNECTORS_SMOKE,
    "plugin/trino-snowflake": ALL_CONNECTORS_SMOKE | {"SuiteSnowflake"},
    "plugin/trino-sqlserver": ALL_CONNECTORS_SMOKE | {"SuiteSqlServer"},
    "plugin/trino-tpcds": ALL_CONNECTORS_SMOKE | {"SuiteFunctions", "SuiteParquet", "SuiteTpcds"},
    "plugin/trino-thrift": ALL_CONNECTORS_SMOKE,
    "plugin/trino-thrift-api": ALL_CONNECTORS_SMOKE,
    "plugin/trino-thrift-testing-server": ALL_CONNECTORS_SMOKE,

    # Security and function plugins exercised outside connector-named suites.
    "plugin/trino-ldap-group-provider": {"SuiteLdap"},
    "plugin/trino-password-authenticators": {"SuiteLdap"},
    "plugin/trino-ranger": {"SuiteRanger"},
    "plugin/trino-teradata-functions": {"SuiteFunctions"},
}


def main():
    parser = argparse.ArgumentParser(
        description="Build the JUnit product-test matrix, optionally filtered by GIB impacted modules."
    )
    parser.add_argument(
        "-i",
        "--impacted",
        type=argparse.FileType("r"),
        help="File containing affected Maven module paths, one per line",
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
    logging.basicConfig(level=args.loglevel, format="%(levelname)s: %(message)s")

    if args.test:
        sys.argv = [sys.argv[0]]
        unittest.main()
        return

    impacted_modules = None
    if args.impacted is not None:
        impacted_modules = {line.strip() for line in args.impacted if line.strip()}
    print(json.dumps(build_matrix(impacted_modules)))


def build_matrix(impacted_modules):
    selected_suites = suites_for_impacted_modules(impacted_modules)
    if selected_suites is None:
        selected_suites = ALL_SUITES

    include = []
    for suite in SUITES:
        if suite in selected_suites:
            include.append({"suite": suite})
    return {"include": include} if include else {}


def suites_for_impacted_modules(impacted_modules):
    if impacted_modules is None:
        logging.info("Impact filtering was not requested; using the full product-test matrix")
        return None
    if not impacted_modules:
        logging.info("GIB reported no modules; using the full product-test matrix")
        return None

    unknown_modules = impacted_modules - MODULE_TO_SUITES.keys()
    if unknown_modules:
        logging.info(
            "Modules without an unambiguous product-test mapping (%s); using the full product-test matrix",
            ", ".join(sorted(unknown_modules)),
        )
        return None

    selected_suites = set()
    for module in impacted_modules:
        selected_suites.update(MODULE_TO_SUITES[module])
    logging.info("GIB impacted modules: %s", ", ".join(sorted(impacted_modules)))
    logging.info("Selected product-test suites: %s", ", ".join(sorted(selected_suites)))
    return selected_suites


def validate_configuration(suite_dir=SUITE_DIR):
    declared_suites = SUITES
    duplicate_suites = sorted({suite for suite in declared_suites if declared_suites.count(suite) > 1})
    if duplicate_suites:
        raise ValueError(f"Suites declared more than once: {', '.join(duplicate_suites)}")

    actual_suites = {path.stem for path in suite_dir.glob("Suite*.java")} - SUITE_HELPERS
    missing_suites = sorted(set(declared_suites) - actual_suites)
    if missing_suites:
        raise ValueError(f"Declared product test suites are missing: {', '.join(missing_suites)}")

    unwired_suites = sorted(actual_suites - set(declared_suites))
    if unwired_suites:
        raise ValueError(f"Product test suites are missing from the CI matrix: {', '.join(unwired_suites)}")

    invalid_mapped_suites = sorted(set().union(*MODULE_TO_SUITES.values()) - set(declared_suites))
    if invalid_mapped_suites:
        raise ValueError(f"Module mappings contain unknown suites: {', '.join(invalid_mapped_suites)}")

    missing_modules = sorted(module for module in MODULE_TO_SUITES if not Path(module, "pom.xml").is_file())
    if missing_modules:
        raise ValueError(f"Mapped Maven modules are missing: {', '.join(missing_modules)}")


class TestBuildMatrix(unittest.TestCase):
    def test_connector_only_change(self):
        self.assertEqual(
            build_matrix({"plugin/trino-mysql"}),
            {
                "include": [
                    {"suite": "SuiteMysql"},
                    {"suite": "SuiteAllConnectorsSmoke"},
                ],
            },
        )

    def test_secondary_connector(self):
        self.assertEqual(
            build_matrix({"plugin/trino-mariadb"}),
            {
                "include": [
                    {"suite": "SuiteMysql"},
                    {"suite": "SuiteAllConnectorsSmoke"},
                    {"suite": "SuiteRanger"},
                ],
            },
        )

    def test_shared_connector_module(self):
        # GIB includes all downstream modules. trino-example-jdbc is understood to have no product suite.
        impacted_modules = {
            "plugin/trino-base-jdbc",
            "plugin/trino-clickhouse",
            "plugin/trino-druid",
            "plugin/trino-duckdb",
            "plugin/trino-example-jdbc",
            "plugin/trino-exasol",
            "plugin/trino-ignite",
            "plugin/trino-mariadb",
            "plugin/trino-mysql",
            "plugin/trino-oracle",
            "plugin/trino-postgresql",
            "plugin/trino-redshift",
            "plugin/trino-singlestore",
            "plugin/trino-snowflake",
            "plugin/trino-sqlserver",
        }
        matrix = build_matrix(impacted_modules)
        selected_suites = suites_from_matrix(matrix)
        self.assertEqual(selected_suites, JDBC_CONNECTOR_SUITES)

    def test_multiple_understood_modules(self):
        matrix = build_matrix({"plugin/trino-kafka", "plugin/trino-loki"})
        self.assertEqual(
            suites_from_matrix(matrix),
            {"SuiteAllConnectorsSmoke", "SuiteKafka", "SuiteLoki"},
        )

    def test_core_change_runs_full_matrix(self):
        self.assertEqual(build_matrix({"core/trino-main"}), build_matrix(None))

    def test_shared_infrastructure_change_runs_full_matrix(self):
        self.assertEqual(build_matrix({"lib/trino-plugin-toolkit"}), build_matrix(None))

    def test_product_test_framework_change_runs_full_matrix(self):
        self.assertEqual(build_matrix({"testing/trino-product-tests"}), build_matrix(None))

    def test_unknown_module_runs_full_matrix(self):
        self.assertEqual(build_matrix({"plugin/trino-new-connector"}), build_matrix(None))

    def test_unknown_module_mixed_with_understood_module_runs_full_matrix(self):
        self.assertEqual(
            build_matrix({"plugin/trino-mysql", "plugin/trino-new-connector"}),
            build_matrix(None),
        )

    def test_empty_gib_output_runs_full_matrix(self):
        self.assertEqual(build_matrix(set()), build_matrix(None))

    def test_forced_full_run(self):
        matrix = build_matrix(None)
        self.assertEqual(suites_from_matrix(matrix), ALL_SUITES)
        self.assertEqual(len(matrix["include"]), len(ALL_SUITES))

    def test_understood_module_without_product_tests_produces_empty_matrix(self):
        self.assertEqual(build_matrix({"plugin/trino-example-jdbc"}), {})

    def test_matrix_is_complete(self):
        validate_configuration()


def suites_from_matrix(matrix):
    return {item["suite"] for item in matrix.get("include", [])}


validate_configuration()


if __name__ == "__main__":
    main()
