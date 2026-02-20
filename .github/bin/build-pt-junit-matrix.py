#!/usr/bin/env python3

import json
from pathlib import Path


SUITE_DIR = Path("testing/trino-product-tests/src/test/java/io/trino/tests/product/suite")
LEGACY_SUITE_DIR = Path("testing/trino-product-tests-launcher/src/main/java/io/trino/tests/product/launcher/suite/suites")


def legacy_removed(*legacy_suites):
    return all(not (LEGACY_SUITE_DIR / f"{suite}.java").exists() for suite in legacy_suites)


def junit_exists(suite):
    return (SUITE_DIR / f"{suite}.java").exists()


AGGREGATE_LEGACY_SUITES = (
    "Suite1",
    "Suite2",
    "Suite3",
    "Suite5",
    "Suite6NonGeneric",
    "Suite7NonGeneric",
)

# Use this when coverage is removed from an aggregate legacy suite that still
# exists for unrelated tests.
LEGACY_COVERAGE_REMOVED = ()

BUCKETS = [
    ("jdbc-core", [
        ("SuiteMysql", ("SuiteMysql",)),
        ("SuitePostgresql", LEGACY_COVERAGE_REMOVED),
        ("SuiteSqlServer", LEGACY_COVERAGE_REMOVED),
        ("SuiteFunctions", LEGACY_COVERAGE_REMOVED),
        ("SuiteTpch", ("SuiteTpch",)),
        ("SuiteTpcds", ("SuiteTpcds",)),
    ]),
    ("jdbc-external", [
        ("SuiteExasol", LEGACY_COVERAGE_REMOVED),
        ("SuiteSnowflake", LEGACY_COVERAGE_REMOVED),
    ]),
    ("connector-smoke", [
        ("SuiteCassandra", LEGACY_COVERAGE_REMOVED),
        ("SuiteClickhouse", LEGACY_COVERAGE_REMOVED),
        ("SuiteBlackHole", LEGACY_COVERAGE_REMOVED),
        ("SuiteAllConnectorsSmoke", LEGACY_COVERAGE_REMOVED),
        ("SuiteIgnite", LEGACY_COVERAGE_REMOVED),
    ]),
    ("auth-and-clients", [
        ("SuiteKafka", LEGACY_COVERAGE_REMOVED),
        ("SuiteLdap", LEGACY_COVERAGE_REMOVED),
        ("SuiteOauth2", LEGACY_COVERAGE_REMOVED),
        ("SuiteClients", LEGACY_COVERAGE_REMOVED),
        ("SuiteJdbcKerberos", LEGACY_COVERAGE_REMOVED),
        ("SuiteLoki", LEGACY_COVERAGE_REMOVED),
        ("SuiteRanger", LEGACY_COVERAGE_REMOVED),
        ("SuiteTls", LEGACY_COVERAGE_REMOVED),
    ]),
    ("hive-basic", [
        ("SuiteHiveBasic", LEGACY_COVERAGE_REMOVED),
        ("SuiteHmsOnly", LEGACY_COVERAGE_REMOVED),
        ("SuiteHiveStorageFormats", LEGACY_COVERAGE_REMOVED),
    ]),
    ("hive-kerberos", [
        ("SuiteHdfsImpersonation", LEGACY_COVERAGE_REMOVED),
        ("SuiteTwoHives", LEGACY_COVERAGE_REMOVED),
        ("SuiteHive4", LEGACY_COVERAGE_REMOVED),
        ("SuiteHudi", LEGACY_COVERAGE_REMOVED),
    ]),
    ("hive-transactional", [
        ("SuiteHiveTransactional", LEGACY_COVERAGE_REMOVED),
        ("SuiteAuthorization", LEGACY_COVERAGE_REMOVED),
        ("SuiteFaultTolerant", LEGACY_COVERAGE_REMOVED),
    ]),
    ("hive-storage", [
        ("SuiteHiveSpark", LEGACY_COVERAGE_REMOVED),
        ("SuiteHiveAlluxioCaching", LEGACY_COVERAGE_REMOVED),
        ("SuiteStorageFormatsDetailed", LEGACY_COVERAGE_REMOVED),
        ("SuiteParquet", ("SuiteParquet",)),
    ]),
    ("iceberg", [
        ("SuiteIceberg", LEGACY_COVERAGE_REMOVED),
    ]),
    ("delta-lake", [
        ("SuiteDeltaLakeOss", LEGACY_COVERAGE_REMOVED),
        ("SuiteCompatibility", ("SuiteCompatibility",)),
    ]),
    ("cloud-object-store", [
        ("SuiteGcs", ("SuiteGcs",)),
        ("SuiteAzure", ("SuiteAzure",)),
    ]),
    ("databricks-133", [
        ("SuiteDeltaLakeDatabricks133", LEGACY_COVERAGE_REMOVED),
    ]),
    ("databricks-143", [
        ("SuiteDeltaLakeDatabricks143", LEGACY_COVERAGE_REMOVED),
    ]),
    ("databricks-154", [
        ("SuiteDeltaLakeDatabricks154", LEGACY_COVERAGE_REMOVED),
    ]),
    ("databricks-164", [
        ("SuiteDeltaLakeDatabricks164", LEGACY_COVERAGE_REMOVED),
    ]),
    ("databricks-173", [
        ("SuiteDeltaLakeDatabricks173", LEGACY_COVERAGE_REMOVED),
    ]),
]


include = []
for bucket, suites in BUCKETS:
    selected_suites = [
        suite
        for suite, legacy_suites in suites
        if junit_exists(suite) and legacy_removed(*legacy_suites)
    ]
    if selected_suites:
        include.append({
            "bucket": bucket,
            "suites": " ".join(selected_suites),
        })

print(json.dumps({"include": include} if include else {}))
