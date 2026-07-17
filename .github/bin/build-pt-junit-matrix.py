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
        ("SuiteFunctions", ("SuiteFunctions",)),
        ("SuiteTpch", ("SuiteTpch",)),
        ("SuiteTpcds", ("SuiteTpcds",)),
    ]),
    ("jdbc-external", [
        ("SuiteExasol", ("SuiteExasol",)),
        ("SuiteSnowflake", ("SuiteSnowflake",)),
    ]),
    ("connector-smoke", [
        ("SuiteCassandra", LEGACY_COVERAGE_REMOVED),
        ("SuiteClickhouse", LEGACY_COVERAGE_REMOVED),
        ("SuiteBlackHole", LEGACY_COVERAGE_REMOVED),
        ("SuiteAllConnectorsSmoke", ("SuiteAllConnectorsSmoke",)),
        ("SuiteIgnite", ("SuiteIgnite",)),
    ]),
    ("auth-and-clients", [
        ("SuiteKafka", LEGACY_COVERAGE_REMOVED),
        ("SuiteLdap", ("SuiteLdap",)),
        ("SuiteOauth2", ("SuiteOauth2",)),
        ("SuiteClients", ("SuiteClients",)),
        ("SuiteJdbcKerberos", AGGREGATE_LEGACY_SUITES),
        ("SuiteLoki", ("SuiteLoki",)),
        ("SuiteRanger", ("SuiteRanger",)),
        ("SuiteTls", AGGREGATE_LEGACY_SUITES),
    ]),
    ("hive-basic", [
        ("SuiteHiveBasic", AGGREGATE_LEGACY_SUITES),
        ("SuiteHmsOnly", ("SuiteHmsOnly",)),
        ("SuiteHiveStorageFormats", AGGREGATE_LEGACY_SUITES),
    ]),
    ("hive-kerberos", [
        ("SuiteHiveKerberos", AGGREGATE_LEGACY_SUITES),
        ("SuiteHdfsImpersonation", AGGREGATE_LEGACY_SUITES),
        ("SuiteTwoHives", AGGREGATE_LEGACY_SUITES),
        ("SuiteHive4", ("SuiteHive4",)),
        ("SuiteHudi", ("SuiteHudi",)),
    ]),
    ("hive-transactional", [
        ("SuiteHiveTransactional", ("SuiteHiveTransactional",)),
        ("SuiteAuthorization", ("SuiteHiveTransactional",)),
        ("SuiteFaultTolerant", AGGREGATE_LEGACY_SUITES),
    ]),
    ("hive-storage", [
        ("SuiteHiveSpark", AGGREGATE_LEGACY_SUITES),
        ("SuiteHiveAlluxioCaching", AGGREGATE_LEGACY_SUITES),
        ("SuiteStorageFormatsDetailed", ("SuiteStorageFormatsDetailed",)),
        ("SuiteParquet", ("SuiteParquet",)),
    ]),
    ("iceberg", [
        ("SuiteIceberg", ("SuiteIceberg",)),
    ]),
    ("delta-lake", [
        ("SuiteDeltaLakeOss", ("SuiteDeltaLakeOss",)),
        ("SuiteCompatibility", ("SuiteCompatibility",)),
    ]),
    ("cloud-object-store", [
        ("SuiteGcs", ("SuiteGcs",)),
        ("SuiteAzure", ("SuiteAzure",)),
    ]),
    ("databricks-133", [
        ("SuiteDeltaLakeDatabricks133", ("SuiteDeltaLakeDatabricks133",)),
    ]),
    ("databricks-143", [
        ("SuiteDeltaLakeDatabricks143", ("SuiteDeltaLakeDatabricks143",)),
    ]),
    ("databricks-154", [
        ("SuiteDeltaLakeDatabricks154", ("SuiteDeltaLakeDatabricks154",)),
    ]),
    ("databricks-164", [
        ("SuiteDeltaLakeDatabricks164", ("SuiteDeltaLakeDatabricks164",)),
    ]),
    ("databricks-173", [
        ("SuiteDeltaLakeDatabricks173", ("SuiteDeltaLakeDatabricks173",)),
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
