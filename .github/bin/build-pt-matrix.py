#!/usr/bin/env python3

import json
from pathlib import Path


SUITE_DIR = Path("testing/trino-product-tests/src/test/java/io/trino/tests/product/suite")
SUITE_HELPERS = {"SuiteRunner", "SuiteTag"}

BUCKETS = [
    ("jdbc-core", [
        "SuiteMysql",
        "SuitePostgresql",
        "SuiteSqlServer",
        "SuiteFunctions",
        "SuiteTpch",
        "SuiteTpcds",
    ]),
    ("jdbc-external", [
        "SuiteExasol",
        "SuiteSnowflake",
    ]),
    ("connector-smoke", [
        "SuiteCassandra",
        "SuiteClickhouse",
        "SuiteBlackHole",
        "SuiteAllConnectorsSmoke",
        "SuiteIgnite",
    ]),
    ("auth-and-clients", [
        "SuiteKafka",
        "SuiteLdap",
        "SuiteOauth2",
        "SuiteClients",
        "SuiteJdbcKerberos",
        "SuiteLoki",
        "SuiteRanger",
        "SuiteTls",
    ]),
    ("hive-basic", [
        "SuiteHiveBasic",
        "SuiteHmsOnly",
        "SuiteHiveStorageFormats",
        "SuiteSqlCancel",
    ]),
    ("hive-kerberos", [
        "SuiteHdfsImpersonation",
        "SuiteTwoHives",
        "SuiteHive4",
        "SuiteHudi",
    ]),
    ("hive-transactional", [
        "SuiteHiveTransactional",
        "SuiteAuthorization",
        "SuiteFaultTolerant",
    ]),
    ("hive-storage", [
        "SuiteHiveSpark",
        "SuiteHiveAlluxioCaching",
        "SuiteStorageFormatsDetailed",
        "SuiteParquet",
    ]),
    ("iceberg", [
        "SuiteIceberg",
    ]),
    ("delta-lake", [
        "SuiteDeltaLakeOss",
        "SuiteCompatibility",
    ]),
    ("cloud-object-store", [
        "SuiteGcs",
        "SuiteAzure",
    ]),
    ("databricks-133", [
        "SuiteDeltaLakeDatabricks133",
    ]),
    ("databricks-143", [
        "SuiteDeltaLakeDatabricks143",
    ]),
    ("databricks-154", [
        "SuiteDeltaLakeDatabricks154",
    ]),
    ("databricks-164", [
        "SuiteDeltaLakeDatabricks164",
    ]),
    ("databricks-173", [
        "SuiteDeltaLakeDatabricks173",
    ]),
]


declared_suites = [suite for _, suites in BUCKETS for suite in suites]
duplicate_suites = sorted({suite for suite in declared_suites if declared_suites.count(suite) > 1})
if duplicate_suites:
    raise SystemExit(f"Suites declared more than once: {', '.join(duplicate_suites)}")

actual_suites = {path.stem for path in SUITE_DIR.glob("Suite*.java")} - SUITE_HELPERS
missing_suites = sorted(set(declared_suites) - actual_suites)
if missing_suites:
    raise SystemExit(f"Declared product test suites are missing: {', '.join(missing_suites)}")

unwired_suites = sorted(actual_suites - set(declared_suites))
if unwired_suites:
    raise SystemExit(f"Product test suites are missing from the CI matrix: {', '.join(unwired_suites)}")

include = [
    {
        "bucket": bucket,
        "suites": " ".join(suites),
    }
    for bucket, suites in BUCKETS
]

print(json.dumps({"include": include}))
