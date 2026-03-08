# Lane Audit: Hive4

## Lane Summary

- Audit status: `complete`
- Section start commit: `Add Hive4 environments`
- Section end commit: `Remove legacy SuiteHive4`
- Introduced JUnit suites: `SuiteHive4`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteHive4`.
- Environment classes introduced: none.
- Method status counts: verified `0`, intentional difference `15`, needs follow-up `0`.

## Semantic Audit Status

- Manual review note: the legacy Hive4 lane was re-read from `SuiteHive4`, `EnvMultinodeHive4`, and `TestHiveOnOrcLegacyDateCompatibility`, then compared against the current `MultinodeHive4Environment`, `SuiteHive4`, and the two current-only Hive4 environment verification classes.

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: `HDP to Hive 3.1 migration`.
- Needs-follow-up methods: none identified in the manual source-body pass for this lane.

## Environment Semantic Audit
### `MultinodeHive4Environment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `EnvMultinodeHive4` against current `MultinodeHive4Environment`.
- Container/service inventory parity: preserved in intent. Both use a multinode Trino cluster backed by Hive4 + MinIO storage.
- Config/resource wiring parity: preserved in intent. Legacy launcher used `StandardMultinode` plus `Hive4WithMinio` extenders and a mounted `hive.properties`; current environment constructs the same Hive4/MinIO topology and the same Hive catalog directly in code.
- Startup parity: preserved. MinIO and Hive4 services start before the multinode Trino cluster.
- Recorded differences: outside-Docker execution, JUnit/Testcontainers framework replacement, and direct Testcontainers cluster construction instead of launcher environment composition.
- Reviewer note: no Hive4-environment fidelity gap is currently identified for the migrated suite coverage.

## Suite Semantic Audit
### `SuiteHive4`
- Suite semantic audit status: `complete`
- CI bucket: `hive-kerberos`
- Relationship to lane: `owned by this lane`.
- Legacy/current basis: manual comparison of legacy launcher `SuiteHive4` and current `SuiteHive4`.
- Environment-run parity: preserved. Both execute a single Hive4 run on the multinode Hive4 environment family.
- Tag parity: preserved. Legacy suite used `(HIVE4, CONFIGURED_FEATURES)`; current suite selects `Hive4`, and the only migrated class carries both `Hive4` and `ConfiguredFeatures`, so effective coverage is unchanged.
- Recorded differences: current execution is direct JUnit suite execution instead of launcher-driven Docker-side execution.
- Reviewer note: no Hive4 suite-level fidelity gap is currently identified.

## Ported Test Classes

No primary test-class migrations were introduced in this lane.


## Current-only Environment Verification Coverage


### `TestHive4Environment`

- Owning introduction commit: `Add Hive environments`
- Current class added in same commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHive4Environment.java`
- Legacy class removed in same commit:
  none. This is current-only JUnit environment verification coverage added during the framework rewrite.
- Class-level environment requirement: `MultinodeHive4Environment`.
- Class-level tags: `none`.
- Method inventory complete: Current-only class. Legacy methods: `0`. Current methods: `6`.
- Legacy helper/resource dependencies accounted for: not applicable; current class source reviewed directly.
- Intentional differences summary: `new JUnit-side environment verification coverage`
- Method statuses present: `intentional difference`. These methods were manually reviewed as current-only environment verification coverage and are not counted as legacy parity replacements.

#### Methods

##### `verifyTrinoConnectivity`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHive4Environment.java` ->
  `TestHive4Environment.verifyTrinoConnectivity`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultinodeHive4Environment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `trino connectivity`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultinodeHive4Environment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

##### `verifyHiveCatalogExists`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHive4Environment.java` ->
  `TestHive4Environment.verifyHiveCatalogExists`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultinodeHive4Environment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `hive catalog exists`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultinodeHive4Environment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

##### `verifyHiveDefaultSchema`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHive4Environment.java` ->
  `TestHive4Environment.verifyHiveDefaultSchema`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultinodeHive4Environment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `hive default schema`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultinodeHive4Environment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

##### `verifyCreateAndReadTable`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHive4Environment.java` ->
  `TestHive4Environment.verifyCreateAndReadTable`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultinodeHive4Environment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `create and read table`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultinodeHive4Environment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

##### `verifyS3Storage`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHive4Environment.java` ->
  `TestHive4Environment.verifyS3Storage`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultinodeHive4Environment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `s3 storage`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultinodeHive4Environment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

##### `verifyHive4Interoperability`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHive4Environment.java` ->
  `TestHive4Environment.verifyHive4Interoperability`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultinodeHive4Environment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `hive4 interoperability`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultinodeHive4Environment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`


### `TestMultinodeHive4Environment`

- Owning introduction commit: `Add Hive environments`
- Current class added in same commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestMultinodeHive4Environment.java`
- Legacy class removed in same commit:
  none. This is current-only JUnit environment verification coverage added during the framework rewrite.
- Class-level environment requirement: `MultinodeHive4Environment`.
- Class-level tags: `none`.
- Method inventory complete: Current-only class. Legacy methods: `0`. Current methods: `9`.
- Legacy helper/resource dependencies accounted for: not applicable; current class source reviewed directly.
- Intentional differences summary: `new JUnit-side environment verification coverage`
- Method statuses present: `intentional difference`. These methods were manually reviewed as current-only environment verification coverage and are not counted as legacy parity replacements.

#### Methods

##### `verifyTrinoConnectivity`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestMultinodeHive4Environment.java` ->
  `TestMultinodeHive4Environment.verifyTrinoConnectivity`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultinodeHive4Environment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `trino connectivity`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultinodeHive4Environment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

##### `verifyHiveCatalogExists`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestMultinodeHive4Environment.java` ->
  `TestMultinodeHive4Environment.verifyHiveCatalogExists`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultinodeHive4Environment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `hive catalog exists`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultinodeHive4Environment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

##### `verifyTpchCatalogExists`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestMultinodeHive4Environment.java` ->
  `TestMultinodeHive4Environment.verifyTpchCatalogExists`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultinodeHive4Environment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `tpch catalog exists`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultinodeHive4Environment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

##### `verifyTpchQueryable`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestMultinodeHive4Environment.java` ->
  `TestMultinodeHive4Environment.verifyTpchQueryable`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultinodeHive4Environment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `tpch queryable`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultinodeHive4Environment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

##### `verifyHiveDefaultSchema`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestMultinodeHive4Environment.java` ->
  `TestMultinodeHive4Environment.verifyHiveDefaultSchema`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultinodeHive4Environment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `hive default schema`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultinodeHive4Environment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

##### `verifyCreateAndReadTable`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestMultinodeHive4Environment.java` ->
  `TestMultinodeHive4Environment.verifyCreateAndReadTable`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultinodeHive4Environment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `create and read table`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultinodeHive4Environment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

##### `verifyMultinodeCluster`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestMultinodeHive4Environment.java` ->
  `TestMultinodeHive4Environment.verifyMultinodeCluster`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultinodeHive4Environment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `multinode cluster`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultinodeHive4Environment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

##### `verifyDistributedQueryExecution`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestMultinodeHive4Environment.java` ->
  `TestMultinodeHive4Environment.verifyDistributedQueryExecution`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultinodeHive4Environment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `distributed query execution`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultinodeHive4Environment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

##### `verifyClusterUtilityMethods`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestMultinodeHive4Environment.java` ->
  `TestMultinodeHive4Environment.verifyClusterUtilityMethods`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultinodeHive4Environment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `cluster utility methods`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultinodeHive4Environment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

