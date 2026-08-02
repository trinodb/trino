# Suite Audit: SuiteIceberg

## Suite Summary

- Purpose: JUnit suite for Iceberg coverage.
- Owning lane: `iceberg`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteIceberg.java`
- CI bucket: `iceberg`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `SparkIcebergEnvironment`
- Include tags: `Iceberg`.
- Exclude tags: `IcebergFormatVersionCompatibility`, `StorageFormats`.
- Expected mapped classes covered: `TestCreateDropSchema`, `TestIcebergInsert`, `TestIcebergOptimize`,
  `TestIcebergPartitionEvolution`, `TestIcebergProcedureCalls`, `TestIcebergSparkCompatibility`,
  `TestIcebergSparkDropTableCompatibility`, `TestSparkIcebergEnvironment`.
- Expected mapped methods covered: `122` method(s).

### Run 2

- Run name: `default`
- Environment: `HiveIcebergRedirectionsEnvironment`
- Include tags: `HiveIcebergRedirections`.
- Exclude tags: none.
- Expected mapped classes covered: `TestHiveRedirectionToIceberg`, `TestIcebergHiveMetadataListing`,
  `TestIcebergHiveViewsCompatibility`, `TestIcebergRedirectionToHive`,
  `TestHiveIcebergRedirectionsEnvironment`.
- Expected mapped methods covered: `62` method(s).

### Run 3

- Run name: `default`
- Environment: `SparkIcebergRestEnvironment`
- Include tags: `IcebergRest`.
- Exclude tags: none.
- Expected mapped classes covered: `TestSparkIcebergRestEnvironment`.
- Expected mapped methods covered: `7` method(s).

### Run 4

- Run name: `default`
- Environment: `SparkIcebergJdbcCatalogEnvironment`
- Include tags: `IcebergJdbc`.
- Exclude tags: none.
- Expected mapped classes covered: `TestSparkIcebergJdbcCatalogEnvironment`.
- Expected mapped methods covered: `7` method(s).

### Run 5

- Run name: `default`
- Environment: `SparkIcebergNessieEnvironment`
- Include tags: `IcebergNessie`.
- Exclude tags: none.
- Expected mapped classes covered: `TestSparkIcebergNessieEnvironment`.
- Expected mapped methods covered: `6` method(s).

### Run 6

- Run name: `default`
- Environment: `MultiNodeIcebergMinioCachingEnvironment`
- Include tags: `IcebergAlluxioCaching`.
- Exclude tags: `StorageFormats`.
- Expected mapped classes covered: `TestIcebergAlluxioCaching`, `TestMultiNodeIcebergMinioCachingEnvironment`.
- Expected mapped methods covered: `7` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `iceberg`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteIceberg`

## Parity Checklist

- Legacy suite or lane source: `iceberg` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteIceberg`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected ported class count: `12`
- Expected current-only environment verification class count: `6`
- Expected audited class count: `18`
- Expected ported method count: `169`
- Expected current-only environment verification method count: `42`
- Expected audited method count: `211`
- Expected migrated classes covered: `TestCreateDropSchema`, `TestHiveRedirectionToIceberg`,
  `TestIcebergAlluxioCaching`, `TestIcebergHiveMetadataListing`, `TestIcebergHiveViewsCompatibility`,
  `TestIcebergInsert`, `TestIcebergOptimize`, `TestIcebergPartitionEvolution`, `TestIcebergProcedureCalls`,
  `TestIcebergRedirectionToHive`, `TestIcebergSparkCompatibility`, `TestIcebergSparkDropTableCompatibility`.
- Expected current-only environment verification classes covered: `TestHiveIcebergRedirectionsEnvironment`,
  `TestMultiNodeIcebergMinioCachingEnvironment`, `TestSparkIcebergEnvironment`,
  `TestSparkIcebergJdbcCatalogEnvironment`, `TestSparkIcebergNessieEnvironment`,
  `TestSparkIcebergRestEnvironment`.
- Expected migrated methods covered: `TestCreateDropSchema.testDropSchemaFiles`,
  `TestCreateDropSchema.testDropSchemaFilesWithLocation`, `TestCreateDropSchema.testDropWithExternalFiles`,
  `TestCreateDropSchema.testDropWithExternalFilesInSubdirectory`,
  `TestHiveRedirectionToIceberg.testAlterTableAddColumn`, `TestHiveRedirectionToIceberg.testAlterTableDropColumn`,
  `TestHiveRedirectionToIceberg.testAlterTableRename`, `TestHiveRedirectionToIceberg.testAlterTableRenameColumn`,
  `TestHiveRedirectionToIceberg.testCommentColumn`, `TestHiveRedirectionToIceberg.testCommentTable`,
  `TestHiveRedirectionToIceberg.testDelete`, `TestHiveRedirectionToIceberg.testDeny`,
  `TestHiveRedirectionToIceberg.testDescribe`, `TestHiveRedirectionToIceberg.testDropTable`,
  `TestHiveRedirectionToIceberg.testGrant`, `TestHiveRedirectionToIceberg.testInformationSchemaColumns`,
  `TestHiveRedirectionToIceberg.testInsert`, `TestHiveRedirectionToIceberg.testMerge`,
  `TestHiveRedirectionToIceberg.testRedirect`, `TestHiveRedirectionToIceberg.testRedirectPartitionsToPartitioned`,
  `TestHiveRedirectionToIceberg.testRedirectPartitionsToUnpartitioned`,
  `TestHiveRedirectionToIceberg.testRedirectToNonexistentCatalog`,
  `TestHiveRedirectionToIceberg.testRedirectWithDefaultSchemaInSession`,
  `TestHiveRedirectionToIceberg.testRedirectWithNonDefaultSchema`, `TestHiveRedirectionToIceberg.testRevoke`,
  `TestHiveRedirectionToIceberg.testSetTableAuthorization`, `TestHiveRedirectionToIceberg.testShowCreateTable`,
  `TestHiveRedirectionToIceberg.testShowGrants`, `TestHiveRedirectionToIceberg.testShowStats`,
  `TestHiveRedirectionToIceberg.testSystemJdbcColumns` ....
- Parity status: `verified`
