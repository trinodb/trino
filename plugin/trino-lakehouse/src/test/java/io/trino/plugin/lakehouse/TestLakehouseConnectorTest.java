/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.lakehouse;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.TestingSession;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.SystemSessionProperties.ITERATIVE_OPTIMIZER_TIMEOUT;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLakehouseConnectorTest
        extends BaseConnectorTest
{
    protected final String bucketName = "test-bucket-" + randomNameSuffix();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Hive3MinioDataLake hiveMinio = closeAfterClass(new Hive3MinioDataLake(bucketName));
        hiveMinio.start();

        return LakehouseQueryRunner.builder()
                .addExtraProperty("sql.path", "lakehouse.functions")
                .addExtraProperty("sql.default-function-catalog", "lakehouse")
                .addExtraProperty("sql.default-function-schema", "functions")
                .addLakehouseProperty("hive.metastore.uri", hiveMinio.getHiveMetastoreEndpoint().toString())
                .addLakehouseProperty("fs.hadoop.enabled", "true")
                .addLakehouseProperty("fs.native-s3.enabled", "true")
                .addLakehouseProperty("s3.aws-access-key", MINIO_ACCESS_KEY)
                .addLakehouseProperty("s3.aws-secret-key", MINIO_SECRET_KEY)
                .addLakehouseProperty("s3.region", MINIO_REGION)
                .addLakehouseProperty("s3.endpoint", hiveMinio.getMinio().getMinioAddress())
                .addLakehouseProperty("s3.path-style-access", "true")
                .addLakehouseProperty("s3.streaming.part-size", "5MB")
                .addLakehouseProperty("hive.metastore-cache-ttl", "1d")
                .addLakehouseProperty("hive.metastore-refresh-interval", "1d")
                .addLakehouseProperty("hive.partition-projection-enabled", "true")
                .build();
    }

    @BeforeAll
    public void setUp()
    {
        computeActual(createSchemaSql("tpch"));
        computeActual(createSchemaSql("functions"));
        copyTpchTables(getQueryRunner(), "tpch", TINY_SCHEMA_NAME, REQUIRED_TPCH_TABLES);
    }

    @BeforeAll
    public void initMockMetricsCatalog()
    {
        QueryRunner queryRunner = getQueryRunner();
        String mockConnector = "mock_metrics";
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withName(mockConnector)
                .withListSchemaNames(_ -> ImmutableList.of("default"))
                .withGetTableStatistics(_ -> {
                    try {
                        Thread.sleep(110);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    return TableStatistics.empty();
                })
                .build()));

        queryRunner.createCatalog("mock_metrics", mockConnector);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_OR_REPLACE_TABLE,
                 SUPPORTS_CREATE_FUNCTION,
                 SUPPORTS_REPORTING_WRITTEN_BYTES -> true;
            case SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT,
                 SUPPORTS_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_REFRESH_VIEW,
                 SUPPORTS_RENAME_MATERIALIZED_VIEW_ACROSS_SCHEMAS,
                 SUPPORTS_TOPN_PUSHDOWN -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected String createSchemaSql(String schemaName)
    {
        return "CREATE SCHEMA %s WITH (location = 's3://%s/%s')".formatted(schemaName, bucketName, schemaName);
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return "NULL value not allowed for NOT NULL column: " + columnName;
    }

    @Override
    protected Session withoutSmallFileThreshold(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "parquet_small_file_threshold", "0B")
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "orc_tiny_stripe_threshold", "0B")
                .build();
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return abort("Iceberg connector does not support column default values");
    }

    @Override
    protected void verifyConcurrentUpdateFailurePermissible(Exception e)
    {
        assertThat(e).hasMessageMatching("Failed to commit( the transaction)? during write.*");
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e).hasMessageMatching(
                "Failed to add column: Metadata location \\[.*] is not same as table metadata location \\[.*] for tpch.test_add_column.*");
    }

    @Override
    protected void verifySetColumnTypeFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(".*(Failed to set column type: Cannot change (column type:|type from .* to )" +
                "|Time(stamp)? precision \\(3\\) not supported for Iceberg. Use \"time(stamp)?\\(6\\)\" instead" +
                "|Type not supported for Iceberg: tinyint|smallint|char\\(20\\)).*");
    }

    @Override
    protected void verifySetFieldTypeFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(".*(Failed to set field type: Cannot change (column type:|type from .* to )" +
                "|Time(stamp)? precision \\(3\\) not supported for Iceberg. Use \"time(stamp)?\\(6\\)\" instead" +
                "|Type not supported for Iceberg: tinyint|smallint|char\\(20\\)" +
                "|Iceberg doesn't support changing field type (from|to) non-primitive types).*");
    }

    @Override
    protected void verifyVersionedQueryFailurePermissible(Exception e)
    {
        assertThat(e).hasMessageMatching("Version pointer type is not supported: .*|" +
                "Unsupported type for temporal table version: .*|" +
                "Unsupported type for table version: .*|" +
                "No version history table tpch.nation at or before .*|" +
                "Iceberg snapshot ID does not exists: .*|" +
                "Cannot find snapshot with reference name: .*");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("char(3)")) {
            return Optional.of(new DataMappingTestSetup(typeName, "'ab '", dataMappingTestSetup.getHighValueLiteral()));
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected Optional<SetColumnTypeSetup> filterSetColumnTypesDataProvider(SetColumnTypeSetup setup)
    {
        if (setup.sourceColumnType().equals("timestamp(3) with time zone")) {
            return Optional.of(setup.withNewValueLiteral("TIMESTAMP '2020-02-12 14:03:00.123000 +00:00'"));
        }
        switch ("%s -> %s".formatted(setup.sourceColumnType(), setup.newColumnType())) {
            case "row(x integer) -> row(\"y\" integer)":
                return Optional.of(setup.withNewValueLiteral("NULL"));
            case "tinyint -> smallint":
            case "bigint -> integer":
            case "bigint -> smallint":
            case "bigint -> tinyint":
            case "decimal(5,3) -> decimal(5,2)":
            case "char(25) -> char(20)":
            case "varchar -> char(20)":
            case "time(6) -> time(3)":
            case "timestamp(6) -> timestamp(3)":
            case "array(integer) -> array(bigint)":
                return Optional.of(setup.asUnsupported());
            case "varchar(100) -> varchar(50)":
                return Optional.empty();
        }
        return Optional.of(setup);
    }

    @Override
    protected Optional<SetColumnTypeSetup> filterSetFieldTypesDataProvider(SetColumnTypeSetup setup)
    {
        if (setup.sourceColumnType().equals("timestamp(3) with time zone")) {
            // The connector returns UTC instead of the given time zone
            return Optional.of(setup.withNewValueLiteral("TIMESTAMP '2020-02-12 14:03:00.123000 +00:00'"));
        }
        switch ("%s -> %s".formatted(setup.sourceColumnType(), setup.newColumnType())) {
            case "tinyint -> smallint":
            case "bigint -> integer":
            case "bigint -> smallint":
            case "bigint -> tinyint":
            case "decimal(5,3) -> decimal(5,2)":
            case "char(25) -> char(20)":
            case "varchar -> char(20)":
            case "time(6) -> time(3)":
            case "timestamp(6) -> timestamp(3)":
            case "array(integer) -> array(bigint)":
            case "row(x integer) -> row(\"x\" bigint)":
            case "row(x integer) -> row(\"y\" integer)":
            case "row(x integer, y integer) -> row(\"x\" integer, \"z\" integer)":
            case "row(x integer) -> row(\"x\" integer, \"y\" integer)":
            case "row(x integer, y integer) -> row(\"x\" integer)":
            case "row(x integer, y integer) -> row(\"y\" integer, \"x\" integer)":
            case "row(x integer, y integer) -> row(\"z\" integer, \"y\" integer, \"x\" integer)":
            case "row(x row(nested integer)) -> row(\"x\" row(\"nested\" bigint))":
            case "row(x row(a integer, b integer)) -> row(\"x\" row(\"b\" integer, \"a\" integer))":
                return Optional.of(setup.asUnsupported());
            case "varchar(100) -> varchar(50)":
                return Optional.empty();
        }
        return Optional.of(setup);
    }

    @Disabled("Long names cause metastore timeouts")
    @Test
    @Override
    public void testCreateTableWithLongTableName() {}

    @Disabled("Long names cause metastore timeouts")
    @Test
    @Override
    public void testRenameTableToLongTableName() {}

    @Disabled("Long names cause metastore timeouts")
    @Test
    @Override
    public void testCreateSchemaWithLongName() {}

    @Disabled("Long names cause metastore timeouts")
    @Test
    @Override
    public void testRenameSchemaToLongName() {}

    @Test
    @Override
    public void testDropRowFieldWhenDuplicates()
    {
        assertThatThrownBy(super::testDropRowFieldWhenDuplicates)
                .hasMessage("Field name 'a' specified more than once");
    }

    @Test
    @Override
    public void testDropAmbiguousRowFieldCaseSensitivity()
    {
        assertThatThrownBy(super::testDropAmbiguousRowFieldCaseSensitivity)
                .hasMessage("Field name 'some_field' specified more than once");
    }

    @Test
    @Override
    public void testSetFieldMapKeyType()
    {
        assertThatThrownBy(super::testSetFieldMapKeyType)
                .hasMessageContaining("Failed to set field type: Cannot alter map keys");
    }

    @Test
    @Override
    public void testSetNestedFieldMapKeyType()
    {
        assertThatThrownBy(super::testSetNestedFieldMapKeyType)
                .hasMessageContaining("Failed to set field type: Cannot alter map keys");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertQueryFails("ALTER SCHEMA tpch RENAME TO tpch_renamed", "Hive metastore does not support renaming schemas");
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        try (TestTable table = newTrinoTable(
                "test_char_varchar",
                "(k, v) AS VALUES" +
                        "   (-1, CAST(NULL AS CHAR(3))), " +
                        "   (3, CAST('   ' AS CHAR(3)))," +
                        "   (6, CAST('x  ' AS CHAR(3)))")) {
            assertThat(query("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('  ' AS varchar(2))")).returnsEmptyResult();
            assertThat(query("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('    ' AS varchar(4))")).returnsEmptyResult();
            assertThat(query("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('x ' AS varchar(2))")).returnsEmptyResult();
            assertQuery("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('   ' AS varchar(3))", "VALUES (3, '   ')");
        }
    }

    @Test
    @Override
    public void testShowCreateSchema()
    {
        assertThat(computeActual("SHOW CREATE SCHEMA tpch").getOnlyValue().toString()).matches(
                """
                \\QCREATE SCHEMA lakehouse.tpch
                WITH (
                   location = \\E's3://test-bucket-.*/tpch'\\Q
                )\\E""");
    }

    @Override
    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue()).matches(
                """
                \\QCREATE TABLE lakehouse.tpch.orders (
                   orderkey bigint,
                   custkey bigint,
                   orderstatus varchar,
                   totalprice double,
                   orderdate date,
                   orderpriority varchar,
                   clerk varchar,
                   shippriority integer,
                   comment varchar
                )
                WITH (
                   format = 'PARQUET',
                   format_version = 2,
                   location = \\E's3://test-bucket-.*/tpch/orders-.*'\\Q,
                   type = 'ICEBERG'
                )\\E""");
    }

    @Test
    public void testCatalogMetadataMetrics()
    {
        QueryRunner.MaterializedResultWithPlan result = getQueryRunner().executeWithPlan(
                getSession(),
                "SELECT count(*) FROM region r, nation n WHERE r.regionkey = n.regionkey");
        Map<String, Metrics> metrics = getCatalogMetadataMetrics(result.queryId());
        assertCountMetricExists(metrics, "lakehouse", "iceberg.metastore.all.time.total");
        assertDistributionMetricExists(metrics, "lakehouse", "iceberg.metastore.all.time.distribution");
        assertCountMetricExists(metrics, "lakehouse", "iceberg.metastore.getTable.time.total");
        assertDistributionMetricExists(metrics, "lakehouse", "iceberg.metastore.getTable.time.distribution");
    }

    @Test
    public void testCatalogMetadataMetricsWithOptimizerTimeoutExceeded()
    {
        String query = "SELECT count(*) FROM region r, nation n, mock_metrics.default.mock_table m WHERE r.regionkey = n.regionkey";
        try {
            Session smallOptimizerTimeout = TestingSession.testSessionBuilder(getSession())
                    .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "100ms")
                    .build();
            QueryRunner.MaterializedResultWithPlan result = getQueryRunner().executeWithPlan(smallOptimizerTimeout, query);
            fail(format("Expected query to fail: %s [QueryId: %s]", query, result.queryId()));
        }
        catch (QueryFailedException e) {
            assertThat(e.getMessage()).contains("The optimizer exhausted the time limit");
            Map<String, Metrics> metrics = getCatalogMetadataMetrics(e.getQueryId());
            assertCountMetricExists(metrics, "lakehouse", "iceberg.metastore.all.time.total");
            assertCountMetricExists(metrics, "lakehouse", "iceberg.metastore.getTable.time.total");
        }
    }
}
