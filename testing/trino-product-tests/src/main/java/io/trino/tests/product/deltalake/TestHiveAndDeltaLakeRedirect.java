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
package io.trino.tests.product.deltalake;

import com.google.common.collect.ImmutableList;
import io.trino.tempto.assertions.QueryAssert.Row;
import io.trino.tempto.query.QueryExecutionException;
import io.trino.tempto.query.QueryResult;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Condition;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.query.QueryExecutor.param;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.sql.JDBCType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveAndDeltaLakeRedirect
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testHiveToDeltaRedirect()
    {
        String tableName = "test_redirect_to_delta_" + randomNameSuffix();

        onDelta().executeQuery(createTableOnDelta(tableName, false));

        try {
            QueryResult sparkResult = onDelta().executeQuery("SELECT * FROM " + tableName);
            QueryResult hiveResult = onTrino().executeQuery(format("SELECT * FROM hive.default.\"%s\"", tableName));
            assertThat(sparkResult).containsOnly(hiveResult.rows().stream()
                    .map(Row::new)
                    .collect(toImmutableList()));
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testHiveToDeltaNonDefaultSchemaRedirect()
    {
        String schemaName = "test_extraordinary_" + randomNameSuffix();
        String tableName = "test_redirect_to_delta_non_default_schema_" + randomNameSuffix();

        String schemaLocation = format("s3://%s/delta-redirect-test-%s", bucketName, schemaName);
        onDelta().executeQuery(format("CREATE SCHEMA IF NOT EXISTS %s LOCATION \"%s\"", schemaName, schemaLocation));
        onDelta().executeQuery(createTableOnDelta(schemaName, tableName, false));
        try {
            QueryResult sparkResult = onDelta().executeQuery(format("SELECT * FROM %s.%s", schemaName, tableName));
            QueryResult hiveResult = onTrino().executeQuery(format("SELECT * FROM hive.%s.\"%s\"", schemaName, tableName));
            assertThat(sparkResult).containsOnly(hiveResult.rows().stream()
                    .map(Row::new)
                    .collect(toImmutableList()));
        }
        finally {
            dropDeltaTableWithRetry(format("%s.%s", schemaName, tableName));
            onDelta().executeQuery("DROP SCHEMA " + schemaName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testHiveToNonexistentDeltaCatalogRedirectFailure()
    {
        String tableName = "test_redirect_to_nonexistent_delta_" + randomNameSuffix();

        try {
            onDelta().executeQuery(createTableOnDelta(tableName, false));

            onTrino().executeQuery("SET SESSION hive.delta_lake_catalog_name = 'epsilon'");

            assertQueryFailure(() -> onTrino().executeQuery(format("SELECT * FROM hive.default.\"%s\"", tableName)))
                    .hasMessageMatching(".*Table 'hive.default.test_redirect_to_nonexistent_delta_.*' redirected to 'epsilon.default.test_redirect_to_nonexistent_delta_.*', but the target catalog 'epsilon' does not exist");
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }

    // Note: this tests engine more than connectors. Still good scenario to test.
    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testHiveToDeltaRedirectWithDefaultSchemaInSession()
    {
        String tableName = "test_redirect_to_delta_with_use_" + randomNameSuffix();

        onDelta().executeQuery(createTableOnDelta(tableName, false));

        try {
            onTrino().executeQuery("USE hive.default");

            QueryResult sparkResult = onDelta().executeQuery("SELECT * FROM " + tableName);
            QueryResult hiveResult = onTrino().executeQuery(format("SELECT * FROM \"%s\"", tableName));
            assertThat(sparkResult).containsOnly(hiveResult.rows().stream()
                    .map(Row::new)
                    .collect(toImmutableList()));
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testHiveToUnpartitionedDeltaPartitionsRedirectFailure()
    {
        String tableName = "test_delta_lake_unpartitioned_table_" + randomNameSuffix();

        onDelta().executeQuery(createTableOnDelta(tableName, false));

        try {
            assertQueryFailure(() -> onTrino().executeQuery(format("SELECT * FROM hive.default.\"%s$partitions\"", tableName)))
                    .hasMessageMatching(".*Table 'hive.default.test_delta_lake_unpartitioned_table_.*\\$partitions' redirected to 'delta.default.test_delta_lake_unpartitioned_table_.*\\$partitions', " +
                            "but the target table 'delta.default.test_delta_lake_unpartitioned_table_.*\\$partitions' does not exist");
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testHiveToPartitionedDeltaPartitionsRedirectFailure()
    {
        String tableName = "test_delta_lake_partitioned_table_" + randomNameSuffix();

        onDelta().executeQuery(createTableOnDelta(tableName, true));

        try {
            assertQueryFailure(() -> onTrino().executeQuery(format("SELECT * FROM hive.default.\"%s$partitions\"", tableName)))
                    .hasMessageMatching(".*Table 'hive.default.test_delta_lake_partitioned_table_.*\\$partitions' redirected to 'delta.default.test_delta_lake_partitioned_table_.*\\$partitions', " +
                            "but the target table 'delta.default.test_delta_lake_partitioned_table_.*\\$partitions' does not exist");
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeltaToHiveRedirect()
    {
        String tableName = "test_redirect_to_hive_" + randomNameSuffix();

        onTrino().executeQuery(createTableInHiveConnector("default", tableName, false));

        try {
            List<Row> expectedResults = ImmutableList.of(
                    row(1, false, -128),
                    row(2, true, 127),
                    row(3, false, 0),
                    row(4, false, 1),
                    row(5, true, 37));
            QueryResult deltaResult = onTrino().executeQuery(format("SELECT * FROM delta.default.\"%s\"", tableName));
            QueryResult hiveResult = onTrino().executeQuery(format("SELECT * FROM hive.default.\"%s\"", tableName));
            assertThat(deltaResult).containsOnly(expectedResults);
            assertThat(hiveResult).containsOnly(expectedResults);
        }
        finally {
            onTrino().executeQuery("DROP TABLE hive.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeltaToHiveNonDefaultSchemaRedirect()
    {
        String schemaName = "test_extraordinary" + randomNameSuffix();
        String schemaLocation = format("s3://%s/delta-redirect-test-%s", bucketName, schemaName);
        String tableName = "test_redirect_to_hive_non_default_schema_" + randomNameSuffix();

        onTrino().executeQuery(format("CREATE SCHEMA IF NOT EXISTS hive.%s WITH (location='%s')", schemaName, schemaLocation));

        onTrino().executeQuery(createTableInHiveConnector(schemaName, tableName, false));

        try {
            List<Row> expectedResults = ImmutableList.of(
                    row(1, false, -128),
                    row(2, true, 127),
                    row(3, false, 0),
                    row(4, false, 1),
                    row(5, true, 37));
            QueryResult deltaResult = onTrino().executeQuery(format("SELECT * FROM delta.%s.\"%s\"", schemaName, tableName));
            QueryResult hiveResult = onTrino().executeQuery(format("SELECT * FROM hive.%s.\"%s\"", schemaName, tableName));
            assertThat(deltaResult).containsOnly(expectedResults);
            assertThat(hiveResult).containsOnly(expectedResults);
        }
        finally {
            onTrino().executeQuery(format("DROP TABLE hive.%s.%s", schemaName, tableName));
            onTrino().executeQuery("DROP SCHEMA " + schemaName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeltaToNonexistentHiveCatalogRedirectFailure()
    {
        String tableName = "test_redirect_to_nonexistent_hive_" + randomNameSuffix();

        onTrino().executeQuery(createTableInHiveConnector("default", tableName, false));

        try {
            onTrino().executeQuery("SET SESSION delta.hive_catalog_name = 'spark'");

            assertQueryFailure(() -> onTrino().executeQuery(format("SELECT * FROM delta.default.\"%s\"", tableName)))
                    .hasMessageMatching(".*Table 'delta.default.test_redirect_to_nonexistent_hive_.*' redirected to 'spark.default.test_redirect_to_nonexistent_hive_.*', but the target catalog 'spark' does not exist");
        }
        finally {
            onTrino().executeQuery("DROP TABLE hive.default." + tableName);
        }
    }

    // Note: this tests engine more than connectors. Still good scenario to test.
    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeltaToHiveRedirectWithDefaultSchemaInSession()
    {
        String tableName = "test_redirect_to_hive_with_use_" + randomNameSuffix();

        onTrino().executeQuery("USE hive.default");

        onTrino().executeQuery(createTableInHiveConnector("default", tableName, false));

        try {
            List<Row> expectedResults = ImmutableList.of(
                    row(1, false, -128),
                    row(2, true, 127),
                    row(3, false, 0),
                    row(4, false, 1),
                    row(5, true, 37));
            QueryResult deltaResult = onTrino().executeQuery(format("SELECT * FROM delta.default.\"%s\"", tableName));
            QueryResult hiveResult = onTrino().executeQuery(format("SELECT * FROM \"%s\"", tableName));
            assertThat(deltaResult).containsOnly(expectedResults);
            assertThat(hiveResult).containsOnly(expectedResults);
        }
        finally {
            onTrino().executeQuery("DROP TABLE hive.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeltaToPartitionedHivePartitionsRedirect()
    {
        String tableName = "test_hive_partitioned_table_" + randomNameSuffix();

        onTrino().executeQuery(createTableInHiveConnector("default", tableName, true));

        try {
            List<Row> expectedResults = ImmutableList.of(
                    row(-128),
                    row(127),
                    row(0),
                    row(1),
                    row(37));
            QueryResult deltaResult = onTrino().executeQuery(format("SELECT * FROM delta.default.\"%s$partitions\"", tableName));
            QueryResult hiveResult = onTrino().executeQuery(format("SELECT * FROM hive.default.\"%s$partitions\"", tableName));
            assertThat(deltaResult).containsOnly(expectedResults);
            assertThat(hiveResult).containsOnly(expectedResults);
        }
        finally {
            onTrino().executeQuery("DROP TABLE hive.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeltaToUnpartitionedHivePartitionsRedirectFailure()
    {
        String tableName = "test_hive_unpartitioned_table_" + randomNameSuffix();

        onTrino().executeQuery(createTableInHiveConnector("default", tableName, false));

        try {
            assertQueryFailure(() -> onTrino().executeQuery(format("SELECT * FROM delta.default.\"%s$partitions\"", tableName)))
                    .hasMessageMatching(".*Table 'delta.default.test_hive_unpartitioned_table.*partitions' does not exist");
        }
        finally {
            onTrino().executeQuery("DROP TABLE hive.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeltaToHiveInsert()
    {
        String tableName = "test_hive_insert_by_delta_" + randomNameSuffix();

        onTrino().executeQuery(createTableInHiveConnector("default", tableName, true));

        try {
            onTrino().executeQuery(format("INSERT INTO delta.default.\"%s\" VALUES (6, false, -17), (7, true, 1)", tableName));

            List<Row> expectedResults = ImmutableList.of(
                    row(1, false, -128),
                    row(2, true, 127),
                    row(3, false, 0),
                    row(4, false, 1),
                    row(5, true, 37),
                    row(6, false, -17),
                    row(7, true, 1));

            QueryResult deltaResult = onTrino().executeQuery(format("SELECT * FROM delta.default.\"%s\"", tableName));
            QueryResult hiveResult = onTrino().executeQuery(format("SELECT * FROM hive.default.\"%s\"", tableName));
            assertThat(deltaResult).containsOnly(expectedResults);
            assertThat(hiveResult).containsOnly(expectedResults);
        }
        finally {
            onTrino().executeQuery("DROP TABLE hive.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testHiveToDeltaInsert()
    {
        String tableName = "test_delta_insert_by_hive_" + randomNameSuffix();

        onDelta().executeQuery(createTableOnDelta(tableName, true));

        try {
            onTrino().executeQuery(format("INSERT INTO hive.default.\"%s\" VALUES (1234567890, 'San Escobar', 5, 'If I had a world of my own, everything would be nonsense')", tableName));

            assertThat(onTrino().executeQuery(format("SELECT count(*) FROM delta.default.\"%s\"", tableName))).containsOnly(row(5));
            assertThat(onTrino().executeQuery(format("SELECT count(*) FROM hive.default.\"%s\"", tableName))).containsOnly(row(5));
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeltaToHiveDescribe()
    {
        String tableName = "test_hive_describe_by_delta_" + randomNameSuffix();

        onTrino().executeQuery(createTableInHiveConnector("default", tableName, true));

        try {
            List<Row> expectedResults = ImmutableList.of(
                    row("id", "integer", "", ""),
                    row("flag", "boolean", "", ""),
                    row("rate", "tinyint", "partition key", ""));
            assertThat(onTrino().executeQuery(format("DESCRIBE delta.default.\"%s\"", tableName)))
                    .containsOnly(expectedResults);
        }
        finally {
            onTrino().executeQuery("DROP TABLE hive.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testHiveToDeltaDescribe()
    {
        String tableName = "test_delta_describe_by_hive_" + randomNameSuffix();

        onDelta().executeQuery(createTableOnDelta(tableName, true));

        try {
            List<Row> expectedResults = ImmutableList.of(
                    row("nationkey", "bigint", "", ""),
                    row("name", "varchar", "", ""),
                    row("regionkey", "bigint", "", ""),
                    row("comment", "varchar", "", ""));
            assertThat(onTrino().executeQuery(format("DESCRIBE hive.default.\"%s\"", tableName)))
                    .containsOnly(expectedResults);
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeltaToHiveShowCreateTable()
    {
        String tableName = "test_hive_show_create_table_by_delta_" + randomNameSuffix();

        onTrino().executeQuery(createTableInHiveConnector("default", tableName, true));

        try {
            assertThat(onTrino().executeQuery(format("SHOW CREATE TABLE delta.default.\"%s\"", tableName)))
                    .hasRowsCount(1);
        }
        finally {
            onTrino().executeQuery("DROP TABLE hive.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testHiveToDeltaShowCreateTable()
    {
        String tableName = "test_delta_show_create_table_by_hive_" + randomNameSuffix();

        onDelta().executeQuery(createTableOnDelta(tableName, true));

        try {
            assertThat(onTrino().executeQuery(format("SHOW CREATE TABLE hive.default.\"%s\"", tableName)))
                    .hasRowsCount(1);
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeltaToHiveAlterTable()
    {
        String tableName = "test_hive_alter_table_by_delta_" + randomNameSuffix();
        // TODO set the partitioning for the table to `true` after the fix of https://github.com/trinodb/trino/issues/11826
        onTrino().executeQuery(createTableInHiveConnector("default", tableName, false));
        String newTableName = tableName + "_new";
        try {
            onTrino().executeQuery("ALTER TABLE delta.default." + tableName + " RENAME TO " + newTableName);
        }
        catch (QueryExecutionException e) {
            onTrino().executeQuery("DROP TABLE hive.default." + tableName);
            throw e;
        }

        try {
            assertResultsEqual(
                    onTrino().executeQuery("TABLE hive.default." + newTableName),
                    onTrino().executeQuery("TABLE delta.default." + newTableName));
        }
        finally {
            onTrino().executeQuery("DROP TABLE hive.default." + newTableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testHiveToDeltaAlterTable()
    {
        String tableName = "test_delta_alter_table_by_hive_" + randomNameSuffix();
        String newTableName = tableName + "_new";

        onDelta().executeQuery(createTableOnDelta(tableName, true));

        try {
            onTrino().executeQuery("ALTER TABLE hive.default.\"" + tableName + "\" RENAME TO \"" + newTableName + "\"");
            dropDeltaTableWithRetry(newTableName);
        }
        catch (QueryExecutionException e) {
            dropDeltaTableWithRetry(tableName);
            throw e;
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeltaToHiveCommentTable()
    {
        String tableName = "test_hive_comment_table_by_delta_" + randomNameSuffix();

        onTrino().executeQuery(createTableInHiveConnector("default", tableName, true));
        try {
            assertThat(onTrino().executeQuery("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'hive' AND schema_name = 'default' AND table_name = '" + tableName + "'"))
                    .is(new Condition<>(queryResult -> queryResult.getOnlyValue() == null, "Unexpected table comment"));
            String tableComment = "This is my table, there are many like it but this one is mine";
            onTrino().executeQuery(format("COMMENT ON TABLE delta.default.\"" + tableName + "\" IS '%s'", tableComment));

            assertTableComment("hive", "default", tableName).isEqualTo(tableComment);
            assertTableComment("delta", "default", tableName).isEqualTo(tableComment);
        }
        finally {
            onTrino().executeQuery("DROP TABLE hive.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testHiveToDeltaCommentTable()
    {
        String tableName = "test_delta_comment_table_by_hive_" + randomNameSuffix();

        onDelta().executeQuery(createTableOnDelta(tableName, true));

        try {
            assertThat(onTrino().executeQuery("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'delta' AND schema_name = 'default' AND table_name = '" + tableName + "'"))
                    .is(new Condition<>(queryResult -> queryResult.getOnlyValue() == null, "Unexpected table comment"));

            String tableComment = "This is my table, there are many like it but this one is mine";
            onTrino().executeQuery(format("COMMENT ON TABLE hive.default.\"" + tableName + "\" IS '%s'", tableComment));
            assertTableComment("hive", "default", tableName).isEqualTo(tableComment);
            assertTableComment("delta", "default", tableName).isEqualTo(tableComment);
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testDeltaToHiveCommentColumn()
    {
        String tableName = "test_hive_comment_column_by_delta_" + randomNameSuffix();
        String columnName = "id";

        onTrino().executeQuery(createTableInHiveConnector("default", tableName, true));
        try {
            assertColumnComment("hive", "default", tableName, columnName).isNull();
            assertColumnComment("delta", "default", tableName, columnName).isNull();

            String columnComment = "Internal identifier";
            onTrino().executeQuery(format("COMMENT ON COLUMN delta.default.%s.%s IS '%s'", tableName, columnName, columnComment));

            assertColumnComment("hive", "default", tableName, columnName).isEqualTo(columnComment);
            assertColumnComment("delta", "default", tableName, columnName).isEqualTo(columnComment);
        }
        finally {
            onTrino().executeQuery("DROP TABLE hive.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testHiveToDeltaCommentColumn()
    {
        String tableName = "test_delta_comment_column_by_hive_" + randomNameSuffix();
        String columnName = "nationkey";

        onDelta().executeQuery(createTableOnDelta(tableName, true));

        try {
            assertColumnComment("hive", "default", tableName, columnName).isNull();
            assertColumnComment("delta", "default", tableName, columnName).isNull();

            String columnComment = "Internal identifier for the nation";
            onTrino().executeQuery(format("COMMENT ON COLUMN hive.default.%s.%s IS '%s'", tableName, columnName, columnComment));

            assertColumnComment("hive", "default", tableName, columnName).isEqualTo(columnComment);
            assertColumnComment("delta", "default", tableName, columnName).isEqualTo(columnComment);
        }
        finally {
            dropDeltaTableWithRetry(tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testInsertIntoDeltaTableFromHiveNonDefaultSchemaRedirect()
    {
        String destSchema = "test_extraordinary_" + randomNameSuffix();
        String destTableName = "test_create_delta_table_from_hive_non_default_schema_" + randomNameSuffix();

        String schemaLocation = format("s3://%s/delta-redirect-test-%s", bucketName, destSchema);
        onDelta().executeQuery(format("CREATE SCHEMA IF NOT EXISTS %s LOCATION \"%s\"", destSchema, schemaLocation));
        onDelta().executeQuery(createTableOnDelta(destSchema, destTableName, false));

        try {
            onTrino().executeQuery(format("INSERT INTO hive.%s.\"%s\" (nationkey, name, regionkey) VALUES (26, 'POLAND', 3)", destSchema, destTableName));

            QueryResult hiveResult = onTrino().executeQuery(format("SELECT * FROM hive.%s.\"%s\"", destSchema, destTableName));
            QueryResult deltaResult = onTrino().executeQuery(format("SELECT * FROM delta.%s.\"%s\"", destSchema, destTableName));

            List<Row> expectedDestinationTableRows = ImmutableList.<Row>builder()
                    .add(new Row(0, "ALGERIA", 0, "haggle. carefully final deposits detect slyly agai"))
                    .add(new Row(1, "ARGENTINA", 1, "al foxes promise slyly according to the regular accounts. bold requests alon"))
                    .add(new Row(2, "BRAZIL", 1, "y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special"))
                    .add(new Row(3, "CANADA", 1, "eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold"))
                    .add(new Row(26, "POLAND", 3, null))
                    .build();

            assertThat(hiveResult)
                    .containsOnly(expectedDestinationTableRows);
            assertThat(deltaResult)
                    .containsOnly(expectedDestinationTableRows);
        }
        finally {
            dropDeltaTableWithRetry(format("%s.%s", destSchema, destTableName));
            onTrino().executeQuery("DROP SCHEMA " + destSchema);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testInformationSchemaColumnsHiveToDeltaRedirect()
    {
        // use dedicated schema so we control the number and shape of tables
        String schemaName = "test_redirect_to_delta_information_schema_columns_schema_" + randomNameSuffix();
        String schemaLocation = format("s3://%s/delta-redirect-test-%s", bucketName, schemaName);
        onTrino().executeQuery(format("CREATE SCHEMA IF NOT EXISTS hive.%s WITH (location = '%s')", schemaName, schemaLocation));

        String tableName = "redirect_to_delta_information_schema_columns_table_" + randomNameSuffix();
        try {
            onDelta().executeQuery(createTableOnDelta(schemaName, tableName, false));

            // via redirection with table filter
            assertThat(onTrino().executeQuery(
                    format("SELECT * FROM hive.information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", schemaName, tableName)))
                    .containsOnly(
                            row("hive", schemaName, tableName, "nationkey", 1, null, "YES", "bigint"),
                            row("hive", schemaName, tableName, "name", 2, null, "YES", "varchar"),
                            row("hive", schemaName, tableName, "regionkey", 3, null, "YES", "bigint"),
                            row("hive", schemaName, tableName, "comment", 4, null, "YES", "varchar"));

            // test via redirection with just schema filter
            assertThat(onTrino().executeQuery(
                    format("SELECT * FROM hive.information_schema.columns WHERE table_schema = '%s'", schemaName)))
                    .containsOnly(
                            row("hive", schemaName, tableName, "nationkey", 1, null, "YES", "bigint"),
                            row("hive", schemaName, tableName, "name", 2, null, "YES", "varchar"),
                            row("hive", schemaName, tableName, "regionkey", 3, null, "YES", "bigint"),
                            row("hive", schemaName, tableName, "comment", 4, null, "YES", "varchar"));

            // sanity check that getting columns info without redirection produces matching result
            assertThat(onTrino().executeQuery(
                    format("SELECT * FROM delta.information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", schemaName, tableName)))
                    .containsOnly(
                            row("delta", schemaName, tableName, "nationkey", 1, null, "YES", "bigint"),
                            row("delta", schemaName, tableName, "name", 2, null, "YES", "varchar"),
                            row("delta", schemaName, tableName, "regionkey", 3, null, "YES", "bigint"),
                            row("delta", schemaName, tableName, "comment", 4, null, "YES", "varchar"));
        }
        finally {
            dropDeltaTableWithRetry(format("%s.%s", schemaName, tableName));
            onTrino().executeQuery("DROP SCHEMA " + schemaName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testInformationSchemaColumnsDeltaToHiveRedirect()
    {
        // use dedicated schema so we control the number and shape of tables
        String schemaName = "test_redirect_to_hive_information_schema_columns_schema_" + randomNameSuffix();
        String schemaLocation = format("s3://%s/delta-redirect-test-%s", bucketName, schemaName);
        onTrino().executeQuery(format("CREATE SCHEMA IF NOT EXISTS hive.%s WITH (location='%s')", schemaName, schemaLocation));

        String tableName = "test_redirect_to_hive_information_schema_columns_table_" + randomNameSuffix();
        try {
            onTrino().executeQuery(createTableInHiveConnector(schemaName, tableName, false));

            // via redirection with table filter
            assertThat(onTrino().executeQuery(
                    format("SELECT * FROM delta.information_schema.columns WHERE table_schema = '%s' AND table_name='%s'", schemaName, tableName)))
                    .containsOnly(
                            row("delta", schemaName, tableName, "id", 1, null, "YES", "integer"),
                            row("delta", schemaName, tableName, "flag", 2, null, "YES", "boolean"),
                            row("delta", schemaName, tableName, "rate", 3, null, "YES", "tinyint"));

            // test via redirection with just schema filter
            assertThat(onTrino().executeQuery(
                    format("SELECT * FROM delta.information_schema.columns WHERE table_schema = '%s'", schemaName)))
                    .containsOnly(
                            row("delta", schemaName, tableName, "id", 1, null, "YES", "integer"),
                            row("delta", schemaName, tableName, "flag", 2, null, "YES", "boolean"),
                            row("delta", schemaName, tableName, "rate", 3, null, "YES", "tinyint"));

            // sanity check that getting columns info without redirection produces matching result
            assertThat(onTrino().executeQuery(
                    format("SELECT * FROM hive.information_schema.columns WHERE table_schema = '%s' AND table_name='%s'", schemaName, tableName)))
                    .containsOnly(
                            row("hive", schemaName, tableName, "id", 1, null, "YES", "integer"),
                            row("hive", schemaName, tableName, "flag", 2, null, "YES", "boolean"),
                            row("hive", schemaName, tableName, "rate", 3, null, "YES", "tinyint"));
        }
        finally {
            onTrino().executeQuery(format("DROP TABLE IF EXISTS hive.%s.%s", schemaName, tableName));
            onTrino().executeQuery("DROP SCHEMA " + schemaName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testSystemJdbcColumnsHiveToDeltaRedirect()
    {
        // use dedicated schema so we control the number and shape of tables
        String schemaName = "test_redirect_to_delta_system_jdbc_columns_schema_" + randomNameSuffix();
        String schemaLocation = format("s3://%s/delta-redirect-test-%s", bucketName, schemaName);
        onTrino().executeQuery(format("CREATE SCHEMA IF NOT EXISTS hive.%s WITH (location='%s')", schemaName, schemaLocation));

        String tableName = "test_redirect_to_delta_system_jdbc_columns_table_" + randomNameSuffix();
        try {
            onDelta().executeQuery(createTableOnDelta(schemaName, tableName, false));

            // via redirection with table filter
            assertThat(onTrino().executeQuery(
                    format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns  WHERE table_cat = 'hive' AND table_schem = '%s' AND table_name = '%s'", schemaName, tableName)))
                    .containsOnly(
                            row("hive", schemaName, tableName, "nationkey"),
                            row("hive", schemaName, tableName, "name"),
                            row("hive", schemaName, tableName, "regionkey"),
                            row("hive", schemaName, tableName, "comment"));

            // test via redirection with just schema filter
            // via redirection with table filter
            assertThat(onTrino().executeQuery(
                    format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns  WHERE table_cat = 'hive' AND table_schem = '%s'", schemaName)))
                    .containsOnly(
                            row("hive", schemaName, tableName, "nationkey"),
                            row("hive", schemaName, tableName, "name"),
                            row("hive", schemaName, tableName, "regionkey"),
                            row("hive", schemaName, tableName, "comment"));

            // sanity check that getting columns info without redirection produces matching result
            assertThat(onTrino().executeQuery(
                    format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns  WHERE table_cat = 'delta' AND table_schem = '%s' AND table_name = '%s'", schemaName, tableName)))
                    .containsOnly(
                            row("delta", schemaName, tableName, "nationkey"),
                            row("delta", schemaName, tableName, "name"),
                            row("delta", schemaName, tableName, "regionkey"),
                            row("delta", schemaName, tableName, "comment"));
        }
        finally {
            dropDeltaTableWithRetry(format("%s.%s", schemaName, tableName));
            onTrino().executeQuery("DROP SCHEMA " + schemaName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testSystemJdbcColumnsDeltaToHiveRedirect()
    {
        // use dedicated schema so we control the number and shape of tables
        String schemaName = "test_redirect_to_hive_system_jdbc_columns_schema_" + randomNameSuffix();
        String schemaLocation = format("s3://%s/delta-redirect-test-%s", bucketName, schemaName);
        onTrino().executeQuery(format("CREATE SCHEMA IF NOT EXISTS hive.%s WITH (location='%s')", schemaName, schemaLocation));

        String tableName = "test_redirect_to_hive_system_jdbc_columns_table_" + randomNameSuffix();
        try {
            onTrino().executeQuery(createTableInHiveConnector(schemaName, tableName, false));

            // via redirection with table filter
            assertThat(onTrino().executeQuery(
                    format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns  WHERE table_cat = 'delta' AND table_schem = '%s' AND table_name = '%s'", schemaName, tableName)))
                    .containsOnly(
                            row("delta", schemaName, tableName, "id"),
                            row("delta", schemaName, tableName, "flag"),
                            row("delta", schemaName, tableName, "rate"));

            // test via redirection with just schema filter
            assertThat(onTrino().executeQuery(
                    format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns  WHERE table_cat = 'delta' AND table_schem = '%s'", schemaName)))
                    .containsOnly(
                            row("delta", schemaName, tableName, "id"),
                            row("delta", schemaName, tableName, "flag"),
                            row("delta", schemaName, tableName, "rate"));

            // sanity check that getting columns info without redirection produces matching result
            assertThat(onTrino().executeQuery(
                    format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns  WHERE table_cat = 'hive' AND table_schem = '%s' AND table_name = '%s'", schemaName, tableName)))
                    .containsOnly(
                            row("hive", schemaName, tableName, "id"),
                            row("hive", schemaName, tableName, "flag"),
                            row("hive", schemaName, tableName, "rate"));
        }
        finally {
            onTrino().executeQuery(format("DROP TABLE IF EXISTS hive.%s.%s", schemaName, tableName));
            onTrino().executeQuery("DROP SCHEMA " + schemaName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS}, dataProvider = "trueFalse")
    public void testViewReferencingHiveAndDeltaTable(boolean legacyHiveViewTranslation)
    {
        String hiveTableName = "test_view_hive_table_" + randomNameSuffix();
        String deltaTableName = "test_view_delta_table_" + randomNameSuffix();
        String viewName = "test_view_view_" + randomNameSuffix();
        String deltaRegionTableName = "test_view_delta_region_table_" + randomNameSuffix();

        @Language("SQL")
        String deltaTableData = "SELECT " +
                "  true a_boolean, " +
                "  CAST(1 AS integer) an_integer, " +
                "  CAST(1 AS bigint) a_bigint," +
                "  CAST(1 AS real) a_real, " +
                "  CAST(1 AS double) a_double, " +
                "  CAST('13.1' AS decimal(3,1)) a_short_decimal, " +
                "  CAST('123456789123456.123456789' AS decimal(24,9)) a_long_decimal, " +
                "  CAST('abc' AS string) an_unbounded_varchar, " +
                "  X'abcd' a_varbinary, " +
                "  DATE '2005-09-10' a_date, " +
                // TODO this results in: column [a_timestamp] of type timestamp(3) with time zone projected from query view at position 10 cannot be coerced to column [a_timestamp] of type timestamp(3) stored in view definition
                //   This is because Delta/Spark/Databricks declares the column as "timestamp" in view definition,
                //   but Spark timestamp is point in time, so "timestamp" in table gets mapped to "timestamp with time zone" in Delta Lake connector.
                //   This could be alleviated by injecting a CAST while processing the view.
                // "  TIMESTAMP '2005-09-10 13:00:00.123456' a_timestamp, " +
                // TODO Spark doesn't seem to have real `timestamp with time zone` type. The value ends up being stored as "timestamp".
                // "  TIMESTAMP '2005-09-10 13:00:00.123456 Europe/Warsaw' a_timestamp_tz, " +
                "  0 a_last_column ";

        try {
            onTrino().executeQuery("CREATE TABLE hive.default." + hiveTableName + " " +
                    "WITH (external_location = '" + locationForTable(hiveTableName) + "') " +
                    "AS TABLE tpch.tiny.region");
            onDelta().executeQuery("" +
                    "CREATE TABLE " + deltaTableName + " USING DELTA " +
                    "LOCATION '" + locationForTable(deltaTableName) + "' " +
                    " AS " + deltaTableData);
            onDelta().executeQuery("" +
                    "CREATE TABLE " + deltaRegionTableName + " USING DELTA " +
                    "LOCATION '" + locationForTable(deltaRegionTableName) + "' " +
                    " AS VALUES " +
                    "    (CAST(0 AS bigint), 'AFRICA'), " +
                    "    (CAST(1 AS bigint), 'AMERICA'), " +
                    "    (CAST(2 AS bigint), 'ASIA'), " +
                    "    (CAST(3 AS bigint), 'EUROPE'), " +
                    "    (CAST(4 AS bigint), 'MIDDLE EAST') AS data(regionkey, name)");
            onDelta().executeQuery("CREATE VIEW " + viewName + " AS " +
                    "SELECT dt.*, regionkey, name " +
                    "FROM " + deltaTableName + " dt JOIN " + deltaRegionTableName + " ON an_integer = regionkey");

            List<Row> expected = List.of(
                    row(
                            true,
                            1,
                            1L,
                            1.0f,
                            1d,
                            new BigDecimal("13.1"),
                            new BigDecimal("123456789123456.123456789"),
                            "abc",
                            new byte[] {(byte) 0xAB, (byte) 0xCD},
                            Date.valueOf(LocalDate.of(2005, 9, 10)),
                            0, // delta table's a_last_column,
                            1L,
                            "AMERICA"));

            assertThat(onDelta().executeQuery("SELECT * FROM " + viewName))
                    .containsOnly(expected);

            onTrino().executeQuery("SET SESSION hive.hive_views_legacy_translation = " + legacyHiveViewTranslation);
            assertThat(onTrino().executeQuery("SELECT * FROM hive.default." + viewName))
                    .containsOnly(expected);

            // Hive views are currently not supported in Delta Lake connector
            assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM delta.default." + viewName))
                    .hasMessageMatching("\\QQuery failed (\\E#\\S+\\Q): default." + viewName + " is not a Delta Lake table");
        }
        finally {
            onDelta().executeQuery("DROP VIEW IF EXISTS " + viewName);
            dropDeltaTableWithRetry(deltaTableName);
            dropDeltaTableWithRetry(deltaRegionTableName);
            onTrino().executeQuery("DROP TABLE IF EXISTS hive.default." + hiveTableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testHiveToDeltaPropertiesRedirect()
    {
        String tableName = "test_redirect_to_delta_properties_" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE " + tableName + " USING DELTA " +
                "LOCATION '" + locationForTable(tableName) + "' " +
                " AS SELECT true AS a_boolean");

        List<Row> expected = List.of(
                row("delta.minReaderVersion", "1"),
                row("delta.minWriterVersion", "2"));

        try {
            assertThat(onTrino().executeQuery(format("SELECT * FROM delta.default.\"%s$properties\"", tableName))).containsOnly(expected);
            assertThat(onTrino().executeQuery(format("SELECT * FROM hive.default.\"%s$properties\"", tableName))).containsOnly(expected);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @DataProvider
    public Object[][] trueFalse()
    {
        return new Object[][] {{true}, {false}};
    }

    private String createTableOnDelta(String tableName, boolean partitioned)
    {
        return createTableOnDelta("default", tableName, partitioned);
    }

    @Language("SQL")
    private String createTableOnDelta(String schema, String tableName, boolean partitioned)
    {
        return "CREATE TABLE " + schema + "." + tableName + " " +
                "USING DELTA " +
                (partitioned ? "PARTITIONED BY (regionkey) " : "") +
                "LOCATION '" + locationForTable(tableName) + "' " +
                " AS VALUES " +
                "(CAST(0 AS bigint), 'ALGERIA', CAST(0 AS bigint), 'haggle. carefully final deposits detect slyly agai')," +
                "(CAST(1 AS bigint), 'ARGENTINA', CAST(1 AS bigint), 'al foxes promise slyly according to the regular accounts. bold requests alon')," +
                "(CAST(2 AS bigint), 'BRAZIL', CAST(1 AS bigint), 'y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special')," +
                "(CAST(3 AS bigint), 'CANADA', CAST(1 AS bigint), 'eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold')" +
                " AS data(nationkey, name, regionkey, comment)";
    }

    @Language("SQL")
    private String createTableInHiveConnector(String schema, String tableName, boolean partitioned)
    {
        return "CREATE TABLE hive." + schema + "." + tableName +
                "(id, flag, rate) " +
                "WITH (" +
                "    external_location = '" + locationForTable(tableName) + "'" +
                (partitioned ? ", partitioned_by = ARRAY['rate']" : "") +
                ") AS VALUES " +
                "(1, BOOLEAN 'false', TINYINT '-128'), " +
                "(2, BOOLEAN 'true', TINYINT '127'), " +
                "(3, BOOLEAN 'false', TINYINT '0'), " +
                "(4, BOOLEAN 'false', TINYINT '1'), " +
                "(5, BOOLEAN 'true', TINYINT '37')";
    }

    private String locationForTable(String tableName)
    {
        return "s3://" + bucketName + "/hive-and-databricks-redirect-" + tableName;
    }

    private static AbstractStringAssert<?> assertTableComment(String catalog, String schema, String tableName)
    {
        return assertThat((String) readTableComment(catalog, schema, tableName).getOnlyValue());
    }

    private static QueryResult readTableComment(String catalog, String schema, String tableName)
    {
        return onTrino().executeQuery(
                "SELECT comment FROM system.metadata.table_comments WHERE catalog_name = ? AND schema_name = ? AND table_name = ?",
                param(VARCHAR, catalog),
                param(VARCHAR, schema),
                param(VARCHAR, tableName));
    }

    private static AbstractStringAssert<?> assertColumnComment(String catalog, String schema, String tableName, String columnName)
    {
        return assertThat((String) readColumnComment(catalog, schema, tableName, columnName).getOnlyValue());
    }

    private static QueryResult readColumnComment(String catalog, String schema, String tableName, String columnName)
    {
        return onTrino().executeQuery(
                format("SELECT comment FROM %s.information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?", catalog),
                param(VARCHAR, schema),
                param(VARCHAR, tableName),
                param(VARCHAR, columnName));
    }

    private static void assertResultsEqual(QueryResult first, QueryResult second)
    {
        assertThat(first).containsOnly(second.rows().stream()
                .map(Row::new)
                .collect(toImmutableList()));

        // just for symmetry
        assertThat(second).containsOnly(first.rows().stream()
                .map(Row::new)
                .collect(toImmutableList()));
    }
}
