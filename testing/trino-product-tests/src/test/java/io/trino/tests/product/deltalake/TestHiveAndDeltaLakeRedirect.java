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
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.assertj.core.api.AbstractStringAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests Hive to Delta Lake and Delta Lake to Hive table redirection functionality.
 */
@ProductTest
@RequiresEnvironment(HiveDeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeOss
class TestHiveAndDeltaLakeRedirect
{
    @Test
    void testHiveToDeltaRedirect(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_redirect_to_delta_" + randomNameSuffix();

        env.executeSparkUpdate(createTableOnDelta(env, tableName, false));

        try {
            QueryResult sparkResult = env.executeSpark("SELECT * FROM " + tableName);
            QueryResult hiveResult = env.executeTrino(format("SELECT * FROM hive.default.\"%s\"", tableName));
            assertResultsEqual(sparkResult, hiveResult);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testHiveToDeltaNonDefaultSchemaRedirect(HiveDeltaLakeMinioEnvironment env)
    {
        String schemaName = "test_extraordinary_" + randomNameSuffix();
        String tableName = "test_redirect_to_delta_non_default_schema_" + randomNameSuffix();

        String schemaLocation = format("s3://%s/delta-redirect-test-%s", env.getBucketName(), schemaName);
        env.executeSparkUpdate(format("CREATE SCHEMA IF NOT EXISTS %s LOCATION \"%s\"", schemaName, schemaLocation));
        env.executeSparkUpdate(createTableOnDelta(env, schemaName, tableName, false));
        try {
            QueryResult sparkResult = env.executeSpark(format("SELECT * FROM %s.%s", schemaName, tableName));
            QueryResult hiveResult = env.executeTrino(format("SELECT * FROM hive.%s.\"%s\"", schemaName, tableName));
            assertResultsEqual(sparkResult, hiveResult);
        }
        finally {
            env.executeSparkUpdate(format("DROP TABLE IF EXISTS %s.%s", schemaName, tableName));
            env.executeSparkUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }

    @Test
    void testHiveToNonexistentDeltaCatalogRedirectFailure(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_redirect_to_nonexistent_delta_" + randomNameSuffix();

        try {
            env.executeSparkUpdate(createTableOnDelta(env, tableName, false));

            env.executeTrinoInSession(session -> {
                session.executeUpdate("SET SESSION hive.delta_lake_catalog_name = 'epsilon'");

                assertThatThrownBy(() -> session.executeQuery(format("SELECT * FROM hive.default.\"%s\"", tableName)))
                        .hasMessageMatching(".*Table 'hive.default.test_redirect_to_nonexistent_delta_.*' redirected to 'epsilon.default.test_redirect_to_nonexistent_delta_.*', but the target catalog 'epsilon' does not exist.*");
            });
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    // Note: this tests engine more than connectors. Still good scenario to test.
    @Test
    void testHiveToDeltaRedirectWithDefaultSchemaInSession(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_redirect_to_delta_with_use_" + randomNameSuffix();

        env.executeSparkUpdate(createTableOnDelta(env, tableName, false));

        try {
            env.executeTrinoInSession(session -> {
                session.executeUpdate("USE hive.default");

                QueryResult sparkResult = env.executeSpark("SELECT * FROM " + tableName);
                QueryResult hiveResult = session.executeQuery(format("SELECT * FROM \"%s\"", tableName));
                assertResultsEqual(sparkResult, hiveResult);
            });
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testHiveToUnpartitionedDeltaPartitionsRedirect(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_delta_lake_unpartitioned_table_" + randomNameSuffix();

        env.executeSparkUpdate(createTableOnDelta(env, tableName, false));

        try {
            assertThat(env.executeTrino(format("SELECT * FROM hive.default.\"%s$partitions\"", tableName)))
                    .hasRowsCount(0);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testDeltaToHiveRedirect(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_redirect_to_hive_" + randomNameSuffix();

        env.executeTrinoUpdate(createTableInHiveConnector(env, "default", tableName, false));

        try {
            List<Row> expectedResults = ImmutableList.of(
                    row(1, false, (byte) -128),
                    row(2, true, (byte) 127),
                    row(3, false, (byte) 0),
                    row(4, false, (byte) 1),
                    row(5, true, (byte) 37));
            QueryResult deltaResult = env.executeTrino(format("SELECT * FROM delta.default.\"%s\"", tableName));
            QueryResult hiveResult = env.executeTrino(format("SELECT * FROM hive.default.\"%s\"", tableName));
            assertThat(deltaResult).containsOnly(expectedResults);
            assertThat(hiveResult).containsOnly(expectedResults);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
        }
    }

    @Test
    void testDeltaToHiveNonDefaultSchemaRedirect(HiveDeltaLakeMinioEnvironment env)
    {
        String schemaName = "test_extraordinary" + randomNameSuffix();
        String schemaLocation = format("s3://%s/delta-redirect-test-%s", env.getBucketName(), schemaName);
        String tableName = "test_redirect_to_hive_non_default_schema_" + randomNameSuffix();

        env.executeTrinoUpdate(format("CREATE SCHEMA IF NOT EXISTS hive.%s WITH (location='%s')", schemaName, schemaLocation));

        env.executeTrinoUpdate(createTableInHiveConnector(env, schemaName, tableName, false));

        try {
            List<Row> expectedResults = ImmutableList.of(
                    row(1, false, (byte) -128),
                    row(2, true, (byte) 127),
                    row(3, false, (byte) 0),
                    row(4, false, (byte) 1),
                    row(5, true, (byte) 37));
            QueryResult deltaResult = env.executeTrino(format("SELECT * FROM delta.%s.\"%s\"", schemaName, tableName));
            QueryResult hiveResult = env.executeTrino(format("SELECT * FROM hive.%s.\"%s\"", schemaName, tableName));
            assertThat(deltaResult).containsOnly(expectedResults);
            assertThat(hiveResult).containsOnly(expectedResults);
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE hive.%s.%s", schemaName, tableName));
            env.executeTrinoUpdate("DROP SCHEMA hive." + schemaName);
        }
    }

    @Test
    void testDeltaToNonexistentHiveCatalogRedirectFailure(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_redirect_to_nonexistent_hive_" + randomNameSuffix();

        env.executeTrinoUpdate(createTableInHiveConnector(env, "default", tableName, false));

        try {
            env.executeTrinoInSession(session -> {
                session.executeUpdate("SET SESSION delta.hive_catalog_name = 'spark'");

                assertThatThrownBy(() -> session.executeQuery(format("SELECT * FROM delta.default.\"%s\"", tableName)))
                        .hasMessageMatching(".*Table 'delta.default.test_redirect_to_nonexistent_hive_.*' redirected to 'spark.default.test_redirect_to_nonexistent_hive_.*', but the target catalog 'spark' does not exist.*");
            });
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
        }
    }

    // Note: this tests engine more than connectors. Still good scenario to test.
    @Test
    void testDeltaToHiveRedirectWithDefaultSchemaInSession(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_redirect_to_hive_with_use_" + randomNameSuffix();

        env.executeTrinoUpdate(createTableInHiveConnector(env, "default", tableName, false));

        try {
            env.executeTrinoInSession(session -> {
                session.executeUpdate("USE hive.default");

                List<Row> expectedResults = ImmutableList.of(
                        row(1, false, (byte) -128),
                        row(2, true, (byte) 127),
                        row(3, false, (byte) 0),
                        row(4, false, (byte) 1),
                        row(5, true, (byte) 37));
                QueryResult deltaResult = session.executeQuery(format("SELECT * FROM delta.default.\"%s\"", tableName));
                QueryResult hiveResult = session.executeQuery(format("SELECT * FROM \"%s\"", tableName));
                assertThat(deltaResult).containsOnly(expectedResults);
                assertThat(hiveResult).containsOnly(expectedResults);
            });
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
        }
    }

    @Test
    void testDeltaToPartitionedHivePartitionsRedirect(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_hive_partitioned_table_" + randomNameSuffix();

        env.executeTrinoUpdate(createTableInHiveConnector(env, "default", tableName, true));

        try {
            List<Row> expectedResults = ImmutableList.of(
                    row((byte) -128),
                    row((byte) 127),
                    row((byte) 0),
                    row((byte) 1),
                    row((byte) 37));
            QueryResult deltaResult = env.executeTrino(format("SELECT * FROM delta.default.\"%s$partitions\"", tableName));
            QueryResult hiveResult = env.executeTrino(format("SELECT * FROM hive.default.\"%s$partitions\"", tableName));
            assertThat(deltaResult).containsOnly(expectedResults);
            assertThat(hiveResult).containsOnly(expectedResults);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
        }
    }

    @Test
    void testDeltaToUnpartitionedHivePartitionsRedirectFailure(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_hive_unpartitioned_table_" + randomNameSuffix();

        env.executeTrinoUpdate(createTableInHiveConnector(env, "default", tableName, false));

        try {
            assertThatThrownBy(() -> env.executeTrino(format("SELECT * FROM delta.default.\"%s$partitions\"", tableName)))
                    .hasMessageContaining("does not exist");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
        }
    }

    @Test
    void testDeltaToHiveInsert(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_hive_insert_by_delta_" + randomNameSuffix();

        env.executeTrinoUpdate(createTableInHiveConnector(env, "default", tableName, true));

        try {
            env.executeTrinoUpdate(format("INSERT INTO delta.default.\"%s\" VALUES (6, false, -17), (7, true, 1)", tableName));

            List<Row> expectedResults = ImmutableList.of(
                    row(1, false, (byte) -128),
                    row(2, true, (byte) 127),
                    row(3, false, (byte) 0),
                    row(4, false, (byte) 1),
                    row(5, true, (byte) 37),
                    row(6, false, (byte) -17),
                    row(7, true, (byte) 1));

            QueryResult deltaResult = env.executeTrino(format("SELECT * FROM delta.default.\"%s\"", tableName));
            QueryResult hiveResult = env.executeTrino(format("SELECT * FROM hive.default.\"%s\"", tableName));
            assertThat(deltaResult).containsOnly(expectedResults);
            assertThat(hiveResult).containsOnly(expectedResults);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
        }
    }

    @Test
    void testHiveToDeltaInsert(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_delta_insert_by_hive_" + randomNameSuffix();

        env.executeSparkUpdate(createTableOnDelta(env, tableName, true));

        try {
            env.executeTrinoUpdate(format("INSERT INTO hive.default.\"%s\" VALUES (1234567890, 'San Escobar', 5, 'If I had a world of my own, everything would be nonsense')", tableName));

            assertThat(env.executeTrino(format("SELECT count(*) FROM delta.default.\"%s\"", tableName))).containsOnly(row(5L));
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.\"%s\"", tableName))).containsOnly(row(5L));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testDeltaToHiveDescribe(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_hive_describe_by_delta_" + randomNameSuffix();

        env.executeTrinoUpdate(createTableInHiveConnector(env, "default", tableName, true));

        try {
            List<Row> expectedResults = ImmutableList.of(
                    row("id", "integer", "", ""),
                    row("flag", "boolean", "", ""),
                    row("rate", "tinyint", "partition key", ""));
            assertThat(env.executeTrino(format("DESCRIBE delta.default.\"%s\"", tableName)))
                    .containsOnly(expectedResults);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
        }
    }

    @Test
    void testHiveToDeltaDescribe(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_delta_describe_by_hive_" + randomNameSuffix();

        env.executeSparkUpdate(createTableOnDelta(env, tableName, true));

        try {
            List<Row> expectedResults = ImmutableList.of(
                    row("nationkey", "bigint", "", ""),
                    row("name", "varchar", "", ""),
                    row("regionkey", "bigint", "", ""),
                    row("comment", "varchar", "", ""));
            assertThat(env.executeTrino(format("DESCRIBE hive.default.\"%s\"", tableName)))
                    .containsOnly(expectedResults);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testDeltaToHiveShowCreateTable(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_hive_show_create_table_by_delta_" + randomNameSuffix();

        env.executeTrinoUpdate(createTableInHiveConnector(env, "default", tableName, true));

        try {
            assertThat(env.executeTrino(format("SHOW CREATE TABLE delta.default.\"%s\"", tableName)))
                    .hasRowsCount(1);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
        }
    }

    @Test
    void testHiveToDeltaShowCreateTable(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_delta_show_create_table_by_hive_" + randomNameSuffix();

        env.executeSparkUpdate(createTableOnDelta(env, tableName, true));

        try {
            assertThat(env.executeTrino(format("SHOW CREATE TABLE hive.default.\"%s\"", tableName)))
                    .hasRowsCount(1);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testDeltaToHiveAlterTable(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_hive_alter_table_by_delta_" + randomNameSuffix();
        // TODO set the partitioning for the table to `true` after the fix of https://github.com/trinodb/trino/issues/11826
        env.executeTrinoUpdate(createTableInHiveConnector(env, "default", tableName, false));
        String newTableName = tableName + "_new";
        try {
            env.executeTrinoUpdate("ALTER TABLE delta.default." + tableName + " RENAME TO " + newTableName);
        }
        catch (RuntimeException e) {
            env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
            throw e;
        }

        try {
            assertResultsEqual(
                    env.executeTrino("TABLE hive.default." + newTableName),
                    env.executeTrino("TABLE delta.default." + newTableName));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE hive.default." + newTableName);
        }
    }

    @Test
    void testHiveToDeltaAlterTable(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_delta_alter_table_by_hive_" + randomNameSuffix();
        String newTableName = tableName + "_new";

        env.executeSparkUpdate(createTableOnDelta(env, tableName, true));

        try {
            env.executeTrinoUpdate("ALTER TABLE hive.default.\"" + tableName + "\" RENAME TO \"" + newTableName + "\"");
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + newTableName);
        }
        catch (RuntimeException e) {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
            throw e;
        }
    }

    @Test
    void testDeltaToHiveCommentTable(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_hive_comment_table_by_delta_" + randomNameSuffix();

        env.executeTrinoUpdate(createTableInHiveConnector(env, "default", tableName, true));
        try {
            assertTableComment(env, "hive", "default", tableName).isNull();
            String tableComment = "This is my table, there are many like it but this one is mine";
            env.executeTrinoUpdate(format("COMMENT ON TABLE delta.default.\"%s\" IS '%s'", tableName, tableComment));

            assertTableComment(env, "hive", "default", tableName).isEqualTo(tableComment);
            assertTableComment(env, "delta", "default", tableName).isEqualTo(tableComment);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
        }
    }

    @Test
    void testHiveToDeltaCommentTable(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_delta_comment_table_by_hive_" + randomNameSuffix();

        env.executeSparkUpdate(createTableOnDelta(env, tableName, true));

        try {
            assertTableComment(env, "delta", "default", tableName).isNull();

            String tableComment = "This is my table, there are many like it but this one is mine";
            env.executeTrinoUpdate(format("COMMENT ON TABLE hive.default.\"%s\" IS '%s'", tableName, tableComment));
            assertTableComment(env, "hive", "default", tableName).isEqualTo(tableComment);
            assertTableComment(env, "delta", "default", tableName).isEqualTo(tableComment);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testDeltaToHiveCommentColumn(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_hive_comment_column_by_delta_" + randomNameSuffix();
        String columnName = "id";

        env.executeTrinoUpdate(createTableInHiveConnector(env, "default", tableName, true));
        try {
            assertColumnComment(env, "hive", "default", tableName, columnName).isNull();
            assertColumnComment(env, "delta", "default", tableName, columnName).isNull();

            String columnComment = "Internal identifier";
            env.executeTrinoUpdate(format("COMMENT ON COLUMN delta.default.%s.%s IS '%s'", tableName, columnName, columnComment));

            assertColumnComment(env, "hive", "default", tableName, columnName).isEqualTo(columnComment);
            assertColumnComment(env, "delta", "default", tableName, columnName).isEqualTo(columnComment);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
        }
    }

    @Test
    void testHiveToDeltaCommentColumn(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_delta_comment_column_by_hive_" + randomNameSuffix();
        String columnName = "nationkey";

        env.executeSparkUpdate(createTableOnDelta(env, tableName, true));

        try {
            assertColumnComment(env, "hive", "default", tableName, columnName).isNull();
            assertColumnComment(env, "delta", "default", tableName, columnName).isNull();

            String columnComment = "Internal identifier for the nation";
            env.executeTrinoUpdate(format("COMMENT ON COLUMN hive.default.%s.%s IS '%s'", tableName, columnName, columnComment));

            assertColumnComment(env, "hive", "default", tableName, columnName).isEqualTo(columnComment);
            assertColumnComment(env, "delta", "default", tableName, columnName).isEqualTo(columnComment);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testInsertIntoDeltaTableFromHiveNonDefaultSchemaRedirect(HiveDeltaLakeMinioEnvironment env)
    {
        String destSchema = "test_extraordinary_" + randomNameSuffix();
        String destTableName = "test_create_delta_table_from_hive_non_default_schema_" + randomNameSuffix();

        String schemaLocation = format("s3://%s/delta-redirect-test-%s", env.getBucketName(), destSchema);
        env.executeSparkUpdate(format("CREATE SCHEMA IF NOT EXISTS %s LOCATION \"%s\"", destSchema, schemaLocation));
        env.executeSparkUpdate(createTableOnDelta(env, destSchema, destTableName, false));

        try {
            env.executeTrinoUpdate(format("INSERT INTO hive.%s.\"%s\" (nationkey, name, regionkey) VALUES (26, 'POLAND', 3)", destSchema, destTableName));

            QueryResult hiveResult = env.executeTrino(format("SELECT * FROM hive.%s.\"%s\"", destSchema, destTableName));
            QueryResult deltaResult = env.executeTrino(format("SELECT * FROM delta.%s.\"%s\"", destSchema, destTableName));

            List<Row> expectedDestinationTableRows = ImmutableList.<Row>builder()
                    .add(row(0L, "ALGERIA", 0L, "haggle. carefully final deposits detect slyly agai"))
                    .add(row(1L, "ARGENTINA", 1L, "al foxes promise slyly according to the regular accounts. bold requests alon"))
                    .add(row(2L, "BRAZIL", 1L, "y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special"))
                    .add(row(3L, "CANADA", 1L, "eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold"))
                    .add(row(26L, "POLAND", 3L, null))
                    .build();

            assertThat(hiveResult)
                    .containsOnly(expectedDestinationTableRows);
            assertThat(deltaResult)
                    .containsOnly(expectedDestinationTableRows);
        }
        finally {
            env.executeSparkUpdate(format("DROP TABLE IF EXISTS %s.%s", destSchema, destTableName));
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS hive." + destSchema);
        }
    }

    @Test
    void testInformationSchemaColumnsHiveToDeltaRedirect(HiveDeltaLakeMinioEnvironment env)
    {
        // use dedicated schema so we control the number and shape of tables
        String schemaName = "test_redirect_to_delta_information_schema_columns_schema_" + randomNameSuffix();
        String schemaLocation = format("s3://%s/delta-redirect-test-%s", env.getBucketName(), schemaName);
        env.executeTrinoUpdate(format("CREATE SCHEMA IF NOT EXISTS hive.%s WITH (location = '%s')", schemaName, schemaLocation));

        String tableName = "redirect_to_delta_information_schema_columns_table_" + randomNameSuffix();
        try {
            env.executeSparkUpdate(createTableOnDelta(env, schemaName, tableName, false));

            // via redirection with table filter
            assertThat(env.executeTrino(
                    format("SELECT * FROM hive.information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", schemaName, tableName)))
                    .containsOnly(
                            row("hive", schemaName, tableName, "nationkey", 1L, null, "YES", "bigint"),
                            row("hive", schemaName, tableName, "name", 2L, null, "YES", "varchar"),
                            row("hive", schemaName, tableName, "regionkey", 3L, null, "YES", "bigint"),
                            row("hive", schemaName, tableName, "comment", 4L, null, "YES", "varchar"));

            // test via redirection with just schema filter
            assertThat(env.executeTrino(
                    format("SELECT * FROM hive.information_schema.columns WHERE table_schema = '%s'", schemaName)))
                    .containsOnly(
                            row("hive", schemaName, tableName, "nationkey", 1L, null, "YES", "bigint"),
                            row("hive", schemaName, tableName, "name", 2L, null, "YES", "varchar"),
                            row("hive", schemaName, tableName, "regionkey", 3L, null, "YES", "bigint"),
                            row("hive", schemaName, tableName, "comment", 4L, null, "YES", "varchar"));

            // sanity check that getting columns info without redirection produces matching result
            assertThat(env.executeTrino(
                    format("SELECT * FROM delta.information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", schemaName, tableName)))
                    .containsOnly(
                            row("delta", schemaName, tableName, "nationkey", 1L, null, "YES", "bigint"),
                            row("delta", schemaName, tableName, "name", 2L, null, "YES", "varchar"),
                            row("delta", schemaName, tableName, "regionkey", 3L, null, "YES", "bigint"),
                            row("delta", schemaName, tableName, "comment", 4L, null, "YES", "varchar"));
        }
        finally {
            env.executeSparkUpdate(format("DROP TABLE IF EXISTS %s.%s", schemaName, tableName));
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS hive." + schemaName);
        }
    }

    @Test
    void testInformationSchemaColumnsDeltaToHiveRedirect(HiveDeltaLakeMinioEnvironment env)
    {
        // use dedicated schema so we control the number and shape of tables
        String schemaName = "test_redirect_to_hive_information_schema_columns_schema_" + randomNameSuffix();
        String schemaLocation = format("s3://%s/delta-redirect-test-%s", env.getBucketName(), schemaName);
        env.executeTrinoUpdate(format("CREATE SCHEMA IF NOT EXISTS hive.%s WITH (location='%s')", schemaName, schemaLocation));

        String tableName = "test_redirect_to_hive_information_schema_columns_table_" + randomNameSuffix();
        try {
            env.executeTrinoUpdate(createTableInHiveConnector(env, schemaName, tableName, false));

            // via redirection with table filter
            assertThat(env.executeTrino(
                    format("SELECT * FROM delta.information_schema.columns WHERE table_schema = '%s' AND table_name='%s'", schemaName, tableName)))
                    .containsOnly(
                            row("delta", schemaName, tableName, "id", 1L, null, "YES", "integer"),
                            row("delta", schemaName, tableName, "flag", 2L, null, "YES", "boolean"),
                            row("delta", schemaName, tableName, "rate", 3L, null, "YES", "tinyint"));

            // test via redirection with just schema filter
            assertThat(env.executeTrino(
                    format("SELECT * FROM delta.information_schema.columns WHERE table_schema = '%s'", schemaName)))
                    .containsOnly(
                            row("delta", schemaName, tableName, "id", 1L, null, "YES", "integer"),
                            row("delta", schemaName, tableName, "flag", 2L, null, "YES", "boolean"),
                            row("delta", schemaName, tableName, "rate", 3L, null, "YES", "tinyint"));

            // sanity check that getting columns info without redirection produces matching result
            assertThat(env.executeTrino(
                    format("SELECT * FROM hive.information_schema.columns WHERE table_schema = '%s' AND table_name='%s'", schemaName, tableName)))
                    .containsOnly(
                            row("hive", schemaName, tableName, "id", 1L, null, "YES", "integer"),
                            row("hive", schemaName, tableName, "flag", 2L, null, "YES", "boolean"),
                            row("hive", schemaName, tableName, "rate", 3L, null, "YES", "tinyint"));
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.%s.%s", schemaName, tableName));
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS hive." + schemaName);
        }
    }

    @Test
    void testSystemJdbcColumnsHiveToDeltaRedirect(HiveDeltaLakeMinioEnvironment env)
    {
        // use dedicated schema so we control the number and shape of tables
        String schemaName = "test_redirect_to_delta_system_jdbc_columns_schema_" + randomNameSuffix();
        String schemaLocation = format("s3://%s/delta-redirect-test-%s", env.getBucketName(), schemaName);
        env.executeTrinoUpdate(format("CREATE SCHEMA IF NOT EXISTS hive.%s WITH (location='%s')", schemaName, schemaLocation));

        String tableName = "test_redirect_to_delta_system_jdbc_columns_table_" + randomNameSuffix();
        try {
            env.executeSparkUpdate(createTableOnDelta(env, schemaName, tableName, false));

            // via redirection with table filter
            assertThat(env.executeTrino(
                    format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns  WHERE table_cat = 'hive' AND table_schem = '%s' AND table_name = '%s'", schemaName, tableName)))
                    .containsOnly(
                            row("hive", schemaName, tableName, "nationkey"),
                            row("hive", schemaName, tableName, "name"),
                            row("hive", schemaName, tableName, "regionkey"),
                            row("hive", schemaName, tableName, "comment"));

            // test via redirection with just schema filter
            // via redirection with table filter
            assertThat(env.executeTrino(
                    format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns  WHERE table_cat = 'hive' AND table_schem = '%s'", schemaName)))
                    .containsOnly(
                            row("hive", schemaName, tableName, "nationkey"),
                            row("hive", schemaName, tableName, "name"),
                            row("hive", schemaName, tableName, "regionkey"),
                            row("hive", schemaName, tableName, "comment"));

            // sanity check that getting columns info without redirection produces matching result
            assertThat(env.executeTrino(
                    format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns  WHERE table_cat = 'delta' AND table_schem = '%s' AND table_name = '%s'", schemaName, tableName)))
                    .containsOnly(
                            row("delta", schemaName, tableName, "nationkey"),
                            row("delta", schemaName, tableName, "name"),
                            row("delta", schemaName, tableName, "regionkey"),
                            row("delta", schemaName, tableName, "comment"));
        }
        finally {
            env.executeSparkUpdate(format("DROP TABLE IF EXISTS %s.%s", schemaName, tableName));
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS hive." + schemaName);
        }
    }

    @Test
    void testSystemJdbcColumnsDeltaToHiveRedirect(HiveDeltaLakeMinioEnvironment env)
    {
        // use dedicated schema so we control the number and shape of tables
        String schemaName = "test_redirect_to_hive_system_jdbc_columns_schema_" + randomNameSuffix();
        String schemaLocation = format("s3://%s/delta-redirect-test-%s", env.getBucketName(), schemaName);
        env.executeTrinoUpdate(format("CREATE SCHEMA IF NOT EXISTS hive.%s WITH (location='%s')", schemaName, schemaLocation));

        String tableName = "test_redirect_to_hive_system_jdbc_columns_table_" + randomNameSuffix();
        try {
            env.executeTrinoUpdate(createTableInHiveConnector(env, schemaName, tableName, false));

            // via redirection with table filter
            assertThat(env.executeTrino(
                    format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns  WHERE table_cat = 'delta' AND table_schem = '%s' AND table_name = '%s'", schemaName, tableName)))
                    .containsOnly(
                            row("delta", schemaName, tableName, "id"),
                            row("delta", schemaName, tableName, "flag"),
                            row("delta", schemaName, tableName, "rate"));

            // test via redirection with just schema filter
            assertThat(env.executeTrino(
                    format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns  WHERE table_cat = 'delta' AND table_schem = '%s'", schemaName)))
                    .containsOnly(
                            row("delta", schemaName, tableName, "id"),
                            row("delta", schemaName, tableName, "flag"),
                            row("delta", schemaName, tableName, "rate"));

            // sanity check that getting columns info without redirection produces matching result
            assertThat(env.executeTrino(
                    format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns  WHERE table_cat = 'hive' AND table_schem = '%s' AND table_name = '%s'", schemaName, tableName)))
                    .containsOnly(
                            row("hive", schemaName, tableName, "id"),
                            row("hive", schemaName, tableName, "flag"),
                            row("hive", schemaName, tableName, "rate"));
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.%s.%s", schemaName, tableName));
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS hive." + schemaName);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testViewReferencingHiveAndDeltaTable(boolean legacyHiveViewTranslation, HiveDeltaLakeMinioEnvironment env)
    {
        String hiveTableName = "test_view_hive_table_" + randomNameSuffix();
        String deltaTableName = "test_view_delta_table_" + randomNameSuffix();
        String viewName = "test_view_view_" + randomNameSuffix();
        String deltaRegionTableName = "test_view_delta_region_table_" + randomNameSuffix();

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
            env.executeTrinoUpdate("CREATE TABLE hive.default." + hiveTableName + " " +
                    "WITH (external_location = '" + locationForTable(env, hiveTableName) + "') " +
                    "AS TABLE tpch.tiny.region");
            env.executeSparkUpdate("" +
                    "CREATE TABLE " + deltaTableName + " USING DELTA " +
                    "LOCATION '" + locationForTable(env, deltaTableName) + "' " +
                    " AS " + deltaTableData);
            env.executeSparkUpdate("" +
                    "CREATE TABLE " + deltaRegionTableName + " USING DELTA " +
                    "LOCATION '" + locationForTable(env, deltaRegionTableName) + "' " +
                    " AS VALUES " +
                    "    (CAST(0 AS bigint), 'AFRICA'), " +
                    "    (CAST(1 AS bigint), 'AMERICA'), " +
                    "    (CAST(2 AS bigint), 'ASIA'), " +
                    "    (CAST(3 AS bigint), 'EUROPE'), " +
                    "    (CAST(4 AS bigint), 'MIDDLE EAST') AS data(regionkey, name)");
            env.executeSparkUpdate("CREATE VIEW " + viewName + " AS " +
                    "SELECT dt.*, regionkey, name " +
                    "FROM " + deltaTableName + " dt JOIN " + deltaRegionTableName + " ON an_integer = regionkey");

            List<Row> expected = List.of(
                    row(
                            true,
                            1,
                            1L,
                            1.0d,
                            1d,
                            new BigDecimal("13.1"),
                            new BigDecimal("123456789123456.123456789"),
                            "abc",
                            new byte[] {(byte) 0xAB, (byte) 0xCD},
                            Date.valueOf(LocalDate.of(2005, 9, 10)),
                            0, // delta table's a_last_column,
                            1L,
                            "AMERICA"));

            assertThat(env.executeSpark("SELECT * FROM " + viewName))
                    .containsOnly(expected);

            env.executeTrinoInSession(session -> {
                session.executeUpdate("SET SESSION hive.hive_views_legacy_translation = " + legacyHiveViewTranslation);
                assertThat(session.executeQuery("SELECT * FROM hive.default." + viewName))
                        .containsOnly(expected);
            });

            // Hive views are currently not supported in Delta Lake connector
            assertThatThrownBy(() -> env.executeTrino("SELECT * FROM delta.default." + viewName))
                    .hasMessageMatching(".*default." + viewName + " is not a Delta Lake table.*");
        }
        finally {
            env.executeSparkUpdate("DROP VIEW IF EXISTS " + viewName);
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + deltaTableName);
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + deltaRegionTableName);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + hiveTableName);
        }
    }

    @Test
    void testHiveToDeltaPropertiesRedirect(HiveDeltaLakeMinioEnvironment env)
    {
        String tableName = "test_redirect_to_delta_properties_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE " + tableName + " USING DELTA " +
                "LOCATION '" + locationForTable(env, tableName) + "' " +
                " AS SELECT true AS a_boolean");

        List<Row> expected = List.of(
                row("delta.minReaderVersion", "1"),
                row("delta.minWriterVersion", "2"));

        try {
            assertThat(env.executeTrino(format("SELECT * FROM delta.default.\"%s$properties\"", tableName))).containsOnly(expected);
            assertThat(env.executeTrino(format("SELECT * FROM hive.default.\"%s$properties\"", tableName))).containsOnly(expected);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    // ==================== Helper Methods ====================

    private String createTableOnDelta(HiveDeltaLakeMinioEnvironment env, String tableName, boolean partitioned)
    {
        return createTableOnDelta(env, "default", tableName, partitioned);
    }

    private String createTableOnDelta(HiveDeltaLakeMinioEnvironment env, String schema, String tableName, boolean partitioned)
    {
        return "CREATE TABLE " + schema + "." + tableName + " " +
                "USING DELTA " +
                (partitioned ? "PARTITIONED BY (regionkey) " : "") +
                "LOCATION '" + locationForTable(env, tableName) + "' " +
                " AS VALUES " +
                "(CAST(0 AS bigint), 'ALGERIA', CAST(0 AS bigint), 'haggle. carefully final deposits detect slyly agai')," +
                "(CAST(1 AS bigint), 'ARGENTINA', CAST(1 AS bigint), 'al foxes promise slyly according to the regular accounts. bold requests alon')," +
                "(CAST(2 AS bigint), 'BRAZIL', CAST(1 AS bigint), 'y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special')," +
                "(CAST(3 AS bigint), 'CANADA', CAST(1 AS bigint), 'eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold')" +
                " AS data(nationkey, name, regionkey, comment)";
    }

    private String createTableInHiveConnector(HiveDeltaLakeMinioEnvironment env, String schema, String tableName, boolean partitioned)
    {
        return "CREATE TABLE hive." + schema + "." + tableName +
                "(id, flag, rate) " +
                "WITH (" +
                "    external_location = '" + locationForTable(env, tableName) + "'" +
                (partitioned ? ", partitioned_by = ARRAY['rate']" : "") +
                ") AS VALUES " +
                "(1, BOOLEAN 'false', TINYINT '-128'), " +
                "(2, BOOLEAN 'true', TINYINT '127'), " +
                "(3, BOOLEAN 'false', TINYINT '0'), " +
                "(4, BOOLEAN 'false', TINYINT '1'), " +
                "(5, BOOLEAN 'true', TINYINT '37')";
    }

    private String locationForTable(HiveDeltaLakeMinioEnvironment env, String tableName)
    {
        return "s3://" + env.getBucketName() + "/hive-and-databricks-redirect-" + tableName;
    }

    private AbstractStringAssert<?> assertTableComment(HiveDeltaLakeMinioEnvironment env, String catalog, String schema, String tableName)
    {
        return assertThat((String) readTableComment(env, catalog, schema, tableName).getOnlyValue());
    }

    private QueryResult readTableComment(HiveDeltaLakeMinioEnvironment env, String catalog, String schema, String tableName)
    {
        try (Connection conn = env.createTrinoConnection();
                PreparedStatement stmt = conn.prepareStatement(
                        "SELECT comment FROM system.metadata.table_comments WHERE catalog_name = ? AND schema_name = ? AND table_name = ?")) {
            stmt.setString(1, catalog);
            stmt.setString(2, schema);
            stmt.setString(3, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                return QueryResult.forResultSet(rs);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to read table comment", e);
        }
    }

    private AbstractStringAssert<?> assertColumnComment(HiveDeltaLakeMinioEnvironment env, String catalog, String schema, String tableName, String columnName)
    {
        return assertThat((String) readColumnComment(env, catalog, schema, tableName, columnName).getOnlyValue());
    }

    private QueryResult readColumnComment(HiveDeltaLakeMinioEnvironment env, String catalog, String schema, String tableName, String columnName)
    {
        try (Connection conn = env.createTrinoConnection();
                PreparedStatement stmt = conn.prepareStatement(
                        "SELECT comment FROM " + catalog + ".information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?")) {
            stmt.setString(1, schema);
            stmt.setString(2, tableName);
            stmt.setString(3, columnName);
            try (ResultSet rs = stmt.executeQuery()) {
                return QueryResult.forResultSet(rs);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to read column comment", e);
        }
    }

    private static void assertResultsEqual(QueryResult first, QueryResult second)
    {
        List<Row> firstRows = first.rows().stream()
                .map(Row::fromList)
                .toList();
        List<Row> secondRows = second.rows().stream()
                .map(Row::fromList)
                .toList();

        assertThat(first).containsOnly(secondRows.toArray(new Row[0]));

        // just for symmetry
        assertThat(second).containsOnly(firstRows.toArray(new Row[0]));
    }
}
