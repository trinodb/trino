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
import io.trino.testing.minio.MinioClient;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests Delta Lake checkpoint compatibility between Trino and Spark.
 * <p>
 * Ported from the Tempto-based TestDeltaLakeCheckpointsCompatibility (DELTA_LAKE_OSS tests).
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeCheckpointsCompatibility
{
    @Test
    void testSparkCanReadTrinoCheckpoint(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_checkpoints_compat_" + randomNameSuffix();
        String tableDirectory = "delta-compatibility-test-" + tableName;
        // using mixed case column names for extend test coverage

        env.executeSparkUpdate(format(
                "CREATE TABLE default.%s" +
                        "      (a_NuMbEr INT, a_StRiNg STRING)" +
                        "      USING delta" +
                        "      PARTITIONED BY (a_NuMbEr)" +
                        "      LOCATION 's3://%s/%s'",
                tableName, env.getBucketName(), tableDirectory));
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1,'ala'), (2, 'kota')");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (3, 'osla')");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (3, 'psa'), (4, 'bobra')");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (4, 'lwa'), (5, 'jeza')");
            env.executeSparkUpdate("DELETE FROM default." + tableName + " WHERE a_string = 'jeza'");
            env.executeTrinoUpdate("DELETE FROM delta.default." + tableName + " WHERE a_string = 'bobra'");

            List<Row> expectedRows = ImmutableList.of(
                    row(1, "ala"),
                    row(2, "kota"),
                    row(3, "osla"),
                    row(3, "psa"),
                    row(4, "lwa"));

            // sanity check
            assertThat(listCheckpointFiles(env, tableDirectory)).isEmpty();
            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            assertThat(env.executeSpark("SELECT * FROM default." + tableName + " WHERE a_string <> 'fill'"))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName + " WHERE a_string <> 'fill'"))
                    .containsOnly(expectedRows);

            // fill with inserts to trigger checkpoint
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (1, 'fill')");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (1, 'fill')");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (1, 'fill')");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (1, 'fill')");

            // check we can still query data
            assertThat(listCheckpointFiles(env, tableDirectory)).hasSize(1);
            assertThat(env.executeSpark("SELECT * FROM default." + tableName + " WHERE a_string <> 'fill'"))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName + " WHERE a_string <> 'fill'"))
                    .containsOnly(expectedRows);
        }
        finally {
            // cleanup
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testSparkCanReadTrinoCheckpointWithMultiplePartitionColumns(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_checkpoints_multi_part_compat_" + randomNameSuffix();
        try {
            env.executeTrinoUpdate(format(
                    "CREATE TABLE delta.default.%1$s (" +
                            "    data INT," +
                            "    part_boolean BOOLEAN," +
                            "    part_tinyint TINYINT," +
                            "    part_smallint SMALLINT," +
                            "    part_int INT," +
                            "    part_bigint BIGINT," +
                            "    part_decimal_5_2 DECIMAL(5,2)," +
                            "    part_decimal_21_3 DECIMAL(21,3)," +
                            "    part_double DOUBLE," +
                            "    part_float REAL," +
                            "    part_varchar VARCHAR," +
                            "    part_date DATE," +
                            "    part_timestamp TIMESTAMP(3) WITH TIME ZONE" +
                            ") " +
                            "WITH (" +
                            "    location = 's3://%2$s/databricks-compatibility-test-%1$s'," +
                            "    partitioned_by = ARRAY['part_boolean', 'part_tinyint', 'part_smallint', 'part_int', 'part_bigint', 'part_decimal_5_2', 'part_decimal_21_3', 'part_double', 'part_float', 'part_varchar', 'part_date', 'part_timestamp']," +
                            "    checkpoint_interval = 2" +
                            ")", tableName, env.getBucketName()));

            env.executeTrinoUpdate("" +
                    "INSERT INTO delta.default." + tableName +
                    " VALUES " +
                    "(" +
                    "   1, " +
                    "   true, " +
                    "   1, " +
                    "   10," +
                    "   100, " +
                    "   1000, " +
                    "   CAST('123.12' AS DECIMAL(5,2)), " +
                    "   CAST('123456789012345678.123' AS DECIMAL(21,3)), " +
                    "   DOUBLE '0', " +
                    "   REAL '0', " +
                    "   'a', " +
                    "   DATE '2020-08-21', " +
                    "   TIMESTAMP '2020-10-21 01:00:00.123 UTC'" +
                    ")");
            env.executeTrinoUpdate("" +
                    "INSERT INTO delta.default." + tableName +
                    " VALUES " +
                    "(" +
                    "   2, " +
                    "   true, " +
                    "   2, " +
                    "   20," +
                    "   200, " +
                    "   2000, " +
                    "   CAST('223.12' AS DECIMAL(5,2)), " +
                    "   CAST('223456789012345678.123' AS DECIMAL(21,3)), " +
                    "   DOUBLE '0', " +
                    "   REAL '0', " +
                    "   'b', " +
                    "   DATE '2020-08-22', " +
                    "   TIMESTAMP '2020-10-22 02:00:00.456 UTC'" +
                    ")");

            String selectValues = "SELECT " +
                    "data, part_boolean, part_tinyint, part_smallint, part_int, part_bigint, part_decimal_5_2, part_decimal_21_3, part_double , part_float, part_varchar, part_date " +
                    "FROM %s." + tableName;
            Row firstRow = row(1, true, 1, 10, 100, 1000L, new BigDecimal("123.12"), new BigDecimal("123456789012345678.123"), 0d, 0f, "a", Date.valueOf("2020-08-21"));
            Row secondRow = row(2, true, 2, 20, 200, 2000L, new BigDecimal("223.12"), new BigDecimal("223456789012345678.123"), 0d, 0f, "b", Date.valueOf("2020-08-22"));
            List<Row> expectedRows = ImmutableList.of(firstRow, secondRow);
            assertThat(env.executeSpark(format(selectValues, "default"))).containsOnly(expectedRows);
            // Make sure that the checkpoint is being processed
            env.executeTrinoUpdate("CALL delta.system.flush_metadata_cache(schema_name => 'default', table_name => '" + tableName + "')");
            assertThat(env.executeTrino(format(selectValues, "delta.default"))).containsOnly(expectedRows);
            QueryResult selectSparkTimestamps = env.executeSpark("SELECT date_format(part_timestamp, \"yyyy-MM-dd HH:mm:ss.SSS\") FROM default." + tableName);
            QueryResult selectTrinoTimestamps = env.executeTrino("SELECT format_datetime(part_timestamp, 'yyyy-MM-dd HH:mm:ss.SSS') FROM delta.default." + tableName);
            assertThat(selectSparkTimestamps).containsOnly(selectTrinoTimestamps.getRows().stream()
                    .map(row -> Row.fromList(row.getValues()))
                    .collect(toImmutableList()));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testTrinoUsesCheckpointInterval(DeltaLakeMinioEnvironment env)
    {
        trinoUsesCheckpointInterval(env, "'delta.checkpointInterval' = '5'");
    }

    @Test
    void testTrinoUsesCheckpointIntervalWithTableFeature(DeltaLakeMinioEnvironment env)
    {
        trinoUsesCheckpointInterval(env, "'delta.checkpointInterval' = '5', 'delta.feature.columnMapping'='supported'");
    }

    private void trinoUsesCheckpointInterval(DeltaLakeMinioEnvironment env, String deltaTableProperties)
    {
        String tableName = "test_dl_checkpoints_compat_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeSparkUpdate(format(
                "CREATE TABLE default.%s" +
                        "      (a_NuMbEr INT, a_StRiNg STRING)" +
                        "      USING delta" +
                        "      PARTITIONED BY (a_NuMbEr)" +
                        "      LOCATION 's3://%s/%s'" +
                        "      TBLPROPERTIES (%s)",
                tableName, env.getBucketName(), tableDirectory, deltaTableProperties));

        try {
            // validate that we can see the checkpoint interval
            assertThat((String) env.executeTrino("SHOW CREATE TABLE delta.default." + tableName).getOnlyValue())
                    .contains("checkpoint_interval = 5");

            // sanity check
            fillWithInserts(env, "delta.default." + tableName, "(1, 'trino')", 4);
            assertThat(listCheckpointFiles(env, tableDirectory)).isEmpty();
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName + " WHERE a_string <> 'trino'")).hasNoRows();

            // fill to first checkpoint using Trino
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (1, 'ala'), (2, 'kota')");
            assertThat(listCheckpointFiles(env, tableDirectory)).hasSize(1);

            // fill to next checkpoint using a mix of Trino and Spark
            fillWithInserts(env, "delta.default." + tableName, "(2, 'trino')", 3);
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (3, 'psa'), (4, 'bobra')");
            env.executeSparkUpdate("DELETE FROM default." + tableName + " WHERE a_string = 'trino'");

            env.executeSparkUpdate("ALTER TABLE default." + tableName + " SET TBLPROPERTIES ('delta.checkpointInterval' = '2')");
            // Starting with Databricks Runtime 8.4 checkpoint writing is dynamic rather than relying on a set interval
            int initialCheckpointCount = listCheckpointFiles(env, tableDirectory).size();

            fillWithInserts(env, "delta.default." + tableName, "(3, 'trino')", 4);
            assertThat(listCheckpointFiles(env, tableDirectory)).hasSize(initialCheckpointCount + 2);
        }
        finally {
            // cleanup
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testTrinoWriteStatsAsJsonDisabled(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_checkpoints_write_stats_as_json_disabled_trino_" + randomNameSuffix();
        testWriteStatsAsJsonDisabled(env, sql -> env.executeTrinoUpdate(sql), tableName, "delta.default." + tableName, 3.0, 1.0);
    }

    @Test
    void testSparkWriteStatsAsJsonDisabled(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_checkpoints_write_stats_as_json_disabled_spark_" + randomNameSuffix();
        testWriteStatsAsJsonDisabled(env, sql -> env.executeSparkUpdate(sql), tableName, "default." + tableName, null, null);
    }

    private void testWriteStatsAsJsonDisabled(DeltaLakeMinioEnvironment env, Consumer<String> sqlExecutor, String tableName, String qualifiedTableName, Double dataSize, Double distinctValues)
    {
        env.executeSparkUpdate(format(
                "CREATE TABLE default.%s" +
                        "(a_number INT, a_string STRING) " +
                        "USING DELTA " +
                        "PARTITIONED BY (a_number) " +
                        "LOCATION 's3://%s/databricks-compatibility-test-%1$s' " +
                        "TBLPROPERTIES (" +
                        " delta.checkpointInterval = 5, " +
                        " delta.checkpoint.writeStatsAsJson = false)",
                tableName, env.getBucketName()));

        try {
            sqlExecutor.accept("INSERT INTO " + qualifiedTableName + " VALUES (1,'ala')");

            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 1.0, 0.0, null, null, null),
                            row("a_string", dataSize, distinctValues, 0.0, null, null, null),
                            row(null, null, null, null, 1.0, null, null)));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testTrinoWriteStatsAsStructDisabled(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_checkpoints_write_stats_as_struct_disabled_trino_" + randomNameSuffix();
        testWriteStatsAsStructDisabled(env, sql -> env.executeTrinoUpdate(sql), tableName, "delta.default." + tableName);
    }

    @Test
    void testSparkWriteStatsAsStructDisabled(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_checkpoints_write_stats_as_struct_disabled_spark_" + randomNameSuffix();
        testWriteStatsAsStructDisabled(env, sql -> env.executeSparkUpdate(sql), tableName, "default." + tableName);
    }

    private void testWriteStatsAsStructDisabled(DeltaLakeMinioEnvironment env, Consumer<String> sqlExecutor, String tableName, String qualifiedTableName)
    {
        env.executeSparkUpdate(format(
                "CREATE TABLE default.%s" +
                        "(a_number INT, a_string STRING) " +
                        "USING DELTA " +
                        "PARTITIONED BY (a_number) " +
                        "LOCATION 's3://%s/databricks-compatibility-test-%1$s' " +
                        "TBLPROPERTIES (" +
                        " delta.checkpointInterval = 1, " +
                        " delta.checkpoint.writeStatsAsJson = false, " + // Disable json stats to avoid merging statistics with 'stats' field
                        " delta.checkpoint.writeStatsAsStruct = false)",
                tableName, env.getBucketName()));

        try {
            sqlExecutor.accept("INSERT INTO " + qualifiedTableName + " VALUES (1,'ala')");

            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, null, null, null, null, null),
                            row("a_string", null, null, null, null, null, null),
                            row(null, null, null, null, null, null, null)));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("testTrinoCheckpointWriteStatsAsJsonDataProvider")
    void testTrinoWriteStatsAsJsonEnabled(String type, String inputValue, Double dataSize, Double distinctValues, Double nullsFraction, Object statsValue, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_checkpoints_write_stats_as_json_enabled_trino_" + randomNameSuffix();
        testWriteStatsAsJsonEnabled(env, sql -> env.executeTrinoUpdate(sql), tableName, "delta.default." + tableName, type, inputValue, dataSize, distinctValues, nullsFraction, statsValue);
    }

    private void testWriteStatsAsJsonEnabled(DeltaLakeMinioEnvironment env, Consumer<String> sqlExecutor, String tableName, String qualifiedTableName, String type, String inputValue, Double dataSize, Double distinctValues, Double nullsFraction, Object statsValue)
    {
        String createTableSql = format(
                "CREATE TABLE default.%s" +
                        "(col %s) " +
                        "USING DELTA " +
                        "LOCATION 's3://%s/databricks-compatibility-test-%1$s' " +
                        "TBLPROPERTIES (" +
                        " delta.checkpointInterval = 2, " +
                        " delta.checkpoint.writeStatsAsJson = false, " +
                        " delta.checkpoint.writeStatsAsStruct = true)",
                tableName, type, env.getBucketName());

        env.executeSparkUpdate(createTableSql);

        try {
            sqlExecutor.accept("INSERT INTO " + qualifiedTableName + " SELECT " + inputValue);
            sqlExecutor.accept("INSERT INTO " + qualifiedTableName + " SELECT " + inputValue);

            // SET TBLPROPERTIES increments checkpoint
            env.executeSparkUpdate("" +
                    "ALTER TABLE default." + tableName + " SET TBLPROPERTIES (" +
                    "'delta.checkpoint.writeStatsAsJson' = true, " +
                    "'delta.checkpoint.writeStatsAsStruct' = false)");

            sqlExecutor.accept("INSERT INTO " + qualifiedTableName + " SELECT " + inputValue);

            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("col", dataSize, distinctValues, nullsFraction, null, statsValue, statsValue),
                            row(null, null, null, null, 3.0, null, null)));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    static Stream<Arguments> testTrinoCheckpointWriteStatsAsJsonDataProvider()
    {
        return Stream.of(
                Arguments.of("boolean", "true", null, 1.0, 0.0, null),
                Arguments.of("integer", "1", null, 1.0, 0.0, "1"),
                Arguments.of("tinyint", "2", null, 1.0, 0.0, "2"),
                Arguments.of("smallint", "3", null, 1.0, 0.0, "3"),
                Arguments.of("bigint", "1000", null, 1.0, 0.0, "1000"),
                Arguments.of("real", "0.1", null, 1.0, 0.0, "0.1"),
                Arguments.of("double", "1.0", null, 1.0, 0.0, "1.0"),
                Arguments.of("decimal(3,2)", "3.14", null, 1.0, 0.0, "3.14"),
                Arguments.of("decimal(30,1)", "12345", null, 1.0, 0.0, "12345.0"),
                Arguments.of("string", "'test'", 12.0, 1.0, 0.0, null),
                Arguments.of("binary", "X'65683F'", 9.0, 1.0, 0.0, null),
                Arguments.of("date", "date '2021-02-03'", null, 1.0, 0.0, "2021-02-03"),
                Arguments.of("timestamp", "timestamp '2001-08-22 11:04:05.321 UTC'", null, 1.0, 0.0, "2001-08-22 11:04:05.321 UTC"),
                Arguments.of("array<int>", "ARRAY[1]", null, null, null, null),
                Arguments.of("map<string,int>", "MAP(ARRAY['key1', 'key2'], ARRAY[1, 2])", null, null, null, null),
                Arguments.of("struct<x bigint>", "CAST(ROW(1) AS ROW(x BIGINT))", null, null, null, null));
    }

    @Test
    void testTrinoWriteStatsAsStructEnabled(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_checkpoints_write_stats_as_struct_enabled_trino_" + randomNameSuffix();
        testWriteStatsAsStructEnabled(env, sql -> env.executeTrinoUpdate(sql), tableName, "delta.default." + tableName, 3.0, 1.0);
    }

    private void testWriteStatsAsStructEnabled(DeltaLakeMinioEnvironment env, Consumer<String> sqlExecutor, String tableName, String qualifiedTableName, Double dataSize, Double distinctValues)
    {
        env.executeSparkUpdate(format(
                "CREATE TABLE default.%s" +
                        "(a_number INT, a_string STRING) " +
                        "USING DELTA " +
                        "PARTITIONED BY (a_number) " +
                        "LOCATION 's3://%s/databricks-compatibility-test-%1$s' " +
                        "TBLPROPERTIES (" +
                        " delta.checkpointInterval = 1, " +
                        " delta.checkpoint.writeStatsAsJson = false, " +
                        " delta.checkpoint.writeStatsAsStruct = true)",
                tableName, env.getBucketName()));

        try {
            sqlExecutor.accept("INSERT INTO " + qualifiedTableName + " VALUES (1,'ala')");

            assertThat(env.executeTrino("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 1.0, 0.0, null, null, null),
                            row("a_string", dataSize, distinctValues, 0.0, null, null, null),
                            row(null, null, null, null, 1.0, null, null)));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"json", "parquet"})
    void testV2CheckpointMultipleSidecars(String format, DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_v2_checkpoint_multiple_sidecars_" + randomNameSuffix();
        String tableDirectory = "delta-compatibility-test-" + tableName;

        env.executeSparkUpdate("SET spark.databricks.delta.checkpointV2.topLevelFileFormat = " + format);
        env.executeSparkUpdate("SET spark.databricks.delta.checkpoint.partSize = 1");

        env.executeSparkUpdate("CREATE TABLE default." + tableName +
                "(id INT, part STRING)" +
                "USING delta " +
                "PARTITIONED BY (part)" +
                "LOCATION 's3://" + env.getBucketName() + "/" + tableDirectory + "'" +
                "TBLPROPERTIES ('delta.checkpointPolicy' = 'v2', 'delta.checkpointInterval' = '1')");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 'part1'), (2, 'part2')");

            List<Row> expectedRows = ImmutableList.of(row(1, "part1"), row(2, "part2"));
            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            assertThat(listSidecarFiles(env, tableDirectory)).isNotEmpty();
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    private void fillWithInserts(DeltaLakeMinioEnvironment env, String tableName, String values, int toCreate)
    {
        for (int i = 0; i < toCreate; i++) {
            assertThat(env.executeTrinoUpdate(format("INSERT INTO %s VALUES %s", tableName, values)))
                    .isEqualTo(1);
        }
    }

    private List<String> listCheckpointFiles(DeltaLakeMinioEnvironment env, String tableDirectory)
    {
        List<String> allFiles = listS3Directory(env, tableDirectory + "/_delta_log");
        return allFiles.stream()
                .filter(path -> path.contains("checkpoint.parquet"))
                .collect(toImmutableList());
    }

    private List<String> listSidecarFiles(DeltaLakeMinioEnvironment env, String tableDirectory)
    {
        return listS3Directory(env, tableDirectory + "/_delta_log/_sidecars");
    }

    private List<String> listS3Directory(DeltaLakeMinioEnvironment env, String directory)
    {
        try (MinioClient minioClient = env.createMinioClient()) {
            return minioClient.listObjects(env.getBucketName(), directory);
        }
    }
}
