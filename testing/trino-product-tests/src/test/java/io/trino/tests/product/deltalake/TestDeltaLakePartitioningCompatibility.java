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
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;

/**
 * Tests for Delta Lake partitioning compatibility with Spark/Delta OSS.
 * <p>
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakePartitioningCompatibility
{
    @Test
    void testSparkCanReadFromCtasTableCreatedByTrinoWithSpecialCharactersInPartitioningColumn(DeltaLakeMinioEnvironment env)
    {
        testSparkCanReadFromCtasTableCreatedByTrinoWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(1, env);
        testSparkCanReadFromCtasTableCreatedByTrinoWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(20, env);
    }

    private void testSparkCanReadFromCtasTableCreatedByTrinoWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(int interval, DeltaLakeMinioEnvironment env)
    {
        String tableName = format("test_dl_create_table_partition_by_special_char_with_%d_partitions_%s", interval, randomNameSuffix());
        String tableDirectory = "delta-compatibility-test-" + tableName;

        List<Row> expected = ImmutableList.of(
                row(1, "with-hyphen"),
                row(2, "with.dot"),
                row(3, "with:colon"),
                row(4, "with/slash"),
                row(5, "with\\\\backslash"),
                row(6, "with=equal"),
                row(7, "with?question"),
                row(8, "with!exclamation"),
                row(9, "with%percent"),
                row(10, "with space"));

        env.executeTrinoUpdate(format("CREATE TABLE delta.default.%s (id, col_name)" +
                        "WITH(location = 's3://%s/%s', partitioned_by = ARRAY['col_name'], checkpoint_interval = %s) " +
                        "AS VALUES " +
                        "(1, 'with-hyphen')," +
                        "(2, 'with.dot')," +
                        "(3, 'with:colon')," +
                        "(4, 'with/slash')," +
                        "(5, 'with\\\\backslash')," +
                        "(6, 'with=equal')," +
                        "(7, 'with?question')," +
                        "(8, 'with!exclamation')," +
                        "(9, 'with%%percent')," +
                        "(10, 'with space')",
                tableName,
                env.getBucketName(),
                tableDirectory,
                interval));

        try {
            assertThat(env.executeSpark("SELECT * FROM " + tableName)).containsOnly(expected);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(expected);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testTrinoCanReadFromCtasTableCreatedBySparkWithSpecialCharactersInPartitioningColumn(DeltaLakeMinioEnvironment env)
    {
        testTrinoCanReadFromCtasTableCreatedBySparkWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(1, env);
        testTrinoCanReadFromCtasTableCreatedBySparkWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(20, env);
    }

    private void testTrinoCanReadFromCtasTableCreatedBySparkWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(int interval, DeltaLakeMinioEnvironment env)
    {
        String tableName = format("test_dl_create_table_partition_by_special_char_with_%d_partitions_%s", interval, randomNameSuffix());
        String tableDirectory = "delta-compatibility-test-" + tableName;

        List<Row> expected = ImmutableList.of(
                row(1, "with-hyphen"),
                row(2, "with.dot"),
                row(3, "with:colon"),
                row(4, "with/slash"),
                row(5, "with\\backslash"),
                row(6, "with=equal"),
                row(7, "with?question"),
                row(8, "with!exclamation"),
                row(9, "with%percent"),
                row(10, "with space"));

        env.executeSparkUpdate(format("CREATE TABLE default.%s " +
                        "USING DELTA " +
                        "OPTIONS (checkpointInterval = %s) " +
                        "PARTITIONED BY (`col_name`) LOCATION 's3://%s/%s' AS " +
                        "SELECT * FROM (VALUES " +
                        "(1, 'with-hyphen')," +
                        "(2, 'with.dot')," +
                        "(3, 'with:colon')," +
                        "(4, 'with/slash')," +
                        "(5, 'with\\\\backslash')," +
                        "(6, 'with=equal')," +
                        "(7, 'with?question')," +
                        "(8, 'with!exclamation')," +
                        "(9, 'with%%percent')," +
                        "(10, 'with space')" +
                        ") t(id, col_name)",
                tableName,
                interval,
                env.getBucketName(),
                tableDirectory));

        try {
            assertThat(env.executeSpark("SELECT * FROM " + tableName)).containsOnly(expected);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(expected);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testSparkCanReadTableCreatedByTrinoWithSpecialCharactersInPartitioningColumn(DeltaLakeMinioEnvironment env)
    {
        testSparkCanReadTableCreatedByTrinoWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(1, env);
        testSparkCanReadTableCreatedByTrinoWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(20, env);
    }

    private void testSparkCanReadTableCreatedByTrinoWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(int interval, DeltaLakeMinioEnvironment env)
    {
        String tableName = format("test_dl_create_table_partition_by_special_char_with_%d_partitions_%s", interval, randomNameSuffix());
        String tableDirectory = "delta-compatibility-test-" + tableName;

        List<Row> expected = ImmutableList.of(
                row(1, "with-hyphen"),
                row(2, "with.dot"),
                row(3, "with:colon"),
                row(4, "with/slash"),
                row(5, "with\\\\backslash"),
                row(6, "with=equal"),
                row(7, "with?question"),
                row(8, "with!exclamation"),
                row(9, "with%percent"),
                row(10, "with space"));

        env.executeTrinoUpdate(format("CREATE TABLE delta.default.%s (id INTEGER, col_name VARCHAR) " +
                        "WITH(location = 's3://%s/%s', partitioned_by = ARRAY['col_name'], checkpoint_interval = %s) ",
                tableName,
                env.getBucketName(),
                tableDirectory,
                interval));

        try {
            env.executeTrinoUpdate(format("INSERT INTO delta.default.%s " +
                            "VALUES" +
                            "(1, 'with-hyphen'), " +
                            "(2, 'with.dot'), " +
                            "(3, 'with:colon'), " +
                            "(4, 'with/slash'), " +
                            "(5, 'with\\\\backslash'), " +
                            "(6, 'with=equal'), " +
                            "(7, 'with?question'), " +
                            "(8, 'with!exclamation'), " +
                            "(9, 'with%%percent')," +
                            "(10, 'with space')",
                    tableName));
            assertThat(env.executeSpark("SELECT * FROM " + tableName)).containsOnly(expected);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(expected);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testTrinoCanReadTableCreatedBySparkWithSpecialCharactersInPartitioningColumn(DeltaLakeMinioEnvironment env)
    {
        testTrinoCanReadTableCreatedBySparkWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(1, env);
        testTrinoCanReadTableCreatedBySparkWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(20, env);
    }

    private void testTrinoCanReadTableCreatedBySparkWithSpecialCharactersInPartitioningColumnWithCpIntervalSet(int interval, DeltaLakeMinioEnvironment env)
    {
        String tableName = format("test_dl_create_table_partition_by_special_char_with_%d_partitions_%s", interval, randomNameSuffix());
        String tableDirectory = "delta-compatibility-test-" + tableName;

        List<Row> expected = ImmutableList.of(
                row(1, "with-hyphen"),
                row(2, "with.dot"),
                row(3, "with:colon"),
                row(4, "with/slash"),
                row(5, "with\\backslash"),
                row(6, "with=equal"),
                row(7, "with?question"),
                row(8, "with!exclamation"),
                row(9, "with%percent"),
                row(10, "with space"));

        env.executeSparkUpdate(format("CREATE TABLE default.%s (id INTEGER, col_name STRING) " +
                        "USING DELTA " +
                        "OPTIONS (checkpointInterval = %s) " +
                        "PARTITIONED BY (`col_name`) LOCATION 's3://%s/%s'",
                tableName,
                interval,
                env.getBucketName(),
                tableDirectory));

        try {
            env.executeSparkUpdate(format("INSERT INTO default.%s " +
                            "VALUES" +
                            "(1, 'with-hyphen'), " +
                            "(2, 'with.dot'), " +
                            "(3, 'with:colon'), " +
                            "(4, 'with/slash'), " +
                            "(5, 'with\\\\backslash'), " +
                            "(6, 'with=equal'), " +
                            "(7, 'with?question'), " +
                            "(8, 'with!exclamation'), " +
                            "(9, 'with%%percent')," +
                            "(10, 'with space')",
                    tableName));

            assertThat(env.executeSpark("SELECT * FROM " + tableName)).containsOnly(expected);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(expected);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testSparkCanReadFromTableUpdatedByTrino(DeltaLakeMinioEnvironment env)
    {
        testSparkCanReadFromTableUpdatedByTrinoWithCpIntervalSet(1, env);
        testSparkCanReadFromTableUpdatedByTrinoWithCpIntervalSet(20, env);
    }

    private void testSparkCanReadFromTableUpdatedByTrinoWithCpIntervalSet(int interval, DeltaLakeMinioEnvironment env)
    {
        String tableName = format("test_dl_create_table_partition_by_special_char_with_%d_partitions_%s", interval, randomNameSuffix());
        String tableDirectory = "delta-compatibility-test-" + tableName;

        List<Row> expected = ImmutableList.of(
                row(101, "with-hyphen"),
                row(102, "with.dot"),
                row(103, "with:colon"),
                row(104, "with/slash"),
                row(105, "with\\\\backslash"),
                row(106, "with=equal"),
                row(107, "with?question"),
                row(108, "with!exclamation"),
                row(109, "with%percent"),
                row(110, "with space"));

        env.executeTrinoUpdate(format("CREATE TABLE delta.default.%s (id, col_name) " +
                        "WITH(location = 's3://%s/%s', partitioned_by = ARRAY['col_name'], checkpoint_interval = %s) " +
                        "AS VALUES " +
                        "(1, 'with-hyphen')," +
                        "(2, 'with.dot')," +
                        "(3, 'with:colon')," +
                        "(4, 'with/slash')," +
                        "(5, 'with\\\\backslash')," +
                        "(6, 'with=equal')," +
                        "(7, 'with?question')," +
                        "(8, 'with!exclamation')," +
                        "(9, 'with%%percent')," +
                        "(10, 'with space')",
                tableName,
                env.getBucketName(),
                tableDirectory,
                interval));

        try {
            env.executeTrinoUpdate(format("UPDATE delta.default.%s SET id = id + 100", tableName));

            assertThat(env.executeSpark("SELECT * FROM " + tableName)).containsOnly(expected);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(expected);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testTrinoCanReadFromTableUpdatedBySpark(DeltaLakeMinioEnvironment env)
    {
        testTrinoCanReadFromTableUpdatedBySparkWithCpIntervalSet(1, env);
        testTrinoCanReadFromTableUpdatedBySparkWithCpIntervalSet(20, env);
    }

    private void testTrinoCanReadFromTableUpdatedBySparkWithCpIntervalSet(int interval, DeltaLakeMinioEnvironment env)
    {
        String tableName = format("test_dl_create_table_partition_by_special_char_with_%d_partitions_%s", interval, randomNameSuffix());
        String tableDirectory = "delta-compatibility-test-" + tableName;

        List<Row> expected = ImmutableList.of(
                row(101, "with-hyphen"),
                row(102, "with.dot"),
                row(103, "with:colon"),
                row(104, "with/slash"),
                row(105, "with\\backslash"),
                row(106, "with=equal"),
                row(107, "with?question"),
                row(108, "with!exclamation"),
                row(109, "with%percent"),
                row(110, "with space"));

        env.executeSparkUpdate(format("CREATE TABLE default.%s " +
                        "USING DELTA " +
                        "OPTIONS (checkpointInterval = %s) " +
                        "PARTITIONED BY (`col_name`) LOCATION 's3://%s/%s' AS " +
                        "SELECT * FROM (VALUES " +
                        "(1, 'with-hyphen')," +
                        "(2, 'with.dot')," +
                        "(3, 'with:colon')," +
                        "(4, 'with/slash')," +
                        "(5, 'with\\\\backslash')," +
                        "(6, 'with=equal')," +
                        "(7, 'with?question')," +
                        "(8, 'with!exclamation')," +
                        "(9, 'with%%percent')," +
                        "(10, 'with space')" +
                        ") t(id, col_name)",
                tableName,
                interval,
                env.getBucketName(),
                tableDirectory));

        try {
            env.executeSparkUpdate(format("UPDATE default.%s SET id = id + 100", tableName));

            assertThat(env.executeSpark("SELECT * FROM " + tableName)).containsOnly(expected);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(expected);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testTrinoCanReadFromTablePartitionChangedBySpark(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_create_table_partition_changed_by_spark_" + randomNameSuffix();
        String tableDirectory = "delta-compatibility-test-" + tableName;

        List<Row> expected = ImmutableList.of(row(1, "part"));

        env.executeSparkUpdate(format("CREATE TABLE default.%s " +
                        "USING DELTA " +
                        "PARTITIONED BY (`original_part_col`) LOCATION 's3://%s/%s' AS " +
                        "SELECT 1 AS original_part_col, 'part' AS new_part_col",
                tableName,
                env.getBucketName(),
                tableDirectory));

        try {
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(expected);

            env.executeSparkUpdate("REPLACE TABLE default." + tableName + " USING DELTA PARTITIONED BY (new_part_col) AS SELECT * FROM " + tableName);

            // This 2nd SELECT query caused NPE when the connector had cache for partitions and the column was changed remotely
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(expected);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testPartitionedByNonLowercaseColumn(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_partitioned_by_non_lowercase_" + randomNameSuffix();
        String tableDirectory = "delta-compatibility-test-" + tableName;

        env.executeSparkUpdate(format("CREATE TABLE default.%s " +
                        "USING DELTA " +
                        "PARTITIONED BY (`PART`) LOCATION 's3://%s/%s' AS " +
                        "SELECT 1 AS data, 2 AS `PART`",
                tableName,
                env.getBucketName(),
                tableDirectory));
        try {
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(row(1, 2));

            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (3, 4)");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(row(1, 2), row(3, 4));

            env.executeTrinoUpdate("DELETE FROM delta.default." + tableName + " WHERE data = 3");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(row(1, 2));

            env.executeTrinoUpdate("UPDATE delta.default." + tableName + " SET part = 20");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).containsOnly(row(1, 20));

            env.executeTrinoUpdate("MERGE INTO delta.default." + tableName + " USING (SELECT 1 a) input ON true WHEN MATCHED THEN DELETE");
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName)).hasNoRows();
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }
}
