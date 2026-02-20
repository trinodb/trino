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
import io.trino.testing.minio.MinioClient;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Delta Lake CREATE OR REPLACE TABLE AS SELECT compatibility with Spark/Delta OSS.
 * <p>
 * Ported from the Tempto-based TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility (DELTA_LAKE_OSS tests only).
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility
{
    @Test
    void testCreateOrReplaceTableOnDeltaWithSchemaChange(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_replace_table_with_schema_change_" + randomNameSuffix();

        env.executeTrinoUpdate("CREATE TABLE delta.default." + tableName + " (ts VARCHAR) " +
                "with (location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "', checkpoint_interval = 10)");
        try {
            List<Row> expectedRows = performInsert(env::executeSparkUpdate, tableName, 12);

            assertTransactionLogVersion(env, tableName, 12);
            env.executeSparkUpdate("CREATE OR REPLACE TABLE " + tableName + " USING DELTA AS SELECT CAST(ts AS TIMESTAMP) FROM " + tableName);
            assertThat(env.executeTrino("SELECT to_iso8601(ts) FROM delta.default." + tableName)).containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    void testCreateOrReplaceTableOnTrinoWithSchemaChange(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_replace_table_with_schema_change_" + randomNameSuffix();

        env.executeTrinoUpdate("CREATE TABLE delta.default." + tableName + " (ts VARCHAR) " +
                "with (location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "', checkpoint_interval = 10)");
        try {
            List<Row> expectedRows = performInsert(env::executeSparkUpdate, tableName, 12);

            assertTransactionLogVersion(env, tableName, 12);
            env.executeTrinoUpdate("CREATE OR REPLACE TABLE delta.default." + tableName + " AS SELECT CAST(ts AS TIMESTAMP(6)) as ts FROM delta.default." + tableName);
            assertThat(env.executeSpark("SELECT date_format(ts, \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\") FROM default." + tableName)).containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    void testCreateOrReplaceTableAndInsertOnTrinoWithSchemaChange(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_replace_table_and_insert_on_trino_with_schema_change_" + randomNameSuffix();

        env.executeTrinoUpdate("CREATE TABLE delta.default." + tableName + " (ts VARCHAR) " +
                "with (location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "', checkpoint_interval = 10)");
        try {
            List<Row> expectedRows = performInsert(env::executeTrinoUpdate, "delta.default." + tableName, 12);

            env.executeTrinoUpdate("CREATE OR REPLACE TABLE delta.default." + tableName + " AS SELECT CAST(ts AS TIMESTAMP(6)) as ts FROM delta.default." + tableName);
            assertThat(env.executeSpark("SELECT date_format(ts, \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\") FROM default." + tableName)).containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    void testCreateOrReplaceTableWithSchemaChangeOnCheckpoint(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_replace_table_with_schema_change_" + randomNameSuffix();
        env.executeTrinoUpdate("CREATE TABLE delta.default." + tableName + " (ts VARCHAR) " +
                "with (location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "', checkpoint_interval = 10)");
        try {
            ImmutableList.Builder<Row> expected = ImmutableList.builder();
            expected.addAll(performInsert(env::executeSparkUpdate, tableName, 9));

            env.executeSparkUpdate("CREATE OR REPLACE TABLE " + tableName + " USING DELTA AS SELECT CAST(ts AS TIMESTAMP) FROM " + tableName);
            assertLastEntryIsCheckpointed(env, tableName);

            env.executeSparkUpdate("INSERT INTO " + tableName + " VALUES \"1960-01-01 01:02:03\", \"1961-01-01 01:02:03\", \"1962-01-01 01:02:03\"");
            expected.add(row("1960-01-01T01:02:03.000Z"), row("1961-01-01T01:02:03.000Z"), row("1962-01-01T01:02:03.000Z"));
            assertTransactionLogVersion(env, tableName, 11);

            assertThat(env.executeTrino("SELECT to_iso8601(ts) FROM delta.default." + tableName)).containsOnly(expected.build());
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    void testCreateOrReplaceTableWithOnlyFeaturesChangedOnTrino(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_create_or_replace_table_discard_features_" + randomNameSuffix();

        env.executeTrinoUpdate("CREATE TABLE delta.default." + tableName + " (x INT) " +
                "WITH (location = 's3://" + env.getBucketName() + "/delta-lake-test-" + tableName + "', deletion_vectors_enabled = true)");
        try {
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES 1, 2, 3");

            env.executeTrinoUpdate("CREATE OR REPLACE TABLE delta.default." + tableName + " (x INT)");
            assertThat(env.executeSpark("SELECT * FROM " + tableName)).hasNoRows();
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + tableName);
        }
    }

    private static List<Row> performInsert(Consumer<String> sqlExecutor, String tableName, int numberOfRows)
    {
        ImmutableList.Builder<Row> expectedRowBuilder = ImmutableList.builder();
        // Write to the table until a checkpoint file is written
        for (int i = 0; i < numberOfRows; i++) {
            sqlExecutor.accept("INSERT INTO " + tableName + " VALUES '1960-01-01 01:02:03', '1961-01-01 01:02:03', '1962-01-01 01:02:03'");
            expectedRowBuilder.add(row("1960-01-01T01:02:03.000Z"), row("1961-01-01T01:02:03.000Z"), row("1962-01-01T01:02:03.000Z"));
        }
        return expectedRowBuilder.build();
    }

    private static void assertTransactionLogVersion(DeltaLakeMinioEnvironment env, String tableName, int versionNumber)
    {
        List<String> jsonEntries = listJsonLogEntries(env, tableName);
        assertThat(jsonEntries).isNotEmpty();
        String lastJsonEntry = jsonEntries.stream().max(String::compareTo).orElseThrow();
        assertThat(lastJsonEntry).isEqualTo(format("%020d.json", versionNumber));
    }

    private static void assertLastEntryIsCheckpointed(DeltaLakeMinioEnvironment env, String tableName)
    {
        List<String> jsonEntries = listJsonLogEntries(env, tableName);
        assertThat(jsonEntries).isNotEmpty();
        String lastJsonEntry = jsonEntries.stream().max(String::compareTo).orElseThrow();

        List<String> checkpointEntries = listCheckpointEntries(env, tableName);
        assertThat(checkpointEntries).isNotEmpty();
        String lastCheckpointEntry = checkpointEntries.stream().max(String::compareTo).orElseThrow();

        assertThat(lastJsonEntry.replace(".json", ""))
                .isEqualTo(lastCheckpointEntry.replace(".checkpoint.parquet", ""));
    }

    private static List<String> listJsonLogEntries(DeltaLakeMinioEnvironment env, String tableName)
    {
        return listLogEntries(env, tableName, ".json");
    }

    private static List<String> listCheckpointEntries(DeltaLakeMinioEnvironment env, String tableName)
    {
        return listLogEntries(env, tableName, ".checkpoint.parquet");
    }

    private static List<String> listLogEntries(DeltaLakeMinioEnvironment env, String tableName, String suffix)
    {
        String prefix = "databricks-compatibility-test-" + tableName + "/_delta_log/";
        try (MinioClient minioClient = env.createMinioClient()) {
            return minioClient.listObjects(env.getBucketName(), prefix).stream()
                    .map(path -> path.substring(path.lastIndexOf('/') + 1))
                    .filter(file -> file.endsWith(suffix))
                    .collect(toImmutableList());
        }
    }
}
