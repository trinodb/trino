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
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Delta Lake insert compatibility with Spark/Delta OSS.
 * <p>
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeInsertCompatibility
{
    @Test
    void testInsertCompatibility(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_insert_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "         (a_number INT, a_string STRING)" +
                "         USING delta " +
                "         LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1,'ala'), (2, 'kota')");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (3, 'psa')");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (4, 'lwa'), (5, 'jeza')");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (6, 'wilka')");

            List<Row> expectedRows = ImmutableList.of(
                    row(1, "ala"),
                    row(2, "kota"),
                    row(3, "psa"),
                    row(4, "lwa"),
                    row(5, "jeza"),
                    row(6, "wilka"));

            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testTimestampInsertCompatibility(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_timestamp_ntz_insert_" + randomNameSuffix();

        env.executeTrinoUpdate("" +
                "CREATE TABLE delta.default." + tableName +
                "(id INT, ts TIMESTAMP(6))" +
                "WITH (location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "')");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES" +
                    "(1, TIMESTAMP '0001-01-01 00:00:00.000')," +
                    "(2, TIMESTAMP '2023-01-02 01:02:03.999')");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES" +
                    "(3, TIMESTAMP '2023-03-04 01:02:03.999')," +
                    "(4, TIMESTAMP '9999-12-31 23:59:59.999')");

            List<Row> expected = ImmutableList.<Row>builder()
                    .add(row(1, "0001-01-01 00:00:00.000"))
                    .add(row(2, "2023-01-02 01:02:03.999"))
                    .add(row(3, "2023-03-04 01:02:03.999"))
                    .add(row(4, "9999-12-31 23:59:59.999"))
                    .build();
            assertThat(env.executeSpark("SELECT id, date_format(ts, \"yyyy-MM-dd HH:mm:ss.SSS\") FROM default." + tableName))
                    .containsOnly(expected);
            assertThat(env.executeTrino("SELECT id, format_datetime(ts, 'yyyy-MM-dd HH:mm:ss.SSS') FROM delta.default." + tableName))
                    .containsOnly(expected);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE delta.default." + tableName);
        }
    }

    @Test
    void testTimestampWithTimeZonePartitionedInsertCompatibility(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_timestamp_tz_partitioned_insert_" + randomNameSuffix();

        env.executeTrinoUpdate("" +
                "CREATE TABLE delta.default." + tableName +
                "(id INT, part TIMESTAMP WITH TIME ZONE)" +
                "WITH (partitioned_by = ARRAY['part'], location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "')");
        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES" +
                    "(1, TIMESTAMP '0001-01-01 00:00:00.000 UTC')," +
                    "(2, TIMESTAMP '2023-01-02 01:02:03.999 +01:00')");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES" +
                    "(3, TIMESTAMP '2023-03-04 01:02:03.999 -01:00')," +
                    "(4, TIMESTAMP '9999-12-31 23:59:59.999 UTC')");

            List<Row> expectedRows = ImmutableList.<Row>builder()
                    .add(row(1, "0001-01-01 00:00:00.000"))
                    .add(row(2, "2023-01-02 00:02:03.999"))
                    .add(row(3, "2023-03-04 02:02:03.999"))
                    .add(row(4, "9999-12-31 23:59:59.999"))
                    .build();

            assertThat(env.executeSpark("SELECT id, date_format(part, \"yyyy-MM-dd HH:mm:ss.SSS\") FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT id, format_datetime(part, 'yyyy-MM-dd HH:mm:ss.SSS') FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            assertThat((String) env.executeTrino("SELECT \"$path\" FROM delta.default." + tableName + " WHERE id = 1").getOnlyValue())
                    .contains("/part=0001-01-01 00%3A00%3A00/");
            assertThat((String) env.executeTrino("SELECT \"$path\" FROM delta.default." + tableName + " WHERE id = 2").getOnlyValue())
                    .contains("/part=2023-01-02 00%3A02%3A03.999/");
            assertThat((String) env.executeTrino("SELECT \"$path\" FROM delta.default." + tableName + " WHERE id = 3").getOnlyValue())
                    .contains("/part=2023-03-04 02%3A02%3A03.999/");
            assertThat((String) env.executeTrino("SELECT \"$path\" FROM delta.default." + tableName + " WHERE id = 4").getOnlyValue())
                    .contains("/part=9999-12-31 23%3A59%3A59.999/");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE delta.default." + tableName);
        }
    }

    @Test
    void testTrinoPartitionedDifferentOrderInsertCompatibility(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_trino_partitioned_different_order_insert_" + randomNameSuffix();

        env.executeTrinoUpdate("" +
                "CREATE TABLE delta.default." + tableName +
                "         (a_number INT, first VARCHAR, second VARCHAR)" +
                "         WITH (" +
                "         partitioned_by = ARRAY['second', 'first']," +
                "         location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "')");

        try {
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (1, 'first', 'second')");

            List<Row> expectedRows = ImmutableList.of(row(1, "first", "second"));
            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE delta.default." + tableName);
        }
    }

    @Test
    void testDeltaPartitionedDifferentOrderInsertCompatibility(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_delta_partitioned_different_order_insert_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "         (a_number INT, first STRING, second STRING)" +
                "         USING delta " +
                "         PARTITIONED BY (second, first)" +
                "         LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 'first', 'second')");

            List<Row> expectedRows = ImmutableList.of(row(1, "first", "second"));
            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testInsertNonLowercaseColumnsCompatibility(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_insert_nonlowercase_columns_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "         (lower_case_string STRING, UPPER_CASE_STRING STRING, MiXeD_CaSe_StRiNg STRING)" +
                "         USING delta " +
                "         LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('databricks', 'DATABRICKS', 'DaTaBrIcKs'), ('databricks', 'DATABRICKS', NULL)");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (NULL, NULL, 'DaTaBrIcKs'), (NULL, NULL, NULL)");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES ('trino', 'TRINO', 'TrInO'), ('trino', 'TRINO', NULL)");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (NULL, NULL, 'TrInO'), (NULL, NULL, NULL)");

            List<Row> expectedRows = ImmutableList.of(
                    row("databricks", "DATABRICKS", "DaTaBrIcKs"),
                    row("databricks", "DATABRICKS", null),
                    row(null, null, "DaTaBrIcKs"),
                    row(null, null, null),
                    row("trino", "TRINO", "TrInO"),
                    row("trino", "TRINO", null),
                    row(null, null, "TrInO"),
                    row(null, null, null));

            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testInsertNestedNonLowercaseColumnsCompatibility(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_insert_nested_nonlowercase_columns_" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "         (an_int INT, nested STRUCT<lower_case_string: STRING, UPPER_CASE_STRING: STRING, MiXeD_CaSe_StRiNg: STRING>)" +
                "         USING delta " +
                "         LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, struct('databricks', 'DATABRICKS', 'DaTaBrIcKs')), (2, struct('databricks', 'DATABRICKS', NULL))");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES " +
                    "(3, struct(NULL, NULL, 'DaTaBrIcKs'))," +
                    "(4, struct(NULL, NULL, NULL))");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (10, ROW('trino', 'TRINO', 'TrInO')), (20, ROW('trino', 'TRINO', NULL))");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (30, ROW(NULL, NULL, 'TrInO')), (40, ROW(NULL, NULL, NULL))");

            List<Row> expectedRows = ImmutableList.of(
                    row(1, "databricks", "DATABRICKS", "DaTaBrIcKs"),
                    row(2, "databricks", "DATABRICKS", null),
                    row(3, null, null, "DaTaBrIcKs"),
                    row(4, null, null, null),
                    row(10, "trino", "TRINO", "TrInO"),
                    row(20, "trino", "TRINO", null),
                    row(30, null, null, "TrInO"),
                    row(40, null, null, null));

            assertThat(env.executeSpark("SELECT an_int, nested.lower_case_string, nested.UPPER_CASE_STRING, nested.MiXeD_CaSe_StRiNg FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT an_int, nested.lower_case_string, nested.UPPER_CASE_STRING, nested.MiXeD_CaSe_StRiNg FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testPartitionedInsertNonLowercaseColumnsCompatibility(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_partitioned_insert_nonlowercase_columns" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "         (lower_case_string STRING, UPPER_CASE_STRING STRING, MiXeD_CaSe_StRiNg STRING)" +
                "         USING delta " +
                "         PARTITIONED BY (MiXeD_CaSe_StRiNg)" +
                "         LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES ('databricks', 'DATABRICKS', 'DaTaBrIcKs'), ('databricks', 'DATABRICKS', NULL)");
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (NULL, NULL, 'DaTaBrIcKs'), (NULL, NULL, NULL)");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES ('trino', 'TRINO', 'TrInO'), ('trino', 'TRINO', NULL)");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES (NULL, NULL, 'TrInO'), (NULL, NULL, NULL)");

            List<Row> expectedRows = ImmutableList.of(
                    row("databricks", "DATABRICKS", "DaTaBrIcKs"),
                    row("databricks", "DATABRICKS", null),
                    row(null, null, "DaTaBrIcKs"),
                    row(null, null, null),
                    row("trino", "TRINO", "TrInO"),
                    row("trino", "TRINO", null),
                    row(null, null, "TrInO"),
                    row(null, null, null));

            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void verifyCompressionCodecsDataProvider(DeltaLakeMinioEnvironment env)
    {
        assertThat(env.executeTrino("SHOW SESSION LIKE 'delta.compression_codec'"))
                .containsOnly(row(
                        "delta.compression_codec",
                        "ZSTD",
                        "ZSTD",
                        "varchar",
                        "Compression codec to use when writing new data files. Possible values: " +
                                Stream.of(compressionCodecs())
                                        .map(arguments -> (String) getOnlyElement(asList(arguments)))
                                        .collect(toImmutableList())));
    }

    private Object[][] compressionCodecs()
    {
        return new Object[][] {
                {"NONE"},
                {"SNAPPY"},
                {"LZ4"},
                {"ZSTD"},
                {"GZIP"},
        };
    }
}
