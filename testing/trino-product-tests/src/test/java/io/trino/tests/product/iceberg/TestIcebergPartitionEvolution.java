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
package io.trino.tests.product.iceberg;

import io.trino.jdbc.Row;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Iceberg partition evolution.
 * <p>
 * Ported from the Tempto-based TestIcebergPartitionEvolution.
 */
@ProductTest
@RequiresEnvironment(SparkIcebergEnvironment.class)
@TestGroup.Iceberg
class TestIcebergPartitionEvolution
{
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDroppedPartitionField(boolean dropFirst, SparkIcebergEnvironment env)
    {
        String tableName = "test_dropped_partition_field_" + randomNameSuffix();
        String trinoTable = "iceberg.default." + tableName;
        String sparkTable = "iceberg_test.default." + tableName;

        try {
            env.executeTrinoUpdate("CREATE TABLE " + trinoTable + "(a varchar, b varchar, c varchar) WITH (format_version = 1, partitioning = ARRAY['a','b'])");
            env.executeTrinoUpdate("INSERT INTO " + trinoTable + " VALUES " +
                    "('one', 'small', 'snake')," +
                    "('one', 'small', 'rabbit')," +
                    "('one', 'big', 'rabbit')," +
                    "('another', 'small', 'snake')," +
                    "('something', 'completely', 'else')," +
                    "(NULL, NULL, 'nothing')");

            // Use Spark to drop partition field
            env.executeSparkUpdate("ALTER TABLE " + sparkTable + " DROP PARTITION FIELD " + (dropFirst ? "a" : "b"));

            // Verify SHOW CREATE TABLE has correct partitioning (using regex like original)
            assertThat((String) env.executeTrino("SHOW CREATE TABLE " + trinoTable).getOnlyValue())
                    .matches(
                            "\\QCREATE TABLE iceberg.default." + tableName + " (\n" +
                                    "   a varchar,\n" +
                                    "   b varchar,\n" +
                                    "   c varchar\n" +
                                    ")\n" +
                                    "WITH (\n" +
                                    "   format = 'PARQUET',\n" +
                                    "   format_version = 1,\n" +
                                    "   location = 'hdfs://hadoop-master:9000/user/hive/warehouse/" + tableName + "-\\E.*\\Q',\n" +
                                    "   partitioning = ARRAY[" + (dropFirst ? "'void(a)','b'" : "'a','void(b)'") + "]\n" +
                                    ")\\E");

            // Verify data is intact
            assertThat(env.executeTrino("SELECT * FROM " + trinoTable))
                    .containsOnly(
                            row("one", "small", "snake"),
                            row("one", "small", "rabbit"),
                            row("one", "big", "rabbit"),
                            row("another", "small", "snake"),
                            row("something", "completely", "else"),
                            row(null, null, "nothing"));

            // Verify SHOW STATS
            assertThat(env.executeTrino("SHOW STATS FOR " + trinoTable))
                    .containsOnly(
                            row("a", 599.0, 3.0, 1. / 6, null, null, null),
                            row("b", 602.0, 3.0, 1. / 6, null, null, null),
                            row("c", 585.0, 4.0, 0., null, null, null),
                            row(null, null, null, null, 6., null, null));

            // Verify $partitions table schema (query iceberg catalog's information_schema)
            assertThat(env.executeTrino("SELECT column_name, data_type FROM iceberg.information_schema.columns WHERE table_schema = 'default' AND table_name = '" + tableName + "$partitions'"))
                    .containsOnly(
                            row("partition", "row(\"a\" varchar, \"b\" varchar)"),
                            row("record_count", "bigint"),
                            row("file_count", "bigint"),
                            row("total_size", "bigint"),
                            row("data", "row(" +
                                    "\"" + (dropFirst ? "a" : "b") + "\" row(\"min\" varchar, \"max\" varchar, \"null_count\" bigint, \"nan_count\" bigint), " +
                                    "\"c\" row(\"min\" varchar, \"max\" varchar, \"null_count\" bigint, \"nan_count\" bigint))"));

            // Verify $partitions data
            assertThat(env.executeTrino("SELECT partition, record_count, file_count, data FROM \"iceberg\".\"default\".\"" + tableName + "$partitions\""))
                    .containsOnly(
                            row(
                                    rowBuilder().addField("a", "one").addField("b", "small").build(),
                                    2L,
                                    1L,
                                    rowBuilder()
                                            .addField(
                                                    dropFirst ? "a" : "b",
                                                    dropFirst ? singletonMetrics("one") : singletonMetrics("small"))
                                            .addField("c", dataMetrics("rabbit", "snake", 0, null))
                                            .build()),
                            row(
                                    rowBuilder().addField("a", "one").addField("b", "big").build(),
                                    1L,
                                    1L,
                                    rowBuilder()
                                            .addField(
                                                    dropFirst ? "a" : "b",
                                                    dropFirst ? singletonMetrics("one") : singletonMetrics("big"))
                                            .addField("c", singletonMetrics("rabbit"))
                                            .build()),
                            row(
                                    rowBuilder().addField("a", "another").addField("b", "small").build(),
                                    1L,
                                    1L,
                                    rowBuilder()
                                            .addField(
                                                    dropFirst ? "a" : "b",
                                                    dropFirst ? singletonMetrics("another") : singletonMetrics("small"))
                                            .addField("c", singletonMetrics("snake"))
                                            .build()),
                            row(
                                    rowBuilder().addField("a", "something").addField("b", "completely").build(),
                                    1L,
                                    1L,
                                    rowBuilder()
                                            .addField(
                                                    dropFirst ? "a" : "b",
                                                    dropFirst ? singletonMetrics("something") : singletonMetrics("completely"))
                                            .addField("c", singletonMetrics("else"))
                                            .build()),
                            row(
                                    rowBuilder().addField("a", null).addField("b", null).build(),
                                    1L,
                                    1L,
                                    rowBuilder()
                                            .addField(dropFirst ? "a" : "b", dataMetrics(null, null, 1, null))
                                            .addField("c", singletonMetrics("nothing"))
                                            .build()));

            // Insert new row and verify partition evolution works
            env.executeTrinoUpdate("INSERT INTO " + trinoTable + " VALUES ('yet', 'another', 'row')");
            assertThat(env.executeTrino("SELECT * FROM " + trinoTable))
                    .containsOnly(
                            row("one", "small", "snake"),
                            row("one", "small", "rabbit"),
                            row("one", "big", "rabbit"),
                            row("another", "small", "snake"),
                            row("something", "completely", "else"),
                            row(null, null, "nothing"),
                            row("yet", "another", "row"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTable);
        }
    }

    private static Row singletonMetrics(Object value)
    {
        return dataMetrics(value, value, 0, null);
    }

    private static Row dataMetrics(Object min, Object max, long nullCount, Long nanCount)
    {
        return rowBuilder()
                .addField("min", min)
                .addField("max", max)
                .addField("null_count", nullCount)
                .addField("nan_count", nanCount)
                .build();
    }

    private static Row.Builder rowBuilder()
    {
        return Row.builder();
    }
}
