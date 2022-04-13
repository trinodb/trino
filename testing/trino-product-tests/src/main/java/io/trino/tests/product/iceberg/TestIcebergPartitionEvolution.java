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

import io.trino.tempto.ProductTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onSpark;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestIcebergPartitionEvolution
        extends ProductTest
{
    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "testDroppedPartitionFieldDataProvider")
    public void testDroppedPartitionField(boolean dropFirst)
    {
        onTrino().executeQuery("USE iceberg.default");
        onTrino().executeQuery("DROP TABLE IF EXISTS test_dropped_partition_field");
        onTrino().executeQuery("CREATE TABLE test_dropped_partition_field(a varchar, b varchar, c varchar) WITH (partitioning = ARRAY['a','b'])");
        onTrino().executeQuery("INSERT INTO test_dropped_partition_field VALUES " +
                "('one', 'small', 'snake')," +
                "('one', 'small', 'rabbit')," +
                "('one', 'big', 'rabbit')," +
                "('another', 'small', 'snake')," +
                "('something', 'completely', 'else')," +
                "(NULL, NULL, 'nothing')");

        onSpark().executeQuery("ALTER TABLE iceberg_test.default.test_dropped_partition_field DROP PARTITION FIELD " + (dropFirst ? "a" : "b"));

        assertThat(onTrino().executeQuery("SHOW CREATE TABLE test_dropped_partition_field"))
                .containsOnly(
                        row("CREATE TABLE iceberg.default.test_dropped_partition_field (\n" +
                                "   a varchar,\n" +
                                "   b varchar,\n" +
                                "   c varchar\n" +
                                ")\n" +
                                "WITH (\n" +
                                "   format = 'ORC',\n" +
                                "   location = 'hdfs://hadoop-master:9000/user/hive/warehouse/test_dropped_partition_field',\n" +
                                "   partitioning = ARRAY[" + (dropFirst ? "'void(a)','b'" : "'a','void(b)'") + "]\n" +
                                ")"));

        assertThat(onTrino().executeQuery("SELECT * FROM test_dropped_partition_field"))
                .containsOnly(
                        row("one", "small", "snake"),
                        row("one", "small", "rabbit"),
                        row("one", "big", "rabbit"),
                        row("another", "small", "snake"),
                        row("something", "completely", "else"),
                        row(null, null, "nothing"));

        assertThat(onTrino().executeQuery("SHOW STATS FOR test_dropped_partition_field"))
                .containsOnly(
                        row("a", null, null, 1. / 6, null, null, null),
                        row("b", null, null, 1. / 6, null, null, null),
                        row("c", null, null, 0., null, null, null),
                        row(null, null, null, null, 6., null, null));

        assertThat(onTrino().executeQuery("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'test_dropped_partition_field$partitions'"))
                .containsOnly(
                        row("partition", "row(a varchar, b varchar)"),
                        row("record_count", "bigint"),
                        row("file_count", "bigint"),
                        row("total_size", "bigint"),
                        row("data", "row(" +
                                // A/B is now partitioning column in the first partitioning spec, and non-partitioning in new one
                                (dropFirst ? "a" : "b") + " row(min varchar, max varchar, null_count bigint, nan_count bigint), " +
                                "c row(min varchar, max varchar, null_count bigint, nan_count bigint))"));
        assertThat(onTrino().executeQuery("SELECT partition, record_count, file_count, data FROM \"test_dropped_partition_field$partitions\""))
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

        onTrino().executeQuery("INSERT INTO test_dropped_partition_field VALUES ('yet', 'another', 'row')");
        assertThat(onTrino().executeQuery("SELECT * FROM test_dropped_partition_field"))
                .containsOnly(
                        row("one", "small", "snake"),
                        row("one", "small", "rabbit"),
                        row("one", "big", "rabbit"),
                        row("another", "small", "snake"),
                        row("something", "completely", "else"),
                        row(null, null, "nothing"),
                        row("yet", "another", "row"));
    }

    @DataProvider
    public Object[][] testDroppedPartitionFieldDataProvider()
    {
        return new Object[][] {{true}, {false}};
    }

    private static io.trino.jdbc.Row singletonMetrics(Object value)
    {
        return dataMetrics(value, value, 0, null);
    }

    private static io.trino.jdbc.Row dataMetrics(Object min, Object max, long nullCount, Long nanCount)
    {
        return rowBuilder()
                .addField("min", min)
                .addField("max", max)
                .addField("null_count", nullCount)
                .addField("nan_count", nanCount)
                .build();
    }

    private static io.trino.jdbc.Row.Builder rowBuilder()
    {
        return io.trino.jdbc.Row.builder();
    }
}
