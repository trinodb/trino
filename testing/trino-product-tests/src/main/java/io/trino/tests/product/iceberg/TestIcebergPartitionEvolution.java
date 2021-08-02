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
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
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
                        row("a", "varchar"),
                        row("b", "varchar"),
                        // A/B is now partitioning column in the first partitioning spec, and non-partitioning in new one
                        // TODO (https://github.com/trinodb/trino/issues/8729): $partitions table (as every other table) cannot have duplicate column names
                        row(dropFirst ? "a" : "b", "row(min varchar, max varchar, null_count bigint)"),
                        row("row_count", "bigint"),
                        row("file_count", "bigint"),
                        row("total_size", "bigint"),
                        row("c", "row(min varchar, max varchar, null_count bigint)"));
        assertQueryFailure(() -> onTrino().executeQuery("SELECT a, b, c.min, c.max, row_count FROM \"test_dropped_partition_field$partitions\""))
                // TODO (https://github.com/trinodb/trino/issues/8729): cannot read from $partitions table because of duplicate column names
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q Multiple entries with same key: " + (dropFirst ? "a=a and a=a" : "b=b and b=b"));

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
}
