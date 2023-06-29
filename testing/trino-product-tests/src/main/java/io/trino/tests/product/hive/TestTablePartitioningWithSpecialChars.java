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
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableList;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tests.product.TestGroups.HIVE_PARTITIONING;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTablePartitioningWithSpecialChars
        extends ProductTest
{
    private static final String INSERTED_PARTITION_VALUES = "" +
            "(1, 'with-hyphen')," +
            "(2, 'with.dot')," +
            "(3, 'with:colon')," +
            "(4, 'with/slash')," +
            "(5, 'with\\\\backslashes')," +
            "(6, 'with\\backslash')," +
            "(7, 'with=equal')," +
            "(8, 'with?question')," +
            "(9, 'with!exclamation')," +
            "(10, 'with%%percent')," +
            "(11, 'with%%%%percents')," +
            "(12, 'with space')";

    private static final ImmutableList<QueryAssert.Row> EXPECTED_PARTITION_VALUES = ImmutableList.of(
            row(1, "with-hyphen"),
            row(2, "with.dot"),
            row(3, "with:colon"),
            row(4, "with/slash"),
            row(5, "with\\\\backslashes"),
            row(6, "with\\backslash"),
            row(7, "with=equal"),
            row(8, "with?question"),
            row(9, "with!exclamation"),
            row(10, "with%percent"),
            row(11, "with%%percents"),
            row(12, "with space"));

    @Test(groups = HIVE_PARTITIONING)
    public void testStringPartitioningWithSpecialCharactersCtasInTrino()
    {
        String tableName = "test_string_partitioning_with_special_chars_ctas_in_trino";

        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onTrino().executeQuery(format("CREATE TABLE %s (id, part_col) " +
                        "WITH (partitioned_by = ARRAY['part_col']) " +
                        "AS VALUES " + INSERTED_PARTITION_VALUES,
                tableName));

        assertThat(onHive().executeQuery("SELECT * FROM " + tableName)).containsOnly(EXPECTED_PARTITION_VALUES);
        assertThat(onTrino().executeQuery("SELECT * FROM " + tableName)).contains(EXPECTED_PARTITION_VALUES);
    }

    @Test(groups = HIVE_PARTITIONING)
    public void testStringPartitioningWithSpecialCharactersInsertInTrino()
    {
        String tableName = "test_string_partitioning_with_special_chars_insert_in_trino";

        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onTrino().executeQuery(format("CREATE TABLE %s (id BIGINT, part_col VARCHAR) WITH (partitioned_by = ARRAY['part_col']) ", tableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES " + INSERTED_PARTITION_VALUES, tableName));

        assertThat(onHive().executeQuery("SELECT * FROM " + tableName)).containsOnly(EXPECTED_PARTITION_VALUES);
        assertThat(onTrino().executeQuery("SELECT * FROM " + tableName)).contains(EXPECTED_PARTITION_VALUES);
    }

    @Test(groups = HIVE_PARTITIONING)
    public void testStringPartitioningWithSpecialCharactersInsertInHive()
    {
        String sourceTableName = "test_string_partitioning_with_special_chars_insert_in_hive_source";
        String tableName = "test_string_partitioning_with_special_chars_insert_in_hive";

        onTrino().executeQuery("DROP TABLE IF EXISTS " + sourceTableName);
        onTrino().executeQuery(format("CREATE TABLE %s (id, part_col) AS VALUES " + INSERTED_PARTITION_VALUES, sourceTableName));

        onHive().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onHive().executeQuery(format("CREATE TABLE %s (id BIGINT) PARTITIONED BY (part_col STRING) ", tableName));
        onHive().executeQuery("set hive.exec.dynamic.partition.mode=nonstrict"); // needed for dynamic partitioning with no static partitions in insert statement
        onHive().executeQuery(format("INSERT INTO %s PARTITION(part_col) SELECT * FROM %s", tableName, sourceTableName));

        assertThat(onHive().executeQuery("SELECT * FROM " + tableName)).containsOnly(EXPECTED_PARTITION_VALUES);
        assertThat(onTrino().executeQuery("SELECT * FROM " + tableName)).contains(EXPECTED_PARTITION_VALUES);
    }

    @Test(groups = HIVE_PARTITIONING)
    public void testStringPartitioningWithUtfChars()
    {
        String tableName = "test_string_partitioning_with_utf_chars";
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onTrino().executeQuery(format("CREATE TABLE %s (id BIGINT, part_col VARCHAR) WITH (partitioned_by = ARRAY['part_col']) ", tableName));

        assertQueryFailure(() -> onTrino().executeQuery(format("INSERT INTO %s VALUES (1, 'łąka')", tableName)))
                .hasMessageContaining("Hive partition keys can only contain printable ASCII characters");

        // not testing on Hive. It allows inserting data to partition with utf chars. But then data is not visible, and table is not usable from Trino (e.g. one cannot drop a table)
    }
}
