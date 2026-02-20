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
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for table partitioning with special characters in partition values.
 * <p>
 * Ported from the Tempto-based TestTablePartitioningWithSpecialChars.
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
class TestTablePartitioningWithSpecialChars
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

    private static final List<Row> EXPECTED_PARTITION_VALUES = ImmutableList.of(
            row(1L, "with-hyphen"),
            row(2L, "with.dot"),
            row(3L, "with:colon"),
            row(4L, "with/slash"),
            row(5L, "with\\\\backslashes"),
            row(6L, "with\\backslash"),
            row(7L, "with=equal"),
            row(8L, "with?question"),
            row(9L, "with!exclamation"),
            row(10L, "with%percent"),
            row(11L, "with%%percents"),
            row(12L, "with space"));

    @Test
    void testStringPartitioningWithSpecialCharactersCtasInTrino(HiveBasicEnvironment env)
    {
        String tableName = "test_string_partitioning_with_special_chars_ctas_in_trino";

        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (id, part_col) " +
                        "WITH (partitioned_by = ARRAY['part_col']) " +
                        "AS VALUES " + INSERTED_PARTITION_VALUES,
                tableName));

        assertThat(env.executeHive("SELECT * FROM " + tableName)).containsOnly(EXPECTED_PARTITION_VALUES);
        assertThat(env.executeTrino("SELECT * FROM hive.default." + tableName)).contains(EXPECTED_PARTITION_VALUES);

        env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
    }

    @Test
    void testStringPartitioningWithSpecialCharactersInsertInTrino(HiveBasicEnvironment env)
    {
        String tableName = "test_string_partitioning_with_special_chars_insert_in_trino";

        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (id BIGINT, part_col VARCHAR) WITH (partitioned_by = ARRAY['part_col']) ", tableName));
        env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES " + INSERTED_PARTITION_VALUES, tableName));

        assertThat(env.executeHive("SELECT * FROM " + tableName)).containsOnly(EXPECTED_PARTITION_VALUES);
        assertThat(env.executeTrino("SELECT * FROM hive.default." + tableName)).contains(EXPECTED_PARTITION_VALUES);

        env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
    }

    @Test
    void testStringPartitioningWithSpecialCharactersInsertInHive(HiveBasicEnvironment env)
    {
        String sourceTableName = "test_string_partitioning_with_special_chars_insert_in_hive_source";
        String tableName = "test_string_partitioning_with_special_chars_insert_in_hive";

        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + sourceTableName);
        env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (id, part_col) AS VALUES " + INSERTED_PARTITION_VALUES, sourceTableName));

        env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        env.executeHiveUpdate(format("CREATE TABLE %s (id BIGINT) PARTITIONED BY (part_col STRING) ", tableName));
        env.executeHiveUpdate("set hive.exec.dynamic.partition.mode=nonstrict"); // needed for dynamic partitioning with no static partitions in insert statement
        env.executeHiveUpdate(format("INSERT INTO %s PARTITION(part_col) SELECT * FROM %s", tableName, sourceTableName));

        assertThat(env.executeHive("SELECT * FROM " + tableName)).containsOnly(EXPECTED_PARTITION_VALUES);
        assertThat(env.executeTrino("SELECT * FROM hive.default." + tableName)).contains(EXPECTED_PARTITION_VALUES);

        env.executeTrinoUpdate("DROP TABLE hive.default." + sourceTableName);
        env.executeHiveUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testStringPartitioningWithUtfChars(HiveBasicEnvironment env)
    {
        String tableName = "test_string_partitioning_with_utf_chars";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (id BIGINT, part_col VARCHAR) WITH (partitioned_by = ARRAY['part_col']) ", tableName));

        assertThatThrownBy(() -> env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES (1, 'łąka')", tableName)))
                .hasMessageContaining("Hive partition keys can only contain printable ASCII characters");

        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        // not testing on Hive. It allows inserting data to partition with utf chars. But then data is not visible, and table is not usable from Trino (e.g. one cannot drop a table)
    }
}
