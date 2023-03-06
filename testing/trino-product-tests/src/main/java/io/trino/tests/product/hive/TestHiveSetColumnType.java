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

import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestHiveSetColumnType
        extends HiveProductTest
{
    @Test
    public void testSetColumnType()
    {
        String tableName = "test_set_column_type_" + randomNameSuffix();

        onTrino().executeQuery("CREATE TABLE hive.default." + tableName + " AS SELECT CAST(123 AS integer) AS col");
        onTrino().executeQuery("ALTER TABLE hive.default." + tableName + " ALTER COLUMN col SET DATA TYPE bigint");

        assertThat(onTrino().executeQuery("SELECT * FROM hive.default." + tableName))
                .containsOnly(row(123));
        assertThat(onHive().executeQuery("SELECT * FROM default." + tableName))
                .containsOnly(row(123));

        onTrino().executeQuery("DROP TABLE hive.default." + tableName);
    }

    @Test
    public void testSetColumnTypeOnPartitionedTable()
    {
        String tableName = "test_set_column_type_partitioned_" + randomNameSuffix();

        onTrino().executeQuery("CREATE TABLE hive.default." + tableName + " WITH (partitioned_by = ARRAY['part']) AS SELECT CAST(123 AS integer) AS col, 'test_partition' AS part");
        assertThat(onHive().executeQuery("SHOW TABLE EXTENDED LIKE " + tableName + " PARTITION (part='test_partition')"))
                .contains(row("columns:struct columns { i32 col}"));

        // Verif SET DATA TYPE changes a column types in partitions
        onTrino().executeQuery("ALTER TABLE hive.default." + tableName + " ALTER COLUMN col SET DATA TYPE bigint");
        assertThat(onHive().executeQuery("SHOW TABLE EXTENDED LIKE " + tableName + " PARTITION (part='test_partition')"))
                .contains(row("columns:struct columns { i64 col}"));

        assertThat(onTrino().executeQuery("SELECT * FROM hive.default." + tableName))
                .containsOnly(row(123, "test_partition"));
        assertThat(onHive().executeQuery("SELECT * FROM default." + tableName))
                .containsOnly(row(123, "test_partition"));

        onTrino().executeQuery("DROP TABLE hive.default." + tableName);
    }

    @Test
    public void testUnsupportedFileFormatInPartition()
    {
        String tableName = "test_unsupported_file_format_partition_" + randomNameSuffix();

        onTrino().executeQuery("CREATE TABLE hive.default." + tableName + " WITH (format = 'PARQUET', partitioned_by = ARRAY['part']) AS SELECT CAST(123 AS integer) AS col, 'test_partition' AS part");
        onHive().executeQuery("ALTER TABLE " + tableName + " ADD PARTITION (part='test_text_partition')");
        onHive().executeQuery("ALTER TABLE " + tableName + " PARTITION (part='test_text_partition') SET FILEFORMAT RCFILE");

        assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE hive.default." + tableName + " ALTER COLUMN col SET DATA TYPE bigint"))
                .hasMessageContaining("Unsupported storage format for changing column type: RCBINARY");

        // Verify table and partition definitions haven't been changed
        Assertions.assertThat((String) onTrino().executeQuery("SHOW CREATE TABLE " + tableName).getOnlyValue())
                .contains("col integer");
        assertThat(onHive().executeQuery("SHOW TABLE EXTENDED LIKE " + tableName + " PARTITION (part='test_text_partition')"))
                .contains(row("columns:struct columns { i32 col}"));

        onTrino().executeQuery("DROP TABLE hive.default." + tableName);
    }
}
