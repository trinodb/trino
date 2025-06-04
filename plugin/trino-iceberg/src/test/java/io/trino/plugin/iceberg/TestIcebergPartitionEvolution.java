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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergPartitionEvolution
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setInitialTables(ImmutableList.of(TpchTable.NATION))
                .build();
    }

    @Test
    public void testRemovePartitioning()
    {
        String tableName = "test_remove_partition_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey', 'truncate(name, 1)']) AS SELECT * FROM nation WHERE nationkey < 10", 10);
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY[]");
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM nation WHERE nationkey >= 10", 15);

        List<MaterializedRow> files = computeActual("SELECT file_path, record_count FROM \"" + tableName + "$files\"").getMaterializedRows();
        List<MaterializedRow> unpartitionedFiles = files.stream()
                .filter(file -> !((String) file.getField(0)).contains("regionkey="))
                .collect(toImmutableList());

        List<MaterializedRow> partitionedFiles = files.stream()
                .filter(file -> ((String) file.getField(0)).contains("regionkey="))
                .collect(toImmutableList());

        int expectedFileCount = computeActual("SELECT DISTINCT regionkey, substring(name, 1, 1) FROM nation WHERE nationkey < 10").getRowCount();
        assertThat(partitionedFiles).hasSize(expectedFileCount);
        assertThat(partitionedFiles.stream().mapToLong(row -> (long) row.getField(1)).sum()).isEqualTo(10L);

        assertThat(unpartitionedFiles).hasSize(1);
        assertThat((long) unpartitionedFiles.get(0).getField(1)).isEqualTo(15);

        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
        // Most partitions have one record each. regionkey=2, trunc_name=I has two records, and 15 records are unpartitioned
        assertQuery("SELECT record_count, count(*) FROM \"" + tableName + "$partitions\" GROUP BY record_count", "VALUES (1, 8), (2, 1), (15, 1)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAddPartitionColumn()
    {
        String tableName = "test_add_partition_column_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey']) AS SELECT * FROM nation WHERE nationkey < 10", 10);
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['regionkey', 'truncate(name, 1)']");
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM nation WHERE nationkey >= 10", 15);
        assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue()).contains("partitioning = ARRAY['regionkey','truncate(name, 1)']");

        List<MaterializedRow> files = computeActual("SELECT file_path, record_count FROM \"" + tableName + "$files\"").getMaterializedRows();
        List<MaterializedRow> initialFiles = files.stream()
                .filter(file -> !((String) file.getField(0)).contains("name_trunc"))
                .collect(toImmutableList());

        List<MaterializedRow> partitionedFiles = files.stream()
                .filter(file -> ((String) file.getField(0)).contains("name_trunc"))
                .collect(toImmutableList());

        int expectedInitialFiles = toIntExact((long) computeActual("SELECT count(distinct regionkey) FROM nation WHERE nationkey < 10").getOnlyValue());
        assertThat(initialFiles).hasSize(expectedInitialFiles);
        assertThat(initialFiles.stream().mapToLong(row -> (long) row.getField(1)).sum()).isEqualTo(10L);

        int expectedFinalFileCount = computeActual("SELECT DISTINCT regionkey, substring(name, 1, 1) FROM nation WHERE nationkey >= 10").getRowCount();
        assertThat(partitionedFiles).hasSize(expectedFinalFileCount);
        assertThat(partitionedFiles.stream().mapToLong(row -> (long) row.getField(1)).sum()).isEqualTo(15L);

        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['truncate(name, 1)']) AS SELECT * FROM nation WHERE nationkey < 10", 10);
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['truncate(name, 1)', 'regionkey']");
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM nation WHERE nationkey >= 10", 15);
        assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue()).contains("partitioning = ARRAY['truncate(name, 1)','regionkey']");

        files = computeActual("SELECT file_path, record_count FROM \"" + tableName + "$files\"").getMaterializedRows();
        initialFiles = files.stream()
                .filter(file -> !((String) file.getField(0)).contains("regionkey="))
                .collect(toImmutableList());

        partitionedFiles = files.stream()
                .filter(file -> ((String) file.getField(0)).contains("regionkey="))
                .collect(toImmutableList());

        expectedInitialFiles = computeActual("SELECT DISTINCT substring(name, 1, 1) FROM nation WHERE nationkey < 10").getRowCount();
        assertThat(initialFiles).hasSize(expectedInitialFiles);
        assertThat(initialFiles.stream().mapToLong(row -> (long) row.getField(1)).sum()).isEqualTo(10L);

        expectedFinalFileCount = computeActual("SELECT DISTINCT regionkey, substring(name, 1, 1) FROM nation WHERE nationkey >= 10").getRowCount();
        assertThat(partitionedFiles).hasSize(expectedFinalFileCount);
        assertThat(partitionedFiles.stream().mapToLong(row -> (long) row.getField(1)).sum()).isEqualTo(15L);

        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testChangePartitionTransform()
    {
        String tableName = "test_change_partition_transform_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (ts, a) WITH (partitioning = ARRAY['year(ts)']) " +
                "AS VALUES (TIMESTAMP '2021-01-01 01:01:01.111111', 1), (TIMESTAMP '2022-02-02 02:02:02.222222', 2), (TIMESTAMP '2023-03-03 03:03:03.333333', 3)", 3);
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['month(ts)']");
        assertUpdate("INSERT INTO " + tableName + " VALUES (TIMESTAMP '2024-04-04 04:04:04.444444', 4), (TIMESTAMP '2025-05-05 05:05:05.555555', 5)", 2);
        assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue()).contains("partitioning = ARRAY['month(ts)']");

        List<MaterializedRow> files = computeActual("SELECT file_path, record_count FROM \"" + tableName + "$files\"").getMaterializedRows();
        List<MaterializedRow> yearPartitionedFiles = files.stream()
                .filter(file -> {
                    String filePath = ((String) file.getField(0));
                    return filePath.contains("ts_year") && !filePath.contains("ts_month");
                })
                .collect(toImmutableList());

        List<MaterializedRow> monthPartitionedFiles = files.stream()
                .filter(file -> {
                    String filePath = ((String) file.getField(0));
                    return !filePath.contains("ts_year") && filePath.contains("ts_month");
                })
                .collect(toImmutableList());

        assertThat(yearPartitionedFiles).hasSize(3);
        assertThat(monthPartitionedFiles).hasSize(2);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAddNestedPartitioning()
    {
        String tableName = "test_add_nested_partition_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, district ROW(name VARCHAR), state ROW(name VARCHAR)) WITH (partitioning = ARRAY['\"state.name\"'])");
        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(1, ROW('Patna'), ROW('BH')), " +
                        "(2, ROW('Gaya'), ROW('BH')), " +
                        "(3, ROW('Bengaluru'), ROW('KA')), " +
                        "(4, ROW('Mengaluru'), ROW('KA'))",
                4);

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['\"state.name\"', '\"district.name\"']");

        assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue()).contains("partitioning = ARRAY['\"state.name\"','\"district.name\"']");

        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(1, ROW('Patna'), ROW('BH')), " +
                        "(2, ROW('Patna'), ROW('BH')), " +
                        "(3, ROW('Bengaluru'), ROW('KA')), " +
                        "(4, ROW('Mengaluru'), ROW('KA'))",
                4);

        List<MaterializedRow> files = computeActual("SELECT file_path, record_count FROM \"" + tableName + "$files\"").getMaterializedRows();
        List<MaterializedRow> initialPartitionedFiles = files.stream()
                .filter(file -> !((String) file.getField(0)).contains("district.name="))
                .collect(toImmutableList());

        List<MaterializedRow> laterPartitionedFiles = files.stream()
                .filter(file -> ((String) file.getField(0)).contains("district.name="))
                .collect(toImmutableList());

        assertThat(initialPartitionedFiles).hasSize(2);
        assertThat(initialPartitionedFiles.stream().mapToLong(row -> (long) row.getField(1)).sum()).isEqualTo(4L);

        assertThat(laterPartitionedFiles).hasSize(3);
        assertThat(laterPartitionedFiles.stream().mapToLong(row -> (long) row.getField(1)).sum()).isEqualTo(4L);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRemoveNestedPartitioning()
    {
        String tableName = "test_remove_nested_partition_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, district ROW(name VARCHAR), state ROW(name VARCHAR)) WITH (partitioning = ARRAY['\"state.name\"'])");
        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(1, ROW('Patna'), ROW('BH')), " +
                        "(2, ROW('Gaya'), ROW('BH')), " +
                        "(3, ROW('Bengaluru'), ROW('KA')), " +
                        "(4, ROW('Mengaluru'), ROW('KA'))",
                4);

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY[]");

        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(1, ROW('Patna'), ROW('BH')), " +
                        "(2, ROW('Gaya'), ROW('BH')), " +
                        "(3, ROW('Bengaluru'), ROW('KA')), " +
                        "(4, ROW('Mengaluru'), ROW('KA'))",
                4);

        List<MaterializedRow> files = computeActual("SELECT file_path, record_count FROM \"" + tableName + "$files\"").getMaterializedRows();
        List<MaterializedRow> unpartitionedFiles = files.stream()
                .filter(file -> !((String) file.getField(0)).contains("state.name="))
                .collect(toImmutableList());

        List<MaterializedRow> partitionedFiles = files.stream()
                .filter(file -> ((String) file.getField(0)).contains("state.name="))
                .collect(toImmutableList());

        assertThat(partitionedFiles).hasSize(2);
        assertThat(partitionedFiles.stream().mapToLong(row -> (long) row.getField(1)).sum()).isEqualTo(4L);

        assertThat(unpartitionedFiles).hasSize(1);
        assertThat((long) unpartitionedFiles.get(0).getField(1)).isEqualTo(4);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testNestedFieldChangePartitionTransform()
    {
        String tableName = "test_nested_field_change_partition_transform_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (grandparent ROW(parent ROW(ts TIMESTAMP(6), a INT), b INT), c INT) " +
                "WITH (partitioning = ARRAY['year(\"grandparent.parent.ts\")'])");
        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(ROW(ROW(TIMESTAMP '2021-01-01 01:01:01.111111', 1), 1), 1), " +
                        "(ROW(ROW(TIMESTAMP '2022-02-02 02:02:02.222222', 2), 2), 2), " +
                        "(ROW(ROW(TIMESTAMP '2023-03-03 03:03:03.333333', 3), 3), 3)",
                3);
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['month(\"grandparent.parent.ts\")']");
        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(ROW(ROW(TIMESTAMP '2024-04-04 04:04:04.444444', 4), 4), 4), " +
                        "(ROW(ROW(TIMESTAMP '2025-05-05 05:05:05.555555', 5), 5), 5)",
                2);

        assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue()).contains("partitioning = ARRAY['month(\"grandparent.parent.ts\")']");

        List<MaterializedRow> files = computeActual("SELECT file_path, record_count FROM \"" + tableName + "$files\"").getMaterializedRows();
        List<MaterializedRow> yearPartitionedFiles = files.stream()
                .filter(file -> {
                    String filePath = ((String) file.getField(0));
                    return filePath.contains("grandparent.parent.ts_year=") && !filePath.contains("grandparent.parent.ts_month=");
                })
                .collect(toImmutableList());

        List<MaterializedRow> monthPartitionedFiles = files.stream()
                .filter(file -> {
                    String filePath = ((String) file.getField(0));
                    return !filePath.contains("grandparent.parent.ts_year=") && filePath.contains("grandparent.parent.ts_month=");
                })
                .collect(toImmutableList());

        assertThat(yearPartitionedFiles).hasSize(3);
        assertThat(monthPartitionedFiles).hasSize(2);
        assertUpdate("DROP TABLE " + tableName);
    }
}
