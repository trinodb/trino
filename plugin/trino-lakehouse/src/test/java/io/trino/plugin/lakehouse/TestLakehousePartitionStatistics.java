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
package io.trino.plugin.lakehouse;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.iceberg.IcebergTestUtils.registerPartitionStatistics;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The class path of these tests, like the plugin's, has no {@code parquet-hadoop}, so the
 * Iceberg library reads Parquet partition statistics files only with the reader Trino registers.
 */
final class TestLakehousePartitionStatistics
        extends AbstractTestQueryFramework
{
    private Path rootDirectory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        rootDirectory = createTempDirectory("lakehouse_partition_statistics");
        QueryRunner queryRunner = LakehouseQueryRunner.builder()
                .addLakehouseProperty("hive.metastore", "file")
                .addLakehouseProperty("hive.metastore.catalog.dir", "local:///")
                .addLakehouseProperty("fs.local.enabled", "true")
                .addLakehouseProperty("local.location", rootDirectory.toString())
                // use partition statistics files however few data files there are
                .addLakehouseProperty("iceberg.partition-statistics.min-data-files", "0")
                .build();
        queryRunner.execute("CREATE SCHEMA lakehouse.tpch");
        return queryRunner;
    }

    @Test
    void testClassPathHasNoLibraryParquetReader()
    {
        assertThatThrownBy(() -> Class.forName("org.apache.parquet.hadoop.ParquetFileReader"))
                .isInstanceOf(ClassNotFoundException.class);
    }

    @Test
    void testReadParquetPartitionStatistics()
            throws Exception
    {
        try (TestTable table = newTrinoTable(
                "test_partition_statistics_parquet",
                "(id integer, part integer) WITH (format = 'PARQUET', format_version = 2, partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 1), (2, 2)", 2);
            // written by the library's own writers, see the README next to the file
            registerPartitionStatistics(rootDirectory, "tpch", table.getName(), Path.of(getResource("partition-stats/partition-stats.parquet").toURI()));

            // the row counts of the file, not of the table
            assertThat(showStatsRowCount(table.getName() + " WHERE part = 1")).isEqualTo(1000.0);
            assertThat(showStatsRowCount(table.getName() + " WHERE part IN (1, 2)")).isEqualTo(3000.0);
        }
    }

    @Test
    void testComputePartitionStatsOnParquetTable()
            throws Exception
    {
        try (TestTable table = newTrinoTable(
                "test_compute_partition_statistics_parquet",
                "(id integer, part integer) WITH (format = 'PARQUET', format_version = 3, partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 1), (2, 1), (3, 2)", 3);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 2", 1);
            // the manifest walk ignores the deletion vector
            assertThat(showStatsRowCount(table.getName() + " WHERE part = 1")).isEqualTo(2.0);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE compute_partition_stats");

            assertThat(partitionStatisticsFiles(table.getName())).singleElement().satisfies(file -> assertThat(file).endsWith(".parquet"));
            // the partition statistics file subtracts it
            assertThat(showStatsRowCount(table.getName() + " WHERE part = 1")).isEqualTo(1.0);
            assertThat(showStatsRowCount(table.getName() + " WHERE part = 2")).isEqualTo(1.0);
        }
    }

    @Test
    void testComputePartitionStatsContinuesFromParquetFixture()
            throws Exception
    {
        try (TestTable table = newTrinoTable(
                "test_compute_partition_statistics_fixture_parquet",
                "(id integer, part integer) WITH (format = 'PARQUET', format_version = 2, partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 1), (2, 2)", 2);
            registerPartitionStatistics(rootDirectory, "tpch", table.getName(), Path.of(getResource("partition-stats/partition-stats.parquet").toURI()));
            // not updated on write, so that the procedure computes the file
            Session withoutUpdateOnWrite = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "collect_partition_statistics_on_write", "false")
                    .build();
            assertUpdate(withoutUpdateOnWrite, "INSERT INTO " + table.getName() + " VALUES (3, 1)", 1);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE compute_partition_stats");

            // continued from the rows of the fixture (1000 and 2000), not computed in full
            assertThat(partitionStatisticsFiles(table.getName())).hasSize(1);
            assertThat(showStatsRowCount(table.getName() + " WHERE part = 1")).isEqualTo(1001.0);
            assertThat(showStatsRowCount(table.getName() + " WHERE part = 2")).isEqualTo(2000.0);
        }
    }

    @Test
    void testPartitionStatisticsUpdatedOnInsert()
            throws Exception
    {
        try (TestTable table = newTrinoTable(
                "test_partition_statistics_update_on_insert",
                "(id integer, part integer) WITH (format = 'PARQUET', format_version = 3, partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 1), (2, 1), (3, 2)", 3);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 2", 1);
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE compute_partition_stats");

            assertUpdate("INSERT INTO " + table.getName() + " VALUES (4, 1), (5, 3)", 2);

            assertThat(partitionStatisticsFiles(table.getName())).hasSize(2).allSatisfy(file -> assertThat(file).endsWith(".parquet"));
            // the file of the insert subtracts the deletion vector, the manifest walk would report 3
            assertThat(showStatsRowCount(table.getName() + " WHERE part = 1")).isEqualTo(2.0);
            assertThat(showStatsRowCount(table.getName() + " WHERE part = 3")).isEqualTo(1.0);
        }
    }

    @Test
    void testDropPartitionStats()
    {
        try (TestTable table = newTrinoTable(
                "test_drop_partition_statistics",
                "(id integer, part integer) WITH (format_version = 3, partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 1), (2, 1), (3, 2)", 3);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 2", 1);
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE compute_partition_stats");
            // the partition statistics file subtracts the deletion vector
            assertThat(showStatsRowCount(table.getName() + " WHERE part = 1")).isEqualTo(1.0);

            assertThat(computeActual("ALTER TABLE " + table.getName() + " EXECUTE drop_partition_stats").getMaterializedRows())
                    .singleElement().satisfies(row -> assertThat(row.getFields()).containsExactly("removed_partition_statistics_count", 1L));

            // the manifest walk ignores the deletion vector
            assertThat(showStatsRowCount(table.getName() + " WHERE part = 1")).isEqualTo(2.0);
        }
    }

    // the files Trino wrote, not the registered fixture
    private List<String> partitionStatisticsFiles(String tableName)
            throws IOException
    {
        try (Stream<Path> files = Files.walk(rootDirectory)) {
            return files
                    .filter(file -> file.toString().contains(tableName))
                    .map(file -> file.getFileName().toString())
                    .filter(name -> name.startsWith("partition-stats-"))
                    .collect(toImmutableList());
        }
    }

    private double showStatsRowCount(String from)
    {
        return (double) computeActual("SHOW STATS FOR (SELECT * FROM " + from + ")").getMaterializedRows().stream()
                .filter(row -> row.getField(0) == null)
                .findFirst()
                .orElseThrow()
                .getField(4);
    }
}
