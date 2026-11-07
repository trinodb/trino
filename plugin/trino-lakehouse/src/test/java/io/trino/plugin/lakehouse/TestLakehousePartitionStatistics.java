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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

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

    private double showStatsRowCount(String from)
    {
        return (double) computeActual("SHOW STATS FOR (SELECT * FROM " + from + ")").getMaterializedRows().stream()
                .filter(row -> row.getField(0) == null)
                .findFirst()
                .orElseThrow()
                .getField(4);
    }
}
