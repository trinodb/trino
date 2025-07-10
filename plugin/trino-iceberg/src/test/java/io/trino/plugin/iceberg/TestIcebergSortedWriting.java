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
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.apache.iceberg.FileFormat;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergSortedWriting
        extends AbstractTestQueryFramework
{
    private TrinoFileSystem fileSystem;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setInitialTables(ImmutableList.of(TpchTable.LINE_ITEM))
                .addIcebergProperty("iceberg.sorted-writing-enabled", "true")
                // Test staging of sorted writes to local disk
                .addIcebergProperty("iceberg.sorted-writing.local-staging-path", "/tmp/trino-${USER}")
                // Allows testing the sorting writer flushing to the file system with smaller tables
                .addIcebergProperty("iceberg.writer-sort-buffer-size", "1MB")
                .build();
    }

    @BeforeAll
    public void initFileSystem()
    {
        fileSystem = getFileSystemFactory(getDistributedQueryRunner()).create(SESSION);
    }

    @Test
    public void testSortedWritingWithLocalStaging()
    {
        testSortedWritingWithLocalStaging(FileFormat.ORC);
        testSortedWritingWithLocalStaging(FileFormat.PARQUET);
    }

    private void testSortedWritingWithLocalStaging(FileFormat format)
    {
        // Using a larger table forces buffered data to be written to disk
        Session withSmallRowGroups = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "orc_writer_max_stripe_rows", "200")
                .setCatalogSessionProperty("iceberg", "parquet_writer_block_size", "20kB")
                .setCatalogSessionProperty("iceberg", "parquet_writer_batch_size", "200")
                .build();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sorted_lineitem_table",
                "WITH (sorted_by = ARRAY['comment'], format = '" + format.name() + "') AS TABLE tpch.tiny.lineitem WITH NO DATA")) {
            assertUpdate(
                    withSmallRowGroups,
                    "INSERT INTO " + table.getName() + " TABLE tpch.tiny.lineitem",
                    "VALUES 60175");
            for (Object filePath : computeActual("SELECT file_path from \"" + table.getName() + "$files\"").getOnlyColumnAsSet()) {
                assertThat(isFileSorted(Location.of((String) filePath), "comment", format)).isTrue();
            }
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM lineitem");
        }
    }

    private boolean isFileSorted(Location path, String sortColumnName, FileFormat format)
    {
        if (format == PARQUET) {
            return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
        }
        return checkOrcFileSorting(fileSystem, path, sortColumnName);
    }
}
