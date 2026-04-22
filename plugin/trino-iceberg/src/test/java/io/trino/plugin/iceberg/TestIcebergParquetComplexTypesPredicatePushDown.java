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

import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.testing.BaseComplexTypesPredicatePushDownTest;
import io.trino.testing.QueryRunner;
import org.apache.parquet.column.statistics.Statistics;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getParquetFileMetadata;
import static io.trino.plugin.iceberg.IcebergTestUtils.withSmallRowGroups;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergParquetComplexTypesPredicatePushDown
        extends BaseComplexTypesPredicatePushDownTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.file-format", "PARQUET")
                .build();
    }

    @Override
    protected final Session getSession()
    {
        return withSmallRowGroups(super.getSession());
    }

    // The Iceberg table scan differs from Hive in that the coordinator also uses file statistics when generating the splits .
    // As a result, if the predicates fall outside the bounds of the file statistics,
    // the split is not created for the worker and worker won't call getParquetTupleDomain().
    // The test increased the number of row groups and introduced predicates that are within the file statistics but outside the statistics of the row groups.
    @Test
    public void testIcebergParquetRowTypeRowGroupPruning()
            throws IOException
    {
        String tableName = "test_nested_column_pruning_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (col1Row ROW(a BIGINT, b BIGINT), col2 BIGINT) WITH (sorted_by=ARRAY['col2'])");
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM unnest(transform(SEQUENCE(1, 10000), x -> ROW(ROW(x*2, 100), x)))", 10000);

        long valueBetweenRowGroups = getValueBetweenRowGroups(tableName, "col1row.a");
        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col1Row.a = " + valueBetweenRowGroups);
        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col1Row.a IS NULL");

        assertUpdate("DROP TABLE " + tableName);
    }

    // Find a value that is between the max of one row group and the min of the next row group,
    // so the value is within the file statistics but outside every row group's statistics.
    private long getValueBetweenRowGroups(String tableName, String columnPath)
            throws IOException
    {
        String filePath = (String) computeScalar("SELECT file_path FROM \"" + tableName + "$files\"");
        TrinoFileSystem fileSystem = getFileSystemFactory(getDistributedQueryRunner()).create(SESSION);
        ParquetMetadata parquetMetadata = getParquetFileMetadata(fileSystem.newInputFile(Location.of(filePath)));
        List<BlockMetadata> blocks = parquetMetadata.getBlocks();
        assertThat(blocks).hasSizeGreaterThan(1);

        long previousMax = getColumnMax(blocks.getFirst(), columnPath);
        for (BlockMetadata block : blocks.subList(1, blocks.size())) {
            long nextMin = getColumnMin(block, columnPath);
            if (previousMax + 1 < nextMin) {
                return previousMax + 1;
            }
            previousMax = getColumnMax(block, columnPath);
        }
        throw new AssertionError("No gap between row group statistics for " + columnPath);
    }

    private static long getColumnMin(BlockMetadata block, String columnPath)
    {
        Object min = getColumnStatistics(block, columnPath).genericGetMin();
        assertThat(min).isInstanceOf(Long.class);
        return (long) min;
    }

    private static long getColumnMax(BlockMetadata block, String columnPath)
    {
        Object max = getColumnStatistics(block, columnPath).genericGetMax();
        assertThat(max).isInstanceOf(Long.class);
        return (long) max;
    }

    private static Statistics<?> getColumnStatistics(BlockMetadata block, String columnPath)
    {
        Statistics<?> statistics = block.columns().stream()
                .filter(column -> column.getPath().toDotString().equals(columnPath))
                .collect(onlyElement())
                .getStatistics();
        assertThat(statistics.hasNonNullValue()).isTrue();
        return statistics;
    }
}
