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
import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.Type;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.apache.iceberg.PartitionSpec.unpartitioned;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergStatisticsBuilder
{
    private static final int COLUMN_A_ID = 1;
    private static final int COLUMN_B_ID = 2;
    private static final Types.NestedField COLUMN_A = optional(COLUMN_A_ID, "a", Types.LongType.get());
    private static final Types.NestedField COLUMN_B = optional(COLUMN_B_ID, "b", Types.DoubleType.get());
    private static final List<Types.NestedField> COLUMNS = ImmutableList.of(COLUMN_A, COLUMN_B);
    private static final List<Type> COLUMN_TYPES = ImmutableList.of(BIGINT, DOUBLE);

    private static final long RECORD_COUNT = 10;
    private static final long FILE_SIZE = 1024;

    @Test
    void testMergeInvalidatesBounds()
    {
        IcebergStatistics.Builder target = new IcebergStatistics.Builder(COLUMNS, COLUMN_TYPES, TESTING_TYPE_MANAGER);
        target.acceptDataFile(dataFile(bounds(COLUMN_A, 1L), bounds(COLUMN_A, 5L), perColumn(COLUMN_A, 0, COLUMN_B, RECORD_COUNT)), unpartitioned());

        // no bounds for column a, and the column is not all nulls, so the bounds are unknown rather than absent
        IcebergStatistics.Builder other = new IcebergStatistics.Builder(COLUMNS, COLUMN_TYPES, TESTING_TYPE_MANAGER);
        other.acceptDataFile(dataFile(ImmutableMap.of(), ImmutableMap.of(), perColumn(COLUMN_A, 0, COLUMN_B, RECORD_COUNT)), unpartitioned());

        target.merge(other);

        IcebergStatistics statistics = target.build();
        assertThat(statistics.minValues()).doesNotContainKey(COLUMN_A_ID);
        assertThat(statistics.maxValues()).doesNotContainKey(COLUMN_A_ID);
    }

    @Test
    void testMergeKeepsBoundsInvalidated()
    {
        IcebergStatistics.Builder target = new IcebergStatistics.Builder(COLUMNS, COLUMN_TYPES, TESTING_TYPE_MANAGER);
        target.acceptDataFile(dataFile(ImmutableMap.of(), ImmutableMap.of(), perColumn(COLUMN_A, 0, COLUMN_B, RECORD_COUNT)), unpartitioned());

        IcebergStatistics.Builder other = new IcebergStatistics.Builder(COLUMNS, COLUMN_TYPES, TESTING_TYPE_MANAGER);
        other.acceptDataFile(dataFile(bounds(COLUMN_A, 1L), bounds(COLUMN_A, 5L), perColumn(COLUMN_A, 0, COLUMN_B, RECORD_COUNT)), unpartitioned());

        target.merge(other);

        IcebergStatistics statistics = target.build();
        assertThat(statistics.minValues()).doesNotContainKey(COLUMN_A_ID);
        assertThat(statistics.maxValues()).doesNotContainKey(COLUMN_A_ID);
    }

    @Test
    void testMergeCopiesNewColumn()
    {
        // column b is all nulls here, so the target never records statistics for it
        IcebergStatistics.Builder target = new IcebergStatistics.Builder(COLUMNS, COLUMN_TYPES, TESTING_TYPE_MANAGER);
        target.acceptDataFile(dataFile(bounds(COLUMN_A, 1L), bounds(COLUMN_A, 5L), perColumn(COLUMN_A, 0, COLUMN_B, RECORD_COUNT)), unpartitioned());

        IcebergStatistics.Builder other = new IcebergStatistics.Builder(COLUMNS, COLUMN_TYPES, TESTING_TYPE_MANAGER);
        other.acceptDataFile(dataFile(bounds(COLUMN_B, 2.5), bounds(COLUMN_B, 7.5), perColumn(COLUMN_A, RECORD_COUNT, COLUMN_B, 0)), unpartitioned());

        target.merge(other);

        assertThat(target.build().minValues()).isEqualTo(ImmutableMap.of(COLUMN_A_ID, 1L, COLUMN_B_ID, 2.5));
        assertThat(target.build().maxValues()).isEqualTo(ImmutableMap.of(COLUMN_A_ID, 5L, COLUMN_B_ID, 7.5));

        // the copy is defensive, so later updates to other do not reach the merged statistics
        other.acceptDataFile(dataFile(bounds(COLUMN_B, 0.5), bounds(COLUMN_B, 9.5), perColumn(COLUMN_A, RECORD_COUNT, COLUMN_B, 0)), unpartitioned());
        assertThat(target.build().minValues()).containsEntry(COLUMN_B_ID, 2.5);
        assertThat(target.build().maxValues()).containsEntry(COLUMN_B_ID, 7.5);
    }

    @Test
    void testMergeCopiesInvalidatedColumn()
    {
        IcebergStatistics.Builder target = new IcebergStatistics.Builder(COLUMNS, COLUMN_TYPES, TESTING_TYPE_MANAGER);
        target.acceptDataFile(dataFile(bounds(COLUMN_A, 1L), bounds(COLUMN_A, 5L), perColumn(COLUMN_A, 0, COLUMN_B, RECORD_COUNT)), unpartitioned());

        // column b has no bounds and is not all nulls, so other holds an invalidated entry for it
        IcebergStatistics.Builder other = new IcebergStatistics.Builder(COLUMNS, COLUMN_TYPES, TESTING_TYPE_MANAGER);
        other.acceptDataFile(dataFile(ImmutableMap.of(), ImmutableMap.of(), perColumn(COLUMN_A, RECORD_COUNT, COLUMN_B, 0)), unpartitioned());

        target.merge(other);

        assertThat(target.build().minValues()).isEqualTo(ImmutableMap.of(COLUMN_A_ID, 1L));
        assertThat(target.build().maxValues()).isEqualTo(ImmutableMap.of(COLUMN_A_ID, 5L));

        // the copied entry is invalidated rather than absent, so bounds from a later file cannot revive it
        target.acceptDataFile(dataFile(bounds(COLUMN_B, 2.5), bounds(COLUMN_B, 7.5), perColumn(COLUMN_A, RECORD_COUNT, COLUMN_B, 0)), unpartitioned());
        assertThat(target.build().minValues()).doesNotContainKey(COLUMN_B_ID);
        assertThat(target.build().maxValues()).doesNotContainKey(COLUMN_B_ID);
    }

    @Test
    void testMergeSumsMetrics()
    {
        IcebergStatistics.Builder target = new IcebergStatistics.Builder(COLUMNS, COLUMN_TYPES, TESTING_TYPE_MANAGER);
        target.acceptDataFile(
                dataFile(bounds(COLUMN_A, 1L), bounds(COLUMN_A, 5L), perColumn(COLUMN_A, 1, COLUMN_B, 2), perColumn(COLUMN_B, 3), perColumn(COLUMN_A, 64, COLUMN_B, 128)),
                unpartitioned());

        IcebergStatistics.Builder other = new IcebergStatistics.Builder(COLUMNS, COLUMN_TYPES, TESTING_TYPE_MANAGER);
        other.acceptDataFile(
                dataFile(bounds(COLUMN_A, 2L), bounds(COLUMN_A, 9L), perColumn(COLUMN_A, 4, COLUMN_B, 0), perColumn(COLUMN_B, 5), perColumn(COLUMN_A, 32)),
                unpartitioned());

        target.merge(other);

        IcebergStatistics statistics = target.build();
        assertThat(statistics.recordCount()).isEqualTo(2 * RECORD_COUNT);
        assertThat(statistics.fileCount()).isEqualTo(2);
        assertThat(statistics.size()).isEqualTo(2 * FILE_SIZE);
        assertThat(statistics.nullCounts()).isEqualTo(ImmutableMap.of(COLUMN_A_ID, 5L, COLUMN_B_ID, 2L));
        assertThat(statistics.nanCounts()).isEqualTo(ImmutableMap.of(COLUMN_B_ID, 8L));
        assertThat(statistics.columnSizes()).isEqualTo(ImmutableMap.of(COLUMN_A_ID, 96L, COLUMN_B_ID, 128L));
        assertThat(statistics.minValues()).containsEntry(COLUMN_A_ID, 1L);
        assertThat(statistics.maxValues()).containsEntry(COLUMN_A_ID, 9L);
    }

    @Test
    void testMergeRejectsDifferentColumns()
    {
        IcebergStatistics.Builder target = new IcebergStatistics.Builder(COLUMNS, COLUMN_TYPES, TESTING_TYPE_MANAGER);
        IcebergStatistics.Builder other = new IcebergStatistics.Builder(ImmutableList.of(COLUMN_A), ImmutableList.of(BIGINT), TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> target.merge(other))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot merge statistics collected for different columns");
    }

    private static DataFile dataFile(Map<Integer, ByteBuffer> lowerBounds, Map<Integer, ByteBuffer> upperBounds, Map<Integer, Long> nullValueCounts)
    {
        return dataFile(lowerBounds, upperBounds, nullValueCounts, ImmutableMap.of(), ImmutableMap.of());
    }

    private static DataFile dataFile(
            Map<Integer, ByteBuffer> lowerBounds,
            Map<Integer, ByteBuffer> upperBounds,
            Map<Integer, Long> nullValueCounts,
            Map<Integer, Long> nanValueCounts,
            Map<Integer, Long> columnSizes)
    {
        Metrics metrics = new Metrics(
                RECORD_COUNT,
                columnSizes,
                ImmutableMap.of(),
                nullValueCounts,
                nanValueCounts,
                lowerBounds,
                upperBounds);
        return DataFiles.builder(unpartitioned())
                .withPath("/test/data.parquet")
                .withFormat(PARQUET)
                .withFileSizeInBytes(FILE_SIZE)
                .withRecordCount(RECORD_COUNT)
                .withMetrics(metrics)
                .build();
    }

    private static Map<Integer, ByteBuffer> bounds(Types.NestedField column, Object value)
    {
        return ImmutableMap.of(column.fieldId(), Conversions.toByteBuffer(column.type(), value));
    }

    private static Map<Integer, Long> perColumn(Types.NestedField column, long value)
    {
        return ImmutableMap.of(column.fieldId(), value);
    }

    private static Map<Integer, Long> perColumn(Types.NestedField column, long value, Types.NestedField otherColumn, long otherValue)
    {
        return ImmutableMap.of(column.fieldId(), value, otherColumn.fieldId(), otherValue);
    }
}
