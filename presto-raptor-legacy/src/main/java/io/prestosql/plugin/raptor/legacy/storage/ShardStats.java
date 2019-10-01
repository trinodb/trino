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
package io.prestosql.plugin.raptor.legacy.storage;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcPredicate;
import io.prestosql.orc.OrcReader;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.plugin.raptor.legacy.metadata.ColumnStats;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.VarcharType;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.prestosql.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_ERROR;
import static io.prestosql.plugin.raptor.legacy.storage.OrcStorageManager.toOrcFileType;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static org.joda.time.DateTimeZone.UTC;

public final class ShardStats
{
    /**
     * Maximum length of a binary value stored in an index.
     */
    public static final int MAX_BINARY_INDEX_SIZE = 100;

    private ShardStats() {}

    public static Slice truncateIndexValue(Slice slice)
    {
        if (slice.length() > MAX_BINARY_INDEX_SIZE) {
            return slice.slice(0, MAX_BINARY_INDEX_SIZE);
        }
        return slice;
    }

    public static Optional<ColumnStats> computeColumnStats(OrcReader orcReader, long columnId, Type type, TypeManager typeManager)
            throws IOException
    {
        return Optional.ofNullable(doComputeColumnStats(orcReader, columnId, type, typeManager));
    }

    private static ColumnStats doComputeColumnStats(OrcReader orcReader, long columnId, Type type, TypeManager typeManager)
            throws IOException
    {
        OrcColumn column = getColumn(orcReader.getRootColumn().getNestedColumns(), columnId);
        Type columnType = toOrcFileType(type, typeManager);
        OrcRecordReader reader = orcReader.createRecordReader(
                ImmutableList.of(column),
                ImmutableList.of(columnType),
                OrcPredicate.TRUE,
                UTC,
                newSimpleAggregatedMemoryContext(),
                INITIAL_BATCH_SIZE,
                exception -> new PrestoException(RAPTOR_ERROR, "Error reading column: " + columnId, exception));

        if (type.equals(BooleanType.BOOLEAN)) {
            return indexBoolean(reader, columnId);
        }
        if (type.equals(BigintType.BIGINT) ||
                type.equals(DateType.DATE) ||
                type.equals(TimestampType.TIMESTAMP)) {
            return indexLong(type, reader, columnId);
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return indexDouble(reader, columnId);
        }
        if (type instanceof VarcharType) {
            return indexString(type, reader, columnId);
        }
        return null;
    }

    private static OrcColumn getColumn(List<OrcColumn> columnNames, long columnId)
    {
        String columnName = String.valueOf(columnId);
        return columnNames.stream()
                .filter(column -> column.getColumnName().equals(columnName))
                .findFirst()
                .orElseThrow(() -> new PrestoException(RAPTOR_ERROR, "Missing column ID: " + columnId));
    }

    private static ColumnStats indexBoolean(OrcRecordReader reader, long columnId)
            throws IOException
    {
        boolean minSet = false;
        boolean maxSet = false;
        boolean min = false;
        boolean max = false;

        while (true) {
            Page page = reader.nextPage();
            if (page == null) {
                break;
            }
            Block block = page.getBlock(0).getLoadedBlock();

            for (int i = 0; i < page.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    continue;
                }
                boolean value = BooleanType.BOOLEAN.getBoolean(block, i);
                if (!minSet || Boolean.compare(value, min) < 0) {
                    minSet = true;
                    min = value;
                }
                if (!maxSet || Boolean.compare(value, max) > 0) {
                    maxSet = true;
                    max = value;
                }
            }
        }

        return new ColumnStats(columnId,
                minSet ? min : null,
                maxSet ? max : null);
    }

    private static ColumnStats indexLong(Type type, OrcRecordReader reader, long columnId)
            throws IOException
    {
        boolean minSet = false;
        boolean maxSet = false;
        long min = 0;
        long max = 0;

        while (true) {
            Page page = reader.nextPage();
            if (page == null) {
                break;
            }
            Block block = page.getBlock(0).getLoadedBlock();

            for (int i = 0; i < page.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    continue;
                }
                long value = type.getLong(block, i);
                if (!minSet || (value < min)) {
                    minSet = true;
                    min = value;
                }
                if (!maxSet || (value > max)) {
                    maxSet = true;
                    max = value;
                }
            }
        }

        return new ColumnStats(columnId,
                minSet ? min : null,
                maxSet ? max : null);
    }

    private static ColumnStats indexDouble(OrcRecordReader reader, long columnId)
            throws IOException
    {
        boolean minSet = false;
        boolean maxSet = false;
        double min = 0;
        double max = 0;

        while (true) {
            Page page = reader.nextPage();
            if (page == null) {
                break;
            }
            Block block = page.getBlock(0).getLoadedBlock();

            for (int i = 0; i < page.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    continue;
                }
                double value = DoubleType.DOUBLE.getDouble(block, i);
                if (isNaN(value)) {
                    continue;
                }
                if (value == -0.0) {
                    value = 0.0;
                }
                if (!minSet || (value < min)) {
                    minSet = true;
                    min = value;
                }
                if (!maxSet || (value > max)) {
                    maxSet = true;
                    max = value;
                }
            }
        }

        if (isInfinite(min)) {
            minSet = false;
        }
        if (isInfinite(max)) {
            maxSet = false;
        }

        return new ColumnStats(columnId,
                minSet ? min : null,
                maxSet ? max : null);
    }

    private static ColumnStats indexString(Type type, OrcRecordReader reader, long columnId)
            throws IOException
    {
        boolean minSet = false;
        boolean maxSet = false;
        Slice min = null;
        Slice max = null;

        while (true) {
            Page page = reader.nextPage();
            if (page == null) {
                break;
            }
            Block block = page.getBlock(0).getLoadedBlock();

            for (int i = 0; i < page.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    continue;
                }
                Slice slice = type.getSlice(block, i);
                slice = truncateIndexValue(slice);
                if (!minSet || (slice.compareTo(min) < 0)) {
                    minSet = true;
                    min = slice;
                }
                if (!maxSet || (slice.compareTo(max) > 0)) {
                    maxSet = true;
                    max = slice;
                }
            }
        }

        return new ColumnStats(columnId,
                minSet ? min.toStringUtf8() : null,
                maxSet ? max.toStringUtf8() : null);
    }
}
