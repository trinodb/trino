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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.pinot.client.PinotDataFetcher;
import io.trino.plugin.pinot.client.PinotDataTableWithSize;
import io.trino.plugin.pinot.conversion.PinotTimestamps;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.roaringbitmap.RoaringBitmap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_DECODE_ERROR;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static io.trino.plugin.pinot.decoders.VarbinaryDecoder.toBytes;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToIntBits;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PinotSegmentPageSource
        implements ConnectorPageSource
{
    private final List<PinotColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final long targetSegmentPageSizeBytes;
    private final PinotDataFetcher pinotDataFetcher;

    private long completedBytes;
    private long estimatedMemoryUsageInBytes;
    private PinotDataTableWithSize currentDataTable;
    private boolean closed;

    public PinotSegmentPageSource(
            long targetSegmentPageSizeBytes,
            List<PinotColumnHandle> columnHandles,
            PinotDataFetcher pinotDataFetcher)
    {
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.columnTypes = columnHandles
                .stream()
                .map(PinotColumnHandle::getDataType)
                .collect(Collectors.toList());
        this.targetSegmentPageSizeBytes = targetSegmentPageSizeBytes;
        this.pinotDataFetcher = requireNonNull(pinotDataFetcher, "pinotDataFetcher is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return pinotDataFetcher.getReadTimeNanos();
    }

    @Override
    public long getMemoryUsage()
    {
        return estimatedMemoryUsageInBytes;
    }

    /**
     * @return true if is closed or all Pinot data have been processed.
     */
    @Override
    public boolean isFinished()
    {
        return closed || (pinotDataFetcher.isDataFetched() && pinotDataFetcher.endOfData());
    }

    /**
     * @return constructed page for pinot data.
     */
    @Override
    public SourcePage getNextSourcePage()
    {
        if (isFinished()) {
            close();
            return null;
        }
        if (!pinotDataFetcher.isDataFetched()) {
            pinotDataFetcher.fetchData();
            estimatedMemoryUsageInBytes = pinotDataFetcher.getMemoryUsageBytes();
        }
        if (pinotDataFetcher.endOfData()) {
            close();
            return null;
        }

        long pageSizeBytes = 0L;
        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        while (!pinotDataFetcher.endOfData() && pageSizeBytes < targetSegmentPageSizeBytes) {
            // To reduce memory usage, remove dataTable from dataTableList once it's processed.
            if (currentDataTable != null) {
                estimatedMemoryUsageInBytes -= currentDataTable.estimatedSizeInBytes();
            }
            currentDataTable = pinotDataFetcher.getNextDataTable();
            estimatedMemoryUsageInBytes += currentDataTable.estimatedSizeInBytes();
            pageSizeBytes += currentDataTable.estimatedSizeInBytes();
            pageBuilder.declarePositions(currentDataTable.dataTable().getNumberOfRows());
            Map<Integer, RoaringBitmap> nullRowIds = buildColumnIdToNullRowId(currentDataTable.dataTable(), columnHandles);
            for (int rowIndex = 0; rowIndex < currentDataTable.dataTable().getNumberOfRows(); rowIndex++) {
                for (int columnHandleIdx = 0; columnHandleIdx < columnHandles.size(); columnHandleIdx++) {
                    BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(columnHandleIdx);
                    Type columnType = columnTypes.get(columnHandleIdx);
                    // Write a block for each column in the original order.
                    if (nullRowIds.containsKey(columnHandleIdx) && nullRowIds.get(columnHandleIdx).contains(rowIndex)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        writeBlock(blockBuilder, columnType, rowIndex, columnHandleIdx);
                    }
                }
            }
        }

        return SourcePage.create(pageBuilder.build());
    }

    private static Map<Integer, RoaringBitmap> buildColumnIdToNullRowId(DataTable dataTable, List<PinotColumnHandle> columnHandles)
    {
        ImmutableMap.Builder<Integer, RoaringBitmap> nullRowIds = ImmutableMap.builder();
        for (int i = 0; i < columnHandles.size(); i++) {
            RoaringBitmap nullRowId = dataTable.getNullRowIds(i);
            if (nullRowId != null) {
                nullRowIds.put(i, nullRowId);
            }
        }
        return nullRowIds.buildOrThrow();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
    }

    /**
     * Generates the {@link io.trino.spi.block.Block} for the specific column from the {@link #currentDataTable}.
     *
     * <p>Based on the original Pinot column types, write as Trino-supported values to {@link io.trino.spi.block.BlockBuilder}, e.g.
     * FLOAT -> Double, INT -> Long, String -> Slice.
     *
     * @param blockBuilder blockBuilder for the current column
     * @param columnType type of the column
     * @param rowIdx row index
     * @param columnIdx column index
     */

    private void writeBlock(BlockBuilder blockBuilder, Type columnType, int rowIdx, int columnIdx)
    {
        Class<?> javaType = columnType.getJavaType();
        DataSchema.ColumnDataType pinotColumnType = currentDataTable.dataTable().getDataSchema().getColumnDataType(columnIdx);
        if (javaType.equals(boolean.class)) {
            writeBooleanBlock(blockBuilder, columnType, rowIdx, columnIdx);
        }
        else if (javaType.equals(long.class)) {
            if (columnType instanceof TimestampType) {
                // Pinot TimestampType is always ShortTimestampType.
                writeShortTimestampBlock(blockBuilder, columnType, rowIdx, columnIdx);
            }
            else {
                writeLongBlock(blockBuilder, columnType, rowIdx, columnIdx);
            }
        }
        else if (javaType.equals(double.class)) {
            writeDoubleBlock(blockBuilder, columnType, rowIdx, columnIdx);
        }
        else if (javaType.equals(Slice.class)) {
            writeSliceBlock(blockBuilder, columnType, rowIdx, columnIdx);
        }
        else if (javaType.equals(Block.class)) {
            writeArrayBlock(blockBuilder, columnType, rowIdx, columnIdx);
        }
        else {
            throw new TrinoException(
                    PINOT_UNSUPPORTED_COLUMN_TYPE,
                    format(
                            "Failed to write column %s. pinotColumnType %s, javaType %s",
                            columnHandles.get(columnIdx).getColumnName(), pinotColumnType, javaType));
        }
    }

    private void writeBooleanBlock(BlockBuilder blockBuilder, Type columnType, int rowIndex, int columnIndex)
    {
        columnType.writeBoolean(blockBuilder, getBoolean(rowIndex, columnIndex));
        completedBytes++;
    }

    private void writeLongBlock(BlockBuilder blockBuilder, Type columnType, int rowIndex, int columnIndex)
    {
        columnType.writeLong(blockBuilder, getLong(rowIndex, columnIndex));
        completedBytes += Long.BYTES;
    }

    private void writeDoubleBlock(BlockBuilder blockBuilder, Type columnType, int rowIndex, int columnIndex)
    {
        columnType.writeDouble(blockBuilder, getDouble(rowIndex, columnIndex));
        completedBytes += Double.BYTES;
    }

    private void writeSliceBlock(BlockBuilder blockBuilder, Type columnType, int rowIndex, int columnIndex)
    {
        Slice slice = getSlice(rowIndex, columnIndex);
        columnType.writeSlice(blockBuilder, slice, 0, slice.length());
        completedBytes += slice.getBytes().length;
    }

    private void writeArrayBlock(BlockBuilder blockBuilder, Type columnType, int rowIndex, int columnIndex)
    {
        Block block = getArrayBlock(rowIndex, columnIndex);
        columnType.writeObject(blockBuilder, block);
        completedBytes += block.getSizeInBytes();
    }

    private void writeShortTimestampBlock(BlockBuilder blockBuilder, Type columnType, int rowIndex, int columnIndex)
    {
        // Trino is using micros since epoch for ShortTimestampType, Pinot uses millis since epoch.
        columnType.writeLong(blockBuilder, PinotTimestamps.toMicros(getLong(rowIndex, columnIndex)));
        completedBytes += Long.BYTES;
    }

    private Type getType(int columnIndex)
    {
        checkArgument(columnIndex < columnHandles.size(), "Invalid field index");
        return columnHandles.get(columnIndex).getDataType();
    }

    private boolean getBoolean(int rowIdx, int columnIndex)
    {
        return currentDataTable.dataTable().getInt(rowIdx, columnIndex) != 0;
    }

    private long getLong(int rowIndex, int columnIndex)
    {
        DataSchema.ColumnDataType dataType = currentDataTable.dataTable().getDataSchema().getColumnDataType(columnIndex);
        // Note columnType in the dataTable could be different from the original columnType in the columnHandle.
        // e.g. when original column type is int/long and aggregation value is requested, the returned dataType from Pinot would be double.
        // So need to cast it back to the original columnType.
        return switch (dataType) {
            case DOUBLE -> (long) currentDataTable.dataTable().getDouble(rowIndex, columnIndex);
            case INT -> currentDataTable.dataTable().getInt(rowIndex, columnIndex);
            case FLOAT -> floatToIntBits(currentDataTable.dataTable().getFloat(rowIndex, columnIndex));
            case LONG, TIMESTAMP -> currentDataTable.dataTable().getLong(rowIndex, columnIndex);
            default -> throw new PinotException(PINOT_DECODE_ERROR, Optional.empty(), format("Unexpected pinot type: '%s'", dataType));
        };
    }

    private double getDouble(int rowIndex, int columnIndex)
    {
        DataSchema.ColumnDataType dataType = currentDataTable.dataTable().getDataSchema().getColumnDataType(columnIndex);
        if (dataType.equals(ColumnDataType.FLOAT)) {
            return currentDataTable.dataTable().getFloat(rowIndex, columnIndex);
        }
        return currentDataTable.dataTable().getDouble(rowIndex, columnIndex);
    }

    private Block getArrayBlock(int rowIndex, int columnIndex)
    {
        Type trinoType = getType(columnIndex);
        Type elementType = trinoType.getTypeParameters().get(0);
        DataSchema.ColumnDataType columnType = currentDataTable.dataTable().getDataSchema().getColumnDataType(columnIndex);
        BlockBuilder blockBuilder;
        switch (columnType) {
            case INT_ARRAY:
                int[] intArray = currentDataTable.dataTable().getIntArray(rowIndex, columnIndex);
                blockBuilder = elementType.createBlockBuilder(null, intArray.length);
                for (int element : intArray) {
                    INTEGER.writeInt(blockBuilder, element);
                }
                break;
            case LONG_ARRAY:
                long[] longArray = currentDataTable.dataTable().getLongArray(rowIndex, columnIndex);
                blockBuilder = elementType.createBlockBuilder(null, longArray.length);
                for (long element : longArray) {
                    BIGINT.writeLong(blockBuilder, element);
                }
                break;
            case FLOAT_ARRAY:
                float[] floatArray = currentDataTable.dataTable().getFloatArray(rowIndex, columnIndex);
                blockBuilder = elementType.createBlockBuilder(null, floatArray.length);
                for (float element : floatArray) {
                    REAL.writeFloat(blockBuilder, element);
                }
                break;
            case DOUBLE_ARRAY:
                double[] doubleArray = currentDataTable.dataTable().getDoubleArray(rowIndex, columnIndex);
                blockBuilder = elementType.createBlockBuilder(null, doubleArray.length);
                for (double element : doubleArray) {
                    elementType.writeDouble(blockBuilder, element);
                }
                break;
            case STRING_ARRAY:
                String[] stringArray = currentDataTable.dataTable().getStringArray(rowIndex, columnIndex);
                blockBuilder = elementType.createBlockBuilder(null, stringArray.length);
                for (String element : stringArray) {
                    Slice slice = getUtf8Slice(element);
                    elementType.writeSlice(blockBuilder, slice, 0, slice.length());
                }
                break;
            default:
                throw new UnsupportedOperationException(format("Unexpected pinot type '%s'", columnType));
        }
        return blockBuilder.build();
    }

    private Slice getSlice(int rowIndex, int columnIndex)
    {
        Type trinoType = getType(columnIndex);
        DataTable dataTable = currentDataTable.dataTable();

        if (trinoType instanceof VarcharType) {
            String field = dataTable.getString(rowIndex, columnIndex);
            return getUtf8Slice(field);
        }
        if (trinoType instanceof VarbinaryType) {
            // Pinot 0.11.0 and 0.12.1 default to use V3 data table for server response.
            // Pinot 1.0.0 and above default to use V4 data table.
            // Pinot v4 data table uses variable length encoding for bytes instead of hex string representation in v3.
            // In order to change the data table version, users need to explicitly set:
            // `pinot.server.instance.currentDataTableVersion=3` in pinot server config.
            if (dataTable.getVersion() >= 4) {
                try {
                    return Slices.wrappedBuffer(dataTable.getBytes(rowIndex, columnIndex).getBytes());
                }
                catch (NullPointerException e) {
                    // Pinot throws NPE when the entry is null.
                    return Slices.wrappedBuffer();
                }
            }
            return Slices.wrappedBuffer(toBytes(dataTable.getString(rowIndex, columnIndex)));
        }
        if (trinoType.getTypeSignature().getBase().equalsIgnoreCase(StandardTypes.JSON)) {
            String field = dataTable.getString(rowIndex, columnIndex);
            return jsonParse(getUtf8Slice(field));
        }
        return Slices.EMPTY_SLICE;
    }

    private Slice getUtf8Slice(String value)
    {
        if (isNullOrEmpty(value)) {
            return Slices.EMPTY_SLICE;
        }
        return Slices.utf8Slice(value);
    }
}
