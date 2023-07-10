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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.pinot.client.PinotDataFetcher;
import io.trino.plugin.pinot.client.PinotDataTableWithSize;
import io.trino.plugin.pinot.conversion.PinotTimestamps;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;

import java.util.List;
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
    private long completedBytes;
    private long estimatedMemoryUsageInBytes;
    private PinotDataTableWithSize currentDataTable;
    private boolean closed;
    private long targetSegmentPageSizeBytes;
    private PinotDataFetcher pinotDataFetcher;

    public PinotSegmentPageSource(
            long targetSegmentPageSizeBytes,
            List<PinotColumnHandle> columnHandles,
            PinotDataFetcher pinotDataFetcher)
    {
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.columnTypes = columnHandles
                .stream()
                .map(columnHandle -> columnHandle.getDataType())
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
    public Page getNextPage()
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
                estimatedMemoryUsageInBytes -= currentDataTable.getEstimatedSizeInBytes();
            }
            currentDataTable = pinotDataFetcher.getNextDataTable();
            estimatedMemoryUsageInBytes += currentDataTable.getEstimatedSizeInBytes();
            pageSizeBytes += currentDataTable.getEstimatedSizeInBytes();
            pageBuilder.declarePositions(currentDataTable.getDataTable().getNumberOfRows());
            for (int columnHandleIdx = 0; columnHandleIdx < columnHandles.size(); columnHandleIdx++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(columnHandleIdx);
                Type columnType = columnTypes.get(columnHandleIdx);
                // Write a block for each column in the original order.
                writeBlock(blockBuilder, columnType, columnHandleIdx);
            }
        }

        return pageBuilder.build();
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
     * @param columnIdx column index
     */

    private void writeBlock(BlockBuilder blockBuilder, Type columnType, int columnIdx)
    {
        Class<?> javaType = columnType.getJavaType();
        DataSchema.ColumnDataType pinotColumnType = currentDataTable.getDataTable().getDataSchema().getColumnDataType(columnIdx);
        if (javaType.equals(boolean.class)) {
            writeBooleanBlock(blockBuilder, columnType, columnIdx);
        }
        else if (javaType.equals(long.class)) {
            if (columnType instanceof TimestampType) {
                // Pinot TimestampType is always ShortTimestampType.
                writeShortTimestampBlock(blockBuilder, columnType, columnIdx);
            }
            else {
                writeLongBlock(blockBuilder, columnType, columnIdx);
            }
        }
        else if (javaType.equals(double.class)) {
            writeDoubleBlock(blockBuilder, columnType, columnIdx);
        }
        else if (javaType.equals(Slice.class)) {
            writeSliceBlock(blockBuilder, columnType, columnIdx);
        }
        else if (javaType.equals(Block.class)) {
            writeArrayBlock(blockBuilder, columnType, columnIdx);
        }
        else {
            throw new TrinoException(
                    PINOT_UNSUPPORTED_COLUMN_TYPE,
                    format(
                            "Failed to write column %s. pinotColumnType %s, javaType %s",
                            columnHandles.get(columnIdx).getColumnName(), pinotColumnType, javaType));
        }
    }

    private void writeBooleanBlock(BlockBuilder blockBuilder, Type columnType, int columnIndex)
    {
        for (int i = 0; i < currentDataTable.getDataTable().getNumberOfRows(); i++) {
            columnType.writeBoolean(blockBuilder, getBoolean(i, columnIndex));
            completedBytes++;
        }
    }

    private void writeLongBlock(BlockBuilder blockBuilder, Type columnType, int columnIndex)
    {
        for (int i = 0; i < currentDataTable.getDataTable().getNumberOfRows(); i++) {
            columnType.writeLong(blockBuilder, getLong(i, columnIndex));
            completedBytes += Long.BYTES;
        }
    }

    private void writeDoubleBlock(BlockBuilder blockBuilder, Type columnType, int columnIndex)
    {
        for (int i = 0; i < currentDataTable.getDataTable().getNumberOfRows(); i++) {
            columnType.writeDouble(blockBuilder, getDouble(i, columnIndex));
            completedBytes += Double.BYTES;
        }
    }

    private void writeSliceBlock(BlockBuilder blockBuilder, Type columnType, int columnIndex)
    {
        for (int i = 0; i < currentDataTable.getDataTable().getNumberOfRows(); i++) {
            Slice slice = getSlice(i, columnIndex);
            columnType.writeSlice(blockBuilder, slice, 0, slice.length());
            completedBytes += slice.getBytes().length;
        }
    }

    private void writeArrayBlock(BlockBuilder blockBuilder, Type columnType, int columnIndex)
    {
        for (int i = 0; i < currentDataTable.getDataTable().getNumberOfRows(); i++) {
            Block block = getArrayBlock(i, columnIndex);
            columnType.writeObject(blockBuilder, block);
            completedBytes += block.getSizeInBytes();
        }
    }

    private void writeShortTimestampBlock(BlockBuilder blockBuilder, Type columnType, int columnIndex)
    {
        for (int i = 0; i < currentDataTable.getDataTable().getNumberOfRows(); i++) {
            // Trino is using micros since epoch for ShortTimestampType, Pinot uses millis since epoch.
            columnType.writeLong(blockBuilder, PinotTimestamps.toMicros(getLong(i, columnIndex)));
            completedBytes += Long.BYTES;
        }
    }

    private Type getType(int columnIndex)
    {
        checkArgument(columnIndex < columnHandles.size(), "Invalid field index");
        return columnHandles.get(columnIndex).getDataType();
    }

    private boolean getBoolean(int rowIdx, int columnIndex)
    {
        return currentDataTable.getDataTable().getInt(rowIdx, columnIndex) != 0;
    }

    private long getLong(int rowIndex, int columnIndex)
    {
        DataSchema.ColumnDataType dataType = currentDataTable.getDataTable().getDataSchema().getColumnDataType(columnIndex);
        // Note columnType in the dataTable could be different from the original columnType in the columnHandle.
        // e.g. when original column type is int/long and aggregation value is requested, the returned dataType from Pinot would be double.
        // So need to cast it back to the original columnType.
        switch (dataType) {
            case DOUBLE:
                return (long) currentDataTable.getDataTable().getDouble(rowIndex, columnIndex);
            case INT:
                return currentDataTable.getDataTable().getInt(rowIndex, columnIndex);
            case FLOAT:
                return floatToIntBits(currentDataTable.getDataTable().getFloat(rowIndex, columnIndex));
            case LONG:
            case TIMESTAMP:
                return currentDataTable.getDataTable().getLong(rowIndex, columnIndex);
            default:
                throw new PinotException(PINOT_DECODE_ERROR, Optional.empty(), format("Unexpected pinot type: '%s'", dataType));
        }
    }

    private double getDouble(int rowIndex, int columnIndex)
    {
        DataSchema.ColumnDataType dataType = currentDataTable.getDataTable().getDataSchema().getColumnDataType(columnIndex);
        if (dataType.equals(ColumnDataType.FLOAT)) {
            return currentDataTable.getDataTable().getFloat(rowIndex, columnIndex);
        }
        return currentDataTable.getDataTable().getDouble(rowIndex, columnIndex);
    }

    private Block getArrayBlock(int rowIndex, int columnIndex)
    {
        Type trinoType = getType(columnIndex);
        Type elementType = trinoType.getTypeParameters().get(0);
        DataSchema.ColumnDataType columnType = currentDataTable.getDataTable().getDataSchema().getColumnDataType(columnIndex);
        BlockBuilder blockBuilder;
        switch (columnType) {
            case INT_ARRAY:
                int[] intArray = currentDataTable.getDataTable().getIntArray(rowIndex, columnIndex);
                blockBuilder = elementType.createBlockBuilder(null, intArray.length);
                for (int element : intArray) {
                    INTEGER.writeInt(blockBuilder, element);
                }
                break;
            case LONG_ARRAY:
                long[] longArray = currentDataTable.getDataTable().getLongArray(rowIndex, columnIndex);
                blockBuilder = elementType.createBlockBuilder(null, longArray.length);
                for (long element : longArray) {
                    BIGINT.writeLong(blockBuilder, element);
                }
                break;
            case FLOAT_ARRAY:
                float[] floatArray = currentDataTable.getDataTable().getFloatArray(rowIndex, columnIndex);
                blockBuilder = elementType.createBlockBuilder(null, floatArray.length);
                for (float element : floatArray) {
                    REAL.writeFloat(blockBuilder, element);
                }
                break;
            case DOUBLE_ARRAY:
                double[] doubleArray = currentDataTable.getDataTable().getDoubleArray(rowIndex, columnIndex);
                blockBuilder = elementType.createBlockBuilder(null, doubleArray.length);
                for (double element : doubleArray) {
                    elementType.writeDouble(blockBuilder, element);
                }
                break;
            case STRING_ARRAY:
                String[] stringArray = currentDataTable.getDataTable().getStringArray(rowIndex, columnIndex);
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
        if (trinoType instanceof VarcharType) {
            String field = currentDataTable.getDataTable().getString(rowIndex, columnIndex);
            return getUtf8Slice(field);
        }
        if (trinoType instanceof VarbinaryType) {
            return Slices.wrappedBuffer(toBytes(currentDataTable.getDataTable().getString(rowIndex, columnIndex)));
        }
        if (trinoType.getTypeSignature().getBase() == StandardTypes.JSON) {
            String field = currentDataTable.getDataTable().getString(rowIndex, columnIndex);
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
