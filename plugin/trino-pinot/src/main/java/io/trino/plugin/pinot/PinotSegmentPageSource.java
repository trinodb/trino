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
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.pinot.client.PinotQueryClient;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.transport.ServerInstance;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_DECODE_ERROR;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_EXCEPTION;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static java.lang.Float.floatToIntBits;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PinotSegmentPageSource
        implements ConnectorPageSource
{
    private static final Logger LOG = Logger.get(PinotSegmentPageSource.class);

    private final List<PinotColumnHandle> columnHandles;
    private final PinotSplit split;
    private final PinotQueryClient pinotQueryClient;
    private final ConnectorSession session;
    private final String query;
    private final int limitForSegmentQueries;
    private final AtomicLong currentRowCount = new AtomicLong();
    private final int estimatedNonNumericColumnSize;

    private List<Type> columnTypes;
    // dataTableList stores the dataTable returned from each server. Each dataTable is constructed to a Page, and then destroyed to save memory.
    private LinkedList<PinotDataTableWithSize> dataTableList = new LinkedList<>();
    private long completedBytes;
    private long readTimeNanos;
    private long estimatedMemoryUsageInBytes;
    private PinotDataTableWithSize currentDataTable;
    private boolean closed;
    private boolean isPinotDataFetched;

    public PinotSegmentPageSource(
            ConnectorSession session,
            int estimatedNonNumericColumnSize,
            int limitForSegmentQueries,
            PinotQueryClient pinotQueryClient,
            PinotSplit split,
            List<PinotColumnHandle> columnHandles,
            String query)
    {
        this.limitForSegmentQueries = limitForSegmentQueries;
        this.estimatedNonNumericColumnSize = estimatedNonNumericColumnSize;
        this.split = requireNonNull(split, "split is null");
        this.pinotQueryClient = requireNonNull(pinotQueryClient, "pinotQueryClient is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.session = requireNonNull(session, "session is null");
        this.query = requireNonNull(query, "query is null");
    }

    private static void checkExceptions(DataTable dataTable, PinotSplit split, String query)
    {
        Map<String, String> metadata = dataTable.getMetadata();
        List<String> exceptions = new ArrayList<>();
        metadata.forEach((k, v) -> {
            if (k.startsWith(DataTable.EXCEPTION_METADATA_KEY)) {
                exceptions.add(v);
            }
        });
        if (!exceptions.isEmpty()) {
            throw new PinotException(PinotErrorCode.PINOT_EXCEPTION, Optional.of(query), format("Encountered %d pinot exceptions for split %s: %s", exceptions.size(), split, exceptions));
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
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
        return closed || (isPinotDataFetched && dataTableList.isEmpty());
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
        if (!isPinotDataFetched) {
            fetchPinotData();
        }
        // To reduce memory usage, remove dataTable from dataTableList once it's processed.
        if (currentDataTable != null) {
            estimatedMemoryUsageInBytes -= currentDataTable.getEstimatedSizeInBytes();
        }
        if (dataTableList.size() == 0) {
            close();
            return null;
        }
        currentDataTable = dataTableList.pop();

        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        // Note that declared positions in the Page should be the same with number of rows in each Block
        pageBuilder.declarePositions(currentDataTable.getDataTable().getNumberOfRows());
        for (int columnHandleIdx = 0; columnHandleIdx < columnHandles.size(); columnHandleIdx++) {
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(columnHandleIdx);
            Type columnType = columnTypes.get(columnHandleIdx);
            // Write a block for each column in the original order.
            writeBlock(blockBuilder, columnType, columnHandleIdx);
        }

        return pageBuilder.build();
    }

    /**
     * Fetch data from Pinot for the current split and store the data returned from each Pinot server.
     */
    private void fetchPinotData()
    {
        long startTimeNanos = System.nanoTime();
        try {
            Map<ServerInstance, DataTable> dataTableMap = queryPinot();
            dataTableMap.values().stream()
                    // ignore empty tables and tables with 0 rows
                    .filter(table -> table != null && table.getNumberOfRows() > 0)
                    .forEach(dataTable ->
                    {
                        checkExceptions(dataTable, split, query);
                        checkTooManyRows(dataTable);
                        // Store each dataTable which will later be constructed into Pages.
                        // Also update estimatedMemoryUsage, mostly represented by the size of all dataTables, using numberOfRows and fieldTypes combined as an estimate
                        int estimatedTableSizeInBytes = IntStream.rangeClosed(0, dataTable.getDataSchema().size() - 1)
                                .map(i -> getEstimatedColumnSizeInBytes(dataTable.getDataSchema().getColumnDataType(i)) * dataTable.getNumberOfRows())
                                .reduce(0, Integer::sum);
                        dataTableList.add(new PinotDataTableWithSize(dataTable, estimatedTableSizeInBytes));
                        estimatedMemoryUsageInBytes += estimatedTableSizeInBytes;
                    });

            this.columnTypes = columnHandles
                    .stream()
                    .map(columnHandle -> columnHandle.getDataType())
                    .collect(Collectors.toList());
            isPinotDataFetched = true;
        }
        finally {
            readTimeNanos += System.nanoTime() - startTimeNanos;
        }
    }

    private void checkTooManyRows(DataTable dataTable)
    {
        if (currentRowCount.addAndGet(dataTable.getNumberOfRows()) > limitForSegmentQueries) {
            throw new PinotException(PINOT_EXCEPTION, Optional.of(query), format("Segment query returned '%s' rows per split, maximum allowed is '%s' rows.", currentRowCount.get(), limitForSegmentQueries));
        }
    }

    private Map<ServerInstance, DataTable> queryPinot()
    {
        String host = split.getSegmentHost().orElseThrow(() -> new PinotException(PinotErrorCode.PINOT_INVALID_PQL_GENERATED, Optional.empty(), "Expected the segment split to contain the host"));
        LOG.info("Query '%s' on host '%s' for segment splits: %s", query, split.getSegmentHost(), split.getSegments());
        return ImmutableMap.copyOf(
                pinotQueryClient.queryPinotServerForDataTable(
                        query,
                        host,
                        split.getSegments(),
                        PinotSessionProperties.getConnectionTimeout(session).toMillis(),
                        PinotSessionProperties.getPinotRetryCount(session)));
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
            writeLongBlock(blockBuilder, columnType, columnIdx);
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

    Type getType(int columnIndex)
    {
        checkArgument(columnIndex < columnHandles.size(), "Invalid field index");
        return columnHandles.get(columnIndex).getDataType();
    }

    boolean getBoolean(int rowIdx, int columnIndex)
    {
        return currentDataTable.getDataTable().getInt(rowIdx, columnIndex) != 0;
    }

    long getLong(int rowIndex, int columnIndex)
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
                return currentDataTable.getDataTable().getLong(rowIndex, columnIndex);
            default:
                throw new PinotException(PINOT_DECODE_ERROR, Optional.empty(), format("Unexpected pinot type: '%s'", dataType));
        }
    }

    double getDouble(int rowIndex, int columnIndex)
    {
        DataSchema.ColumnDataType dataType = currentDataTable.getDataTable().getDataSchema().getColumnDataType(columnIndex);
        if (dataType.equals(ColumnDataType.FLOAT)) {
            return currentDataTable.getDataTable().getFloat(rowIndex, columnIndex);
        }
        else {
            return currentDataTable.getDataTable().getDouble(rowIndex, columnIndex);
        }
    }

    Block getArrayBlock(int rowIndex, int columnIndex)
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
                    blockBuilder.writeInt(element);
                }
                break;
            case LONG_ARRAY:
                long[] longArray = currentDataTable.getDataTable().getLongArray(rowIndex, columnIndex);
                blockBuilder = elementType.createBlockBuilder(null, longArray.length);
                for (long element : longArray) {
                    blockBuilder.writeLong(element);
                }
                break;
            case FLOAT_ARRAY:
                float[] floatArray = currentDataTable.getDataTable().getFloatArray(rowIndex, columnIndex);
                blockBuilder = elementType.createBlockBuilder(null, floatArray.length);
                for (float element : floatArray) {
                    blockBuilder.writeInt(floatToIntBits(element));
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

    Slice getSlice(int rowIndex, int columnIndex)
    {
        Type trinoType = getType(columnIndex);
        if (trinoType instanceof VarcharType) {
            String field = currentDataTable.getDataTable().getString(rowIndex, columnIndex);
            return getUtf8Slice(field);
        }
        else if (trinoType instanceof VarbinaryType) {
            return Slices.wrappedBuffer(toBytes(currentDataTable.getDataTable().getString(rowIndex, columnIndex)));
        }
        return Slices.EMPTY_SLICE;
    }

    static byte[] toBytes(String stringValue)
    {
        try {
            return Hex.decodeHex(stringValue.toCharArray());
        }
        catch (DecoderException e) {
            throw new IllegalArgumentException("Value: " + stringValue + " is not Hex encoded", e);
        }
    }

    Slice getUtf8Slice(String value)
    {
        if (isNullOrEmpty(value)) {
            return Slices.EMPTY_SLICE;
        }
        return Slices.utf8Slice(value);
    }

    /**
     * Get estimated size in bytes for the Pinot column.
     * Deterministic for numeric fields; use estimate for other types to save calculation.
     *
     * @param dataType FieldSpec.dataType for Pinot column.
     * @return estimated size in bytes.
     */
    private int getEstimatedColumnSizeInBytes(DataSchema.ColumnDataType dataType)
    {
        if (dataType.isNumber()) {
            switch (dataType) {
                case LONG:
                    return Long.BYTES;
                case FLOAT:
                    return Float.BYTES;
                case DOUBLE:
                    return Double.BYTES;
                case INT:
                default:
                    return Integer.BYTES;
            }
        }
        return estimatedNonNumericColumnSize;
    }

    private static class PinotDataTableWithSize
    {
        DataTable dataTable;
        int estimatedSizeInBytes;

        PinotDataTableWithSize(DataTable dataTable, int estimatedSizeInBytes)
        {
            this.dataTable = dataTable;
            this.estimatedSizeInBytes = estimatedSizeInBytes;
        }

        DataTable getDataTable()
        {
            return dataTable;
        }

        int getEstimatedSizeInBytes()
        {
            return estimatedSizeInBytes;
        }
    }
}
