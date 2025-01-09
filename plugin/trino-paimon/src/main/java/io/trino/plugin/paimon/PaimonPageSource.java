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
package io.trino.plugin.paimon;

import io.airlift.slice.Slice;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.ArrayValueBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.MapValueBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.RowValueBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.InternalRowUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Trino {@link ConnectorPageSource}.
 */
public class PaimonPageSource
        implements ConnectorPageSource
{
    private static final int ROWS_PER_REQUEST = 4096;

    private final CloseableIterator<InternalRow> iterator;
    private final OptionalLong limit;
    private final PageBuilder pageBuilder;
    private final List<Type> columnTypes;
    private final List<DataType> logicalTypes;
    private final AggregatedMemoryContext memoryUsage;

    private boolean isFinished;
    private long numReturn;
    private long readBytes;
    private long readTimeNanos;

    public PaimonPageSource(
            RecordReader<InternalRow> reader,
            List<ColumnHandle> projectedColumns,
            OptionalLong limit,
            AggregatedMemoryContext memoryUsage)
    {
        this.iterator = reader.toCloseableIterator();
        this.limit = limit;
        this.columnTypes = new ArrayList<>();
        this.logicalTypes = new ArrayList<>();
        for (ColumnHandle handle : projectedColumns) {
            PaimonColumnHandle paimonColumnHandle = (PaimonColumnHandle) handle;
            columnTypes.add(paimonColumnHandle.getTrinoType());
            logicalTypes.add(paimonColumnHandle.logicalType());
        }

        this.memoryUsage = requireNonNull(memoryUsage, "memoryUsage is null");
        this.pageBuilder = new PageBuilder(columnTypes);
    }

    private static void writeSlice(BlockBuilder output, Type type, Object value)
    {
        if (type instanceof VarcharType || type instanceof io.trino.spi.type.CharType) {
            type.writeSlice(output, wrappedBuffer(((BinaryString) value).toBytes()));
        }
        else if (type instanceof VarbinaryType) {
            type.writeSlice(output, wrappedBuffer((byte[]) value));
        }
        else {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private static void writeObject(BlockBuilder output, Type type, Object value)
    {
        if (type instanceof DecimalType decimalType) {
            BigDecimal decimal = ((Decimal) value).toBigDecimal();
            type.writeObject(output, Decimals.encodeScaledValue(decimal, decimalType.getScale()));
        }
        else {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Unhandled type for Object: " + type.getTypeSignature());
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return readBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return isFinished;
    }

    @Override
    public Page getNextPage()
    {
        return ClassLoaderUtils.runWithContextClassLoader(
                () -> {
                    try {
                        return nextPage();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                },
                PaimonPageSource.class.getClassLoader());
    }

    @Override
    public long getMemoryUsage()
    {
        return memoryUsage.getBytes();
    }

    @Nullable
    private Page nextPage()
            throws IOException
    {
        long start = System.nanoTime();
        int count = 0;
        while (count < ROWS_PER_REQUEST && !pageBuilder.isFull()) {
            if (limit.isPresent() && numReturn + count >= limit.getAsLong()) {
                isFinished = true;
                break;
            }

            if (!iterator.hasNext()) {
                isFinished = true;
                break;
            }

            InternalRow row = iterator.next();
            pageBuilder.declarePosition();
            count++;
            for (int i = 0; i < columnTypes.size(); i++) {
                BlockBuilder output = pageBuilder.getBlockBuilder(i);
                appendTo(
                        columnTypes.get(i),
                        logicalTypes.get(i),
                        InternalRowUtils.get(row, i, logicalTypes.get(i)),
                        output);
            }
        }

        if (count == 0) {
            return null;
        }
        numReturn += count;
        Page page = pageBuilder.build();
        readBytes += page.getSizeInBytes();
        readTimeNanos += System.nanoTime() - start;
        pageBuilder.reset();
        return page;
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            this.iterator.close();
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    protected void appendTo(Type type, DataType logicalType, Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            type.writeBoolean(output, (Boolean) value);
        }
        else if (javaType == long.class) {
            if (type.equals(BIGINT)
                    || type.equals(INTEGER)
                    || type.equals(TINYINT)
                    || type.equals(SMALLINT)
                    || type.equals(DATE)) {
                type.writeLong(output, ((Number) value).longValue());
            }
            else if (type.equals(REAL)) {
                type.writeLong(output, Float.floatToIntBits((Float) value));
            }
            else if (type instanceof DecimalType decimalType) {
                BigDecimal decimal = ((Decimal) value).toBigDecimal();
                type.writeLong(output, encodeShortScaledValue(decimal, decimalType.getScale()));
            }
            else if (type.equals(TIMESTAMP_MILLIS) || type.equals(TIMESTAMP_SECONDS)) {
                type.writeLong(
                        output,
                        ((Timestamp) value).getMillisecond() * MICROSECONDS_PER_MILLISECOND);
            }
            else if (type.equals(TIMESTAMP_MICROS)) {
                type.writeLong(output, ((Timestamp) value).toMicros());
            }
            else if (type.equals(TIMESTAMP_TZ_MILLIS)) {
                type.writeLong(
                        output,
                        packDateTimeWithZone(((Timestamp) value).getMillisecond(), UTC_KEY));
            }
            else if (type.equals(TIME_MICROS)) {
                type.writeLong(output, ((int) value) * ((long) MICROSECONDS_PER_MILLISECOND));
            }
            else {
                throw new TrinoException(
                        GENERIC_INTERNAL_ERROR,
                        format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        }
        else if (javaType == double.class) {
            type.writeDouble(output, ((Number) value).doubleValue());
        }
        else if (type instanceof DecimalType) {
            writeObject(output, type, value);
        }
        else if (javaType == Slice.class) {
            writeSlice(output, type, value);
        }
        else if (javaType == LongTimestampWithTimeZone.class) {
            checkArgument(type.equals(TIMESTAMP_TZ_MILLIS));
            Timestamp timestamp = (org.apache.paimon.data.Timestamp) value;
            type.writeObject(
                    output, fromEpochMillisAndFraction(timestamp.getMillisecond(), 0, UTC_KEY));
        }
        else if (type instanceof ArrayType
                || type instanceof MapType
                || type instanceof RowType) {
            writeBlock(output, type, logicalType, value);
        }
        else {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
        }
    }

    protected void writeBlock(BlockBuilder output, Type type, DataType logicalType, Object value)
    {
        if (type instanceof ArrayType) {
            ArrayBlockBuilder arrayBlockBuilder = (ArrayBlockBuilder) output;
            try {
                arrayBlockBuilder.buildEntry(
                        (ArrayValueBuilder<Throwable>)
                                elementBuilder -> {
                                    InternalArray arrayData = (InternalArray) value;
                                    DataType elementType =
                                            DataTypeChecks.getNestedTypes(logicalType).get(0);
                                    for (int i = 0; i < arrayData.size(); i++) {
                                        appendTo(
                                                type.getTypeParameters().get(0),
                                                elementType,
                                                InternalRowUtils.get(arrayData, i, elementType),
                                                elementBuilder);
                                    }
                                });
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
            }
            return;
        }
        if (type instanceof RowType) {
            RowBlockBuilder rowBlockBuilder = (RowBlockBuilder) output;
            try {
                rowBlockBuilder.buildEntry(
                        (RowValueBuilder<Throwable>)
                                fieldBuilders -> {
                                    InternalRow rowData = (InternalRow) value;
                                    for (int index = 0;
                                            index < type.getTypeParameters().size();
                                            index++) {
                                        Type fieldType = type.getTypeParameters().get(index);
                                        DataType fieldLogicalType =
                                                ((org.apache.paimon.types.RowType) logicalType)
                                                        .getTypeAt(index);
                                        appendTo(
                                                fieldType,
                                                fieldLogicalType,
                                                InternalRowUtils.get(
                                                        rowData, index, fieldLogicalType),
                                                fieldBuilders.get(index));
                                    }
                                });
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
            }
            return;
        }
        if (type instanceof MapType) {
            InternalMap mapData = (InternalMap) value;
            InternalArray keyArray = mapData.keyArray();
            InternalArray valueArray = mapData.valueArray();
            DataType keyType = ((org.apache.paimon.types.MapType) logicalType).getKeyType();
            DataType valueType = ((org.apache.paimon.types.MapType) logicalType).getValueType();
            MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) output;
            try {
                mapBlockBuilder.buildEntry(
                        (MapValueBuilder<Throwable>)
                                (keyBuilder, valueBuilder) -> {
                                    for (int i = 0; i < keyArray.size(); i++) {
                                        appendTo(
                                                type.getTypeParameters().get(0),
                                                keyType,
                                                InternalRowUtils.get(keyArray, i, keyType),
                                                keyBuilder);
                                        appendTo(
                                                type.getTypeParameters().get(1),
                                                valueType,
                                                InternalRowUtils.get(valueArray, i, valueType),
                                                valueBuilder);
                                    }
                                });
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
            }
            return;
        }
        throw new TrinoException(
                GENERIC_INTERNAL_ERROR, "Unhandled type for Block: " + type.getTypeSignature());
    }
}
