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
package io.trino.plugin.bigquery;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.Consumer;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.bigquery.BigQueryUtil.toBigQueryColumnName;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.arrow.compression.CommonsCompressionFactory.INSTANCE;
import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.OFFSET_WIDTH;
import static org.apache.arrow.vector.types.Types.MinorType.DECIMAL256;

public class BigQueryArrowToPageConverter
        implements AutoCloseable
{
    private final BigQueryTypeManager typeManager;
    private final Schema schema;
    private final List<BigQueryColumnHandle> columns;

    public BigQueryArrowToPageConverter(BigQueryTypeManager typeManager, Schema schema, List<BigQueryColumnHandle> columns)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    public void convert(PageBuilder pageBuilder, ArrowRecordBatch batch, BufferAllocator allocator)
    {
        List<FieldVector> vectors = schema.getFields().stream()
                .map(field -> field.createVector(allocator))
                .collect(toImmutableList());
        VectorSchemaRoot root = new VectorSchemaRoot(vectors);
        VectorLoader loader = new VectorLoader(root, INSTANCE);

        loader.load(batch);
        pageBuilder.declarePositions(root.getRowCount());

        for (int column = 0; column < columns.size(); column++) {
            BigQueryColumnHandle columnHandle = columns.get(column);
            FieldVector fieldVector = getFieldVector(root, columnHandle);
            convertType(
                    allocator,
                    pageBuilder.getBlockBuilder(column),
                    columnHandle.trinoType(),
                    fieldVector,
                    0,
                    fieldVector.getValueCount());
        }

        root.clear();
        root.close();
    }

    private static FieldVector getFieldVector(VectorSchemaRoot root, BigQueryColumnHandle columnHandle)
    {
        FieldVector fieldVector = root.getVector(toBigQueryColumnName(columnHandle.name()));

        for (String dereferenceName : columnHandle.dereferenceNames()) {
            for (FieldVector child : fieldVector.getChildrenFromFields()) {
                if (child.getField().getName().equals(dereferenceName)) {
                    fieldVector = child;
                    break;
                }
            }
        }
        return fieldVector;
    }

    private void convertType(BufferAllocator allocator, BlockBuilder output, Type type, FieldVector vector, int offset, int length)
    {
        Class<?> javaType = type.getJavaType();
        try {
            if (javaType == boolean.class) {
                writeVectorValues(output, vector, index -> type.writeBoolean(output, ((BitVector) vector).get(index) == 1), offset, length);
            }
            else if (javaType == long.class) {
                if (type.equals(BIGINT)) {
                    writeVectorValues(output, vector, index -> type.writeLong(output, ((BigIntVector) vector).get(index)), offset, length);
                }
                else if (type.equals(INTEGER)) {
                    writeVectorValues(output, vector, index -> type.writeLong(output, ((IntVector) vector).get(index)), offset, length);
                }
                else if (type instanceof DecimalType decimalType) {
                    writeVectorValues(output, vector, index -> writeObjectShortDecimal(output, decimalType, vector, index), offset, length);
                }
                else if (type.equals(DATE)) {
                    writeVectorValues(output, vector, index -> type.writeLong(output, ((DateDayVector) vector).get(index)), offset, length);
                }
                else if (type.equals(TIMESTAMP_MICROS)) {
                    writeVectorValues(output, vector, index -> type.writeLong(output, ((TimeStampVector) vector).get(index)), offset, length);
                }
                else if (type.equals(TIME_MICROS)) {
                    writeVectorValues(output, vector, index -> type.writeLong(output, ((TimeMicroVector) vector).get(index) * PICOSECONDS_PER_MICROSECOND), offset, length);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
                }
            }
            else if (javaType == double.class) {
                writeVectorValues(output, vector, index -> type.writeDouble(output, ((Float8Vector) vector).get(index)), offset, length);
            }
            else if (type.getJavaType() == Int128.class) {
                writeVectorValues(output, vector, index -> writeObjectLongDecimal(output, type, vector, index), offset, length);
            }
            else if (javaType == Slice.class) {
                writeVectorValues(output, vector, index -> writeSlice(output, type, vector, index), offset, length);
            }
            else if (javaType == LongTimestampWithTimeZone.class) {
                writeVectorValues(output, vector, index -> writeObjectTimestampWithTimezone(output, type, vector, index), offset, length);
            }
            else if (type instanceof ArrayType arrayType) {
                writeVectorValues(output, vector, index -> writeArrayBlock(allocator, output, arrayType, vector, index), offset, length);
            }
            else if (type instanceof RowType rowType) {
                writeVectorValues(output, vector, index -> writeRowBlock(allocator, output, rowType, vector, index), offset, length);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        }
        catch (ClassCastException ex) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type), ex);
        }
    }

    private void writeVectorValues(BlockBuilder output, FieldVector vector, Consumer<Integer> consumer, int offset, int length)
    {
        for (int i = offset; i < offset + length; i++) {
            if (vector.isNull(i)) {
                output.appendNull();
            }
            else {
                consumer.accept(i);
            }
        }
    }

    private void writeSlice(BlockBuilder output, Type type, FieldVector vector, int index)
    {
        if (type instanceof VarcharType || typeManager.isJsonType(type)) {
            byte[] slice = ((VarCharVector) vector).get(index);
            type.writeSlice(output, wrappedBuffer(slice));
        }
        else if (type instanceof VarbinaryType) {
            byte[] slice = ((VarBinaryVector) vector).get(index);
            type.writeSlice(output, wrappedBuffer(slice));
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private void writeObjectLongDecimal(BlockBuilder output, Type type, FieldVector vector, int index)
    {
        if (type instanceof DecimalType decimalType) {
            verify(!decimalType.isShort(), "The type should be long decimal");
            BigDecimal decimal = vector.getMinorType() == DECIMAL256 ? ((Decimal256Vector) vector).getObject(index) : ((DecimalVector) vector).getObject(index);
            type.writeObject(output, Decimals.encodeScaledValue(decimal, decimalType.getScale()));
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Object: " + type.getTypeSignature());
        }
    }

    private void writeObjectShortDecimal(BlockBuilder output, DecimalType decimalType, FieldVector vector, int index)
    {
        verify(decimalType.isShort(), "The type should be short decimal");
        BigDecimal decimal = vector.getMinorType() == DECIMAL256 ? ((Decimal256Vector) vector).getObject(index) : ((DecimalVector) vector).getObject(index);
        decimalType.writeLong(output, encodeShortScaledValue(decimal, decimalType.getScale()));
    }

    private void writeObjectTimestampWithTimezone(BlockBuilder output, Type type, FieldVector vector, int index)
    {
        verify(type.equals(TIMESTAMP_TZ_MICROS));
        long epochMicros = ((TimeStampVector) vector).get(index);
        int picosOfMillis = toIntExact(floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND)) * PICOSECONDS_PER_MICROSECOND;
        // BigQuery's TIMESTAMP type represents an instant in time so always uses UTC as the zone - the original zone of the input literal is lost
        type.writeObject(output, fromEpochMillisAndFraction(floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND), picosOfMillis, UTC_KEY));
    }

    private void writeArrayBlock(BufferAllocator allocator, BlockBuilder output, ArrayType arrayType, FieldVector vector, int index)
    {
        Type elementType = arrayType.getElementType();
        ((ArrayBlockBuilder) output).buildEntry(elementBuilder -> {
            ArrowBuf offsetBuffer = vector.getOffsetBuffer();

            int start = offsetBuffer.getInt((long) index * OFFSET_WIDTH);
            int end = offsetBuffer.getInt((long) (index + 1) * OFFSET_WIDTH);

            FieldVector innerVector = ((ListVector) vector).getDataVector();

            TransferPair transferPair = innerVector.getTransferPair(allocator);
            transferPair.splitAndTransfer(start, end - start);
            try (FieldVector sliced = (FieldVector) transferPair.getTo()) {
                convertType(allocator, elementBuilder, elementType, sliced, 0, sliced.getValueCount());
            }
        });
    }

    private void writeRowBlock(BufferAllocator allocator, BlockBuilder output, RowType rowType, FieldVector vector, int index)
    {
        List<RowType.Field> fields = rowType.getFields();
        ((RowBlockBuilder) output).buildEntry(fieldBuilders -> {
            for (int i = 0; i < fields.size(); i++) {
                RowType.Field field = fields.get(i);
                FieldVector innerVector = ((StructVector) vector).getChild(field.getName().orElse("field" + i));
                convertType(allocator, fieldBuilders.get(i), field.getType(), innerVector, index, 1);
            }
        });
    }

    @Override
    public void close()
    {
    }
}
