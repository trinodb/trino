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
package io.trino.plugin.lance.internal;

import com.lancedb.lance.ipc.LanceScanner;
import io.airlift.slice.Slice;
import io.trino.plugin.lance.LanceColumnHandle;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.OFFSET_WIDTH;

public class LanceArrowToPageScanner
        implements AutoCloseable
{
    private final BufferAllocator allocator;
    private final List<Type> columnTypes;
    private final ScannerFactory scannerFactory;

    private final LanceScanner lanceScanner;
    private final ArrowReader arrowReader;
    private final VectorSchemaRoot vectorSchemaRoot;

    public LanceArrowToPageScanner(BufferAllocator allocator, String path, List<LanceColumnHandle> columns, ScannerFactory scannerFactory)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.columnTypes = requireNonNull(columns, "columns is null").stream().map(LanceColumnHandle::trinoType)
                .collect(toImmutableList());
        this.scannerFactory = scannerFactory;
        try {
            lanceScanner = scannerFactory.open(path, allocator);
            this.arrowReader = lanceScanner.scanBatches();
            this.vectorSchemaRoot = arrowReader.getVectorSchemaRoot();
        }
        catch (IOException e) {
            throw new RuntimeException("Unalbe to initialize lance DB connection", e);
        }
    }

    public boolean read()
    {
        try {
            return arrowReader.loadNextBatch();
        }
        catch (IOException e) {
            throw new RuntimeException("Error loading next batch!", e);
        }
    }

    public void convert(PageBuilder pageBuilder)
    {
        pageBuilder.declarePositions(vectorSchemaRoot.getRowCount());
        List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();

        for (int column = 0; column < columnTypes.size(); column++) {
            convertType(pageBuilder.getBlockBuilder(column), columnTypes.get(column), fieldVectors.get(column), 0,
                    fieldVectors.get(column).getValueCount());
        }
        vectorSchemaRoot.clear();
    }

    private void convertType(BlockBuilder output, Type type, FieldVector vector, int offset, int length)
    {
        Class<?> javaType = type.getJavaType();
        try {
            if (javaType == boolean.class) {
                writeVectorValues(output, vector,
                        index -> type.writeBoolean(output, ((BitVector) vector).get(index) == 1), offset, length);
            }
            else if (javaType == long.class) {
                if (type.equals(BIGINT)) {
                    writeVectorValues(output, vector,
                            index -> type.writeLong(output, ((BigIntVector) vector).get(index)), offset, length);
                }
                else if (type.equals(INTEGER)) {
                    writeVectorValues(output, vector, index -> type.writeLong(output, ((IntVector) vector).get(index)),
                            offset, length);
                }
                else if (type.equals(DATE)) {
                    writeVectorValues(output, vector,
                            index -> type.writeLong(output, ((DateDayVector) vector).get(index)), offset, length);
                }
                else if (type.equals(TIME_MICROS)) {
                    writeVectorValues(output, vector, index -> type.writeLong(output,
                            ((TimeMicroVector) vector).get(index) * PICOSECONDS_PER_MICROSECOND), offset, length);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR,
                            format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
                }
            }
            else if (javaType == double.class) {
                writeVectorValues(output, vector, index -> type.writeDouble(output, ((Float8Vector) vector).get(index)),
                        offset, length);
            }
            else if (javaType == Slice.class) {
                writeVectorValues(output, vector, index -> writeSlice(output, type, vector, index), offset, length);
            }
            else if (type instanceof ArrayType arrayType) {
                writeVectorValues(output, vector, index -> writeArrayBlock(output, arrayType, vector, index), offset,
                        length);
            }
            else if (type instanceof RowType rowType) {
                writeVectorValues(output, vector, index -> writeRowBlock(output, rowType, vector, index), offset,
                        length);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        }
        catch (ClassCastException ex) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    format("Unhandled type for %s: %s", javaType.getSimpleName(), type), ex);
        }
    }

    private void writeVectorValues(BlockBuilder output, FieldVector vector, Consumer<Integer> consumer, int offset,
            int length)
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
        if (type instanceof VarcharType) {
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

    private void writeArrayBlock(BlockBuilder output, ArrayType arrayType, FieldVector vector, int index)
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
                convertType(elementBuilder, elementType, sliced, 0, sliced.getValueCount());
            }
        });
    }

    private void writeRowBlock(BlockBuilder output, RowType rowType, FieldVector vector, int index)
    {
        List<RowType.Field> fields = rowType.getFields();
        ((RowBlockBuilder) output).buildEntry(fieldBuilders -> {
            for (int i = 0; i < fields.size(); i++) {
                RowType.Field field = fields.get(i);
                FieldVector innerVector = ((StructVector) vector).getChild(field.getName().orElse("field" + i));
                convertType(fieldBuilders.get(i), field.getType(), innerVector, index, 1);
            }
        });
    }

    @Override
    public void close()
    {
        vectorSchemaRoot.close();
        try {
            arrowReader.close();
        }
        catch (IOException ioe) {
            // ignore for now.
        }
        scannerFactory.close();
    }
}
