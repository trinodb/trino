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
package io.trino.parquet.reader;

import io.trino.parquet.PrimitiveField;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Objects;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.trino.parquet.reader.TestData.randomAsciiData;
import static io.trino.parquet.reader.TestData.randomBinaryData;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

public class BenchmarkBinaryColumnReader
        extends AbstractColumnReaderBenchmark<byte[][]>
{
    @Param
    public Encoding encoding;
    @Param
    public FieldType type;
    @Param
    public PositionLength positionLength;

    @Override
    protected PrimitiveField createPrimitiveField()
    {
        PrimitiveType parquetType = Types.optional(BINARY)
                .named("name");
        return new PrimitiveField(
                type.getType(positionLength.getRange()),
                true,
                new ColumnDescriptor(new String[] {"test"}, parquetType, 0, 0),
                0);
    }

    @Override
    protected ValuesWriter createValuesWriter(int bufferSize)
    {
        return encoding.getWriter(bufferSize);
    }

    @Override
    protected byte[][] generateDataBatch(int size)
    {
        return type.generateData(size, positionLength.getRange());
    }

    @Override
    protected void writeValue(ValuesWriter writer, byte[][] batch, int index)
    {
        writer.writeBytes(Binary.fromConstantByteArray(batch[index]));
    }

    public enum Encoding
    {
        PLAIN {
            @Override
            ValuesWriter getWriter(int bufferSize)
            {
                return new PlainValuesWriter(bufferSize, bufferSize, HeapByteBufferAllocator.getInstance());
            }
        },
        DELTA_BYTE_ARRAY {
            @Override
            ValuesWriter getWriter(int bufferSize)
            {
                return new DeltaByteArrayWriter(bufferSize, bufferSize, HeapByteBufferAllocator.getInstance());
            }
        },
        DELTA_LENGTH_BYTE_ARRAY {
            @Override
            ValuesWriter getWriter(int bufferSize)
            {
                return new DeltaLengthByteArrayValuesWriter(bufferSize, bufferSize, HeapByteBufferAllocator.getInstance());
            }
        };

        abstract ValuesWriter getWriter(int bufferSize);
    }

    public enum FieldType
    {
        UNBOUNDED(range -> VARBINARY, (size, range) -> randomBinaryData(size, range.from(), range.to())),
        VARCHAR_ASCII_BOUND_EXACT(range -> VarcharType.createVarcharType(max(1, range.to)), (size, range) -> randomAsciiData(size, range.from(), range.to())),
        CHAR_ASCII_BOUND_HALF(range -> CharType.createCharType(max(1, range.to / 2)), (size, range) -> randomAsciiData(size, range.from(), range.to())),
        CHAR_BOUND_HALF_PADDING_SOMETIMES(range -> CharType.createCharType(max(1, range.to / 2)), (length, range) -> randomAsciiDataWithPadding(length, range, .01)),
        /**/;

        private final Function<Range, Type> type;
        private final BiFunction<Integer, Range, byte[][]> dataGenerator;

        public Type getType(Range length)
        {
            return type.apply(length);
        }

        public byte[][] generateData(int size, Range positionLength)
        {
            return dataGenerator.apply(size, positionLength);
        }

        FieldType(Function<Range, Type> type, BiFunction<Integer, Range, byte[][]> dataGenerator)
        {
            this.type = requireNonNull(type, "type is null");
            this.dataGenerator = requireNonNull(dataGenerator, "dataGenerator is null");
        }
    }

    public enum PositionLength
    {
        VARIABLE_0_100(0, 100),
        VARIABLE_0_1000(0, 1000),
        FIXED_10(10, 10),
        FIXED_100(100, 100),
        /**/;

        private final Range range;

        public Range getRange()
        {
            return range;
        }

        PositionLength(int from, int to)
        {
            this.range = new Range(from, to);
        }
    }

    record Range(int from, int to) {}

    private static byte[][] randomAsciiDataWithPadding(int size, Range positionLength, double paddingChance)
    {
        Random random = new Random(Objects.hash(size, positionLength.from(), positionLength.to()));
        byte[][] data = new byte[size][];
        for (int i = 0; i < size; i++) {
            int length = random.nextInt(positionLength.to() - positionLength.from() + 1) + positionLength.from();
            boolean padding = random.nextDouble() < paddingChance;
            byte[] value = new byte[length + (padding ? 1 : 0)];
            for (int j = 0; j < length; j++) {
                value[j] = (byte) random.nextInt(128);
            }
            if (padding) {
                value[length] = ' ';
            }
            data[i] = value;
        }

        return data;
    }

    public static void main(String[] args)
            throws RunnerException
    {
        run(BenchmarkBinaryColumnReader.class);
    }
}
