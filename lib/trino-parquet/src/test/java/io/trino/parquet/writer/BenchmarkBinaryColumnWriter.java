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
package io.trino.parquet.writer;

import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.runner.RunnerException;

import java.util.function.BiFunction;
import java.util.function.Function;

import static io.trino.parquet.reader.TestData.randomAsciiData;
import static io.trino.parquet.reader.TestData.randomBinaryData;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

public class BenchmarkBinaryColumnWriter
        extends AbstractColumnWriterBenchmark
{
    @Param
    public FieldType type;
    @Param
    public PositionLength positionLength;

    @Override
    protected Type getTrinoType()
    {
        return VARBINARY;
    }

    @Override
    protected PrimitiveType getParquetType()
    {
        return Types.optional(BINARY).named("name");
    }

    @Override
    protected Block generateBlock(int size)
    {
        BlockBuilder blockBuilder = getTrinoType().createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), size);
        for (byte[] value : type.generateData(size, positionLength.getRange())) {
            getTrinoType().writeSlice(blockBuilder, Slices.wrappedBuffer(value));
        }
        return blockBuilder.buildValueBlock();
    }

    public enum FieldType
    {
        UNBOUNDED(range -> VARBINARY, (size, range) -> randomBinaryData(size, range.from(), range.to())),
        VARCHAR_ASCII_BOUND_EXACT(range -> VarcharType.createVarcharType(max(1, range.to)), (size, range) -> randomAsciiData(size, range.from(), range.to())),
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
        FIXED_10(10, 10),
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

    public static void main(String[] args)
            throws RunnerException
    {
        run(BenchmarkBinaryColumnWriter.class);
    }
}
