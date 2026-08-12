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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.trino.jmh.Benchmarks;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.operator.scalar.ArrayElementBlockProjection.Selection.FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 4, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 6, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArrayElementBlockProjection
{
    private static final int POSITION_COUNT = 1024;
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @Param({"array", "map", "row"})
    public String elementType;

    @Param({"1", "16", "128"})
    public int candidatesPerArray;

    @Param({"1", "16", "128"})
    public int valuesPerElement;

    @Param({"0", "10"})
    public int nullPercentage;

    private Type outputType;
    private Block input;

    @Setup
    public void setup()
    {
        outputType = switch (elementType) {
            case "array" -> new ArrayType(BIGINT);
            case "map" -> new MapType(BIGINT, BIGINT, TYPE_OPERATORS);
            case "row" -> RowType.anonymous(ImmutableList.of(
                    new ArrayType(BIGINT),
                    new ArrayType(BIGINT),
                    new ArrayType(BIGINT),
                    new ArrayType(BIGINT)));
            default -> throw new IllegalArgumentException("Unknown element type: " + elementType);
        };
        input = createInput(new ArrayType(outputType));
    }

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public Block dictionary()
    {
        return ArrayElementBlockProjection.project(input, null, FIRST);
    }

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public Block materialized()
    {
        ColumnarArray arrays = ColumnarArray.toColumnarArray(input);
        Block elements = arrays.getElementsBlock();
        BlockBuilder builder = outputType.createBlockBuilder(null, arrays.getPositionCount());
        for (int position = 0; position < arrays.getPositionCount(); position++) {
            if (arrays.isNull(position) || arrays.getLength(position) == 0) {
                builder.appendNull();
            }
            else {
                int sourcePosition = arrays.getOffset(position);
                builder.append(elements.getUnderlyingValueBlock(), elements.getUnderlyingValuePosition(sourcePosition));
            }
        }
        return builder.build();
    }

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public Block dictionaryConsumed()
    {
        return consume(dictionary());
    }

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public Block materializedConsumed()
    {
        return consume(materialized());
    }

    private Block consume(Block block)
    {
        BlockBuilder builder = outputType.createBlockBuilder(null, block.getPositionCount());
        builder.appendBlockRange(block, 0, block.getPositionCount());
        return builder.build();
    }

    private Block createInput(ArrayType inputType)
    {
        ArrayBlockBuilder arrays = inputType.createBlockBuilder(null, POSITION_COUNT);
        for (int position = 0; position < POSITION_COUNT; position++) {
            if (nullPercentage > 0 && position % (100 / nullPercentage) == 0) {
                arrays.appendNull();
                continue;
            }
            int row = position;
            arrays.buildEntry(elements -> {
                for (int candidate = 0; candidate < candidatesPerArray; candidate++) {
                    appendElement(elements, row, candidate);
                }
            });
        }
        return arrays.build();
    }

    private void appendElement(BlockBuilder elements, int row, int candidate)
    {
        switch (elementType) {
            case "array" -> ((ArrayBlockBuilder) elements).buildEntry(values -> appendLongs(values, row, candidate, 0));
            case "map" -> ((MapBlockBuilder) elements).buildEntry((keys, values) -> {
                for (int index = 0; index < valuesPerElement; index++) {
                    BIGINT.writeLong(keys, index);
                    BIGINT.writeLong(values, value(row, candidate, 0, index));
                }
            });
            case "row" -> ((RowBlockBuilder) elements).buildEntry(fields -> {
                for (int field = 0; field < fields.size(); field++) {
                    int fieldIndex = field;
                    ((ArrayBlockBuilder) fields.get(field)).buildEntry(values -> appendLongs(values, row, candidate, fieldIndex));
                }
            });
            default -> throw new IllegalArgumentException("Unknown element type: " + elementType);
        }
    }

    private void appendLongs(BlockBuilder builder, int row, int candidate, int field)
    {
        for (int index = 0; index < valuesPerElement; index++) {
            BIGINT.writeLong(builder, value(row, candidate, field, index));
        }
    }

    private long value(int row, int candidate, int field, int index)
    {
        return (((long) row * candidatesPerArray + candidate) * 4 + field) * valuesPerElement + index;
    }

    @Test
    public void testBenchmark()
    {
        for (String elementType : List.of("array", "map", "row")) {
            this.elementType = elementType;
            candidatesPerArray = 4;
            valuesPerElement = 8;
            for (int nullPercentage : List.of(0, 10)) {
                this.nullPercentage = nullPercentage;
                setup();
                Block dictionary = dictionary();
                Block materialized = materialized();
                assertBlockEquals(outputType, dictionary, materialized);
                assertBlockEquals(outputType, dictionaryConsumed(), materializedConsumed());
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Benchmarks.benchmark(BenchmarkArrayElementBlockProjection.class).run();
    }
}
