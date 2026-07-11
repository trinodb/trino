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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.jmh.Benchmarks;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProjection;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(3)
@Warmup(iterations = 5, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 8, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkColumnarScalarFunctions
{
    private static final int POSITION_COUNT = 1024;
    private static final int ELEMENTS_PER_VALUE = 16;
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @Param({"map_keys", "map_values", "map_entries", "flatten", "reverse", "trim_array", "slice", "array_first", "array_last", "element_at"})
    public String function;

    @Param({"flat", "dictionary", "rle"})
    public String encoding;

    private Page inputPage;
    private Type outputType;
    private PageProcessor columnarProcessor;
    private PageProcessor rowProcessor;

    @Setup
    public void setup()
    {
        Type inputType;
        Expression expression;
        if (ImmutableList.of("flatten", "reverse", "trim_array", "slice", "array_first", "array_last", "element_at").contains(function)) {
            ArrayType innerArrayType = new ArrayType(VARCHAR);
            inputType = new ArrayType(innerArrayType);
            if (function.equals("element_at") || function.equals("trim_array")) {
                expression = call(
                        FUNCTIONS.resolveFunction(function, fromTypes(inputType, BIGINT)),
                        new Reference(inputType, "input"),
                        new Constant(BIGINT, 2L));
            }
            else if (function.equals("slice")) {
                expression = call(
                        FUNCTIONS.resolveFunction(function, fromTypes(inputType, BIGINT, BIGINT)),
                        new Reference(inputType, "input"),
                        new Constant(BIGINT, 2L),
                        new Constant(BIGINT, 4L));
            }
            else {
                expression = call(
                        FUNCTIONS.resolveFunction(function, fromTypes(inputType)),
                        new Reference(inputType, "input"));
            }
            inputPage = new Page(encode(createNestedArrayBlock((ArrayType) inputType), encoding));
        }
        else {
            inputType = new MapType(BIGINT, VARCHAR, TYPE_OPERATORS);
            expression = call(
                    FUNCTIONS.resolveFunction(function, fromTypes(inputType)),
                    new Reference(inputType, "input"));
            inputPage = new Page(encode(createMapBlock((MapType) inputType), encoding));
        }
        Map<Symbol, Integer> layout = ImmutableMap.of(new Symbol(inputType, "input"), 0);

        PageFunctionCompiler compiler = FUNCTIONS.getPageFunctionCompiler();
        outputType = expression.type();
        PageProjection columnar = compiler.compileProjection(expression, layout, Optional.empty(), true).get();
        PageProjection row = compiler.compileProjection(expression, layout, Optional.empty(), false).get();
        columnarProcessor = new PageProcessor(Optional.empty(), Optional.empty(), ImmutableList.of(columnar), OptionalInt.of(POSITION_COUNT));
        rowProcessor = new PageProcessor(Optional.empty(), Optional.empty(), ImmutableList.of(row), OptionalInt.of(POSITION_COUNT));
    }

    @Benchmark
    public void columnar(Blackhole blackhole)
    {
        blackhole.consume(process(columnarProcessor));
    }

    @Benchmark
    public void rowOriented(Blackhole blackhole)
    {
        blackhole.consume(process(rowProcessor));
    }

    private List<Optional<Page>> process(PageProcessor processor)
    {
        return ImmutableList.copyOf(processor.process(
                SESSION,
                newSimpleAggregatedMemoryContext().newLocalMemoryContext("benchmark"),
                SourcePage.create(inputPage)));
    }

    private static Block createMapBlock(MapType mapType)
    {
        int positions = POSITION_COUNT;
        MapBlockBuilder builder = mapType.createBlockBuilder(null, positions);
        for (int position = 0; position < positions; position++) {
            int value = position;
            builder.buildEntry((keyBuilder, valueBuilder) -> {
                for (int element = 0; element < ELEMENTS_PER_VALUE; element++) {
                    BIGINT.writeLong(keyBuilder, value * ELEMENTS_PER_VALUE + element);
                    VARCHAR.writeSlice(valueBuilder, Slices.utf8Slice("value-" + value + "-" + element));
                }
            });
        }
        return builder.build();
    }

    private static Block createNestedArrayBlock(ArrayType outerArrayType)
    {
        ArrayBlockBuilder builder = outerArrayType.createBlockBuilder(null, POSITION_COUNT);
        for (int position = 0; position < POSITION_COUNT; position++) {
            int value = position;
            builder.buildEntry(innerArrays -> {
                ArrayBlockBuilder innerArrayBuilder = (ArrayBlockBuilder) innerArrays;
                for (int array = 0; array < 4; array++) {
                    int arrayIndex = array;
                    innerArrayBuilder.buildEntry(elements -> {
                        for (int element = 0; element < ELEMENTS_PER_VALUE / 4; element++) {
                            VARCHAR.writeSlice(elements, Slices.utf8Slice("value-" + value + "-" + arrayIndex + "-" + element));
                        }
                    });
                }
            });
        }
        return builder.build();
    }

    private static Block encode(Block block, String encoding)
    {
        return switch (encoding) {
            case "flat" -> block;
            case "dictionary" -> {
                int[] ids = new int[POSITION_COUNT];
                for (int position = 0; position < ids.length; position++) {
                    ids[position] = position % 128;
                }
                yield DictionaryBlock.create(POSITION_COUNT, block.getRegion(0, 128), ids);
            }
            case "rle" -> RunLengthEncodedBlock.create(block.getSingleValueBlock(0), POSITION_COUNT);
            default -> throw new IllegalArgumentException("Unknown encoding: " + encoding);
        };
    }

    @Test
    public void testBenchmark()
    {
        for (String function : ImmutableList.of("map_keys", "map_values", "map_entries", "flatten", "reverse", "trim_array", "slice", "array_first", "array_last", "element_at")) {
            for (String encoding : ImmutableList.of("flat", "dictionary", "rle")) {
                this.function = function;
                this.encoding = encoding;
                setup();
                Page columnarResult = process(columnarProcessor).getFirst().orElseThrow();
                Page rowResult = process(rowProcessor).getFirst().orElseThrow();
                assertBlockEquals(outputType, columnarResult.getBlock(0), rowResult.getBlock(0));
                columnar(new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous."));
                rowOriented(new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous."));
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Benchmarks.benchmark(BenchmarkColumnarScalarFunctions.class).run();
    }
}
