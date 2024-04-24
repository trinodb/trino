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
import io.airlift.slice.Slices;
import io.trino.jmh.Benchmarks;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.RowExpression;
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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.TestingConnectorSession.SESSION;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArraysOverlap
{
    private static final int POSITIONS = 1_000;

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public List<Optional<Page>> benchmark(BenchmarkData data)
    {
        return ImmutableList.copyOf(data.getPageProcessor().process(
                SESSION,
                new DriverYieldSignal(),
                newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                data.getPage()));
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private String name = "arrays_overlap";

        @Param({"BIGINT", "VARCHAR"})
        private String type = "BIGINT";

        @Param({"10", "100", "1000"})
        private int arraySize = 10;

        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            Type elementType = switch (type) {
                case "BIGINT" -> BIGINT;
                case "VARCHAR" -> VARCHAR;
                default -> throw new UnsupportedOperationException();
            };

            TestingFunctionResolution functionResolution = new TestingFunctionResolution();
            ArrayType arrayType = new ArrayType(elementType);
            ImmutableList<RowExpression> projections = ImmutableList.of(new CallExpression(
                    functionResolution.resolveFunction(name, fromTypes(arrayType, arrayType)),
                    ImmutableList.of(field(0, arrayType), field(1, arrayType))));

            ExpressionCompiler compiler = functionResolution.getExpressionCompiler();
            pageProcessor = compiler.compilePageProcessor(Optional.empty(), projections).get();

            page = new Page(createChannel(POSITIONS, arraySize, elementType), createChannel(POSITIONS, arraySize, elementType));
        }

        private static Block createChannel(int positionCount, int arraySize, Type elementType)
        {
            ArrayType arrayType = new ArrayType(elementType);
            ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                blockBuilder.buildEntry(elementBuilder -> {
                    for (int i = 0; i < arraySize; i++) {
                        if (elementType.getJavaType() == long.class) {
                            elementType.writeLong(elementBuilder, ThreadLocalRandom.current().nextLong() % arraySize);
                        }
                        else if (elementType.equals(VARCHAR)) {
                            // make sure the size of a varchar is rather small; otherwise the aggregated slice may overflow
                            elementType.writeSlice(elementBuilder, Slices.utf8Slice(Long.toString(ThreadLocalRandom.current().nextLong() % arraySize)));
                        }
                        else {
                            throw new UnsupportedOperationException();
                        }
                    }
                });
            }
            return blockBuilder.build();
        }

        public PageProcessor getPageProcessor()
        {
            return pageProcessor;
        }

        public Page getPage()
        {
            return page;
        }
    }

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkArraysOverlap().benchmark(data);
    }

    public static void main(String[] args)
            throws Exception
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkArraysOverlap().benchmark(data);

        Benchmarks.benchmark(BenchmarkArraysOverlap.class).run();
    }
}
