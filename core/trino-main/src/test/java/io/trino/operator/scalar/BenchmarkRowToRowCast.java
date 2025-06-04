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
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.RowExpression;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.block.RowBlock.fromFieldBlocks;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(NANOSECONDS)
@Fork(3)
@BenchmarkMode(AverageTime)
public class BenchmarkRowToRowCast
{
    private static final int POSITION_COUNT = 100_000;

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public List<Optional<Page>> benchmark(BenchmarkData data)
    {
        return ImmutableList.copyOf(data.getPageProcessor().process(
                SESSION,
                new DriverYieldSignal(),
                newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                SourcePage.create(data.getPage())));
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final List<Type> fromFieldTypes = ImmutableList.of(createVarcharType(20), BIGINT);
        private static final List<Type> toFieldTypes = ImmutableList.of(createVarcharType(30), BIGINT);

        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            TestingFunctionResolution functionResolution = new TestingFunctionResolution();

            List<RowExpression> projections = ImmutableList.of(new CallExpression(
                    functionResolution.getCoercion(RowType.anonymous(toFieldTypes), RowType.anonymous(fromFieldTypes)),
                    ImmutableList.of(field(0, RowType.anonymous(toFieldTypes)))));

            pageProcessor = functionResolution.getExpressionCompiler()
                    .compilePageProcessor(Optional.empty(), projections)
                    .get();

            Block[] fieldBlocks = fromFieldTypes.stream()
                    .map(type -> createBlock(POSITION_COUNT, type))
                    .toArray(Block[]::new);
            Block rowBlock = fromFieldBlocks(POSITION_COUNT, fieldBlocks);

            page = new Page(rowBlock);
        }

        private static Block createBlock(int positionCount, Type type)
        {
            BlockBuilder blockBuilder = type.createBlockBuilder(null, positionCount);
            if (type instanceof VarcharType) {
                for (int i = 0; i < positionCount; i++) {
                    type.writeSlice(blockBuilder, utf8Slice(String.valueOf(ThreadLocalRandom.current().nextInt())));
                }
            }
            else if (type == BIGINT) {
                for (int i = 0; i < positionCount; i++) {
                    type.writeLong(blockBuilder, ThreadLocalRandom.current().nextLong());
                }
            }
            else {
                throw new UnsupportedOperationException();
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
        new BenchmarkRowToRowCast().benchmark(data);
    }

    public static void main(String[] args)
            throws Exception
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkRowToRowCast().benchmark(data);

        Benchmarks.benchmark(BenchmarkRowToRowCast.class, WarmupMode.INDI).run();
    }
}
