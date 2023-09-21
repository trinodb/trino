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
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.tree.QualifiedName;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsDistinctFrom;
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

import static com.google.common.base.Verify.verify;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
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
public class BenchmarkArrayDistinct
{
    private static final int POSITIONS = 100_000;
    private static final int ARRAY_SIZE = 2;
    private static final int NUM_TYPES = 1;
    private static final List<Type> TYPES = ImmutableList.of(VARCHAR);
    private static final BlockTypeOperators BLOCK_TYPE_OPERATORS = new BlockTypeOperators(new TypeOperators());
    private static final BlockPositionIsDistinctFrom DISTINCT_FROM_OPERATOR = BLOCK_TYPE_OPERATORS.getDistinctFromOperator(VARCHAR);
    private static final BlockPositionHashCode HASH_CODE_OPERATOR = BLOCK_TYPE_OPERATORS.getHashCodeOperator(VARCHAR);

    static {
        verify(NUM_TYPES == TYPES.size());
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS * ARRAY_SIZE * NUM_TYPES)
    public List<Optional<Page>> arrayDistinct(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getPageProcessor().process(
                        SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.getPage()));
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"array_distinct", "old_array_distinct"})
        private String name = "array_distinct";

        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            TestingFunctionResolution functionResolution = new TestingFunctionResolution(InternalFunctionBundle.extractFunctions(BenchmarkArrayDistinct.class));
            ExpressionCompiler compiler = functionResolution.getExpressionCompiler();

            ImmutableList.Builder<RowExpression> projectionsBuilder = ImmutableList.builder();
            Block[] blocks = new Block[TYPES.size()];
            for (int i = 0; i < TYPES.size(); i++) {
                Type elementType = TYPES.get(i);
                ArrayType arrayType = new ArrayType(elementType);
                projectionsBuilder.add(new CallExpression(
                        functionResolution.resolveFunction(QualifiedName.of(name), fromTypes(arrayType)),
                        ImmutableList.of(field(i, arrayType))));
                blocks[i] = createChannel(POSITIONS, ARRAY_SIZE, arrayType);
            }

            ImmutableList<RowExpression> projections = projectionsBuilder.build();
            pageProcessor = compiler.compilePageProcessor(Optional.empty(), projections).get();
            page = new Page(blocks);
        }

        private static Block createChannel(int positionCount, int arraySize, ArrayType arrayType)
        {
            ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                blockBuilder.buildEntry(elementBuilder -> {
                    for (int i = 0; i < arraySize; i++) {
                        if (arrayType.getElementType().getJavaType() == long.class) {
                            arrayType.getElementType().writeLong(elementBuilder, ThreadLocalRandom.current().nextLong());
                        }
                        else if (arrayType.getElementType().equals(VARCHAR)) {
                            arrayType.getElementType().writeSlice(elementBuilder, Slices.utf8Slice("test_string"));
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

    public static void main(String[] args)
            throws Exception
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkArrayDistinct().arrayDistinct(data);

        benchmark(BenchmarkArrayDistinct.class).run();
    }

    @ScalarFunction
    @SqlType("array(varchar)")
    public static Block oldArrayDistinct(@SqlType("array(varchar)") Block array)
    {
        if (array.getPositionCount() == 0) {
            return array;
        }

        BlockSet set = new BlockSet(VARCHAR, DISTINCT_FROM_OPERATOR, HASH_CODE_OPERATOR, array.getPositionCount());
        BlockBuilder distinctElementBlockBuilder = VARCHAR.createBlockBuilder(null, array.getPositionCount());
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (set.add(array, i)) {
                VARCHAR.appendTo(array, i, distinctElementBlockBuilder);
            }
        }

        return distinctElementBlockBuilder.build();
    }
}
