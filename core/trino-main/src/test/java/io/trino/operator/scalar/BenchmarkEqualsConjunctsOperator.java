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
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.SpecialForm.Form;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.field;

@State(Scope.Thread)
@Fork(3)
@Warmup(iterations = 5, time = 10)
@Measurement(iterations = 5, time = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class BenchmarkEqualsConjunctsOperator
{
    private static final int FIELDS_COUNT = 10;
    private static final int COMPARISONS_COUNT = 100;
    private static final double NULLS_FRACTION = 0.05;

    private static final DriverYieldSignal SIGNAL = new DriverYieldSignal();
    private static final ConnectorSession SESSION = TEST_SESSION.toConnectorSession();

    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();
    private PageProcessor compiledProcessor;

    @Setup
    public void setup()
    {
        ExpressionCompiler expressionCompiler = functionResolution.getExpressionCompiler();
        RowExpression projection = generateComplexComparisonProjection(FIELDS_COUNT, COMPARISONS_COUNT);
        compiledProcessor = expressionCompiler.compilePageProcessor(Optional.empty(), ImmutableList.of(projection)).get();
    }

    private RowExpression generateComplexComparisonProjection(int fieldsCount, int comparisonsCount)
    {
        checkArgument(fieldsCount > 0, "fieldsCount must be greater than zero");
        checkArgument(comparisonsCount > 0, "comparisonsCount must be greater than zero");

        if (comparisonsCount == 1) {
            return createComparison(0, 0);
        }

        return createConjunction(
                createComparison(0, comparisonsCount % fieldsCount),
                generateComplexComparisonProjection(fieldsCount, comparisonsCount - 1));
    }

    private static RowExpression createConjunction(RowExpression left, RowExpression right)
    {
        return new SpecialForm(Form.OR, BOOLEAN, ImmutableList.of(left, right), ImmutableList.of());
    }

    private RowExpression createComparison(int leftField, int rightField)
    {
        return call(
                functionResolution.resolveOperator(EQUAL, ImmutableList.of(BIGINT, BIGINT)),
                field(leftField, BIGINT),
                field(rightField, BIGINT));
    }

    @Benchmark
    public List<Page> processPage(BenchmarkData data)
    {
        List<Page> output = new ArrayList<>();
        Iterator<Optional<Page>> pageProcessorOutput = compiledProcessor.process(
                SESSION,
                SIGNAL,
                newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                SourcePage.create(data.page));
        while (pageProcessorOutput.hasNext()) {
            pageProcessorOutput.next().ifPresent(output::add);
        }
        return output;
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        Page page;

        @Setup
        public void setup()
        {
            List<Type> types = ImmutableList.copyOf(limit(cycle(BIGINT), FIELDS_COUNT));
            ThreadLocalRandom random = ThreadLocalRandom.current();
            PageBuilder pageBuilder = new PageBuilder(types);
            while (!pageBuilder.isFull()) {
                pageBuilder.declarePosition();
                for (int channel = 0; channel < FIELDS_COUNT; channel++) {
                    BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
                    if (random.nextDouble() < NULLS_FRACTION) {
                        blockBuilder.appendNull();
                    }
                    else {
                        BIGINT.writeLong(blockBuilder, random.nextLong());
                    }
                }
            }
            page = pageBuilder.build();
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkEqualsConjunctsOperator.class).run();
    }
}
