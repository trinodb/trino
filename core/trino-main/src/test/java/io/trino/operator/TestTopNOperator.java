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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.ExceededMemoryLimitException;
import io.trino.connector.TestingColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import io.trino.type.BlockTypeOperators;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.math.BigDecimal.ONE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestTopNOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;
    private OrderingCompiler orderingCompiler;

    @BeforeEach
    public void setUp()
    {
        orderingCompiler = new OrderingCompiler(new TypeOperators());
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    @AfterEach
    public void tearDown()
    {
        orderingCompiler = null;
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @ParameterizedTest
    @EnumSource(SortOrder.class)
    public void testDynamicBoundary(SortOrder order)
    {
        TestingColumnHandle column = new TestingColumnHandle("key");
        TopNDynamicFilter filter = new TopNDynamicFilter(column, 0, BIGINT, order, new BlockTypeOperators());
        long better = order.isAscending() ? 1 : 3;
        long worse = order.isAscending() ? 3 : 1;
        var memory = newSimpleAggregatedMemoryContext();
        try {
            var comparator = orderingCompiler.compilePageWithPositionComparator(List.of(BIGINT), List.of(0), List.of(order));
            var processor = new TopNProcessor(memory, List.of(BIGINT), 2, comparator, Optional.of(filter));
            processor.addInput(rowPagesBuilder(BIGINT).row(2L).build().getFirst());
            assertThat(filter.getCurrentPredicate().isAll()).isTrue();

            processor.addInput(rowPagesBuilder(BIGINT).row(better).build().getFirst());
            Range range = order.isAscending() ? Range.lessThanOrEqual(BIGINT, 2L) : Range.greaterThanOrEqual(BIGINT, 2L);
            var expected = TupleDomain.withColumnDomains(Map.of(column, Domain.create(ValueSet.ofRanges(range), order.isNullsFirst())));
            assertThat(filter.getCurrentPredicate()).isEqualTo(expected);

            var otherDriver = new TopNProcessor(memory, List.of(BIGINT), 2, comparator, Optional.of(filter));
            otherDriver.addInput(rowPagesBuilder(BIGINT).row(worse).row(worse).build().getFirst());
            assertThat(filter.getCurrentPredicate()).isEqualTo(expected);

            processor.addInput(rowPagesBuilder(BIGINT).row((Object) null).row((Object) null).build().getFirst());
            if (order.isNullsFirst()) {
                assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(Map.of(column, Domain.onlyNull(BIGINT))));
            }
            else {
                assertThat(filter.getCurrentPredicate()).isEqualTo(expected);
            }
        }
        finally {
            memory.close();
        }
    }

    @ParameterizedTest
    @EnumSource(SortOrder.class)
    public void testFloatingPointBoundary(SortOrder order)
    {
        for (Type type : List.of(DOUBLE, REAL)) {
            List<Object> values = new ArrayList<>();
            values.add(null);
            for (double number : new double[] {Double.NEGATIVE_INFINITY, -0.0, 0.0, 1, Double.POSITIVE_INFINITY, Double.NaN}) {
                if (type.equals(DOUBLE)) {
                    values.add(number);
                }
                else {
                    values.add((long) Float.floatToRawIntBits((float) number));
                }
            }
            assertBoundaryMatchesOrdering(type, order, values);
        }
    }

    @ParameterizedTest
    @EnumSource(SortOrder.class)
    public void testTimestampBoundary(SortOrder order)
    {
        assertBoundaryMatchesOrdering(TIMESTAMP_MICROS, order, List.of(-1L, 0L, 1L));
        assertBoundaryMatchesOrdering(TIMESTAMP_PICOS, order, List.of(new LongTimestamp(-1, 999999), new LongTimestamp(0, 0), new LongTimestamp(0, 1)));
    }

    @ParameterizedTest
    @EnumSource(SortOrder.class)
    public void testAdditionalOrderableTypes(SortOrder order)
    {
        assertBoundaryMatchesOrdering(VARBINARY, order, List.of(utf8Slice("a"), utf8Slice("z")));
        assertBoundaryMatchesOrdering(NUMBER, order, List.of(
                TrinoNumber.from(new TrinoNumber.Infinity(true)),
                TrinoNumber.from(ONE),
                TrinoNumber.from(new TrinoNumber.Infinity(false)),
                TrinoNumber.from(new TrinoNumber.NotANumber())));

        var type = new ArrayType(BIGINT);
        Block nullElement = BIGINT.createNullBlock();
        Block oneElement = NullableValue.of(BIGINT, 1L).asBlock();
        if (!order.isNullsFirst()) {
            assertBoundaryMatchesOrdering(type, order, List.of(nullElement, oneElement));
            return;
        }

        var filter = new TopNDynamicFilter(new TestingColumnHandle("key"), 0, type, order, new BlockTypeOperators());
        filter.update(new Page(NullableValue.of(type, oneElement).asBlock()), 0);
        assertThat(filter.getCurrentPredicate().isAll()).isTrue();
        filter.update(new Page(type.createNullBlock()), 0);
        assertThat(filter.getCurrentPredicate().getDomains().orElseThrow().values()).containsExactly(Domain.onlyNull(type));
    }

    private static void assertBoundaryMatchesOrdering(Type type, SortOrder order, List<?> values)
    {
        var operators = new BlockTypeOperators();
        var ordering = operators.generateBlockPositionOrdering(type, order);
        var column = new TestingColumnHandle("key");
        for (Object boundary : values) {
            var filter = new TopNDynamicFilter(column, 0, type, order, operators);
            Block bound = new NullableValue(type, boundary).asBlock();
            filter.update(new Page(bound), 0);
            Domain domain = filter.getCurrentPredicate().getDomains().orElseThrow().getOrDefault(column, Domain.all(type));
            for (Object value : values) {
                Block candidate = new NullableValue(type, value).asBlock();
                assertThat(domain.includesNullableValue(value)).isEqualTo(ordering.order(candidate, 0, bound, 0) <= 0);
            }
        }
    }

    @Test
    public void testOversizedVarcharBoundary()
    {
        var column = new TestingColumnHandle("key");
        var filter = new TopNDynamicFilter(column, 0, VARCHAR, ASC_NULLS_LAST, new BlockTypeOperators());
        filter.update(new Page(NullableValue.of(VARCHAR, utf8Slice("a".repeat(65537))).asBlock()), 0);
        assertThat(filter.getCurrentPredicate().isAll()).isTrue();
        filter.update(new Page(NullableValue.of(VARCHAR, utf8Slice("a")).asBlock()), 0);
        assertThat(filter.getCurrentPredicate().isAll()).isFalse();
    }

    @Test
    public void testSingleFieldKey()
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .pageBreak()
                .row(5L, 0.5)
                .row(4L, 0.41)
                .row(6L, 0.6)
                .pageBreak()
                .build();

        OperatorFactory operatorFactory = topNOperatorFactory(
                ImmutableList.of(BIGINT, DOUBLE),
                2,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST));

        MaterializedResult expected = resultBuilder(BIGINT, DOUBLE)
                .row(6L, 0.6)
                .row(5L, 0.5)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testMultiFieldKey()
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .row("a", 1L)
                .row("b", 2L)
                .pageBreak()
                .row("f", 3L)
                .row("a", 4L)
                .pageBreak()
                .row("d", 5L)
                .row("d", 7L)
                .row("e", 6L)
                .build();

        OperatorFactory operatorFactory = topNOperatorFactory(
                ImmutableList.of(VARCHAR, BIGINT),
                3,
                ImmutableList.of(0, 1),
                ImmutableList.of(DESC_NULLS_LAST, DESC_NULLS_LAST));

        MaterializedResult expected = MaterializedResult.resultBuilder(VARCHAR, BIGINT)
                .row("f", 3L)
                .row("e", 6L)
                .row("d", 7L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testReverseOrder()
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .pageBreak()
                .row(5L, 0.5)
                .row(4L, 0.41)
                .row(6L, 0.6)
                .pageBreak()
                .build();

        OperatorFactory operatorFactory = topNOperatorFactory(
                ImmutableList.of(BIGINT, DOUBLE),
                2,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST));

        MaterializedResult expected = resultBuilder(BIGINT, DOUBLE)
                .row(-1L, -0.1)
                .row(1L, 0.1)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testLimitZero()
            throws Exception
    {
        OperatorFactory factory = topNOperatorFactory(
                ImmutableList.of(BIGINT),
                0,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST));

        try (Operator operator = factory.createOperator(driverContext)) {
            assertThat(operator.getOutput()).isNull();
            assertThat(operator.isFinished()).isTrue();
            assertThat(operator.needsInput()).isFalse();
            assertThat(operator.getOutput()).isNull();
        }
    }

    @Test
    public void testExceedMemoryLimit()
            throws Exception
    {
        Page input = rowPagesBuilder(BIGINT)
                .row(1L)
                .buildPage();

        DriverContext smallDiverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.ofBytes(1))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        OperatorFactory operatorFactory = topNOperatorFactory(
                ImmutableList.of(BIGINT),
                100,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST));
        try (Operator operator = operatorFactory.createOperator(smallDiverContext)) {
            operator.addInput(input);
            assertThatThrownBy(operator::getOutput)
                    .isInstanceOf(ExceededMemoryLimitException.class)
                    .hasMessageStartingWith("Query exceeded per-node memory limit of ");
        }
    }

    private OperatorFactory topNOperatorFactory(
            List<Type> types,
            int n,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders)
    {
        List<Type> sortTypes = sortChannels.stream()
                .map(types::get)
                .collect(toImmutableList());
        return TopNOperator.createOperatorFactory(
                0,
                new PlanNodeId("test"),
                types,
                n,
                orderingCompiler.compilePageWithPositionComparator(sortTypes, sortChannels, sortOrders));
    }
}
