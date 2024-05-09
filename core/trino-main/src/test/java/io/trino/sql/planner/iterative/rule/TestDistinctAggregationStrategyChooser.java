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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.cost.CostProvider;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsProvider;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.cost.TaskCountEstimator;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule.Context;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.transaction.TestingTransactionManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY;
import static io.trino.SystemSessionProperties.getTaskConcurrency;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy.MARK_DISTINCT;
import static io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy.PRE_AGGREGATE;
import static io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy.SINGLE_STEP;
import static io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy.SPLIT_TO_SUBQUERIES;
import static io.trino.sql.planner.iterative.rule.DistinctAggregationStrategyChooser.createDistinctAggregationStrategyChooser;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDistinctAggregationStrategyChooser
{
    private static final int NODE_COUNT = 6;
    private static final TaskCountEstimator TASK_COUNT_ESTIMATOR = new TaskCountEstimator(() -> NODE_COUNT);
    private static final TestingFunctionResolution functionResolution = new TestingFunctionResolution();
    private TestingTransactionManager transactionManager;
    private Metadata metadata;

    @BeforeAll
    public final void setUp()
    {
        this.transactionManager = new TestingTransactionManager();
        this.metadata = new AbstractMockMetadata()
        {
            @Override
            public boolean allowSplittingReadIntoMultipleSubQueries(Session session, TableHandle tableHandle)
            {
                return true;
            }
        };
    }

    @Test
    public void testSingleStepPreferredForHighCardinalitySingleGroupByKey()
    {
        DistinctAggregationStrategyChooser aggregationStrategyChooser = createDistinctAggregationStrategyChooser(TASK_COUNT_ESTIMATOR, metadata);
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol groupingKey = symbolAllocator.newSymbol("groupingKey", BIGINT);

        PlanNode source = tableScan();
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(ImmutableList.of(groupingKey), source, symbolAllocator);
        Context context = context(
                ImmutableMap.of(source, new PlanNodeStatsEstimate(1_000_000, ImmutableMap.of(
                        groupingKey, SymbolStatsEstimate.builder().setDistinctValuesCount(1_000_000).build()))),
                symbolAllocator);

        assertShouldUseSingleStep(aggregationStrategyChooser, aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup());
    }

    @Test
    public void testSingleStepPreferredForHighCardinalityMultipleGroupByKeys()
    {
        DistinctAggregationStrategyChooser aggregationStrategyChooser = createDistinctAggregationStrategyChooser(TASK_COUNT_ESTIMATOR, metadata);
        SymbolAllocator symbolAllocator = new SymbolAllocator();

        Symbol lowCardinalityGroupingKey = symbolAllocator.newSymbol("lowCardinalityGroupingKey", BIGINT);
        Symbol highCardinalityGroupingKey = symbolAllocator.newSymbol("highCardinalityGroupingKey", BIGINT);

        PlanNode source = tableScan();
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(ImmutableList.of(lowCardinalityGroupingKey, highCardinalityGroupingKey), source, symbolAllocator);
        Context context = context(
                ImmutableMap.of(source, new PlanNodeStatsEstimate(1_000_000, ImmutableMap.of(
                        lowCardinalityGroupingKey, SymbolStatsEstimate.builder().setDistinctValuesCount(10).build(),
                        highCardinalityGroupingKey, SymbolStatsEstimate.builder().setDistinctValuesCount(1_000_000).build()))),
                symbolAllocator);

        assertShouldUseSingleStep(aggregationStrategyChooser, aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup());
    }

    @Test
    public void testPreAggregatePreferredForLowCardinality2GroupByKeys()
    {
        DistinctAggregationStrategyChooser aggregationStrategyChooser = createDistinctAggregationStrategyChooser(TASK_COUNT_ESTIMATOR, metadata);
        SymbolAllocator symbolAllocator = new SymbolAllocator();

        List<Symbol> groupingKeys = ImmutableList.of(
                symbolAllocator.newSymbol("key1", BIGINT),
                symbolAllocator.newSymbol("key2", BIGINT));

        PlanNode source = tableScan();
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(groupingKeys, source, symbolAllocator);
        Context context = context(
                ImmutableMap.of(source, new PlanNodeStatsEstimate(
                        1_000_000,
                        groupingKeys.stream().collect(toImmutableMap(
                                Function.identity(),
                                _ -> SymbolStatsEstimate.builder().setDistinctValuesCount(10).build())))),
                new SymbolAllocator());

        assertThat(aggregationStrategyChooser.shouldUsePreAggregate(aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup())).isTrue();
        assertThat(aggregationStrategyChooser.shouldAddMarkDistinct(aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup())).isFalse();
    }

    @Test
    public void testPreAggregatePreferredForUnknownStatisticsAnd2GroupByKeys()
    {
        DistinctAggregationStrategyChooser aggregationStrategyChooser = createDistinctAggregationStrategyChooser(TASK_COUNT_ESTIMATOR, metadata);
        SymbolAllocator symbolAllocator = new SymbolAllocator();

        List<Symbol> groupingKeys = ImmutableList.of(
                symbolAllocator.newSymbol("key1", BIGINT),
                symbolAllocator.newSymbol("key2", BIGINT));
        PlanNode source = tableScan();
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(groupingKeys, source, symbolAllocator);
        Context context = context(ImmutableMap.of(), new SymbolAllocator());
        assertThat(aggregationStrategyChooser.shouldUsePreAggregate(aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup())).isTrue();
        assertThat(aggregationStrategyChooser.shouldAddMarkDistinct(aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup())).isFalse();
    }

    @Test
    public void testPreAggregatePreferredForMediumCardinalitySingleGroupByKey()
    {
        DistinctAggregationStrategyChooser aggregationStrategyChooser = createDistinctAggregationStrategyChooser(TASK_COUNT_ESTIMATOR, metadata);
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol groupingKey = symbolAllocator.newSymbol("groupingKey", BIGINT);

        PlanNode source = tableScan();
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(ImmutableList.of(groupingKey), source, symbolAllocator);
        Context context = context(
                ImmutableMap.of(source, new PlanNodeStatsEstimate(NODE_COUNT * getTaskConcurrency(TEST_SESSION) * 10, ImmutableMap.of(
                        groupingKey, SymbolStatsEstimate.builder().setDistinctValuesCount(NODE_COUNT * getTaskConcurrency(TEST_SESSION) * 10).build()))),
                symbolAllocator);

        assertThat(aggregationStrategyChooser.shouldUsePreAggregate(aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup())).isTrue();
    }

    @Test
    public void testSingleStepPreferredForMediumCardinality3GroupByKeys()
    {
        DistinctAggregationStrategyChooser aggregationStrategyChooser = createDistinctAggregationStrategyChooser(TASK_COUNT_ESTIMATOR, metadata);
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        List<Symbol> groupingKeys = ImmutableList.of(
                symbolAllocator.newSymbol("key1", BIGINT),
                symbolAllocator.newSymbol("key2", BIGINT),
                symbolAllocator.newSymbol("key3", BIGINT));

        PlanNode source = tableScan();
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(groupingKeys, source, symbolAllocator);
        Context context = context(
                ImmutableMap.of(source, new PlanNodeStatsEstimate(NODE_COUNT * getTaskConcurrency(TEST_SESSION) * 10,
                        groupingKeys.stream().collect(toImmutableMap(
                                Function.identity(),
                                _ -> SymbolStatsEstimate.builder().setDistinctValuesCount(NODE_COUNT * getTaskConcurrency(TEST_SESSION) * 10).build())))),
                symbolAllocator);

        assertShouldUseSingleStep(aggregationStrategyChooser, aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup());
    }

    @Test
    public void testSplitToSubqueriesPreferredForGlobalAggregation()
    {
        DistinctAggregationStrategyChooser aggregationStrategyChooser = createDistinctAggregationStrategyChooser(TASK_COUNT_ESTIMATOR, metadata);
        SymbolAllocator symbolAllocator = new SymbolAllocator();

        PlanNode source = tableScan();
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(ImmutableList.of(), source, symbolAllocator);
        assertThat((boolean) inTransaction(session -> {
            Context context = context(
                    ImmutableMap.of(source, new PlanNodeStatsEstimate(1_000_000, ImmutableMap.of())),
                    session,
                    symbolAllocator);
            return aggregationStrategyChooser.shouldSplitToSubqueries(aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup());
        }))
                .isTrue();
    }

    @Test
    public void testMarkDistinctPreferredForLowCardinality3GroupByKeys()
    {
        DistinctAggregationStrategyChooser aggregationStrategyChooser = createDistinctAggregationStrategyChooser(TASK_COUNT_ESTIMATOR, metadata);
        SymbolAllocator symbolAllocator = new SymbolAllocator();

        List<Symbol> groupingKeys = ImmutableList.of(
                symbolAllocator.newSymbol("key1", BIGINT),
                symbolAllocator.newSymbol("key2", BIGINT),
                symbolAllocator.newSymbol("key3", BIGINT));

        PlanNode source = tableScan();
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(groupingKeys, source, symbolAllocator);
        Context context = context(
                ImmutableMap.of(source, new PlanNodeStatsEstimate(
                        1_000_000,
                        groupingKeys.stream().collect(toImmutableMap(
                                Function.identity(),
                                _ -> SymbolStatsEstimate.builder().setDistinctValuesCount(10).build())))),
                new SymbolAllocator());
        assertThat(aggregationStrategyChooser.shouldAddMarkDistinct(aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup())).isTrue();
    }

    @Test
    public void testMarkDistinctPreferredForUnknownStatisticsAnd3GroupByKeys()
    {
        DistinctAggregationStrategyChooser aggregationStrategyChooser = createDistinctAggregationStrategyChooser(TASK_COUNT_ESTIMATOR, metadata);
        SymbolAllocator symbolAllocator = new SymbolAllocator();

        List<Symbol> groupingKeys = ImmutableList.of(
                symbolAllocator.newSymbol("key1", BIGINT),
                symbolAllocator.newSymbol("key2", BIGINT),
                symbolAllocator.newSymbol("key3", BIGINT));
        PlanNode source = tableScan();
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(groupingKeys, source, symbolAllocator);
        assertThat((boolean) inTransaction(session -> {
            Context context = context(ImmutableMap.of(), session, symbolAllocator);
            return aggregationStrategyChooser.shouldAddMarkDistinct(aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup());
        }))
                .isTrue();
    }

    @Test
    public void testChoiceForcedByTheSessionProperty()
    {
        int clusterThreadCount = NODE_COUNT * getTaskConcurrency(TEST_SESSION);
        DistinctAggregationStrategyChooser aggregationStrategyChooser = createDistinctAggregationStrategyChooser(TASK_COUNT_ESTIMATOR, metadata);
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol groupingKey = symbolAllocator.newSymbol("groupingKey", BIGINT);

        TableScanNode source = new TableScanNode(
                new PlanNodeId("source"),
                TEST_TABLE_HANDLE,
                ImmutableList.of(),
                ImmutableMap.of(),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.empty());
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(ImmutableList.of(groupingKey), source, symbolAllocator);

        // big NDV, distinct_aggregations_strategy = mark_distinct
        assertThat((boolean) inTransaction(
                testSessionBuilder().setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, MARK_DISTINCT.name()).build(),
                session -> {
                    Context context = context(
                            ImmutableMap.of(source, new PlanNodeStatsEstimate(1000 * clusterThreadCount, ImmutableMap.of(
                                    groupingKey, SymbolStatsEstimate.builder().setDistinctValuesCount(1000 * clusterThreadCount).build()))),
                            session,
                            symbolAllocator);
                    return aggregationStrategyChooser.shouldAddMarkDistinct(aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup());
                }))
                .isTrue();

        // big NDV, distinct_aggregations_strategy = pre-aggregate
        assertThat((boolean) inTransaction(
                testSessionBuilder().setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, PRE_AGGREGATE.name()).build(),
                session -> {
                    Context context = context(
                            ImmutableMap.of(source, new PlanNodeStatsEstimate(1000 * clusterThreadCount, ImmutableMap.of(
                                    groupingKey, SymbolStatsEstimate.builder().setDistinctValuesCount(1000 * clusterThreadCount).build()))),
                            session,
                            symbolAllocator);
                    return aggregationStrategyChooser.shouldUsePreAggregate(aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup());
                }))
                .isTrue();

        // small NDV, distinct_aggregations_strategy = single_step
        Context smallNdvContext = context(
                ImmutableMap.of(source, new PlanNodeStatsEstimate(1000 * clusterThreadCount, ImmutableMap.of(
                        groupingKey, SymbolStatsEstimate.builder().setDistinctValuesCount(1000 * clusterThreadCount).build()))),
                testSessionBuilder().setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, SINGLE_STEP.name()).build(),
                symbolAllocator);
        assertShouldUseSingleStep(aggregationStrategyChooser, aggregationNode, smallNdvContext.getSession(), smallNdvContext.getStatsProvider(), smallNdvContext.getLookup());

                // big NDV, distinct_aggregations_strategy = split_to_subqueries
        assertThat((boolean) inTransaction(
                testSessionBuilder().setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, SPLIT_TO_SUBQUERIES.name()).build(),
                session -> {
                    Context context = context(
                            ImmutableMap.of(source, new PlanNodeStatsEstimate(1000 * clusterThreadCount, ImmutableMap.of(
                                    groupingKey, SymbolStatsEstimate.builder().setDistinctValuesCount(1000 * clusterThreadCount).build()))),
                            session,
                            symbolAllocator);
                    return aggregationStrategyChooser.shouldSplitToSubqueries(aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup());
                }))
                .isTrue();
    }

    private <T> T inTransaction(Function<Session, T> callback)
    {
        return inTransaction(TEST_SESSION, callback);
    }

    private <T> T inTransaction(Session session, Function<Session, T> callback)
    {
        return transaction(transactionManager, metadata, new AllowAllAccessControl())
                .execute(session, callback);
    }

    private static PlanNode tableScan()
    {
        return new TableScanNode(new PlanNodeId("source"), TEST_TABLE_HANDLE, ImmutableList.of(), ImmutableMap.of(), TupleDomain.all(), Optional.empty(), false, Optional.empty());
    }

    private static AggregationNode aggregationWithTwoDistinctAggregations(List<Symbol> groupingKeys, PlanNode source, SymbolAllocator symbolAllocator)
    {
        return singleAggregation(
                new PlanNodeId("aggregation"),
                source,
                twoDistinctAggregations(symbolAllocator),
                singleGroupingSet(groupingKeys));
    }

    private static Map<Symbol, Aggregation> twoDistinctAggregations(SymbolAllocator symbolAllocator)
    {
        return ImmutableMap.of(symbolAllocator.newSymbol("output1", BIGINT), new Aggregation(
                        functionResolution.resolveFunction("sum", fromTypes(BIGINT)),
                        ImmutableList.of(symbolAllocator.newSymbol("input1", BIGINT).toSymbolReference()),
                        true,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                symbolAllocator.newSymbol("output2", BIGINT), new Aggregation(
                        functionResolution.resolveFunction("sum", fromTypes(BIGINT)),
                        ImmutableList.of(symbolAllocator.newSymbol("input2", BIGINT).toSymbolReference()),
                        true,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
    }

    private static void assertShouldUseSingleStep(DistinctAggregationStrategyChooser aggregationStrategyChooser, AggregationNode aggregationNode, Session session, StatsProvider statsProvider, Lookup lookup)
    {
        assertThat(aggregationStrategyChooser.shouldAddMarkDistinct(aggregationNode, session, statsProvider, lookup)).isFalse();
        assertThat(aggregationStrategyChooser.shouldUsePreAggregate(aggregationNode, session, statsProvider, lookup)).isFalse();
    }

    private static Context context(Map<PlanNode, PlanNodeStatsEstimate> stats, SymbolAllocator symbolAllocator)
    {
        return context(stats, TEST_SESSION, symbolAllocator);
    }

    private static Context context(Map<PlanNode, PlanNodeStatsEstimate> stats, Session session, SymbolAllocator symbolAllocator)
    {
        PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
        return new Context()
        {
            @Override
            public Lookup getLookup()
            {
                return Lookup.noLookup();
            }

            @Override
            public PlanNodeIdAllocator getIdAllocator()
            {
                return planNodeIdAllocator;
            }

            @Override
            public SymbolAllocator getSymbolAllocator()
            {
                return symbolAllocator;
            }

            @Override
            public Session getSession()
            {
                return session;
            }

            @Override
            public StatsProvider getStatsProvider()
            {
                return node -> stats.getOrDefault(node, PlanNodeStatsEstimate.unknown());
            }

            @Override
            public CostProvider getCostProvider()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void checkTimeoutNotExhausted()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public WarningCollector getWarningCollector()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
