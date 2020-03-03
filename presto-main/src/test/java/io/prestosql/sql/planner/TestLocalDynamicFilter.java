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

package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.optimizations.PlanNodeSearcher;
import io.prestosql.sql.planner.plan.JoinNode;
import org.testng.annotations.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.prestosql.SystemSessionProperties.FORCE_SINGLE_NODE_OUTPUT;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestLocalDynamicFilter
        extends BasePlanTest
{
    public TestLocalDynamicFilter()
    {
        super(ImmutableMap.of(
                FORCE_SINGLE_NODE_OUTPUT, "false",
                ENABLE_DYNAMIC_FILTERING, "true",
                JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name(),
                JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name()));
    }

    @Test
    public void testSimple()
            throws ExecutionException, InterruptedException
    {
        LocalDynamicFilter filter = new LocalDynamicFilter(
                ImmutableMultimap.of("123", new Symbol("a")),
                ImmutableMap.of("123", 0),
                TypeProvider.copyOf(ImmutableMap.of(new Symbol("a"), INTEGER)),
                1);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of("123", 0));
        Consumer<TupleDomain<String>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<Symbol, Domain>> result = filter.getResultFuture();
        assertFalse(result.isDone());

        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                "123", Domain.singleValue(INTEGER, 7L))));
        assertEquals(result.get(), ImmutableMap.of(
                new Symbol("a"), Domain.singleValue(INTEGER, 7L)));
    }

    @Test
    public void testMultipleProbeSymbols()
            throws ExecutionException, InterruptedException
    {
        LocalDynamicFilter filter = new LocalDynamicFilter(
                ImmutableMultimap.of("123", new Symbol("a1"), "123", new Symbol("a2")),
                ImmutableMap.of("123", 0),
                TypeProvider.copyOf(ImmutableMap.of(new Symbol("a1"), INTEGER, new Symbol("a2"), INTEGER)),
                1);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of("123", 0));
        Consumer<TupleDomain<String>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<Symbol, Domain>> result = filter.getResultFuture();
        assertFalse(result.isDone());

        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                "123", Domain.singleValue(INTEGER, 7L))));
        assertEquals(result.get(), ImmutableMap.of(
                new Symbol("a1"), Domain.singleValue(INTEGER, 7L),
                new Symbol("a2"), Domain.singleValue(INTEGER, 7L)));
    }

    @Test
    public void testMultiplePartitions()
            throws ExecutionException, InterruptedException
    {
        LocalDynamicFilter filter = new LocalDynamicFilter(
                ImmutableMultimap.of("123", new Symbol("a")),
                ImmutableMap.of("123", 0),
                TypeProvider.copyOf(ImmutableMap.of(new Symbol("a"), INTEGER)),
                2);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of("123", 0));
        Consumer<TupleDomain<String>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<Symbol, Domain>> result = filter.getResultFuture();

        assertFalse(result.isDone());
        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                "123", Domain.singleValue(INTEGER, 10L))));

        assertFalse(result.isDone());
        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                "123", Domain.singleValue(INTEGER, 20L))));

        assertEquals(result.get(), ImmutableMap.of(
                new Symbol("a"), Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L))));
    }

    @Test
    public void testNone()
            throws ExecutionException, InterruptedException
    {
        LocalDynamicFilter filter = new LocalDynamicFilter(
                ImmutableMultimap.of("123", new Symbol("a")),
                ImmutableMap.of("123", 0),
                TypeProvider.copyOf(ImmutableMap.of(new Symbol("a"), INTEGER)),
                1);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of("123", 0));
        Consumer<TupleDomain<String>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<Symbol, Domain>> result = filter.getResultFuture();

        assertFalse(result.isDone());
        consumer.accept(TupleDomain.none());

        assertEquals(result.get(), ImmutableMap.of(
                new Symbol("a"), Domain.none(INTEGER)));
    }

    @Test
    public void testMultipleColumns()
            throws ExecutionException, InterruptedException
    {
        LocalDynamicFilter filter = new LocalDynamicFilter(
                ImmutableMultimap.of("123", new Symbol("a"), "456", new Symbol("b")),
                ImmutableMap.of("123", 0, "456", 1),
                TypeProvider.copyOf(ImmutableMap.of(new Symbol("a"), INTEGER)),
                1);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of("123", 0, "456", 1));
        Consumer<TupleDomain<String>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<Symbol, Domain>> result = filter.getResultFuture();
        assertFalse(result.isDone());

        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                "123", Domain.singleValue(INTEGER, 10L),
                "456", Domain.singleValue(INTEGER, 20L))));
        assertEquals(result.get(), ImmutableMap.of(
                new Symbol("a"), Domain.singleValue(INTEGER, 10L),
                new Symbol("b"), Domain.singleValue(INTEGER, 20L)));
    }

    @Test
    public void testMultiplePartitionsAndColumns()
            throws ExecutionException, InterruptedException
    {
        LocalDynamicFilter filter = new LocalDynamicFilter(
                ImmutableMultimap.of("123", new Symbol("a"), "456", new Symbol("b")),
                ImmutableMap.of("123", 0, "456", 1),
                TypeProvider.copyOf(ImmutableMap.of(new Symbol("a"), INTEGER, new Symbol("b"), BIGINT)),
                2);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of("123", 0, "456", 1));
        Consumer<TupleDomain<String>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<Symbol, Domain>> result = filter.getResultFuture();

        assertFalse(result.isDone());
        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                "123", Domain.singleValue(INTEGER, 10L),
                "456", Domain.singleValue(BIGINT, 100L))));

        assertFalse(result.isDone());
        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                "123", Domain.singleValue(INTEGER, 20L),
                "456", Domain.singleValue(BIGINT, 200L))));

        assertEquals(result.get(), ImmutableMap.of(
                new Symbol("a"), Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L)),
                new Symbol("b"), Domain.multipleValues(BIGINT, ImmutableList.of(100L, 200L))));
    }

    @Test
    public void testCreateSingleColumn()
            throws ExecutionException, InterruptedException
    {
        SubPlan subplan = subplan(
                "SELECT count() FROM lineitem, orders WHERE lineitem.orderkey = orders.orderkey " +
                        "AND orders.custkey < 10",
                OPTIMIZED_AND_VALIDATED,
                false);
        JoinNode joinNode = searchJoins(subplan.getChildren().get(0).getFragment()).findOnlyElement();
        LocalDynamicFilter filter = LocalDynamicFilter.create(joinNode, TypeProvider.copyOf(subplan.getFragment().getSymbols()), 1).get();
        String filterId = Iterables.getOnlyElement(filter.getBuildChannels().keySet());
        Symbol probeSymbol = Iterables.getOnlyElement(joinNode.getCriteria()).getLeft();

        filter.getTupleDomainConsumer().accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                filterId, Domain.singleValue(BIGINT, 3L))));
        assertEquals(filter.getResultFuture().get(), ImmutableMap.of(
                probeSymbol, Domain.singleValue(BIGINT, 3L)));
    }

    @Test
    public void testCreateDistributedJoin()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .build();
        SubPlan subplan = subplan(
                "SELECT count() FROM nation, region WHERE nation.regionkey = region.regionkey " +
                        "AND region.comment = 'abc'",
                OPTIMIZED_AND_VALIDATED,
                false,
                session);
        JoinNode joinNode = searchJoins(subplan.getChildren().get(0).getFragment()).findOnlyElement();
        assertFalse(joinNode.getDynamicFilters().isEmpty());
        assertEquals(LocalDynamicFilter.create(joinNode, TypeProvider.copyOf(subplan.getFragment().getSymbols()), 1), Optional.empty());
    }

    @Test
    public void testCreateMultipleCriteria()
            throws ExecutionException, InterruptedException
    {
        SubPlan subplan = subplan(
                "SELECT count() FROM lineitem, partsupp " +
                        "WHERE lineitem.partkey = partsupp.partkey AND lineitem.suppkey = partsupp.suppkey " +
                        "AND partsupp.availqty < 10",
                OPTIMIZED_AND_VALIDATED,
                false);

        JoinNode joinNode = searchJoins(subplan.getChildren().get(0).getFragment()).findOnlyElement();
        LocalDynamicFilter filter = LocalDynamicFilter.create(joinNode, TypeProvider.copyOf(subplan.getFragment().getSymbols()), 1).get();
        List<String> filterIds = filter
                .getBuildChannels()
                .entrySet()
                .stream()
                .sorted(Comparator.comparing(e -> e.getValue()))
                .map(e -> e.getKey())
                .collect(toImmutableList());
        filter.getTupleDomainConsumer().accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                filterIds.get(0), Domain.singleValue(BIGINT, 4L),
                filterIds.get(1), Domain.singleValue(BIGINT, 5L))));

        assertEquals(filter.getResultFuture().get(), ImmutableMap.of(
                new Symbol("partkey"), Domain.singleValue(BIGINT, 4L),
                new Symbol("suppkey"), Domain.singleValue(BIGINT, 5L)));
    }

    @Test
    public void testCreateMultipleJoins()
            throws ExecutionException, InterruptedException
    {
        SubPlan subplan = subplan(
                "SELECT count() FROM lineitem, orders, part " +
                        "WHERE lineitem.orderkey = orders.orderkey AND lineitem.partkey = part.partkey " +
                        "AND orders.custkey < 10 AND part.name = 'abc'",
                OPTIMIZED_AND_VALIDATED,
                false);

        List<JoinNode> joinNodes = searchJoins(subplan.getChildren().get(0).getFragment()).findAll();
        assertEquals(joinNodes.size(), 2);
        for (JoinNode joinNode : joinNodes) {
            LocalDynamicFilter filter = LocalDynamicFilter.create(joinNode, TypeProvider.copyOf(subplan.getFragment().getSymbols()), 1).get();
            String filterId = Iterables.getOnlyElement(filter.getBuildChannels().keySet());
            Symbol probeSymbol = Iterables.getOnlyElement(joinNode.getCriteria()).getLeft();

            filter.getTupleDomainConsumer().accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                    filterId, Domain.singleValue(BIGINT, 6L))));
            assertEquals(filter.getResultFuture().get(), ImmutableMap.of(
                    probeSymbol, Domain.singleValue(BIGINT, 6L)));
        }
    }

    @Test
    public void testCreateProbeSideUnion()
            throws ExecutionException, InterruptedException
    {
        SubPlan subplan = subplan(
                "WITH union_table(key) AS " +
                        "((SELECT partkey FROM part) UNION (SELECT suppkey FROM supplier)) " +
                        "SELECT count() FROM union_table, nation WHERE union_table.key = nation.nationkey " +
                        "AND nation.comment = 'abc'",
                OPTIMIZED_AND_VALIDATED,
                true);

        JoinNode joinNode = searchJoins(subplan.getFragment()).findOnlyElement();
        LocalDynamicFilter filter = LocalDynamicFilter.create(joinNode, TypeProvider.copyOf(subplan.getFragment().getSymbols()), 1).get();
        String filterId = Iterables.getOnlyElement(filter.getBuildChannels().keySet());

        filter.getTupleDomainConsumer().accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                filterId, Domain.singleValue(BIGINT, 7L))));
        assertEquals(filter.getResultFuture().get(), ImmutableMap.of(
                new Symbol("partkey"), Domain.singleValue(BIGINT, 7L),
                new Symbol("suppkey"), Domain.singleValue(BIGINT, 7L)));
    }

    private PlanNodeSearcher searchJoins(PlanFragment fragment)
    {
        return PlanNodeSearcher
                .searchFrom(fragment.getRoot())
                .where(node -> node instanceof JoinNode);
    }
}
