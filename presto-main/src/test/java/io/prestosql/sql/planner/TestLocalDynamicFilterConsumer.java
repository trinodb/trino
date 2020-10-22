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
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.optimizations.PlanNodeSearcher;
import io.prestosql.sql.planner.plan.DynamicFilterId;
import io.prestosql.sql.planner.plan.JoinNode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.prestosql.SystemSessionProperties.FORCE_SINGLE_NODE_OUTPUT;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestLocalDynamicFilterConsumer
        extends BasePlanTest
{
    public TestLocalDynamicFilterConsumer()
    {
        super(ImmutableMap.of(
                FORCE_SINGLE_NODE_OUTPUT, "false",
                ENABLE_DYNAMIC_FILTERING, "true",
                JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name(),
                JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name()));
    }

    @Test
    public void testSimple()
            throws Exception
    {
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(new DynamicFilterId("123"), 0),
                ImmutableMap.of(new DynamicFilterId("123"), INTEGER),
                1);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of(new DynamicFilterId("123"), 0));
        Consumer<TupleDomain<DynamicFilterId>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<DynamicFilterId, Domain>> result = filter.getDynamicFilterDomains();
        assertFalse(result.isDone());

        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 7L))));
        assertEquals(result.get(), ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 7L)));
    }

    @Test
    public void testShortCircuitOnAllTupleDomain()
            throws Exception
    {
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(new DynamicFilterId("123"), 0),
                ImmutableMap.of(new DynamicFilterId("123"), INTEGER),
                2);

        Consumer<TupleDomain<DynamicFilterId>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<DynamicFilterId, Domain>> result = filter.getDynamicFilterDomains();
        assertFalse(result.isDone());

        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.all(INTEGER))));
        assertEquals(result.get(), ImmutableMap.of(new DynamicFilterId("123"), Domain.all(INTEGER)));

        // adding another partition domain won't change final domain
        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 1L))));
        assertEquals(result.get(), ImmutableMap.of(new DynamicFilterId("123"), Domain.all(INTEGER)));
    }

    @Test
    public void testMultiplePartitions()
            throws Exception
    {
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(new DynamicFilterId("123"), 0),
                ImmutableMap.of(new DynamicFilterId("123"), INTEGER),
                2);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of(new DynamicFilterId("123"), 0));
        Consumer<TupleDomain<DynamicFilterId>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<DynamicFilterId, Domain>> result = filter.getDynamicFilterDomains();

        assertFalse(result.isDone());
        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 10L))));

        assertFalse(result.isDone());
        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 20L))));

        assertEquals(result.get(), ImmutableMap.of(
                new DynamicFilterId("123"), Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L))));
    }

    @Test
    public void testAllDomain()
            throws Exception
    {
        DynamicFilterId filter1 = new DynamicFilterId("123");
        DynamicFilterId filter2 = new DynamicFilterId("124");
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(
                        filter1, 0,
                        filter2, 1),
                ImmutableMap.of(
                        filter1, INTEGER,
                        filter2, INTEGER),
                1);

        Consumer<TupleDomain<DynamicFilterId>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<DynamicFilterId, Domain>> result = filter.getDynamicFilterDomains();
        assertFalse(result.isDone());

        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                filter1, Domain.all(INTEGER),
                filter2, Domain.singleValue(INTEGER, 1L))));
        assertEquals(result.get(), ImmutableMap.of(filter1, Domain.all(INTEGER), filter2, Domain.singleValue(INTEGER, 1L)));
    }

    @Test
    public void testNone()
            throws Exception
    {
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(new DynamicFilterId("123"), 0),
                ImmutableMap.of(new DynamicFilterId("123"), INTEGER),
                1);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of(new DynamicFilterId("123"), 0));
        Consumer<TupleDomain<DynamicFilterId>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<DynamicFilterId, Domain>> result = filter.getDynamicFilterDomains();

        assertFalse(result.isDone());
        consumer.accept(TupleDomain.none());

        assertEquals(result.get(), ImmutableMap.of(
                new DynamicFilterId("123"), Domain.none(INTEGER)));
    }

    @Test
    public void testMultipleColumns()
            throws Exception
    {
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(new DynamicFilterId("123"), 0, new DynamicFilterId("456"), 1),
                ImmutableMap.of(new DynamicFilterId("123"), INTEGER, new DynamicFilterId("456"), INTEGER),
                1);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of(new DynamicFilterId("123"), 0, new DynamicFilterId("456"), 1));
        Consumer<TupleDomain<DynamicFilterId>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<DynamicFilterId, Domain>> result = filter.getDynamicFilterDomains();
        assertFalse(result.isDone());

        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 10L),
                new DynamicFilterId("456"), Domain.singleValue(INTEGER, 20L))));
        assertEquals(result.get(), ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 10L),
                new DynamicFilterId("456"), Domain.singleValue(INTEGER, 20L)));
    }

    @Test
    public void testMultiplePartitionsAndColumns()
            throws Exception
    {
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(new DynamicFilterId("123"), 0, new DynamicFilterId("456"), 1),
                ImmutableMap.of(new DynamicFilterId("123"), INTEGER, new DynamicFilterId("456"), BIGINT),
                2);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of(new DynamicFilterId("123"), 0, new DynamicFilterId("456"), 1));
        Consumer<TupleDomain<DynamicFilterId>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<DynamicFilterId, Domain>> result = filter.getDynamicFilterDomains();

        assertFalse(result.isDone());
        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 10L),
                new DynamicFilterId("456"), Domain.singleValue(BIGINT, 100L))));

        assertFalse(result.isDone());
        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 20L),
                new DynamicFilterId("456"), Domain.singleValue(BIGINT, 200L))));

        assertEquals(result.get(), ImmutableMap.of(
                new DynamicFilterId("123"), Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L)),
                new DynamicFilterId("456"), Domain.multipleValues(BIGINT, ImmutableList.of(100L, 200L))));
    }

    @Test
    public void testCreateSingleColumn()
            throws Exception
    {
        SubPlan subplan = subplan(
                "SELECT count() FROM lineitem, orders WHERE lineitem.orderkey = orders.orderkey " +
                        "AND orders.custkey < 10",
                OPTIMIZED_AND_VALIDATED,
                false);
        JoinNode joinNode = searchJoins(subplan.getChildren().get(0).getFragment()).findOnlyElement();
        LocalDynamicFilterConsumer filter = LocalDynamicFilterConsumer.create(
                joinNode,
                ImmutableList.copyOf(subplan.getFragment().getSymbols().values()),
                1,
                joinNode.getDynamicFilters().keySet());
        DynamicFilterId filterId = getOnlyElement(filter.getBuildChannels().keySet());

        filter.getTupleDomainConsumer().accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                filterId, Domain.singleValue(BIGINT, 3L))));
        assertEquals(filter.getDynamicFilterDomains().get(), ImmutableMap.of(
                filterId, Domain.singleValue(BIGINT, 3L)));
    }

    @Test
    public void testCreateDistributedJoin()
            throws Exception
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
        LocalDynamicFilterConsumer filter = LocalDynamicFilterConsumer.create(
                joinNode,
                ImmutableList.copyOf(subplan.getFragment().getSymbols().values()),
                1,
                joinNode.getDynamicFilters().keySet());
        DynamicFilterId filterId = getOnlyElement(filter.getBuildChannels().keySet());
        assertFalse(joinNode.getDynamicFilters().isEmpty());

        filter.getTupleDomainConsumer().accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                filterId, Domain.singleValue(BIGINT, 3L))));
        assertEquals(filter.getDynamicFilterDomains().get(), ImmutableMap.of(
                filterId, Domain.singleValue(BIGINT, 3L)));
    }

    @Test
    public void testCreateMultipleCriteria()
            throws Exception
    {
        SubPlan subplan = subplan(
                "SELECT count() FROM lineitem, partsupp " +
                        "WHERE lineitem.partkey = partsupp.partkey AND lineitem.suppkey = partsupp.suppkey " +
                        "AND partsupp.availqty < 10",
                OPTIMIZED_AND_VALIDATED,
                false);

        JoinNode joinNode = searchJoins(subplan.getChildren().get(0).getFragment()).findOnlyElement();
        LocalDynamicFilterConsumer filter = LocalDynamicFilterConsumer.create(
                joinNode,
                ImmutableList.copyOf(subplan.getFragment().getSymbols().values()),
                1,
                joinNode.getDynamicFilters().keySet());
        List<DynamicFilterId> filterIds = filter
                .getBuildChannels()
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .collect(toImmutableList());

        Map<DynamicFilterId, Domain> domainMap = ImmutableMap.of(
                filterIds.get(0), Domain.singleValue(BIGINT, 4L),
                filterIds.get(1), Domain.singleValue(BIGINT, 5L));
        filter.getTupleDomainConsumer().accept(TupleDomain.withColumnDomains(domainMap));

        assertEquals(filter.getDynamicFilterDomains().get(), domainMap);
    }

    @Test
    public void testCreateMultipleJoins()
            throws Exception
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
            LocalDynamicFilterConsumer filter = LocalDynamicFilterConsumer.create(
                    joinNode,
                    ImmutableList.copyOf(subplan.getFragment().getSymbols().values()),
                    1,
                    joinNode.getDynamicFilters().keySet());
            DynamicFilterId filterId = getOnlyElement(filter.getBuildChannels().keySet());

            filter.getTupleDomainConsumer().accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                    filterId, Domain.singleValue(BIGINT, 6L))));
            assertEquals(filter.getDynamicFilterDomains().get(), ImmutableMap.of(
                    filterId, Domain.singleValue(BIGINT, 6L)));
        }
    }

    @Test
    public void testCreateProbeSideUnion()
            throws Exception
    {
        SubPlan subplan = subplan(
                "WITH union_table(key) AS " +
                        "((SELECT partkey FROM part) UNION (SELECT suppkey FROM supplier)) " +
                        "SELECT count() FROM union_table, nation WHERE union_table.key = nation.nationkey " +
                        "AND nation.comment = 'abc'",
                OPTIMIZED_AND_VALIDATED,
                true);

        JoinNode joinNode = searchJoins(subplan.getFragment()).findOnlyElement();
        LocalDynamicFilterConsumer filter = LocalDynamicFilterConsumer.create(
                joinNode,
                ImmutableList.copyOf(subplan.getFragment().getSymbols().values()),
                1,
                joinNode.getDynamicFilters().keySet());
        DynamicFilterId filterId = getOnlyElement(filter.getBuildChannels().keySet());

        filter.getTupleDomainConsumer().accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                filterId, Domain.singleValue(BIGINT, 7L))));
        assertEquals(filter.getDynamicFilterDomains().get(), ImmutableMap.of(
                filterId, Domain.singleValue(BIGINT, 7L)));
    }

    private PlanNodeSearcher searchJoins(PlanFragment fragment)
    {
        return PlanNodeSearcher
                .searchFrom(fragment.getRoot())
                .where(node -> node instanceof JoinNode);
    }
}
