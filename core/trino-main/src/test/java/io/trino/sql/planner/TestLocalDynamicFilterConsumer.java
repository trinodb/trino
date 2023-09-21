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

package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.metadata.AbstractMockMetadata.dummyMetadata;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestLocalDynamicFilterConsumer
        extends BasePlanTest
{
    public TestLocalDynamicFilterConsumer()
    {
        super(ImmutableMap.of(
                ENABLE_DYNAMIC_FILTERING, "true",
                JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name(),
                JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name()));
    }

    @Test
    public void testSimple()
    {
        TestingDynamicFilterCollector collector = new TestingDynamicFilterCollector();
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(new DynamicFilterId("123"), 0),
                ImmutableMap.of(new DynamicFilterId("123"), INTEGER),
                ImmutableList.of(collector),
                DataSize.of(100, KILOBYTE));
        filter.setPartitionCount(1);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of(new DynamicFilterId("123"), 0));
        assertFalse(collector.isCollectionComplete());

        filter.addPartition(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 7L))));
        assertEquals(collector.getCollectedDomains(), ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 7L)));
    }

    @Test
    public void testShortCircuitOnAllTupleDomain()
    {
        TestingDynamicFilterCollector collector = new TestingDynamicFilterCollector();
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(new DynamicFilterId("123"), 0),
                ImmutableMap.of(new DynamicFilterId("123"), INTEGER),
                ImmutableList.of(collector),
                DataSize.of(100, KILOBYTE));

        assertFalse(collector.isCollectionComplete());

        filter.addPartition(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.all(INTEGER))));
        assertEquals(collector.getCollectedDomains(), ImmutableMap.of(new DynamicFilterId("123"), Domain.all(INTEGER)));

        filter.setPartitionCount(2);
        // adding another partition domain won't change final domain
        filter.addPartition(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 1L))));
        assertEquals(collector.getCollectedDomains(), ImmutableMap.of(new DynamicFilterId("123"), Domain.all(INTEGER)));
    }

    @Test
    public void testMultiplePartitions()
    {
        TestingDynamicFilterCollector collector = new TestingDynamicFilterCollector();
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(new DynamicFilterId("123"), 0),
                ImmutableMap.of(new DynamicFilterId("123"), INTEGER),
                ImmutableList.of(collector),
                DataSize.of(100, KILOBYTE));
        assertEquals(filter.getBuildChannels(), ImmutableMap.of(new DynamicFilterId("123"), 0));

        assertFalse(collector.isCollectionComplete());
        filter.addPartition(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 10L))));

        assertFalse(collector.isCollectionComplete());
        filter.addPartition(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 20L))));

        assertFalse(collector.isCollectionComplete());
        filter.setPartitionCount(2);
        assertEquals(collector.getCollectedDomains(), ImmutableMap.of(
                new DynamicFilterId("123"), Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L))));
    }

    @Test
    public void testAllDomain()
    {
        DynamicFilterId filter1 = new DynamicFilterId("123");
        DynamicFilterId filter2 = new DynamicFilterId("124");
        TestingDynamicFilterCollector collector = new TestingDynamicFilterCollector();
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(
                        filter1, 0,
                        filter2, 1),
                ImmutableMap.of(
                        filter1, INTEGER,
                        filter2, INTEGER),
                ImmutableList.of(collector),
                DataSize.of(100, KILOBYTE));
        filter.setPartitionCount(1);

        assertFalse(collector.isCollectionComplete());

        filter.addPartition(TupleDomain.withColumnDomains(ImmutableMap.of(
                filter1, Domain.all(INTEGER),
                filter2, Domain.singleValue(INTEGER, 1L))));
        assertEquals(collector.getCollectedDomains(), ImmutableMap.of(filter1, Domain.all(INTEGER), filter2, Domain.singleValue(INTEGER, 1L)));
    }

    @Test
    public void testNone()
    {
        TestingDynamicFilterCollector collector = new TestingDynamicFilterCollector();
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(new DynamicFilterId("123"), 0),
                ImmutableMap.of(new DynamicFilterId("123"), INTEGER),
                ImmutableList.of(collector),
                DataSize.of(100, KILOBYTE));
        filter.setPartitionCount(1);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of(new DynamicFilterId("123"), 0));

        assertFalse(collector.isCollectionComplete());
        filter.addPartition(TupleDomain.none());

        assertEquals(collector.getCollectedDomains(), ImmutableMap.of(
                new DynamicFilterId("123"), Domain.none(INTEGER)));
    }

    @Test
    public void testMultipleColumns()
    {
        TestingDynamicFilterCollector collector = new TestingDynamicFilterCollector();
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(new DynamicFilterId("123"), 0, new DynamicFilterId("456"), 1),
                ImmutableMap.of(new DynamicFilterId("123"), INTEGER, new DynamicFilterId("456"), INTEGER),
                ImmutableList.of(collector),
                DataSize.of(100, KILOBYTE));
        filter.setPartitionCount(1);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of(new DynamicFilterId("123"), 0, new DynamicFilterId("456"), 1));
        assertFalse(collector.isCollectionComplete());

        filter.addPartition(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 10L),
                new DynamicFilterId("456"), Domain.singleValue(INTEGER, 20L))));
        assertEquals(collector.getCollectedDomains(), ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 10L),
                new DynamicFilterId("456"), Domain.singleValue(INTEGER, 20L)));
    }

    @Test
    public void testMultiplePartitionsAndColumns()
    {
        TestingDynamicFilterCollector collector = new TestingDynamicFilterCollector();
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(new DynamicFilterId("123"), 0, new DynamicFilterId("456"), 1),
                ImmutableMap.of(new DynamicFilterId("123"), INTEGER, new DynamicFilterId("456"), BIGINT),
                ImmutableList.of(collector),
                DataSize.of(100, KILOBYTE));
        filter.setPartitionCount(2);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of(new DynamicFilterId("123"), 0, new DynamicFilterId("456"), 1));

        assertFalse(collector.isCollectionComplete());
        filter.addPartition(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 10L),
                new DynamicFilterId("456"), Domain.singleValue(BIGINT, 100L))));

        assertFalse(collector.isCollectionComplete());
        filter.addPartition(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 20L),
                new DynamicFilterId("456"), Domain.singleValue(BIGINT, 200L))));

        assertEquals(collector.getCollectedDomains(), ImmutableMap.of(
                new DynamicFilterId("123"), Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L)),
                new DynamicFilterId("456"), Domain.multipleValues(BIGINT, ImmutableList.of(100L, 200L))));
    }

    @Test
    public void testDynamicFilterPruning()
    {
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), dummyMetadata(), getQueryRunner().getDefaultSession());
        Symbol left1 = planBuilder.symbol("left1", BIGINT);
        Symbol left2 = planBuilder.symbol("left2", INTEGER);
        Symbol left3 = planBuilder.symbol("left3", SMALLINT);
        Symbol right1 = planBuilder.symbol("right1", BIGINT);
        Symbol right2 = planBuilder.symbol("right2", INTEGER);
        Symbol right3 = planBuilder.symbol("right3", SMALLINT);
        DynamicFilterId filter1 = new DynamicFilterId("filter1");
        DynamicFilterId filter2 = new DynamicFilterId("filter2");
        DynamicFilterId filter3 = new DynamicFilterId("filter3");
        JoinNode joinNode = planBuilder.join(
                INNER,
                planBuilder.values(left1, left2, left3),
                planBuilder.values(right1, right2, right3),
                ImmutableList.of(
                        new EquiJoinClause(left1, right1),
                        new EquiJoinClause(left2, right2),
                        new EquiJoinClause(left3, right3)),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(filter1, right1, filter2, right2, filter3, right3));
        TestingDynamicFilterCollector collector = new TestingDynamicFilterCollector();
        LocalDynamicFilterConsumer consumer = LocalDynamicFilterConsumer.create(
                joinNode,
                ImmutableList.of(BIGINT, INTEGER, SMALLINT),
                ImmutableSet.of(filter1, filter3),
                ImmutableList.of(collector),
                DataSize.of(100, KILOBYTE));
        assertEquals(consumer.getBuildChannels(), ImmutableMap.of(filter1, 0, filter3, 2));

        // make sure domain types got propagated correctly
        assertFalse(collector.isCollectionComplete());
        consumer.addPartition(TupleDomain.none());
        assertFalse(collector.isCollectionComplete());
        consumer.setPartitionCount(1);
        assertEquals(
                collector.getCollectedDomains(),
                ImmutableMap.of(filter1, Domain.none(BIGINT), filter3, Domain.none(SMALLINT)));
    }

    @Test
    public void testCompactionOnSizeLimitExceeded()
    {
        TestingDynamicFilterCollector collector = new TestingDynamicFilterCollector();
        DataSize sizeLimit = DataSize.of(1, KILOBYTE);
        DynamicFilterId filterId = new DynamicFilterId("123");
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(filterId, 0),
                ImmutableMap.of(filterId, VARCHAR),
                ImmutableList.of(collector),
                sizeLimit);
        assertFalse(collector.isCollectionComplete());

        Domain domain1 = Domain.multipleValues(VARCHAR, LongStream.range(0, 5)
                .mapToObj(i -> utf8Slice("value" + i))
                .collect(toImmutableList()));
        Domain domain2 = Domain.multipleValues(VARCHAR, LongStream.range(6, 31)
                .mapToObj(i -> utf8Slice("value" + i))
                .collect(toImmutableList()));
        assertThat(domain1.getRetainedSizeInBytes()).isLessThan(sizeLimit.toBytes());
        assertThat(domain1.union(domain2).getRetainedSizeInBytes()).isGreaterThanOrEqualTo(sizeLimit.toBytes());

        filter.addPartition(TupleDomain.withColumnDomains(ImmutableMap.of(filterId, domain1)));
        assertFalse(collector.isCollectionComplete());
        filter.addPartition(TupleDomain.withColumnDomains(ImmutableMap.of(filterId, domain2)));
        assertFalse(collector.isCollectionComplete());

        filter.setPartitionCount(2);
        assertTrue(collector.isCollectionComplete());

        Domain collectedDomain = collector.getCollectedDomains().get(filterId);
        assertEquals(collectedDomain.getValues(), ValueSet.ofRanges(range(VARCHAR, utf8Slice("value0"), true, utf8Slice("value9"), true)));
    }

    @Test
    public void testSizeLimitExceededAfterCompaction()
    {
        TestingDynamicFilterCollector collector = new TestingDynamicFilterCollector();
        DataSize sizeLimit = DataSize.of(1, KILOBYTE);
        DynamicFilterId filterId = new DynamicFilterId("123");
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(filterId, 0),
                ImmutableMap.of(filterId, VARCHAR),
                ImmutableList.of(collector),
                sizeLimit);
        assertFalse(collector.isCollectionComplete());

        Domain domain1 = Domain.multipleValues(VARCHAR, LongStream.range(0, 5)
                .mapToObj(i -> utf8Slice("value" + i))
                .collect(toImmutableList()));
        Domain domain2 = Domain.multipleValues(VARCHAR, LongStream.range(6, 31)
                .mapToObj(i -> utf8Slice("value" + i))
                .collect(toImmutableList()));
        assertThat(domain1.getRetainedSizeInBytes()).isLessThan(sizeLimit.toBytes());
        assertThat(domain1.union(domain2).simplify(1).getRetainedSizeInBytes()).isLessThanOrEqualTo(sizeLimit.toBytes());

        filter.addPartition(TupleDomain.withColumnDomains(ImmutableMap.of(filterId, domain1)));
        assertFalse(collector.isCollectionComplete());

        filter.addPartition(TupleDomain.withColumnDomains(ImmutableMap.of(filterId, domain2)));
        assertFalse(collector.isCollectionComplete());

        Domain domain3 = Domain.singleValue(VARCHAR, utf8Slice(IntStream.range(0, 800)
                .mapToObj(i -> "x")
                .collect(joining())));

        assertThat(domain1.union(domain2).union(domain3).simplify(1).getRetainedSizeInBytes())
                .isGreaterThanOrEqualTo(sizeLimit.toBytes());

        filter.addPartition(TupleDomain.withColumnDomains(ImmutableMap.of(filterId, domain3)));
        assertTrue(collector.isCollectionComplete());

        assertEquals(collector.getCollectedDomains(), ImmutableMap.of(filterId, Domain.all(VARCHAR)));
    }

    private static class TestingDynamicFilterCollector
            implements Consumer<Map<DynamicFilterId, Domain>>
    {
        private Map<DynamicFilterId, Domain> collectedDomains;

        @Override
        public void accept(Map<DynamicFilterId, Domain> dynamicFilterDomains)
        {
            verify(collectedDomains == null, "collectedDomains is already set");
            collectedDomains = dynamicFilterDomains;
        }

        public boolean isCollectionComplete()
        {
            return collectedDomains != null;
        }

        public Map<DynamicFilterId, Domain> getCollectedDomains()
        {
            requireNonNull(collectedDomains, "collectedDomains is null");
            return collectedDomains;
        }
    }
}
