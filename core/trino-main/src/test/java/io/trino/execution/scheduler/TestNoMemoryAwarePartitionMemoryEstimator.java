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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.connector.informationschema.InformationSchemaTable;
import io.trino.connector.informationschema.InformationSchemaTableHandle;
import io.trino.cost.StatsAndCosts;
import io.trino.metadata.TableHandle;
import io.trino.operator.RetryPolicy;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.spi.ErrorCode;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.TestingSession;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNoMemoryAwarePartitionMemoryEstimator
{
    @Test
    public void testInformationSchemaScan()
    {
        PlanFragment planFragment = tableScanPlanFragment("ts", new InformationSchemaTableHandle(TEST_CATALOG_NAME, InformationSchemaTable.VIEWS, ImmutableSet.of(), OptionalLong.empty()));

        PartitionMemoryEstimator estimator = createEstimator(planFragment);
        assertThat(estimator).isInstanceOf(NoMemoryPartitionMemoryEstimator.class);

        // test if NoMemoryPartitionMemoryEstimator returns 0 for initial and retry estimates
        PartitionMemoryEstimator.MemoryRequirements noMemoryRequirements = new PartitionMemoryEstimator.MemoryRequirements(DataSize.ofBytes(0));
        assertThat(estimator.getInitialMemoryRequirements()).isEqualTo(noMemoryRequirements);
        assertThat(estimator.getNextRetryMemoryRequirements(
                new PartitionMemoryEstimator.MemoryRequirements(DataSize.ofBytes(1)),
                DataSize.of(5, BYTE),
                StandardErrorCode.NOT_SUPPORTED.toErrorCode()))
                .isEqualTo(noMemoryRequirements);
        assertThat(estimator.getNextRetryMemoryRequirements(
                new PartitionMemoryEstimator.MemoryRequirements(DataSize.ofBytes(1)),
                DataSize.of(5, BYTE),
                StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES.toErrorCode()))
                .isEqualTo(noMemoryRequirements);
    }

    @Test
    public void testTpchTableScan()
    {
        PlanFragment planFragment = tableScanPlanFragment("ts", new TpchTableHandle(TEST_CATALOG_NAME, "nation", 1.0));
        PartitionMemoryEstimator estimator = createEstimator(planFragment);
        assertThat(estimator).isInstanceOf(MockDelegatePatitionMemoryEstimator.class);
    }

    @Test
    public void testRemoteFromInformationSchemaScan()
    {
        PlanFragment tableScanPlanFragment = tableScanPlanFragment("ts", new InformationSchemaTableHandle(TEST_CATALOG_NAME, InformationSchemaTable.VIEWS, ImmutableSet.of(), OptionalLong.empty()));
        PlanFragment parentFragment = getParentFragment(tableScanPlanFragment);

        PartitionMemoryEstimator estimator = createEstimator(parentFragment, tableScanPlanFragment);
        assertThat(estimator).isInstanceOf(NoMemoryPartitionMemoryEstimator.class);
    }

    @Test
    public void testRemoteFromTpchScan()
    {
        PlanFragment tableScanPlanFragment = tableScanPlanFragment("ts", new TpchTableHandle(TEST_CATALOG_NAME, "nation", 1.0));
        PlanFragment parentFragment = getParentFragment(tableScanPlanFragment);

        PartitionMemoryEstimator estimator = createEstimator(parentFragment, tableScanPlanFragment);
        assertThat(estimator).isInstanceOf(MockDelegatePatitionMemoryEstimator.class);
    }

    @Test
    public void testRemoteFromTwoInformationSchemaScans()
    {
        PlanFragment tableScanPlanFragment1 = tableScanPlanFragment("ts1", new InformationSchemaTableHandle(TEST_CATALOG_NAME, InformationSchemaTable.VIEWS, ImmutableSet.of(), OptionalLong.empty()));
        PlanFragment tableScanPlanFragment2 = tableScanPlanFragment("ts2", new InformationSchemaTableHandle(TEST_CATALOG_NAME, InformationSchemaTable.COLUMNS, ImmutableSet.of(), OptionalLong.empty()));
        PlanFragment parentFragment = getParentFragment(tableScanPlanFragment1, tableScanPlanFragment2);

        PartitionMemoryEstimator estimator = createEstimator(parentFragment, tableScanPlanFragment1, tableScanPlanFragment2);
        assertThat(estimator).isInstanceOf(NoMemoryPartitionMemoryEstimator.class);
    }

    @Test
    public void testRemoteFromInformationSchemaAndTpchTableScans()
    {
        PlanFragment tableScanPlanFragment1 = tableScanPlanFragment("ts1", new InformationSchemaTableHandle(TEST_CATALOG_NAME, InformationSchemaTable.VIEWS, ImmutableSet.of(), OptionalLong.empty()));
        PlanFragment tableScanPlanFragment2 = tableScanPlanFragment("ts", new TpchTableHandle(TEST_CATALOG_NAME, "nation", 1.0));
        PlanFragment parentFragment = getParentFragment(tableScanPlanFragment1, tableScanPlanFragment2);

        PartitionMemoryEstimator estimator = createEstimator(parentFragment, tableScanPlanFragment1, tableScanPlanFragment2);
        assertThat(estimator).isInstanceOf(MockDelegatePatitionMemoryEstimator.class);
    }

    private static PlanFragment getParentFragment(PlanFragment... childFragments)
    {
        ImmutableList<PlanFragmentId> childFragmentIds = Stream.of(childFragments)
                .map(PlanFragment::getId)
                .collect(toImmutableList());
        return new PlanFragment(
                new PlanFragmentId("parent"),
                new RemoteSourceNode(new PlanNodeId("rsn"), childFragmentIds, ImmutableList.of(), Optional.empty(), ExchangeNode.Type.GATHER, RetryPolicy.TASK),
                ImmutableMap.of(),
                SOURCE_DISTRIBUTION,
                Optional.empty(),
                ImmutableList.of(),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of()),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                Optional.empty());
    }

    private PartitionMemoryEstimator createEstimator(PlanFragment planFragment, PlanFragment... sourceFragments)
    {
        NoMemoryAwarePartitionMemoryEstimator.Factory noMemoryAwareEstimatorFactory = new NoMemoryAwarePartitionMemoryEstimator.Factory(new MockDelgatePartitionMemoryEstimatorFactory());
        Session session = TestingSession.testSessionBuilder().build();

        Function<PlanFragmentId, PlanFragment> sourceFragmentsLookup = Maps.uniqueIndex(Arrays.asList(sourceFragments), PlanFragment::getId)::get;
        return noMemoryAwareEstimatorFactory.createPartitionMemoryEstimator(
                session,
                planFragment,
                sourceFragmentsLookup);
    }

    private static PlanFragment tableScanPlanFragment(String fragmentId, ConnectorTableHandle tableHandle)
    {
        TableScanNode informationSchemaViewsTableScan = new TableScanNode(
                new PlanNodeId("tableScan"),
                new TableHandle(
                        TEST_CATALOG_HANDLE,
                        tableHandle,
                        TestingTransactionHandle.create()),
                ImmutableList.of(),
                ImmutableMap.of(),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.empty());

        return new PlanFragment(
                new PlanFragmentId(fragmentId),
                informationSchemaViewsTableScan,
                ImmutableMap.of(),
                SOURCE_DISTRIBUTION,
                Optional.empty(),
                ImmutableList.of(),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of()),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                Optional.empty());
    }

    private static class MockDelgatePartitionMemoryEstimatorFactory
            implements PartitionMemoryEstimatorFactory
    {
        @Override
        public PartitionMemoryEstimator createPartitionMemoryEstimator(Session session, PlanFragment planFragment, Function<PlanFragmentId, PlanFragment> sourceFragmentLookup)
        {
            return new MockDelegatePatitionMemoryEstimator();
        }
    }

    private static class MockDelegatePatitionMemoryEstimator
            implements PartitionMemoryEstimator
    {
        @Override
        public MemoryRequirements getInitialMemoryRequirements()
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public MemoryRequirements getNextRetryMemoryRequirements(MemoryRequirements previousMemoryRequirements, DataSize peakMemoryUsage, ErrorCode errorCode)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public void registerPartitionFinished(MemoryRequirements previousMemoryRequirements, DataSize peakMemoryUsage, boolean success, Optional<ErrorCode> errorCode)
        {
            throw new RuntimeException("not implemented");
        }
    }
}
