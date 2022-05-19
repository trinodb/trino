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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.metadata.Split;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static java.util.Objects.requireNonNull;

public class TestingTaskSourceFactory
        implements TaskSourceFactory
{
    private final Optional<CatalogName> catalog;
    private final List<Split> splits;
    private final int tasksPerBatch;

    public TestingTaskSourceFactory(Optional<CatalogName> catalog, List<Split> splits, int tasksPerBatch)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.splits = ImmutableList.copyOf(requireNonNull(splits, "splits is null"));
        this.tasksPerBatch = tasksPerBatch;
    }

    @Override
    public TaskSource create(
            Session session,
            PlanFragment fragment,
            Map<PlanFragmentId, Exchange> sourceExchanges,
            Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles,
            LongConsumer getSplitTimeRecorder,
            Optional<int[]> bucketToPartitionMap,
            Optional<BucketNodeMap> bucketNodeMap)
    {
        List<PlanNodeId> partitionedSources = fragment.getPartitionedSources();
        checkArgument(partitionedSources.size() == 1, "single partitioned source is expected");

        return new TestingTaskSource(
                catalog,
                splits,
                tasksPerBatch,
                getOnlyElement(partitionedSources),
                getHandlesForRemoteSources(fragment.getRemoteSourceNodes(), exchangeSourceHandles));
    }

    private static ListMultimap<PlanNodeId, ExchangeSourceHandle> getHandlesForRemoteSources(
            List<RemoteSourceNode> remoteSources,
            Multimap<PlanFragmentId, ExchangeSourceHandle> exchangeSourceHandles)
    {
        ImmutableListMultimap.Builder<PlanNodeId, ExchangeSourceHandle> result = ImmutableListMultimap.builder();
        for (RemoteSourceNode remoteSource : remoteSources) {
            checkArgument(remoteSource.getExchangeType() == REPLICATE, "expected exchange type to be REPLICATE, got: %s", remoteSource.getExchangeType());
            for (PlanFragmentId fragmentId : remoteSource.getSourceFragmentIds()) {
                Collection<ExchangeSourceHandle> handles = requireNonNull(exchangeSourceHandles.get(fragmentId), () -> "exchange source handle is missing for fragment: " + fragmentId);
                checkArgument(handles.size() == 1, "single exchange source handle is expected, got: %s", handles);
                result.putAll(remoteSource.getId(), handles);
            }
        }
        return result.build();
    }

    public static class TestingTaskSource
            implements TaskSource
    {
        private final Optional<CatalogName> catalogRequirement;
        private final Iterator<Split> splits;
        private final int tasksPerBatch;
        private final PlanNodeId tableScanPlanNodeId;
        private final ListMultimap<PlanNodeId, ExchangeSourceHandle> exchangeSourceHandles;

        private final AtomicInteger nextPartitionId = new AtomicInteger();

        public TestingTaskSource(
                Optional<CatalogName> catalogRequirement,
                List<Split> splits,
                int tasksPerBatch,
                PlanNodeId tableScanPlanNodeId,
                ListMultimap<PlanNodeId, ExchangeSourceHandle> exchangeSourceHandles)
        {
            this.catalogRequirement = requireNonNull(catalogRequirement, "catalogRequirement is null");
            this.splits = ImmutableList.copyOf(requireNonNull(splits, "splits is null")).iterator();
            this.tasksPerBatch = tasksPerBatch;
            this.tableScanPlanNodeId = requireNonNull(tableScanPlanNodeId, "tableScanPlanNodeId is null");
            this.exchangeSourceHandles = ImmutableListMultimap.copyOf(requireNonNull(exchangeSourceHandles, "exchangeSourceHandles is null"));
        }

        @Override
        public List<TaskDescriptor> getMoreTasks()
        {
            checkState(!isFinished(), "already finished");

            ImmutableList.Builder<TaskDescriptor> result = ImmutableList.builder();
            for (int i = 0; i < tasksPerBatch; i++) {
                if (isFinished()) {
                    break;
                }
                Split split = splits.next();
                TaskDescriptor task = new TaskDescriptor(
                        nextPartitionId.getAndIncrement(),
                        ImmutableListMultimap.of(tableScanPlanNodeId, split),
                        exchangeSourceHandles,
                        new NodeRequirements(catalogRequirement, ImmutableSet.copyOf(split.getAddresses()), DataSize.of(4, GIGABYTE)));
                result.add(task);
            }

            return result.build();
        }

        @Override
        public boolean isFinished()
        {
            return !splits.hasNext();
        }

        @Override
        public void close()
        {
        }
    }
}
