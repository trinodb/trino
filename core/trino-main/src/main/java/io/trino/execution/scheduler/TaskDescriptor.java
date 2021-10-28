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

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import io.trino.metadata.Split;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TaskDescriptor
{
    private final int partitionId;
    private final Multimap<PlanNodeId, Split> splits;
    private final Multimap<PlanNodeId, ExchangeSourceHandle> exchangeSourceHandles;
    private final NodeRequirements nodeRequirements;

    public TaskDescriptor(
            int partitionId,
            Multimap<PlanNodeId, Split> splits,
            Multimap<PlanNodeId, ExchangeSourceHandle> exchangeSourceHandles,
            NodeRequirements nodeRequirements)
    {
        this.partitionId = partitionId;
        this.splits = ImmutableMultimap.copyOf(requireNonNull(splits, "splits is null"));
        this.exchangeSourceHandles = ImmutableMultimap.copyOf(requireNonNull(exchangeSourceHandles, "exchangeSourceHandles is null"));
        this.nodeRequirements = requireNonNull(nodeRequirements, "nodeRequirements is null");
    }

    public int getPartitionId()
    {
        return partitionId;
    }

    public Multimap<PlanNodeId, Split> getSplits()
    {
        return splits;
    }

    public Multimap<PlanNodeId, ExchangeSourceHandle> getExchangeSourceHandles()
    {
        return exchangeSourceHandles;
    }

    public NodeRequirements getNodeRequirements()
    {
        return nodeRequirements;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskDescriptor that = (TaskDescriptor) o;
        return partitionId == that.partitionId && Objects.equals(splits, that.splits) && Objects.equals(exchangeSourceHandles, that.exchangeSourceHandles) && Objects.equals(nodeRequirements, that.nodeRequirements);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionId, splits, exchangeSourceHandles, nodeRequirements);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitionId", partitionId)
                .add("splits", splits)
                .add("exchangeSourceHandles", exchangeSourceHandles)
                .add("nodeRequirements", nodeRequirements)
                .toString();
    }
}
