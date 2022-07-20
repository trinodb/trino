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
package io.trino.plugin.tpch;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Comparator.comparing;

public class TpchSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;
    private final int splitsPerNode;

    public TpchSplitManager(NodeManager nodeManager, int splitsPerNode)
    {
        this.nodeManager = nodeManager;
        checkArgument(splitsPerNode > 0, "splitsPerNode must be at least 1");
        this.splitsPerNode = splitsPerNode;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();

        int totalParts = nodes.size() * splitsPerNode;
        int partNumber = 0;

        // sort to ensure the assignment is consistent with TpchNodePartitioningProvider
        List<Node> sortedNodes = nodes.stream()
                .sorted(comparing(node -> node.getHostAndPort().toString()))
                .collect(toImmutableList());

        // Split the data using split and skew by the number of nodes available.
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        for (Node node : sortedNodes) {
            for (int i = 0; i < splitsPerNode; i++) {
                splits.add(new TpchSplit(partNumber, totalParts, ImmutableList.of(node.getHostAndPort())));
                partNumber++;
            }
        }
        return new FixedSplitSource(splits.build());
    }
}
