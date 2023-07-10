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
package io.trino.plugin.tpcds;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.tpcds.TpcdsSessionProperties.getSplitsPerNode;
import static io.trino.plugin.tpcds.TpcdsSessionProperties.isWithNoSexism;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public class TpcdsSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;

    @Inject
    public TpcdsSplitManager(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
        checkState(!nodes.isEmpty(), "No TPCDS nodes available");

        boolean noSexism = isWithNoSexism(session);
        int splitCount = getSplitCount(session, nodes.size());
        int partNumber = 0;

        // sort to ensure the assignment is consistent with TpcdsNodePartitioningProvider
        List<Node> sortedNodes = nodes.stream()
                .sorted(comparing(node -> node.getHostAndPort().toString()))
                .collect(toImmutableList());

        // Split the data using split and skew by the number of nodes available.
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        for (int i = 0; i < splitCount; i++) {
            Node node = sortedNodes.get(i % nodes.size());
            splits.add(new TpcdsSplit(partNumber, splitCount, ImmutableList.of(node.getHostAndPort()), noSexism));
            partNumber++;
        }

        return new FixedSplitSource(splits.build());
    }

    public static int getSplitCount(ConnectorSession session, int nodeCount)
    {
        return TpcdsSessionProperties.getSplitCount(session)
                .orElseGet(() -> getSplitsPerNode(session) * nodeCount);
    }
}
