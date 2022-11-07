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
package io.trino.connector.system;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.SystemTable.Distribution;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;

import java.util.Set;

import static io.trino.metadata.NodeState.ACTIVE;
import static io.trino.spi.connector.SystemTable.Distribution.ALL_COORDINATORS;
import static io.trino.spi.connector.SystemTable.Distribution.ALL_NODES;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static java.util.Objects.requireNonNull;

public class SystemSplitManager
        implements ConnectorSplitManager
{
    private final InternalNodeManager nodeManager;
    private final SystemTablesProvider tables;

    public SystemSplitManager(InternalNodeManager nodeManager, SystemTablesProvider tables)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.tables = requireNonNull(tables, "tables is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        SystemTableHandle table = (SystemTableHandle) tableHandle;
        TupleDomain<ColumnHandle> tableConstraint = table.getConstraint();

        SystemTable systemTable = tables.getSystemTable(session, table.getSchemaTableName())
                // table might disappear in the meantime
                .orElseThrow(() -> new TableNotFoundException(table.getSchemaTableName()));

        Distribution tableDistributionMode = systemTable.getDistribution();
        if (tableDistributionMode == SINGLE_COORDINATOR) {
            HostAddress address = nodeManager.getCurrentNode().getHostAndPort();
            ConnectorSplit split = new SystemSplit(address, tableConstraint);
            return new FixedSplitSource(ImmutableList.of(split));
        }

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        ImmutableSet.Builder<InternalNode> nodes = ImmutableSet.builder();
        if (tableDistributionMode == ALL_COORDINATORS) {
            nodes.addAll(nodeManager.getCoordinators());
        }
        else if (tableDistributionMode == ALL_NODES) {
            nodes.addAll(nodeManager.getNodes(ACTIVE));
        }
        Set<InternalNode> nodeSet = nodes.build();
        for (InternalNode node : nodeSet) {
            splits.add(new SystemSplit(node.getHostAndPort(), tableConstraint));
        }
        return new FixedSplitSource(splits.build());
    }
}
