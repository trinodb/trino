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
package io.trino.plugin.jmx;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.NodeManager;
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
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.jmx.JmxMetadata.NODE_COLUMN_NAME;
import static io.trino.spi.predicate.TupleDomain.fromFixedValues;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class JmxSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;

    @Inject
    public JmxSplitManager(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        JmxTableHandle tableHandle = (JmxTableHandle) table;

        //TODO is there a better way to get the node column?
        Optional<JmxColumnHandle> nodeColumnHandle = tableHandle.getColumnHandles().stream()
                .filter(jmxColumnHandle -> jmxColumnHandle.getColumnName().equals(NODE_COLUMN_NAME))
                .findFirst();
        checkState(nodeColumnHandle.isPresent(), "Failed to find %s column", NODE_COLUMN_NAME);

        TupleDomain<ColumnHandle> nodeFilter = tableHandle.getNodeFilter();

        List<ConnectorSplit> splits = nodeManager.getAllNodes().stream()
                .filter(node -> {
                    NullableValue value = NullableValue.of(createUnboundedVarcharType(), utf8Slice(node.getNodeIdentifier()));
                    return nodeFilter.overlaps(fromFixedValues(ImmutableMap.of(nodeColumnHandle.get(), value)));
                })
                .map(node -> new JmxSplit(ImmutableList.of(node.getHostAndPort())))
                .collect(toList());

        return new FixedSplitSource(splits);
    }
}
