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
package io.trino.testing.tpch;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.tpch.DecimalTypeMapping;
import io.trino.plugin.tpch.TpchNodePartitioningProvider;
import io.trino.plugin.tpch.TpchRecordSetProvider;
import io.trino.plugin.tpch.TpchSplitManager;
import io.trino.plugin.tpch.TpchTransactionHandle;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorIndexProvider;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class IndexedTpchConnectorFactory
        implements ConnectorFactory
{
    private final TpchIndexSpec indexSpec;
    private final int defaultSplitsPerNode;

    public IndexedTpchConnectorFactory(TpchIndexSpec indexSpec, int defaultSplitsPerNode)
    {
        this.indexSpec = requireNonNull(indexSpec, "indexSpec is null");
        this.defaultSplitsPerNode = defaultSplitsPerNode;
    }

    @Override
    public String getName()
    {
        return "tpch_indexed";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> properties, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);

        int splitsPerNode = getSplitsPerNode(properties);
        TpchIndexedData indexedData = new TpchIndexedData(indexSpec);
        NodeManager nodeManager = context.getNodeManager();

        return new Connector()
        {
            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
            {
                return TpchTransactionHandle.INSTANCE;
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
            {
                return new TpchIndexMetadata(indexedData);
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                return new TpchSplitManager(nodeManager, splitsPerNode);
            }

            @Override
            public ConnectorRecordSetProvider getRecordSetProvider()
            {
                return new TpchRecordSetProvider(DecimalTypeMapping.DOUBLE);
            }

            @Override
            public ConnectorIndexProvider getIndexProvider()
            {
                return new TpchIndexProvider(indexedData);
            }

            @Override
            public Set<SystemTable> getSystemTables()
            {
                return ImmutableSet.of(new ExampleSystemTable());
            }

            @Override
            public ConnectorNodePartitioningProvider getNodePartitioningProvider()
            {
                return new TpchNodePartitioningProvider(nodeManager, splitsPerNode);
            }
        };
    }

    private int getSplitsPerNode(Map<String, String> properties)
    {
        try {
            return Integer.parseInt(firstNonNull(properties.get("tpch.splits-per-node"), String.valueOf(defaultSplitsPerNode)));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid property tpch.splits-per-node");
        }
    }
}
