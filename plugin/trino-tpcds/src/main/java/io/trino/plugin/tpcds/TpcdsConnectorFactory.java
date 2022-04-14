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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.Versions.checkSpiVersion;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;

public class TpcdsConnectorFactory
        implements ConnectorFactory
{
    private final int defaultSplitsPerNode;

    public TpcdsConnectorFactory()
    {
        this(Runtime.getRuntime().availableProcessors());
    }

    public TpcdsConnectorFactory(int defaultSplitsPerNode)
    {
        checkState(defaultSplitsPerNode > 0, "default splits per node is negative");
        this.defaultSplitsPerNode = defaultSplitsPerNode;
    }

    @Override
    public String getName()
    {
        return "tpcds";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkSpiVersion(context, this);
        return new TpcdsConnector(config, context);
    }

    private class TpcdsConnector
            implements Connector
    {
        private final int splitsPerNode;
        private final NodeManager nodeManager;
        private final Map<String, String> config;

        public TpcdsConnector(Map<String, String> config, ConnectorContext context)
        {
            this.splitsPerNode = getSplitsPerNode(config);
            this.nodeManager = context.getNodeManager();
            this.config = ImmutableMap.copyOf(config);
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
        {
            return TpcdsTransactionHandle.INSTANCE;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
        {
            return new TpcdsMetadata();
        }

        @Override
        public ConnectorSplitManager getSplitManager()
        {
            return new TpcdsSplitManager(nodeManager, splitsPerNode, isWithNoSexism(config));
        }

        @Override
        public ConnectorRecordSetProvider getRecordSetProvider()
        {
            return new TpcdsRecordSetProvider();
        }

        @Override
        public ConnectorNodePartitioningProvider getNodePartitioningProvider()
        {
            return new TpcdsNodePartitioningProvider(nodeManager, splitsPerNode);
        }
    }

    private int getSplitsPerNode(Map<String, String> properties)
    {
        try {
            return parseInt(firstNonNull(properties.get("tpcds.splits-per-node"), String.valueOf(defaultSplitsPerNode)));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid property tpcds.splits-per-node");
        }
    }

    private boolean isWithNoSexism(Map<String, String> properties)
    {
        return parseBoolean(firstNonNull(properties.get("tpcds.with-no-sexism"), String.valueOf(false)));
    }
}
