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
package io.trino.plugin.varada.dispatcher;

import com.google.inject.Inject;
import io.trino.plugin.varada.VaradaSessionProperties;
import io.trino.plugin.varada.annotations.ForWarp;
import io.trino.plugin.varada.storage.splits.ConnectorSplitNodeDistributor;
import io.trino.plugin.varada.storage.splits.ConnectorSplitSessionNodeDistributor;
import io.trino.spi.NodeManager;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DispatcherSplitManager
        implements ConnectorSplitManager
{
    private final ConnectorSplitManager proxyConnectorSplitManager;
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final ConnectorSplitNodeDistributor connectorSplitNodeDistributor;
    private final NodeManager nodeManager;

    @Inject
    DispatcherSplitManager(@ForWarp ConnectorSplitManager proxyConnectorSplitManager,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            ConnectorSplitNodeDistributor connectorSplitNodeDistributor,
            NodeManager nodeManager)
    {
        this.proxyConnectorSplitManager = requireNonNull(proxyConnectorSplitManager);
        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
        this.connectorSplitNodeDistributor = requireNonNull(connectorSplitNodeDistributor);
        this.nodeManager = requireNonNull(nodeManager);
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        ConnectorSplitNodeDistributor splitNodeDistributor;
        String nodeDistributorSessionKey = VaradaSessionProperties.getNodeByBySession(session);
        if (nodeDistributorSessionKey != null) {
            splitNodeDistributor = new ConnectorSplitSessionNodeDistributor(nodeManager, nodeDistributorSessionKey);
        }
        else {
            splitNodeDistributor = connectorSplitNodeDistributor;
        }

        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) table;

        return new DispatcherSplitSource(
                proxyConnectorSplitManager.getSplits(transactionHandle,
                        session,
                        dispatcherTableHandle.getProxyConnectorTableHandle(),
                        dynamicFilter,
                        constraint),
                dispatcherTableHandle,
                session,
                dispatcherProxiedConnectorTransformer,
                splitNodeDistributor);
    }

    @Override
    public Optional<CacheSplitId> getCacheSplitId(ConnectorSplit split)
    {
        return proxyConnectorSplitManager.getCacheSplitId(((DispatcherSplit) split).getProxyConnectorSplit());
    }
}
