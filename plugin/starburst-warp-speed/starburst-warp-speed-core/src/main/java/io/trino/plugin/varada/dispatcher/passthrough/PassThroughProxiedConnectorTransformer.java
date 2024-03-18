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
package io.trino.plugin.varada.dispatcher.passthrough;

import io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.DispatcherStatisticsProvider;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.storage.splits.ConnectorSplitNodeDistributor;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PassThroughProxiedConnectorTransformer
        implements DispatcherProxiedConnectorTransformer
{
    private final ProxiedConnectorConfiguration proxiedConnectorConfiguration;
    private final DispatcherProxiedConnectorColumnTransformer dispatcherProxiedConnectorColumnTransformer;

    private final String connectorName;

    public PassThroughProxiedConnectorTransformer(
            ProxiedConnectorConfiguration proxiedConnectorConfiguration,
            DispatcherProxiedConnectorColumnTransformer dispatcherProxiedConnectorColumnTransformer,
            String connectorName)
    {
        this.proxiedConnectorConfiguration = requireNonNull(proxiedConnectorConfiguration);
        this.dispatcherProxiedConnectorColumnTransformer = requireNonNull(dispatcherProxiedConnectorColumnTransformer);
        this.connectorName = requireNonNull(connectorName);
    }

    @Override
    public Map<String, Integer> calculateColumnsStatisticsBucketPriority(
            DispatcherStatisticsProvider statisticsProvider,
            Map<ColumnHandle, ColumnStatistics> columnStatistics)
    {
        return Map.of();
    }

    @Override
    public DispatcherSplit createDispatcherSplit(
            ConnectorSplit proxyConnectorSplit,
            DispatcherTableHandle dispatcherTableHandle,
            ConnectorSplitNodeDistributor connectorSplitNodeDistributor,
            ConnectorSession session)
    {
        String splitKey = proxyConnectorSplit.getInfo() != null ?
                proxyConnectorSplit.getInfo().toString() : String.valueOf(proxyConnectorSplit.hashCode());
        List<HostAddress> hostAddresses = getHostAddressForSplit(
                splitKey,
                connectorSplitNodeDistributor);

        return new DispatcherSplit(dispatcherTableHandle.getSchemaName(),
                dispatcherTableHandle.getTableName(),
                "",
                0L,
                0L,
                0L,
                hostAddresses,
                List.of(),
                "",
                proxyConnectorSplit);
    }

    @Override
    @Deprecated //do not add implementation for pass-through connector
    public ConnectorTableHandle createProxyTableHandleForWarming(DispatcherTableHandle dispatcherTableHandle)
    {
        throw new UnsupportedOperationException("warmup is not supported for pass through connector");
    }

    @Override
    public RegularColumn getVaradaRegularColumn(ColumnHandle columnHandle)
    {
        return dispatcherProxiedConnectorColumnTransformer.getVaradaRegularColumn(columnHandle);
    }

    @Override
    public Type getColumnType(ColumnHandle columnHandle)
    {
        return dispatcherProxiedConnectorColumnTransformer.getColumnType(columnHandle);
    }

    @Override
    @Deprecated //do not add implementation for pass-through connector
    public Optional<Object> getConvertedPartitionValue(RowGroupData rowGroupData, ColumnHandle columnHandle, Optional<String> nullPartitionValue)
    {
        throw new UnsupportedOperationException("ConvertedPartitionValue is not supported for pass through connector");
    }

    @Override
    public boolean isValidForAcceleration(DispatcherTableHandle dispatcherTableHandle)
    {
        return !proxiedConnectorConfiguration.getPassThroughDispatcherSet().contains(connectorName);
    }

    @Override
    public SchemaTableName getSchemaTableName(ConnectorTableHandle connectorTableHandle)
    {
        //return a dummy schema table since we never use it for something meaningful
        return new SchemaTableName("dummy", "dummy");
    }

    @Override
    @Deprecated //do not add implementation for pass-through connector
    public ConnectorTableHandle createProxiedConnectorTableHandleForMixedQuery(DispatcherTableHandle dispatcherTableHandle)
    {
        throw new UnsupportedOperationException("Mixed query is not supported for pass through connector");
    }

    @Override
    public ConnectorSplit createProxiedConnectorNonFilteredSplit(ConnectorSplit connectorSplit)
    {
        throw new UnsupportedOperationException("Mixed query is not supported for pass through connector");
    }
}
