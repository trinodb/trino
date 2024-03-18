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
package io.trino.plugin.varada.dispatcher.cache;

import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.DispatcherStatisticsProvider;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.query.classifier.WarpCacheColumnHandle;
import io.trino.plugin.varada.storage.splits.ConnectorSplitNodeDistributor;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Optional;

public class DispatcherCacheTransformer
        implements DispatcherProxiedConnectorTransformer
{
    @Override
    public Map<String, Integer> calculateColumnsStatisticsBucketPriority(DispatcherStatisticsProvider statisticsProvider, Map<ColumnHandle, ColumnStatistics> columnStatistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public DispatcherSplit createDispatcherSplit(ConnectorSplit proxyConnectorSplit, DispatcherTableHandle dispatcherTableHandle, ConnectorSplitNodeDistributor connectorSplitNodeDistributor, ConnectorSession session)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorTableHandle createProxyTableHandleForWarming(DispatcherTableHandle dispatcherTableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Object> getConvertedPartitionValue(RowGroupData rowGroupData, ColumnHandle columnHandle, Optional<String> nullPartitionValue)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaTableName getSchemaTableName(ConnectorTableHandle connectorTableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorTableHandle createProxiedConnectorTableHandleForMixedQuery(DispatcherTableHandle dispatcherTableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorSplit createProxiedConnectorNonFilteredSplit(ConnectorSplit connectorSplit)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RegularColumn getVaradaRegularColumn(ColumnHandle columnHandle)
    {
        return new RegularColumn(((WarpCacheColumnHandle) columnHandle).name());
    }

    @Override
    public Type getColumnType(ColumnHandle columnHandle)
    {
        return ((WarpCacheColumnHandle) columnHandle).type();
    }
}
