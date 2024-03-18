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
package io.trino.plugin.varada.connector;

import com.google.inject.Inject;
import com.google.inject.Singleton;
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
import org.apache.commons.collections4.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Singleton
public class TestingConnectorProxiedConnectorTransformer
        implements DispatcherProxiedConnectorTransformer
{
    @Inject
    public TestingConnectorProxiedConnectorTransformer() {}

    @Override
    public ConnectorTableHandle createProxyTableHandleForWarming(DispatcherTableHandle dispatcherTableHandle)
    {
        return dispatcherTableHandle.getProxyConnectorTableHandle();
    }

    @Override
    public Map<String, Integer> calculateColumnsStatisticsBucketPriority(DispatcherStatisticsProvider statisticsProvider, Map<ColumnHandle, ColumnStatistics> columnStatistics)
    {
        return Collections.emptyMap();
    }

    @Override
    public DispatcherSplit createDispatcherSplit(ConnectorSplit proxyConnectorSplit,
            DispatcherTableHandle dispatcherTableHandle,
            ConnectorSplitNodeDistributor connectorSplitNodeDistributor,
            ConnectorSession session)
    {
        DispatcherSplit dispatcherSplit = (DispatcherSplit) proxyConnectorSplit;

        List<HostAddress> splitAddresses = dispatcherSplit.getAddresses();
        if (CollectionUtils.isEmpty(splitAddresses)) {
            splitAddresses = getHostAddressForSplit(
                    getSplitKey(dispatcherSplit.getPath(), dispatcherSplit.getStart(), dispatcherSplit.getLength()),
                    connectorSplitNodeDistributor);
        }

        return new DispatcherSplit(dispatcherSplit.getSchemaName(),
                dispatcherSplit.getTableName(),
                dispatcherSplit.getPath(),
                dispatcherSplit.getStart(),
                dispatcherSplit.getLength(),
                dispatcherSplit.getFileModifiedTime(),
                splitAddresses,
                List.of(),
                "",
                dispatcherSplit);
    }

    @Override
    public RegularColumn getVaradaRegularColumn(ColumnHandle columnHandle)
    {
        String name = ((TestingConnectorColumnHandle) columnHandle).name();
        return new RegularColumn(name);
    }

    @Override
    public Type getColumnType(ColumnHandle columnHandle)
    {
        return ((TestingConnectorColumnHandle) columnHandle).type();
    }

    @Override
    public Optional<Object> getConvertedPartitionValue(RowGroupData rowGroupData, ColumnHandle columnHandle, Optional<String> nullPartitionValue)
    {
        return Optional.empty();
    }

    @Override
    public SchemaTableName getSchemaTableName(ConnectorTableHandle connectorTableHandle)
    {
        TestingConnectorTableHandle tableHandle = (TestingConnectorTableHandle) connectorTableHandle;
        return new SchemaTableName(tableHandle.getSchemaName(),
                tableHandle.getTableName());
    }

    @Override
    public ConnectorTableHandle createProxiedConnectorTableHandleForMixedQuery(DispatcherTableHandle dispatcherTableHandle)
    {
        return dispatcherTableHandle.getProxyConnectorTableHandle();
    }

    @Override
    public ConnectorSplit createProxiedConnectorNonFilteredSplit(ConnectorSplit connectorSplit)
    {
        return connectorSplit;
    }
}
