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
package io.trino.plugin.warp.proxiedconnector.objectstore;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.DispatcherStatisticsProvider;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.storage.splits.ConnectorSplitNodeDistributor;
import io.trino.plugin.warp.proxiedconnector.deltalake.DeltaLakeProxiedConnectorTransformer;
import io.trino.plugin.warp.proxiedconnector.hive.HiveProxiedConnectorTransformer;
import io.trino.plugin.warp.proxiedconnector.hudi.HudiProxiedConnectorTransformer;
import io.trino.plugin.warp.proxiedconnector.iceberg.IcebergProxiedConnectorTransformer;
import io.trino.spi.Node;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.type.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Singleton
public class ObjectStoreProxiedConnectorTransformer
        implements DispatcherProxiedConnectorTransformer
{
    public static final String HIVE = "hive";
    public static final String DELTALAKE = "deltalake";
    public static final String ICEBERG = "iceberg";
    public static final String HUDI = "hudi";
    public static final String UNKNOWN = "unknown";

    private final Map<String, DispatcherProxiedConnectorTransformer> transformerMap;

    @Inject
    public ObjectStoreProxiedConnectorTransformer(
            DeltaLakeProxiedConnectorTransformer deltaLakeProxiedConnectorTransformer,
            HiveProxiedConnectorTransformer hiveProxiedConnectorTransformer,
            IcebergProxiedConnectorTransformer icebergProxiedConnectorTransformer,
            HudiProxiedConnectorTransformer hudiProxiedConnectorTransformer)
    {
        this.transformerMap = new HashMap<>();
        transformerMap.put(DELTALAKE, requireNonNull(deltaLakeProxiedConnectorTransformer));
        transformerMap.put(HIVE, requireNonNull(hiveProxiedConnectorTransformer));
        transformerMap.put(ICEBERG, requireNonNull(icebergProxiedConnectorTransformer));
        transformerMap.put(HUDI, requireNonNull(hudiProxiedConnectorTransformer));
    }

    @VisibleForTesting
    public ObjectStoreProxiedConnectorTransformer(Map<String, DispatcherProxiedConnectorTransformer> transformerMap)
    {
        this.transformerMap = transformerMap;
    }

    @Override
    public Map<String, Integer> calculateColumnsStatisticsBucketPriority(
            DispatcherStatisticsProvider statisticsProvider,
            Map<ColumnHandle, ColumnStatistics> columnStatistics)
    {
        if (columnStatistics.isEmpty()) {
            return Map.of();
        }
        return transformerMap.get(getTransformerKey(columnStatistics.keySet().stream().findFirst().orElseThrow()))
                .calculateColumnsStatisticsBucketPriority(statisticsProvider, columnStatistics);
    }

    @Override
    public DispatcherSplit createDispatcherSplit(
            ConnectorSplit proxyConnectorSplit,
            DispatcherTableHandle dispatcherTableHandle,
            ConnectorSplitNodeDistributor connectorSplitNodeDistributor,
            ConnectorSession session)
    {
        return transformerMap.get(getTransformerKey(proxyConnectorSplit))
                .createDispatcherSplit(proxyConnectorSplit,
                        dispatcherTableHandle,
                        connectorSplitNodeDistributor,
                        session);
    }

    @Override
    public ConnectorTableHandle createProxyTableHandleForWarming(DispatcherTableHandle dispatcherTableHandle)
    {
        return transformerMap.get(getTransformerKey(dispatcherTableHandle.getProxyConnectorTableHandle()))
                .createProxyTableHandleForWarming(dispatcherTableHandle);
    }

    @Override
    public boolean proxyHasPushedDownFilter(DispatcherTableHandle dispatcherTableHandle)
    {
        return transformerMap.get(getTransformerKey(dispatcherTableHandle.getProxyConnectorTableHandle()))
                .proxyHasPushedDownFilter(dispatcherTableHandle);
    }

    @Override
    public RegularColumn getVaradaRegularColumn(ColumnHandle columnHandle)
    {
        return transformerMap.get(getTransformerKey(columnHandle))
                .getVaradaRegularColumn(columnHandle);
    }

    @Override
    public Type getColumnType(ColumnHandle columnHandle)
    {
        return transformerMap.get(getTransformerKey(columnHandle))
                .getColumnType(columnHandle);
    }

    @Override
    public Object getConvertedPartitionValue(String partitionValue, ColumnHandle columnHandle, Optional<String> nullPartitionValue)
    {
        return transformerMap.get(getTransformerKey(columnHandle))
                .getConvertedPartitionValue(partitionValue, columnHandle, nullPartitionValue);
    }

    @Override
    public Optional<Object> getConvertedPartitionValue(RowGroupData rowGroupData, ColumnHandle columnHandle, Optional<String> nullPartitionValue)
    {
        return transformerMap.get(getTransformerKey(columnHandle))
                .getConvertedPartitionValue(rowGroupData, columnHandle, nullPartitionValue);
    }

    @Override
    public boolean isValidForAcceleration(DispatcherTableHandle dispatcherTableHandle)
    {
        ConnectorTableHandle tableHandle = dispatcherTableHandle.getProxyConnectorTableHandle();
        return transformerMap.get(getTransformerKey(tableHandle))
                .isValidForAcceleration(dispatcherTableHandle);
    }

    @Override
    public SchemaTableName getSchemaTableName(ConnectorTableHandle connectorTableHandle)
    {
        return transformerMap.get(getTransformerKey(connectorTableHandle))
                .getSchemaTableName(connectorTableHandle);
    }

    @Override
    public ConnectorTableHandle createProxiedConnectorTableHandleForMixedQuery(DispatcherTableHandle dispatcherTableHandle)
    {
        return transformerMap.get(getTransformerKey(dispatcherTableHandle.getProxyConnectorTableHandle()))
                .createProxiedConnectorTableHandleForMixedQuery(dispatcherTableHandle);
    }

    @Override
    public ConnectorSplit createProxiedConnectorNonFilteredSplit(ConnectorSplit connectorSplit)
    {
        return transformerMap.get(getTransformerKey(connectorSplit)).createProxiedConnectorNonFilteredSplit(connectorSplit);
    }

    @Override
    public Optional<Object> getPartitionValue(ColumnHandle columnHandle, String partitionName, String partitionValue)
    {
        return transformerMap.get(getTransformerKey(columnHandle))
                .getPartitionValue(columnHandle, partitionName, partitionValue);
    }

    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, ConnectorNodePartitioningProvider nodePartitionProvider, List<Node> nodes)
    {
        return transformerMap.get(getTransformerKey(partitioningHandle)).getBucketNodeMapping(transactionHandle, session, partitioningHandle, nodePartitionProvider, nodes);
    }

    @Override
    public Optional<Long> getRowCount(ConnectorSplit connectorSplit)
    {
        return transformerMap.get(getTransformerKey(connectorSplit)).getRowCount(connectorSplit);
    }

    public static String getTransformerKey(Object obj)
    {
        return getTransformerKey(obj.getClass());
    }

    @SuppressWarnings("rawtypes")
    public static String getTransformerKey(Class clazz)
    {
        if (clazz.getName().toLowerCase(Locale.ROOT).contains(HIVE)) {
            return HIVE;
        }
        if (clazz.getName().toLowerCase(Locale.ROOT).contains(DELTALAKE)) {
            return DELTALAKE;
        }
        if (clazz.getName().toLowerCase(Locale.ROOT).contains(ICEBERG)) {
            return ICEBERG;
        }
        if (clazz.getName().toLowerCase(Locale.ROOT).contains(HUDI)) {
            return HUDI;
        }
        return UNKNOWN;
    }
}
