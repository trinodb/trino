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
package io.trino.plugin.warp.proxiedconnector.hudi;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.HivePartitioningHandle;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.DispatcherStatisticsProvider;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.PartitionKey;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.storage.splits.ConnectorSplitNodeDistributor;
import io.trino.spi.HostAddress;
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
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

@Singleton
public class HudiProxiedConnectorTransformer
        implements DispatcherProxiedConnectorTransformer
{
    private final ProxiedConnectorConfiguration proxiedConnectorConfiguration;

    @Inject
    public HudiProxiedConnectorTransformer(ProxiedConnectorConfiguration proxiedConnectorConfiguration)
    {
        this.proxiedConnectorConfiguration = requireNonNull(proxiedConnectorConfiguration);
    }

    @Override
    public ConnectorTableHandle createProxyTableHandleForWarming(DispatcherTableHandle dispatcherTableHandle)
    {
        HudiTableHandle hudiTableHandle = (HudiTableHandle) dispatcherTableHandle.getProxyConnectorTableHandle();
        return new HudiTableHandle(
                hudiTableHandle.getSchemaName(),
                hudiTableHandle.getTableName(),
                hudiTableHandle.getBasePath(),
                hudiTableHandle.getTableType(),
                hudiTableHandle.getPartitionColumns(),
                TupleDomain.all(),
                TupleDomain.all());
    }

    @Override
    public boolean proxyHasPushedDownFilter(DispatcherTableHandle dispatcherTableHandle)
    {
        HudiTableHandle hudiTableHandle = (HudiTableHandle) dispatcherTableHandle.getProxyConnectorTableHandle();
        return !hudiTableHandle.getRegularPredicates().isAll();
    }

    @Override
    public ConnectorTableHandle createProxiedConnectorTableHandleForMixedQuery(DispatcherTableHandle dispatcherTableHandle)
    {
        HudiTableHandle tableHandle = (HudiTableHandle) dispatcherTableHandle.getProxyConnectorTableHandle();
        return new HudiTableHandle(
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.getBasePath(),
                tableHandle.getTableType(),
                tableHandle.getPartitionColumns(),
                TupleDomain.all(),
                tableHandle.getRegularPredicates());
    }

    @Override
    public ConnectorSplit createProxiedConnectorNonFilteredSplit(ConnectorSplit connectorSplit)
    {
        HudiSplit originalSplit = (HudiSplit) connectorSplit;
        return new HudiSplit(originalSplit.getLocation(),
                originalSplit.getStart(),
                originalSplit.getLength(),
                originalSplit.getFileSize(),
                originalSplit.getFileModifiedTime(),
                TupleDomain.all(),
                originalSplit.getPartitionKeys(),
                originalSplit.getSplitWeight());
    }

    @Override
    public boolean isValidForAcceleration(DispatcherTableHandle dispatcherTableHandle)
    {
        return !proxiedConnectorConfiguration.getPassThroughDispatcherSet()
                .contains(ProxiedConnectorConfiguration.HUDI_CONNECTOR_NAME);
    }

    @Override
    public Map<String, Integer> calculateColumnsStatisticsBucketPriority(DispatcherStatisticsProvider statisticsProvider, Map<ColumnHandle, ColumnStatistics> columnStatistics)
    {
        return columnStatistics.entrySet()
                .stream()
                .filter(entry -> !((HiveColumnHandle) entry.getKey()).isHidden())
                .collect(Collectors.toMap(
                        entry -> ((HiveColumnHandle) entry.getKey()).getName(),
                        entry -> statisticsProvider.getColumnCardinalityBucket(entry.getValue().getDistinctValuesCount())));
    }

    @Override
    public DispatcherSplit createDispatcherSplit(
            ConnectorSplit proxyConnectorSplit,
            DispatcherTableHandle dispatcherTableHandle,
            ConnectorSplitNodeDistributor connectorSplitNodeDistributor,
            ConnectorSession session)
    {
        HudiSplit hudiSplit = (HudiSplit) proxyConnectorSplit;

        List<HostAddress> hostAddresses = getHostAddressForSplit(
                getSplitKey(hudiSplit.getLocation(), hudiSplit.getStart(), hudiSplit.getLength()),
                connectorSplitNodeDistributor);

        List<PartitionKey> partitionKeys = new ArrayList<>();
        for (HivePartitionKey hivePartitionKey : hudiSplit.getPartitionKeys()) {
            partitionKeys.add(new PartitionKey(new RegularColumn(hivePartitionKey.getName()), hivePartitionKey.getValue()));
        }

        return new DispatcherSplit(dispatcherTableHandle.getSchemaName(),
                dispatcherTableHandle.getTableName(),
                hudiSplit.getLocation(),
                hudiSplit.getStart(),
                hudiSplit.getLength(),
                hudiSplit.getFileModifiedTime(),
                hostAddresses,
                partitionKeys,
                "",
                proxyConnectorSplit);
    }

    @Override
    public SchemaTableName getSchemaTableName(ConnectorTableHandle connectorTableHandle)
    {
        return ((HudiTableHandle) connectorTableHandle).getSchemaTableName();
    }

    @Override
    public RegularColumn getVaradaRegularColumn(ColumnHandle columnHandle)
    {
        String name = ((HiveColumnHandle) columnHandle).getName();
        return new RegularColumn(name);
    }

    @Override
    public Type getColumnType(ColumnHandle columnHandle)
    {
        return ((HiveColumnHandle) columnHandle).getType();
    }

    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            ConnectorNodePartitioningProvider nodePartitionProvider,
            List<Node> nodes)
    {
        // sort to ensure the mapping is consistent
        List<Node> sortedNodes = nodes.stream()
                .sorted(comparing(node -> node.getHostAndPort().toString()))
                .collect(toImmutableList());
        int bucketCount = ((HivePartitioningHandle) partitioningHandle).getBucketCount();
        ImmutableList.Builder<Node> bucketToNode = ImmutableList.builder();

        int i = 0;
        for (int j = 0; j < bucketCount; j++) {
            bucketToNode.add(sortedNodes.get(i));
            i++;
            i %= sortedNodes.size();
        }
        return Optional.of(createBucketNodeMap(bucketToNode.build()));
    }
}
