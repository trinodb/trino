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
package io.trino.plugin.warp.proxiedconnector.hive;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.HivePartitioningHandle;
import io.trino.plugin.hive.HiveSplit;
import io.trino.plugin.hive.HiveTableHandle;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

@Singleton
public class HiveProxiedConnectorTransformer
        implements DispatcherProxiedConnectorTransformer
{
    private final ProxiedConnectorConfiguration proxiedConnectorConfiguration;

    @Inject
    public HiveProxiedConnectorTransformer(ProxiedConnectorConfiguration proxiedConnectorConfiguration)
    {
        this.proxiedConnectorConfiguration = requireNonNull(proxiedConnectorConfiguration);
    }

    @Override
    public ConnectorTableHandle createProxyTableHandleForWarming(DispatcherTableHandle dispatcherTableHandle)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) dispatcherTableHandle.getProxyConnectorTableHandle();
        return new HiveTableHandle(hiveTableHandle.getSchemaName(),
                hiveTableHandle.getTableName(),
                hiveTableHandle.getTableParameters(),
                hiveTableHandle.getPartitionColumns(),
                hiveTableHandle.getDataColumns(),
                hiveTableHandle.getPartitionNames(),
                hiveTableHandle.getPartitions(),
                TupleDomain.all(),
                TupleDomain.all(),
                hiveTableHandle.getBucketHandle(),
                Optional.empty(),
                hiveTableHandle.getAnalyzePartitionValues(),
                Collections.emptySet(),
                hiveTableHandle.getProjectedColumns(),
                hiveTableHandle.getTransaction(),
                hiveTableHandle.isRecordScannedFiles(),
                Optional.empty()); // don't limit
    }

    @Override
    public boolean proxyHasPushedDownFilter(DispatcherTableHandle dispatcherTableHandle)
    {
        HiveTableHandle tableHandle = (HiveTableHandle) dispatcherTableHandle.getProxyConnectorTableHandle();
        return !tableHandle.getCompactEffectivePredicate().isAll();
    }

    @Override
    public Map<String, Integer> calculateColumnsStatisticsBucketPriority(
            DispatcherStatisticsProvider statisticsProvider,
            Map<ColumnHandle, ColumnStatistics> columnStatistics)
    {
        return columnStatistics.entrySet()
                .stream()
                .filter(entry -> !((HiveColumnHandle) entry.getKey()).isHidden())
                .collect(Collectors.toMap(
                        entry -> ((HiveColumnHandle) entry.getKey()).getName(),
                        entry -> statisticsProvider.getColumnCardinalityBucket(entry.getValue().getDistinctValuesCount())));
    }

    @Override
    public ConnectorTableHandle createProxiedConnectorTableHandleForMixedQuery(DispatcherTableHandle dispatcherTableHandle)
    {
        HiveTableHandle tableHandle = (HiveTableHandle) dispatcherTableHandle.getProxyConnectorTableHandle();
        return new HiveTableHandle(
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.getTableParameters(),
                tableHandle.getPartitionColumns(),
                tableHandle.getDataColumns(),
                tableHandle.getPartitionNames(),
                tableHandle.getPartitions(),
                tableHandle.getCompactEffectivePredicate(),
                TupleDomain.all(),
                tableHandle.getBucketHandle(),
                Optional.empty(),
                tableHandle.getAnalyzePartitionValues(),
                Collections.emptySet(),
                tableHandle.getProjectedColumns(),
                tableHandle.getTransaction(),
                tableHandle.isRecordScannedFiles(),
                Optional.empty());  // must be empty to allow mixed query (see isValidForAcceleration())
    }

    @Override
    public ConnectorSplit createProxiedConnectorNonFilteredSplit(ConnectorSplit connectorSplit)
    {
        HiveSplit originalSplit = (HiveSplit) connectorSplit;
        return new HiveSplit(originalSplit.getPartitionName(),
                originalSplit.getPath(),
                originalSplit.getStart(),
                originalSplit.getLength(),
                originalSplit.getEstimatedFileSize(),
                originalSplit.getFileModifiedTime(),
                originalSplit.getSchema(),
                originalSplit.getPartitionKeys(),
                originalSplit.getAddresses(),
                originalSplit.getReadBucketNumber(),
                originalSplit.getTableBucketNumber(),
                originalSplit.isForceLocalScheduling(),
                originalSplit.getHiveColumnCoercions(),
                originalSplit.getBucketConversion(),
                originalSplit.getBucketValidation(),
                originalSplit.getAcidInfo(),
                originalSplit.getSplitWeight());
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
    public boolean isValidForAcceleration(DispatcherTableHandle dispatcherTableHandle)
    {
        if (proxiedConnectorConfiguration.getPassThroughDispatcherSet().contains(ProxiedConnectorConfiguration.HIVE_CONNECTOR_NAME)) {
            return false;
        }

        HiveTableHandle tableHandle = (HiveTableHandle) dispatcherTableHandle.getProxyConnectorTableHandle();
        if (tableHandle.getTransaction().isTransactional()) {
            return false;
        }

        // This limitation should only be present upon EXECUTE OPTIMIZE queries that are not relevant for acceleration anyway
        // (see file_size_threshold at https://trino.io/docs/current/connector/hive.html#optimize)
        // but if we got it, we must go to proxy because we don't apply this limitation on the data in Wrap Speed
        return tableHandle.getMaxScannedFileSize().isEmpty();
    }

    @Override
    public DispatcherSplit createDispatcherSplit(
            ConnectorSplit proxyConnectorSplit,
            DispatcherTableHandle dispatcherTableHandle,
            ConnectorSplitNodeDistributor connectorSplitNodeDistributor,
            ConnectorSession session)
    {
        HiveSplit hiveSplit = (HiveSplit) proxyConnectorSplit;

        List<HostAddress> hostAddresses = getHostAddressForSplit(
                getSplitKey(hiveSplit.getPath(), hiveSplit.getStart(), hiveSplit.getLength()),
                connectorSplitNodeDistributor);

        List<PartitionKey> partitionKeys = new ArrayList<>();
        for (HivePartitionKey hivePartitionKey : hiveSplit.getPartitionKeys()) {
            partitionKeys.add(new PartitionKey(new RegularColumn(hivePartitionKey.getName()), hivePartitionKey.getValue()));
        }

        return new DispatcherSplit(dispatcherTableHandle.getSchemaName(),
                dispatcherTableHandle.getTableName(),
                hiveSplit.getPath(),
                hiveSplit.getStart(),
                hiveSplit.getLength(),
                hiveSplit.getFileModifiedTime(),
                hostAddresses,
                partitionKeys,
                "",
                hiveSplit);
    }

    @Override
    public SchemaTableName getSchemaTableName(ConnectorTableHandle connectorTableHandle)
    {
        return ((HiveTableHandle) connectorTableHandle).getSchemaTableName();
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
                .toList();
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
