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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Partition;
import io.trino.metastore.StorageFormat;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.util.Lazy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static io.trino.plugin.hive.metastore.MetastoreUtil.computePartitionKeyFilter;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_PARTITION_NOT_FOUND;
import static io.trino.plugin.hudi.HudiSessionProperties.getDynamicFilteringWaitTimeout;
import static io.trino.plugin.hudi.HudiSessionProperties.getMaxOutstandingSplits;
import static io.trino.plugin.hudi.HudiSessionProperties.getMaxSplitsPerSecond;
import static io.trino.plugin.hudi.partition.HiveHudiPartitionInfo.NON_PARTITION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HudiSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(HudiSplitManager.class);
    private final BiFunction<ConnectorIdentity, HiveTransactionHandle, HiveMetastore> metastoreProvider;
    private final ExecutorService executor;
    private final ScheduledExecutorService splitLoaderExecutorService;
    private final CachingHostAddressProvider cachingHostAddressProvider;

    @Inject
    public HudiSplitManager(
            BiFunction<ConnectorIdentity, HiveTransactionHandle, HiveMetastore> metastoreProvider,
            @ForHudiSplitManager ExecutorService executor,
            @ForHudiSplitSource ScheduledExecutorService splitLoaderExecutorService,
            CachingHostAddressProvider cachingHostAddressProvider)
    {
        this.metastoreProvider = requireNonNull(metastoreProvider, "metastoreProvider is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.splitLoaderExecutorService = requireNonNull(splitLoaderExecutorService, "splitLoaderExecutorService is null");
        this.cachingHostAddressProvider = requireNonNull(cachingHostAddressProvider, "cachingHostAddressProvider is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        HudiTableHandle hudiTableHandle = (HudiTableHandle) tableHandle;
        HiveMetastore metastore = metastoreProvider.apply(session.getIdentity(), (HiveTransactionHandle) transaction);
        Lazy<Map<String, Partition>> lazyAllPartitions = Lazy.lazily(() -> {
            HoodieTimer timer = HoodieTimer.start();
            Map<String, Partition> allPartitions = getPartitions(metastore, hudiTableHandle);
            log.info("Found %s partitions for table %s.%s in %s ms",
                    allPartitions.size(), hudiTableHandle.getSchemaName(), hudiTableHandle.getTableName(), timer.endTimer());
            return allPartitions;
        });

        HudiSplitSource splitSource = new HudiSplitSource(
                session,
                hudiTableHandle,
                executor,
                splitLoaderExecutorService,
                getMaxSplitsPerSecond(session),
                getMaxOutstandingSplits(session),
                lazyAllPartitions,
                dynamicFilter,
                getDynamicFilteringWaitTimeout(session),
                cachingHostAddressProvider);
        return new ClassLoaderSafeConnectorSplitSource(splitSource, HudiSplitManager.class.getClassLoader());
    }

    private static Map<String, Partition> getPartitions(
            HiveMetastore metastore,
            HudiTableHandle tableHandle)
    {
        List<HiveColumnHandle> partitionColumns = tableHandle.getPartitionColumns();
        if (partitionColumns.isEmpty()) {
            return ImmutableMap.of(
                    NON_PARTITION, Partition.builder()
                            .setDatabaseName(tableHandle.getSchemaName())
                            .setTableName(tableHandle.getTableName())
                            .withStorage(storageBuilder ->
                                    storageBuilder.setLocation(tableHandle.getBasePath())
                                            .setStorageFormat(StorageFormat.NULL_STORAGE_FORMAT))
                            .setColumns(ImmutableList.of())
                            .setValues(ImmutableList.of())
                            .build());
        }

        List<String> partitionNames = metastore.getPartitionNamesByFilter(
                        tableHandle.getSchemaName(),
                        tableHandle.getTableName(),
                        partitionColumns.stream().map(HiveColumnHandle::getName).collect(Collectors.toList()),
                        computePartitionKeyFilter(partitionColumns, tableHandle.getPartitionPredicates()))
                .orElseThrow(() -> new TableNotFoundException(tableHandle.getSchemaTableName()));
        Map<String, Optional<Partition>> partitionsByNames = metastore.getPartitionsByNames(tableHandle.getTable(), partitionNames);
        List<String> partitionsNotFound = partitionsByNames.entrySet().stream().filter(e -> e.getValue().isEmpty()).map(Map.Entry::getKey).toList();
        if (!partitionsNotFound.isEmpty()) {
            throw new TrinoException(HUDI_PARTITION_NOT_FOUND, format("Cannot find partitions in metastore: %s", partitionsNotFound));
        }
        return partitionsByNames
                .entrySet().stream()
                .filter(e -> e.getValue().isPresent())
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));
    }
}
