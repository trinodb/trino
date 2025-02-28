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

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HivePartition;
import io.trino.metastore.Table;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hudi.query.HudiFileSkippingManager;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hive.metastore.MetastoreUtil.computePartitionKeyFilter;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeyColumnHandles;
import static io.trino.plugin.hudi.HudiSessionProperties.getMaxOutstandingSplits;
import static io.trino.plugin.hudi.HudiSessionProperties.getMaxSplitsPerSecond;
import static io.trino.plugin.hudi.HudiSessionProperties.isHudiMetadataTableEnabled;
import static io.trino.plugin.hudi.partition.HiveHudiPartitionInfo.NON_PARTITION;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class HudiSplitManager
        implements ConnectorSplitManager
{
    private final TypeManager typeManager;
    private final BiFunction<ConnectorIdentity, HiveTransactionHandle, HiveMetastore> metastoreProvider;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ExecutorService executor;
    private final ScheduledExecutorService splitLoaderExecutorService;

    @Inject
    public HudiSplitManager(
            TypeManager typeManager,
            BiFunction<ConnectorIdentity, HiveTransactionHandle, HiveMetastore> metastoreProvider,
            @ForHudiSplitManager ExecutorService executor,
            TrinoFileSystemFactory fileSystemFactory,
            @ForHudiSplitSource ScheduledExecutorService splitLoaderExecutorService)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.metastoreProvider = requireNonNull(metastoreProvider, "metastoreProvider is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.splitLoaderExecutorService = requireNonNull(splitLoaderExecutorService, "splitLoaderExecutorService is null");
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
        Table table = metastore.getTable(hudiTableHandle.getSchemaName(), hudiTableHandle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName(hudiTableHandle.getSchemaName(), hudiTableHandle.getTableName())));
        List<HiveColumnHandle> partitionColumns = getPartitionKeyColumnHandles(table, typeManager);
        Map<String, HiveColumnHandle> partitionColumnHandles = partitionColumns.stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, identity()));
        List<String> partitions = getPartitions(metastore, hudiTableHandle, partitionColumns);
        boolean enableMetadataTable = isHudiMetadataTableEnabled(session);

        if (enableMetadataTable) {
            Optional<Table> hiveTableOpt = metastore.getTable(table.getDatabaseName(), table.getTableName());
            Verify.verify(hiveTableOpt.isPresent());
            HoodieTableMetaClient metaClient = HoodieTableMetaClient
                    .builder()
                    .setBasePath(((HudiTableHandle) tableHandle).getBasePath())
                    .build();
            HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(new TrinoStorageConfiguration());
            HudiFileSkippingManager hudiFileSkippingManager = new HudiFileSkippingManager(
                    partitions,
                    // TODO(yihua): make this configurable
                    "/tmp",
                    engineContext,
                    metaClient,
                    HoodieTableQueryType.SNAPSHOT,
                    Optional.empty());
            ImmutableList.Builder<HudiSplit> splitsBuilder = ImmutableList.builder();
            Map<String, HivePartition> hudiPartitionMap = getHudiPartitions(hiveTableOpt.get(), hudiTableHandle, partitions);
            hudiFileSkippingManager.listQueryFiles(hudiTableHandle.getTupleDomain())
                    .entrySet()
                    .stream()
                    .flatMap(entry -> entry.getValue().stream().map(fileSlice -> createHudiSplit(table, fileSlice, timestamp, hudiPartitionMap.get(entry.getKey()), splitWeightProvider)))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(splitsBuilder::add);
            List<HudiSplit> splitsList = splitsBuilder.build();
            return splitsList.isEmpty() ? new FixedSplitSource(ImmutableList.of()) : new FixedSplitSource(splitsList);
        }

        HudiSplitSource splitSource = new HudiSplitSource(
                session,
                metastore,
                table,
                hudiTableHandle,
                fileSystemFactory,
                partitionColumnHandles,
                executor,
                splitLoaderExecutorService,
                getMaxSplitsPerSecond(session),
                getMaxOutstandingSplits(session),
                partitions);
        return new ClassLoaderSafeConnectorSplitSource(splitSource, HudiSplitManager.class.getClassLoader());
    }

    private Map<String, HivePartition> getHudiPartitions(Table table, HudiTableHandle tableHandle, List<String> partitions)
    {
        List<String> partitionColumnNames = table.getPartitionColumns().stream().map(f -> f.getName()).collect(Collectors.toList());

        Map<String, Map<String, String>> partitionMap = HudiPartitionManager
                .getPartitions(partitionColumnNames, partitions);
        if (partitions.size() == 1 && partitions.get(0).isEmpty()) {
            // non-partitioned
            return ImmutableMap.of(partitions.get(0), new HivePartition(partitions.get(0), ImmutableList.of(), tableHandle.getConstraintColumns()));
        }
        ImmutableMap.Builder<String, HivePartition> builder = ImmutableMap.builder();
        partitionMap.entrySet().stream().map(entry -> {
            List<String> partitionValues = HudiPartitionManager.extractPartitionValues(entry.getKey(), Optional.of(partitionColumnNames));
            return new HivePartition(entry.getKey(), partitionValues, entry.getValue(), table.getStorage(), fromDataColumns(table.getDataColumns()));
        }).forEach(p -> builder.put(p.getName(), p));
        return builder.build();
    }

    private static List<String> getPartitions(HiveMetastore metastore, HudiTableHandle table, List<HiveColumnHandle> partitionColumns)
    {
        if (partitionColumns.isEmpty()) {
            return ImmutableList.of(NON_PARTITION);
        }

        return metastore.getPartitionNamesByFilter(
                        table.getSchemaName(),
                        table.getTableName(),
                        partitionColumns.stream().map(HiveColumnHandle::getName).collect(Collectors.toList()),
                        computePartitionKeyFilter(partitionColumns, table.getPartitionPredicates()))
                .orElseThrow(() -> new TableNotFoundException(table.getSchemaTableName()));
    }
}
