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

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hudi.HudiSessionProperties.getMaxOutstandingSplits;
import static io.trino.plugin.hudi.HudiSessionProperties.getMaxSplitsPerSecond;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class HudiSplitManager
        implements ConnectorSplitManager
{
    private final HudiTransactionManager transactionManager;
    private final HudiPartitionManager partitionManager;
    private final BiFunction<ConnectorIdentity, HiveTransactionHandle, HiveMetastore> metastoreProvider;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ExecutorService executor;
    private final ScheduledExecutorService splitLoaderExecutorService;

    @Inject
    public HudiSplitManager(
            HudiTransactionManager transactionManager,
            HudiPartitionManager partitionManager,
            BiFunction<ConnectorIdentity, HiveTransactionHandle, HiveMetastore> metastoreProvider,
            @ForHudiSplitManager ExecutorService executor,
            TrinoFileSystemFactory fileSystemFactory,
            @ForHudiSplitSource ScheduledExecutorService splitLoaderExecutorService)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.metastoreProvider = requireNonNull(metastoreProvider, "metastoreProvider is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.splitLoaderExecutorService = requireNonNull(splitLoaderExecutorService, "splitLoaderExecutorService is null");
    }

    @PreDestroy
    public void destroy()
    {
        this.executor.shutdown();
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
        HudiMetadata hudiMetadata = transactionManager.get(transaction, session.getIdentity());
        Map<String, HiveColumnHandle> partitionColumnHandles = hudiMetadata.getColumnHandles(session, tableHandle)
                .values().stream().map(HiveColumnHandle.class::cast)
                .filter(HiveColumnHandle::isPartitionKey)
                .collect(toImmutableMap(HiveColumnHandle::getName, identity()));
        HiveMetastore metastore = metastoreProvider.apply(session.getIdentity(), (HiveTransactionHandle) transaction);
        Table table = metastore.getTable(hudiTableHandle.getSchemaName(), hudiTableHandle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName(hudiTableHandle.getSchemaName(), hudiTableHandle.getTableName())));
        List<String> partitions = partitionManager.getEffectivePartitions(hudiTableHandle, metastore);

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
}
