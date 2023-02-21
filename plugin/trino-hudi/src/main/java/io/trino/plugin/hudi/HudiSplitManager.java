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

import io.airlift.log.Logger;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hudi.HudiSessionProperties.getMaxOutstandingSplits;
import static io.trino.plugin.hudi.HudiSessionProperties.getMaxSplitsPerSecond;
import static io.trino.plugin.hudi.HudiSessionProperties.isHudiMetadataEnabled;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class HudiSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(HudiSplitManager.class);

    private final HudiTransactionManager transactionManager;
    private final HudiPartitionManager partitionManager;
    private final BiFunction<ConnectorIdentity, HiveTransactionHandle, HiveMetastore> metastoreProvider;
    private final HdfsEnvironment hdfsEnvironment;
    private final ExecutorService executor;
    private final ScheduledExecutorService splitLoaderExecutorService;
    private final ExecutorService splitGeneratorExecutorService;

    @Inject
    public HudiSplitManager(
            HudiTransactionManager transactionManager,
            HudiPartitionManager partitionManager,
            BiFunction<ConnectorIdentity, HiveTransactionHandle, HiveMetastore> metastoreProvider,
            HdfsEnvironment hdfsEnvironment,
            @ForHudiSplitManager ExecutorService executor,
            @ForHudiSplitSource ScheduledExecutorService splitLoaderExecutorService,
            @ForHudiBackgroundSplitLoader ExecutorService splitGeneratorExecutorService)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.metastoreProvider = requireNonNull(metastoreProvider, "metastoreProvider is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.splitLoaderExecutorService = requireNonNull(splitLoaderExecutorService, "splitLoaderExecutorService is null");
        this.splitGeneratorExecutorService = requireNonNull(splitGeneratorExecutorService, "splitGeneratorExecutorService is null");
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

        HoodieTimer timer = new HoodieTimer().startTimer();
        Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsContext(session), new Path(table.getStorage().getLocation()));
        boolean metadataEnabled = isHudiMetadataEnabled(session);
        HoodieTableMetaClient metaClient = buildTableMetaClient(configuration, hudiTableHandle.getBasePath());
        List<String> partitions = partitionManager.getEffectivePartitions(hudiTableHandle, metastore, session, metaClient);
        log.debug("Took %d ms to get %d partitions", timer.endTimer(), partitions.size());

        HudiSplitSource splitSource = new HudiSplitSource(
                session,
                metastore,
                table,
                hudiTableHandle,
                configuration,
                partitionColumnHandles,
                executor,
                splitLoaderExecutorService,
                splitGeneratorExecutorService,
                getMaxSplitsPerSecond(session),
                getMaxOutstandingSplits(session),
                partitions);
        return new ClassLoaderSafeConnectorSplitSource(splitSource, HudiSplitManager.class.getClassLoader());
    }

    private static HoodieTableMetaClient buildTableMetaClient(Configuration configuration, String basePath)
    {
        HoodieTableMetaClient client = HoodieTableMetaClient.builder().setConf(configuration).setBasePath(basePath).build();
        client.getTableConfig().setValue("hoodie.bootstrap.index.enable", "false");
        return client;
    }
}
