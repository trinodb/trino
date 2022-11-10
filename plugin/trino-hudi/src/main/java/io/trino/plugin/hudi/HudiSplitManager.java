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
import org.apache.hadoop.fs.Path;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class HudiSplitManager
        implements ConnectorSplitManager
{
    private final HudiTransactionManager transactionManager;
    private final BiFunction<ConnectorIdentity, HiveTransactionHandle, HiveMetastore> metastoreProvider;
    private final HdfsEnvironment hdfsEnvironment;
    private final ExecutorService executor;
    private final int maxSplitsPerSecond;
    private final int maxOutstandingSplits;

    @Inject
    public HudiSplitManager(
            HudiTransactionManager transactionManager,
            BiFunction<ConnectorIdentity, HiveTransactionHandle, HiveMetastore> metastoreProvider,
            HdfsEnvironment hdfsEnvironment,
            @ForHudiSplitManager ExecutorService executor,
            HudiConfig hudiConfig)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.metastoreProvider = requireNonNull(metastoreProvider, "metastoreProvider is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.maxSplitsPerSecond = requireNonNull(hudiConfig, "hudiConfig is null").getMaxSplitsPerSecond();
        this.maxOutstandingSplits = hudiConfig.getMaxOutstandingSplits();
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
        HudiSplitSource splitSource = new HudiSplitSource(
                session,
                metastore,
                table,
                hudiTableHandle,
                hdfsEnvironment.getConfiguration(new HdfsContext(session), new Path(table.getStorage().getLocation())),
                partitionColumnHandles,
                executor,
                maxSplitsPerSecond,
                maxOutstandingSplits);
        return new ClassLoaderSafeConnectorSplitSource(splitSource, HudiSplitManager.class.getClassLoader());
    }
}
