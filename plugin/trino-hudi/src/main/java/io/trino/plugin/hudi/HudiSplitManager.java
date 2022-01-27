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

import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class HudiSplitManager
        implements ConnectorSplitManager
{
    private final HudiTransactionManager transactionManager;
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public HudiSplitManager(HudiTransactionManager transactionManager, HdfsEnvironment hdfsEnvironment)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        HudiTableHandle hudiTable = (HudiTableHandle) tableHandle;
        HudiMetadata hudiMetadata = transactionManager.get(transaction);
        HiveMetastore metastore = hudiMetadata.getMetastore();
        Map<String, HiveColumnHandle> partitionColumnHandles = hudiMetadata.getColumnHandles(session, tableHandle)
                .values().stream().map(HiveColumnHandle.class::cast)
                .filter(HiveColumnHandle::isPartitionKey)
                .collect(Collectors.toMap(HiveColumnHandle::getName, identity()));
        Table hiveTable = hudiMetadata.getTable();
        HdfsEnvironment.HdfsContext context = new HdfsEnvironment.HdfsContext(session);
        Configuration conf = hdfsEnvironment.getConfiguration(
                context, new Path(hiveTable.getStorage().getLocation()));
        HudiSplitSource splitSource = new HudiSplitSource(
                session, metastore, hiveTable, hudiTable, conf, partitionColumnHandles);
        return new ClassLoaderSafeConnectorSplitSource(splitSource, Thread.currentThread().getContextClassLoader());
    }
}
