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
package io.prestosql.plugin.iceberg;

import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorSplitSource;
import io.prestosql.spi.type.TypeManager;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;

import javax.inject.Inject;

import static io.prestosql.plugin.iceberg.IcebergUtil.getIcebergTable;
import static io.prestosql.plugin.iceberg.IcebergUtil.getTableScan;
import static java.util.Objects.requireNonNull;

public class IcebergSplitManager
        implements ConnectorSplitManager
{
    private final IcebergTransactionManager transactionManager;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;

    @Inject
    public IcebergSplitManager(IcebergTransactionManager transactionManager, HdfsEnvironment hdfsEnvironment, TypeManager typeManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle handle, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;

        HiveMetastore metastore = transactionManager.get(transaction).getMetastore();
        Table icebergTable = getIcebergTable(metastore, hdfsEnvironment, session, table.getSchemaTableName());

        TableScan tableScan = getTableScan(session, table.getPredicate(), table.getSnapshotId(), icebergTable);

        // TODO Use residual. Right now there is no way to propagate residual to presto but at least we can
        //      propagate it at split level so the parquet pushdown can leverage it.
        IcebergSplitSource splitSource = new IcebergSplitSource(tableScan.planTasks(), table.getPredicate());

        return new ClassLoaderSafeConnectorSplitSource(splitSource, Thread.currentThread().getContextClassLoader());
    }
}
