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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.plugin.iceberg.functions.tablechanges.TableChangesFunctionHandle;
import io.trino.plugin.iceberg.functions.tablechanges.TableChangesSplitSource;
import io.trino.plugin.iceberg.functions.tables.IcebergTablesFunction.IcebergTables;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.SnapshotUtil;

import java.util.concurrent.ExecutorService;

import static io.trino.plugin.iceberg.IcebergSessionProperties.getDynamicFilteringWaitTimeout;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getMinimumAssignedSplitWeight;
import static io.trino.spi.connector.FixedSplitSource.emptySplitSource;
import static java.util.Objects.requireNonNull;

public class IcebergSplitManager
        implements ConnectorSplitManager
{
    public static final int ICEBERG_DOMAIN_COMPACTION_THRESHOLD = 1000;

    private final IcebergTransactionManager transactionManager;
    private final TypeManager typeManager;
    private final IcebergFileSystemFactory fileSystemFactory;
    private final ListeningExecutorService splitSourceExecutor;
    private final ExecutorService icebergPlanningExecutor;
    private final CachingHostAddressProvider cachingHostAddressProvider;

    @Inject
    public IcebergSplitManager(
            IcebergTransactionManager transactionManager,
            TypeManager typeManager,
            IcebergFileSystemFactory fileSystemFactory,
            @ForIcebergSplitManager ListeningExecutorService splitSourceExecutor,
            @ForIcebergScanPlanning ExecutorService icebergPlanningExecutor,
            CachingHostAddressProvider cachingHostAddressProvider)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.splitSourceExecutor = requireNonNull(splitSourceExecutor, "splitSourceExecutor is null");
        this.icebergPlanningExecutor = requireNonNull(icebergPlanningExecutor, "icebergPlanningExecutor is null");
        this.cachingHostAddressProvider = requireNonNull(cachingHostAddressProvider, "cachingHostAddressProvider is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle handle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;

        if (table.getSnapshotId().isEmpty()) {
            if (table.isRecordScannedFiles()) {
                return new FixedSplitSource(ImmutableList.of(), ImmutableList.of());
            }
            return emptySplitSource();
        }

        IcebergMetadata icebergMetadata = transactionManager.get(transaction, session.getIdentity());
        Table icebergTable = icebergMetadata.getIcebergTable(session, table.getSchemaTableName());
        Duration dynamicFilteringWaitTimeout = getDynamicFilteringWaitTimeout(session);

        Scan scan = getScan(icebergMetadata, icebergTable, table, icebergPlanningExecutor);

        IcebergSplitSource splitSource = new IcebergSplitSource(
                fileSystemFactory,
                session,
                table,
                icebergTable,
                scan,
                table.getMaxScannedFileSize(),
                dynamicFilter,
                dynamicFilteringWaitTimeout,
                constraint,
                typeManager,
                table.isRecordScannedFiles(),
                getMinimumAssignedSplitWeight(session),
                cachingHostAddressProvider,
                splitSourceExecutor);

        return new ClassLoaderSafeConnectorSplitSource(splitSource, IcebergSplitManager.class.getClassLoader());
    }

    private Scan<?, FileScanTask, CombinedScanTask> getScan(IcebergMetadata icebergMetadata, Table icebergTable, IcebergTableHandle table, ExecutorService executor)
    {
        Long fromSnapshot = icebergMetadata.getIncrementalRefreshFromSnapshot().orElse(null);
        if (fromSnapshot != null) {
            // check if fromSnapshot is still part of the table's snapshot history
            if (SnapshotUtil.isAncestorOf(icebergTable, fromSnapshot)) {
                boolean containsModifiedRows = false;
                for (Snapshot snapshot : SnapshotUtil.ancestorsBetween(icebergTable, icebergTable.currentSnapshot().snapshotId(), fromSnapshot)) {
                    if (snapshot.operation().equals(DataOperations.OVERWRITE) || snapshot.operation().equals(DataOperations.DELETE)) {
                        containsModifiedRows = true;
                        break;
                    }
                }
                if (!containsModifiedRows) {
                    return icebergTable.newIncrementalAppendScan().fromSnapshotExclusive(fromSnapshot).planWith(executor);
                }
            }
            // fromSnapshot is missing (could be due to snapshot expiration or rollback), or snapshot range contains modifications
            // (deletes or overwrites), so we cannot perform incremental refresh. Falling back to full refresh.
            icebergMetadata.disableIncrementalRefresh();
        }
        return icebergTable.newScan().useSnapshot(table.getSnapshotId().get()).planWith(executor);
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableFunctionHandle function)
    {
        if (function instanceof TableChangesFunctionHandle functionHandle) {
            Table icebergTable = transactionManager.get(transaction, session.getIdentity()).getIcebergTable(session, functionHandle.schemaTableName());

            TableChangesSplitSource tableChangesSplitSource = new TableChangesSplitSource(
                    icebergTable,
                    icebergTable.newIncrementalChangelogScan()
                            .fromSnapshotExclusive(functionHandle.startSnapshotId())
                            .toSnapshot(functionHandle.endSnapshotId()));
            return new ClassLoaderSafeConnectorSplitSource(tableChangesSplitSource, IcebergSplitManager.class.getClassLoader());
        }
        if (function instanceof IcebergTables icebergTables) {
            return new ClassLoaderSafeConnectorSplitSource(new FixedSplitSource(icebergTables), IcebergSplitManager.class.getClassLoader());
        }

        throw new IllegalStateException("Unknown table function: " + function);
    }
}
