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
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.plugin.iceberg.functions.tablechanges.TableChangesFunctionHandle;
import io.trino.plugin.iceberg.functions.tablechanges.TableChangesSplitSource;
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
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;

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
    private final ExecutorService executor;
    private final CachingHostAddressProvider cachingHostAddressProvider;

    @Inject
    public IcebergSplitManager(
            IcebergTransactionManager transactionManager,
            TypeManager typeManager,
            IcebergFileSystemFactory fileSystemFactory,
            @ForIcebergSplitManager ExecutorService executor,
            CachingHostAddressProvider cachingHostAddressProvider)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.executor = requireNonNull(executor, "executor is null");
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

        Table icebergTable = transactionManager.get(transaction, session.getIdentity()).getIcebergTable(session, table.getSchemaTableName());
        Duration dynamicFilteringWaitTimeout = getDynamicFilteringWaitTimeout(session);

        TableScan tableScan = icebergTable.newScan()
                .useSnapshot(table.getSnapshotId().get())
                .planWith(executor);

        IcebergSplitSource splitSource = new IcebergSplitSource(
                fileSystemFactory,
                session,
                table,
                icebergTable.io().properties(),
                tableScan,
                table.getMaxScannedFileSize(),
                dynamicFilter,
                dynamicFilteringWaitTimeout,
                constraint,
                typeManager,
                table.isRecordScannedFiles(),
                getMinimumAssignedSplitWeight(session),
                cachingHostAddressProvider);

        return new ClassLoaderSafeConnectorSplitSource(splitSource, IcebergSplitManager.class.getClassLoader());
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

        throw new IllegalStateException("Unknown table function: " + function);
    }
}
