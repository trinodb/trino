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
package io.trino.plugin.lakehouse;

import com.google.inject.Inject;
import io.trino.plugin.deltalake.DeltaLakeInsertTableHandle;
import io.trino.plugin.deltalake.DeltaLakeMergeTableHandle;
import io.trino.plugin.deltalake.DeltaLakeOutputTableHandle;
import io.trino.plugin.deltalake.DeltaLakePageSinkProvider;
import io.trino.plugin.deltalake.procedure.DeltaLakeTableExecuteHandle;
import io.trino.plugin.hive.HiveInsertTableHandle;
import io.trino.plugin.hive.HiveMergeTableHandle;
import io.trino.plugin.hive.HiveOutputTableHandle;
import io.trino.plugin.hive.HivePageSinkProvider;
import io.trino.plugin.hive.HiveTableExecuteHandle;
import io.trino.plugin.iceberg.IcebergMergeTableHandle;
import io.trino.plugin.iceberg.IcebergPageSinkProvider;
import io.trino.plugin.iceberg.IcebergWritableTableHandle;
import io.trino.plugin.iceberg.procedure.IcebergTableExecuteHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;

import static java.util.Objects.requireNonNull;

public class LakehousePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final HivePageSinkProvider hivePageSinkProvider;
    private final IcebergPageSinkProvider icebergPageSinkProvider;
    private final DeltaLakePageSinkProvider deltaPageSinkProvider;

    @Inject
    public LakehousePageSinkProvider(
            HivePageSinkProvider hivePageSinkProvider,
            IcebergPageSinkProvider icebergPageSinkProvider,
            DeltaLakePageSinkProvider deltaPageSinkProvider)
    {
        this.hivePageSinkProvider = requireNonNull(hivePageSinkProvider, "hivePageSinkProvider is null");
        this.icebergPageSinkProvider = requireNonNull(icebergPageSinkProvider, "icebergPageSinkProvider is null");
        this.deltaPageSinkProvider = requireNonNull(deltaPageSinkProvider, "deltaPageSinkProvider is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
    {
        return forHandle(outputTableHandle).createPageSink(transactionHandle, session, outputTableHandle, pageSinkId);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId)
    {
        return forHandle(insertTableHandle).createPageSink(transactionHandle, session, insertTableHandle, pageSinkId);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorPageSinkId pageSinkId)
    {
        return forHandle(tableExecuteHandle).createPageSink(transactionHandle, session, tableExecuteHandle, pageSinkId);
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId)
    {
        return forHandle(mergeHandle).createMergeSink(transactionHandle, session, mergeHandle, pageSinkId);
    }

    private ConnectorPageSinkProvider forHandle(ConnectorOutputTableHandle handle)
    {
        return switch (handle) {
            case HiveOutputTableHandle _ -> hivePageSinkProvider;
            case IcebergWritableTableHandle _ -> icebergPageSinkProvider;
            case DeltaLakeOutputTableHandle _ -> deltaPageSinkProvider;
            default -> throw new UnsupportedOperationException("Unsupported output handle " + handle.getClass());
        };
    }

    private ConnectorPageSinkProvider forHandle(ConnectorInsertTableHandle handle)
    {
        return switch (handle) {
            case HiveInsertTableHandle _ -> hivePageSinkProvider;
            case IcebergWritableTableHandle _ -> icebergPageSinkProvider;
            case DeltaLakeInsertTableHandle _ -> deltaPageSinkProvider;
            default -> throw new UnsupportedOperationException("Unsupported insert handle " + handle.getClass());
        };
    }

    private ConnectorPageSinkProvider forHandle(ConnectorTableExecuteHandle handle)
    {
        return switch (handle) {
            case HiveTableExecuteHandle _ -> hivePageSinkProvider;
            case IcebergTableExecuteHandle _ -> icebergPageSinkProvider;
            case DeltaLakeTableExecuteHandle _ -> deltaPageSinkProvider;
            default -> throw new UnsupportedOperationException("Unsupported execute handle " + handle.getClass());
        };
    }

    private ConnectorPageSinkProvider forHandle(ConnectorMergeTableHandle handle)
    {
        return switch (handle) {
            case HiveMergeTableHandle _ -> hivePageSinkProvider;
            case IcebergMergeTableHandle _ -> icebergPageSinkProvider;
            case DeltaLakeMergeTableHandle _ -> deltaPageSinkProvider;
            default -> throw new UnsupportedOperationException("Unsupported merge handle " + handle.getClass());
        };
    }
}
