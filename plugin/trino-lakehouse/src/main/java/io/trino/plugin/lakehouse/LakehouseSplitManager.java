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
import io.trino.plugin.deltalake.DeltaLakeSplitManager;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.functions.tablechanges.TableChangesTableFunctionHandle;
import io.trino.plugin.hive.HiveSplitManager;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hudi.HudiSplitManager;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.iceberg.IcebergSplitManager;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.functions.tablechanges.TableChangesFunctionHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import static java.util.Objects.requireNonNull;

public class LakehouseSplitManager
        implements ConnectorSplitManager
{
    private final HiveSplitManager hiveSplitManager;
    private final IcebergSplitManager icebergSplitManager;
    private final DeltaLakeSplitManager deltaSplitManager;
    private final HudiSplitManager hudiSplitManager;

    @Inject
    public LakehouseSplitManager(
            HiveSplitManager hiveSplitManager,
            IcebergSplitManager icebergSplitManager,
            DeltaLakeSplitManager deltaSplitManager,
            HudiSplitManager hudiSplitManager)
    {
        this.hiveSplitManager = requireNonNull(hiveSplitManager, "hiveSplitManager is null");
        this.icebergSplitManager = requireNonNull(icebergSplitManager, "icebergSplitManager is null");
        this.deltaSplitManager = requireNonNull(deltaSplitManager, "deltaSplitManager is null");
        this.hudiSplitManager = requireNonNull(hudiSplitManager, "hudiSplitManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, DynamicFilter dynamicFilter, Constraint constraint)
    {
        return forHandle(table).getSplits(transaction, session, table, dynamicFilter, constraint);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableFunctionHandle function)
    {
        return forHandle(function).getSplits(transaction, session, function);
    }

    private ConnectorSplitManager forHandle(ConnectorTableHandle handle)
    {
        return switch (handle) {
            case HiveTableHandle _ -> hiveSplitManager;
            case IcebergTableHandle _ -> icebergSplitManager;
            case DeltaLakeTableHandle _ -> deltaSplitManager;
            case HudiTableHandle _ -> hudiSplitManager;
            default -> throw new IllegalArgumentException("Unsupported table handle: " + handle.getClass().getName());
        };
    }

    private ConnectorSplitManager forHandle(ConnectorTableFunctionHandle handle)
    {
        return switch (handle) {
            case TableChangesFunctionHandle _ -> icebergSplitManager;
            case TableChangesTableFunctionHandle _ -> deltaSplitManager;
            default -> throw new IllegalArgumentException("Unsupported table function handle: " + handle.getClass().getName());
        };
    }
}
