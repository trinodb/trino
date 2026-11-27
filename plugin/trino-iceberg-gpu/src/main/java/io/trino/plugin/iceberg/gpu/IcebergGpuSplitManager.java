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
package io.trino.plugin.iceberg.gpu;

import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * A pushed-down aggregation is one split with the snapshot's data files; any other table's splits
 * are the Iceberg connector's.
 */
final class IcebergGpuSplitManager
        implements ConnectorSplitManager
{
    private final ConnectorSplitManager delegate;

    IcebergGpuSplitManager(ConnectorSplitManager delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, Set<ColumnHandle> columns, Constraint constraint)
    {
        if (!(table instanceof GpuTableHandle gpu)) {
            return delegate.getSplits(transaction, session, table, columns, constraint);
        }
        if (gpu.aggregation().isEmpty()) {
            return delegate.getSplits(transaction, session, gpu.base(), sourceColumns(gpu, columns), constraint);
        }
        DataFiles.Listing listing = DataFiles.list(delegate, transaction, session, (IcebergTableHandle) gpu.base())
                .orElseThrow(() -> new TrinoException(GENERIC_INTERNAL_ERROR, "The data files of a pushed-down aggregation are no longer readable on the device"));
        return new FixedSplitSource(new GpuSplit(listing.key(), listing.files(), listing.rows()));
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableFunctionHandle function)
    {
        return delegate.getSplits(transaction, session, function);
    }

    /**
     * The Iceberg columns a scan reads for the requested columns and this connector's predicates.
     */
    static Set<ColumnHandle> sourceColumns(GpuTableHandle table, Collection<ColumnHandle> requested)
    {
        Set<ColumnHandle> read = new LinkedHashSet<>();
        for (ColumnHandle column : requested) {
            if (column instanceof GpuColumnHandle own) {
                for (String name : own.expression().orElseThrow().columns()) {
                    if (!name.isEmpty()) {
                        read.add(table.sources().get(name));
                    }
                }
            }
            else {
                read.add(column);
            }
        }
        for (GpuPredicate predicate : table.predicates()) {
            read.add(table.sources().get(predicate.column()));
        }
        return read;
    }
}
