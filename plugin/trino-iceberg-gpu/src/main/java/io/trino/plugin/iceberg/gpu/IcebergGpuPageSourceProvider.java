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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.gpu.engine.GpuEngine;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The device answers a pushed-down aggregation; the Iceberg connector scans everything else, and
 * this connector filters the scan when it took predicates over.
 */
final class IcebergGpuPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final ConnectorPageSourceProvider delegate;

    IcebergGpuPageSourceProvider(ConnectorPageSourceProvider delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> credentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            MemoryContext memoryContext)
    {
        if (!(table instanceof GpuTableHandle gpu)) {
            return delegate.createPageSource(transaction, session, split, table, credentials, columns, dynamicFilter, memoryContext);
        }
        if (split instanceof GpuSplit gpuSplit) {
            return new GpuAggregationPageSource(gpu, engineTable(gpuSplit.key(), gpuSplit.files(), gpuSplit.rows()), columns);
        }
        List<ColumnHandle> read = ImmutableList.copyOf(IcebergGpuSplitManager.sourceColumns(gpu, columns));
        ConnectorPageSource scan = delegate.createPageSource(transaction, session, split, gpu.base(), credentials, read, dynamicFilter, memoryContext);
        return new FilteringPageSource(scan, gpu, read, columns);
    }

    /**
     * A snapshot's data files for the engine, with a first guess at a string column's bytes: two a row.
     */
    static GpuEngine.Table engineTable(String key, List<String> files, long rows)
    {
        return new GpuEngine.Table(key, files, rows, rows * 2 + (1 << 20));
    }
}
