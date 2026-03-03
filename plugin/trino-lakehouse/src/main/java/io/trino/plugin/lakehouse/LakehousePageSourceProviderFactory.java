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
import io.trino.plugin.deltalake.DeltaLakePageSourceProvider;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hudi.HudiPageSourceProvider;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.iceberg.IcebergPageSourceProviderFactory;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.system.files.FilesTableSplit;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class LakehousePageSourceProviderFactory
        implements ConnectorPageSourceProviderFactory
{
    private final HivePageSourceProvider hivePageSourceProvider;
    private final IcebergPageSourceProviderFactory icebergPageSourceProviderFactory;
    private final DeltaLakePageSourceProvider deltaLakePageSourceProvider;
    private final HudiPageSourceProvider hudiPageSourceProvider;

    @Inject
    public LakehousePageSourceProviderFactory(
            HivePageSourceProvider hivePageSourceProvider,
            IcebergPageSourceProviderFactory icebergPageSourceProviderFactory,
            DeltaLakePageSourceProvider deltaLakePageSourceProvider,
            HudiPageSourceProvider hudiPageSourceProvider)
    {
        this.hivePageSourceProvider = requireNonNull(hivePageSourceProvider, "hivePageSourceProvider is null");
        this.icebergPageSourceProviderFactory = requireNonNull(icebergPageSourceProviderFactory, "icebergPageSourceProviderFactory is null");
        this.deltaLakePageSourceProvider = requireNonNull(deltaLakePageSourceProvider, "deltaLakePageSourceProvider is null");
        this.hudiPageSourceProvider = requireNonNull(hudiPageSourceProvider, "hudiPageSourceProvider is null");
    }

    @Override
    public ConnectorPageSourceProvider createPageSourceProvider()
    {
        // createPageSourceProvider is called for each scan within a query
        // we hold on to ConnectorPageSourceProvider instance to allow IcebergPageSourceProvider to reuse equality deletes between splits of the same scan
        return new ConnectorPageSourceProvider()
        {
            private volatile ConnectorPageSourceProvider delegate;

            @Override
            public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
            {
                if (delegate == null) {
                    synchronized (this) {
                        if (delegate == null) {
                            delegate = forHandle(split, table);
                        }
                    }
                }
                return delegate.createPageSource(transaction, session, split, table, columns, dynamicFilter);
            }

            @Override
            public long getMemoryUsage()
            {
                ConnectorPageSourceProvider provider = delegate;
                if (provider == null) {
                    // No page source was created, so no memory is used
                    return 0;
                }
                return provider.getMemoryUsage();
            }
        };
    }

    private ConnectorPageSourceProvider forHandle(ConnectorSplit split, ConnectorTableHandle handle)
    {
        if (split instanceof FilesTableSplit) {
            return icebergPageSourceProviderFactory.createPageSourceProvider();
        }

        return switch (handle) {
            case HiveTableHandle _ -> hivePageSourceProvider;
            case IcebergTableHandle _ -> icebergPageSourceProviderFactory.createPageSourceProvider();
            case DeltaLakeTableHandle _ -> deltaLakePageSourceProvider;
            case HudiTableHandle _ -> hudiPageSourceProvider;
            default -> throw new UnsupportedOperationException("Unsupported table handle " + handle.getClass() + " with split " + split.getClass());
        };
    }
}
