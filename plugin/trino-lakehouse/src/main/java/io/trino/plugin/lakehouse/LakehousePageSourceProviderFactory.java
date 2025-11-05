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
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorTableHandle;

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
        return (transaction, session, split, table, columns, dynamicFilter) ->
                forHandle(table).createPageSource(transaction, session, split, table, columns, dynamicFilter);
    }

    private ConnectorPageSourceProvider forHandle(ConnectorTableHandle handle)
    {
        return switch (handle) {
            case HiveTableHandle _ -> hivePageSourceProvider;
            case IcebergTableHandle _ -> icebergPageSourceProviderFactory.createPageSourceProvider();
            case DeltaLakeTableHandle _ -> deltaLakePageSourceProvider;
            case HudiTableHandle _ -> hudiPageSourceProvider;
            default -> throw new UnsupportedOperationException("Unsupported table handle " + handle.getClass());
        };
    }
}
