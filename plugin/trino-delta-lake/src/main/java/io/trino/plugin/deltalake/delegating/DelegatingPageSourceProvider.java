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
package io.trino.plugin.deltalake.delegating;

import io.trino.plugin.deltalake.DeltaLakePageSourceProvider;
import io.trino.plugin.deltalake.kernel.KernelDeltaLakePageSourceProvider;
import io.trino.plugin.deltalake.kernel.KernelDeltaLakeTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import jakarta.inject.Inject;

import java.util.List;

public class DelegatingPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final DeltaLakePageSourceProvider deltaLakePageSourceProvider;
    private final KernelDeltaLakePageSourceProvider kernelDeltaLakePageSourceProvider;

    @Inject
    public DelegatingPageSourceProvider(
            DeltaLakePageSourceProvider deltaLakePageSourceProvider,
            KernelDeltaLakePageSourceProvider kernelDeltaLakePageSourceProvider)
    {
        this.deltaLakePageSourceProvider = deltaLakePageSourceProvider;
        this.kernelDeltaLakePageSourceProvider = kernelDeltaLakePageSourceProvider;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        switch (table) {
            case KernelDeltaLakeTableHandle notUsed:
                return kernelDeltaLakePageSourceProvider.createPageSource(
                        transaction,
                        session,
                        split,
                        table,
                        columns,
                        dynamicFilter);
            default:
                return deltaLakePageSourceProvider.createPageSource(
                        transaction,
                        session,
                        split,
                        table,
                        columns,
                        dynamicFilter);
        }
    }
}
