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
package io.trino.plugin.lance;

import com.google.inject.Inject;
import io.trino.plugin.lance.internal.LanceReader;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class LancePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final LanceReader lanceReader;
    private final LanceConfig lanceConfig;

    @Inject
    public LancePageSourceProvider(LanceReader lanceReader, LanceConfig lanceConfig)
    {
        this.lanceReader = requireNonNull(lanceReader, "lanceClient is null");
        this.lanceConfig = lanceConfig;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
            ConnectorSplit split, ConnectorTableHandle tableHandle, List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        requireNonNull(split, "split is null");
        LanceSplit lanceSplit = (LanceSplit) split;
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;
        if (lanceSplit.getFragments().isEmpty()) {
            return new LanceDatasetPageSource(lanceReader, lanceTableHandle, lanceConfig.getFetchRetryCount());
        }
        else {
            // TODO: support multiple fragment per split, now it is 1-to-1 mapping between fragment and split
            return new LanceFragmentPageSource(lanceReader, lanceTableHandle, lanceSplit.getFragments(), lanceConfig.getFetchRetryCount());
        }
    }
}
