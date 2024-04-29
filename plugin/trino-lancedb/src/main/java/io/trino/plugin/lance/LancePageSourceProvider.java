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
import io.trino.plugin.lance.internal.LanceClient;
import io.trino.plugin.lance.internal.LanceDynamicTable;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;


public class LancePageSourceProvider
        implements ConnectorPageSourceProvider
{

    private LanceConfig lanceConfig;
    private LanceClient lanceClient;

    @Inject
    public LancePageSourceProvider(
            LanceConfig lanceConfig,
            LanceClient lanceClient)
    {
        this.lanceConfig = lanceConfig;
        this.lanceClient = lanceClient;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        requireNonNull(split, "split is null");

        LanceSplit lanceSplit = (LanceSplit) split;
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;
        if (lanceConfig.isJni()) {
             return new LanceFragmentPageSource(split, columns, lanceConfig);
        }
        else {
            Optional<LanceDynamicTable> dynamicTable = ((LanceTableHandle) tableHandle).getDynamicTable();
            if (dynamicTable.isPresent()) {
                LanceDynamicTable lanceDynamicTable = dynamicTable.get();
                return new LanceHttpPageSource(session, lanceDynamicTable, columns, lanceClient);
            }
        }
        throw new UnsupportedOperationException("Unknown split type: " + lanceSplit.getSplitType());
    }
}
