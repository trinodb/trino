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
package io.trino.plugin.thrift;

import com.google.inject.Inject;
import io.airlift.drift.client.DriftClient;
import io.trino.plugin.thrift.api.TrinoThriftService;
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

public class ThriftPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final DriftClient<TrinoThriftService> client;
    private final ThriftHeaderProvider thriftHeaderProvider;
    private final long maxBytesPerResponse;
    private final ThriftConnectorStats stats;

    @Inject
    public ThriftPageSourceProvider(DriftClient<TrinoThriftService> client, ThriftHeaderProvider thriftHeaderProvider, ThriftConnectorStats stats, ThriftConnectorConfig config)
    {
        this.client = requireNonNull(client, "client is null");
        this.thriftHeaderProvider = requireNonNull(thriftHeaderProvider, "thriftHeaderProvider is null");
        this.maxBytesPerResponse = config.getMaxResponseSize().toBytes();
        this.stats = requireNonNull(stats, "stats is null");
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
        return new ThriftPageSource(client, thriftHeaderProvider.getHeaders(session), (ThriftConnectorSplit) split, columns, stats, maxBytesPerResponse);
    }
}
