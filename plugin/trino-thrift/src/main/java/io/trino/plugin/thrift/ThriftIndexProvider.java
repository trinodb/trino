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
import io.trino.spi.connector.ConnectorIndex;
import io.trino.spi.connector.ConnectorIndexHandle;
import io.trino.spi.connector.ConnectorIndexProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ThriftIndexProvider
        implements ConnectorIndexProvider
{
    private final DriftClient<TrinoThriftService> client;
    private final ThriftHeaderProvider thriftHeaderProvider;
    private final long maxBytesPerResponse;
    private final int lookupRequestsConcurrency;
    private final ThriftConnectorStats stats;

    @Inject
    public ThriftIndexProvider(DriftClient<TrinoThriftService> client, ThriftHeaderProvider thriftHeaderProvider, ThriftConnectorStats stats, ThriftConnectorConfig config)
    {
        this.client = requireNonNull(client, "client is null");
        this.thriftHeaderProvider = requireNonNull(thriftHeaderProvider, "thriftHeaderProvider is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.maxBytesPerResponse = config.getMaxResponseSize().toBytes();
        this.lookupRequestsConcurrency = config.getLookupRequestsConcurrency();
    }

    @Override
    public ConnectorIndex getIndex(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorIndexHandle indexHandle,
            List<ColumnHandle> lookupSchema,
            List<ColumnHandle> outputSchema)
    {
        return new ThriftConnectorIndex(
                client,
                thriftHeaderProvider.getHeaders(session),
                stats,
                (ThriftIndexHandle) indexHandle,
                lookupSchema,
                outputSchema,
                maxBytesPerResponse,
                lookupRequestsConcurrency);
    }
}
