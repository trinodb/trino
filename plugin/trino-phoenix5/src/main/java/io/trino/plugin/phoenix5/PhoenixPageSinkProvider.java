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
package io.trino.plugin.phoenix5;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.JdbcPageSinkProvider;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

public class PhoenixPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final JdbcPageSinkProvider delegate;
    private final PhoenixClient jdbcClient;
    private final RemoteQueryModifier remoteQueryModifier;

    @Inject
    public PhoenixPageSinkProvider(PhoenixClient jdbcClient, RemoteQueryModifier remoteQueryModifier)
    {
        this.delegate = new JdbcPageSinkProvider(jdbcClient, remoteQueryModifier);
        this.jdbcClient = jdbcClient;
        this.remoteQueryModifier = remoteQueryModifier;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
    {
        return delegate.createPageSink(transactionHandle, session, outputTableHandle, pageSinkId);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId)
    {
        return delegate.createPageSink(transactionHandle, session, insertTableHandle, pageSinkId);
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId)
    {
        return new PhoenixMergeSink(jdbcClient, remoteQueryModifier, session, mergeHandle, pageSinkId);
    }
}
