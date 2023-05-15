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
package io.trino.plugin.cassandra;

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CassandraPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final CassandraTypeManager cassandraTypeManager;
    private final CassandraSession cassandraSession;
    private final int batchSize;

    @Inject
    public CassandraPageSinkProvider(
            CassandraTypeManager cassandraTypeManager,
            CassandraSession cassandraSession,
            CassandraClientConfig cassandraClientConfig)
    {
        this.cassandraTypeManager = requireNonNull(cassandraTypeManager, "cassandraTypeManager is null");
        this.cassandraSession = requireNonNull(cassandraSession, "cassandraSession is null");
        this.batchSize = cassandraClientConfig.getBatchSize();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle, ConnectorPageSinkId pageSinkId)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof CassandraOutputTableHandle, "tableHandle is not an instance of CassandraOutputTableHandle");
        CassandraOutputTableHandle handle = (CassandraOutputTableHandle) tableHandle;

        return new CassandraPageSink(
                cassandraTypeManager,
                cassandraSession,
                cassandraSession.getProtocolVersion(),
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getColumnNames(),
                handle.getColumnTypes(),
                true,
                batchSize);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle, ConnectorPageSinkId pageSinkId)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof CassandraInsertTableHandle, "tableHandle is not an instance of ConnectorInsertTableHandle");
        CassandraInsertTableHandle handle = (CassandraInsertTableHandle) tableHandle;

        return new CassandraPageSink(
                cassandraTypeManager,
                cassandraSession,
                cassandraSession.getProtocolVersion(),
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getColumnNames(),
                handle.getColumnTypes(),
                handle.isGenerateUuid(),
                batchSize);
    }
}
