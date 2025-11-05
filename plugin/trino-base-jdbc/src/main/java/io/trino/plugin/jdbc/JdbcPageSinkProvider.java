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
package io.trino.plugin.jdbc;

import com.google.inject.Inject;
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

import static java.util.Objects.requireNonNull;

public class JdbcPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final JdbcClient jdbcClient;
    private final RemoteQueryModifier queryModifier;
    private final QueryBuilder queryBuilder;

    @Inject
    public JdbcPageSinkProvider(JdbcClient jdbcClient, RemoteQueryModifier remoteQueryModifier, QueryBuilder queryBuilder)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.queryModifier = requireNonNull(remoteQueryModifier, "remoteQueryModifier is null");
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle, ConnectorPageSinkId pageSinkId)
    {
        return new JdbcPageSink(session, (JdbcOutputTableHandle) tableHandle, jdbcClient, pageSinkId, queryModifier, JdbcClient::buildInsertSql);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle, ConnectorPageSinkId pageSinkId)
    {
        return new JdbcPageSink(session, (JdbcOutputTableHandle) tableHandle, jdbcClient, pageSinkId, queryModifier, JdbcClient::buildInsertSql);
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId)
    {
        return new JdbcMergeSink(session, mergeHandle, jdbcClient, pageSinkId, queryModifier, queryBuilder);
    }
}
