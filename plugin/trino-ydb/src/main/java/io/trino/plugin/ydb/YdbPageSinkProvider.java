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
package io.trino.plugin.ydb;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMergeTableHandle;
import io.trino.plugin.jdbc.JdbcPageSinkProvider;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.MemoryContext;

import java.util.Optional;

public class YdbPageSinkProvider
        extends JdbcPageSinkProvider
{
    private final JdbcClient client;
    private final RemoteQueryModifier modifier;

    @Inject
    public YdbPageSinkProvider(JdbcClient client, RemoteQueryModifier modifier, QueryBuilder queryBuilder)
    {
        super(client, modifier, queryBuilder);
        this.client = client;
        this.modifier = modifier;
    }

    @Override
    public ConnectorMergeSink createMergeSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorMergeTableHandle handle,
            Optional<ConnectorTableCredentials> credentials,
            ConnectorPageSinkId pageSinkId,
            MemoryContext memoryContext)
    {
        return new YdbMergeSink(session, (JdbcMergeTableHandle) handle, client, pageSinkId, modifier);
    }
}
