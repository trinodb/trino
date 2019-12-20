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
package io.prestosql.plugin.jdbc;

import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class TransactionScopeCachingJdbcClient
        extends ForwardingJdbcClient
{
    private final Map<JdbcTableHandle, List<JdbcColumnHandle>> getColumnsCache = new ConcurrentHashMap<>();

    private final JdbcClient delegate;

    public TransactionScopeCachingJdbcClient(JdbcClient delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    protected JdbcClient getDelegate()
    {
        return delegate;
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return getColumnsCache.computeIfAbsent(tableHandle, ignored -> super.getColumns(session, tableHandle));
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        getColumnsCache.remove(handle);
        super.addColumn(session, handle, column);
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        getColumnsCache.remove(handle);
        super.renameColumn(identity, handle, jdbcColumn, newColumnName);
    }

    @Override
    public void dropColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        getColumnsCache.remove(handle);
        super.dropColumn(identity, handle, column);
    }
}
