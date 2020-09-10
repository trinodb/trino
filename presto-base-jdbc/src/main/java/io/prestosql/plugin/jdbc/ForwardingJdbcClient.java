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

import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public abstract class ForwardingJdbcClient
        implements JdbcClient
{
    public static JdbcClient of(Supplier<JdbcClient> jdbcClientSupplier)
    {
        requireNonNull(jdbcClientSupplier, "jdbcClientSupplier is null");
        return new ForwardingJdbcClient()
        {
            @Override
            public JdbcClient delegate()
            {
                return requireNonNull(jdbcClientSupplier.get(), "jdbcClientSupplier.get() is null");
            }
        };
    }

    protected abstract JdbcClient delegate();

    @Override
    public boolean schemaExists(JdbcIdentity identity, String schema)
    {
        return delegate().schemaExists(identity, schema);
    }

    @Override
    public Set<String> getSchemaNames(JdbcIdentity identity)
    {
        return delegate().getSchemaNames(identity);
    }

    @Override
    public List<SchemaTableName> getTableNames(JdbcIdentity identity, Optional<String> schema)
    {
        return delegate().getTableNames(identity, schema);
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        return delegate().getTableHandle(identity, schemaTableName);
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return delegate().getColumns(session, tableHandle);
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        return delegate().toPrestoType(session, connection, typeHandle);
    }

    @Override
    public List<ColumnMapping> getColumnMappings(ConnectorSession session, List<JdbcTypeHandle> typeHandles)
    {
        return delegate().getColumnMappings(session, typeHandles);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        return delegate().toWriteMapping(session, type);
    }

    @Override
    public boolean supportsGroupingSets()
    {
        return delegate().supportsGroupingSets();
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return delegate().implementAggregation(session, aggregate, assignments);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcTableHandle layoutHandle)
    {
        return delegate().getSplits(session, layoutHandle);
    }

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcSplit split)
            throws SQLException
    {
        return delegate().getConnection(identity, split);
    }

    @Override
    public void abortReadConnection(Connection connection)
            throws SQLException
    {
        delegate().abortReadConnection(connection);
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        return delegate().buildSql(session, connection, split, tableHandle, columnHandles);
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return delegate().beginCreateTable(session, tableMetadata);
    }

    @Override
    public void commitCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        delegate().commitCreateTable(identity, handle);
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        return delegate().beginInsertTable(session, tableHandle, columns);
    }

    @Override
    public void finishInsertTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        delegate().finishInsertTable(identity, handle);
    }

    @Override
    public void dropTable(JdbcIdentity identity, JdbcTableHandle jdbcTableHandle)
    {
        delegate().dropTable(identity, jdbcTableHandle);
    }

    @Override
    public void rollbackCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        delegate().rollbackCreateTable(identity, handle);
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle)
    {
        return delegate().buildInsertSql(handle);
    }

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcOutputTableHandle handle)
            throws SQLException
    {
        return delegate().getConnection(identity, handle);
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        return delegate().getPreparedStatement(connection, sql);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        return delegate().getTableStatistics(session, handle, tupleDomain);
    }

    @Override
    public boolean supportsLimit()
    {
        return delegate().supportsLimit();
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return delegate().isLimitGuaranteed(session);
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        delegate().addColumn(session, handle, column);
    }

    @Override
    public void dropColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        delegate().dropColumn(identity, handle, column);
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        delegate().renameColumn(identity, handle, jdbcColumn, newColumnName);
    }

    @Override
    public void renameTable(JdbcIdentity identity, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        delegate().renameTable(identity, handle, newTableName);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        delegate().createTable(session, tableMetadata);
    }

    @Override
    public void createSchema(JdbcIdentity identity, String schemaName)
    {
        delegate().createSchema(identity, schemaName);
    }

    @Override
    public void dropSchema(JdbcIdentity identity, String schemaName)
    {
        delegate().dropSchema(identity, schemaName);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return delegate().getSystemTable(session, tableName);
    }

    @Override
    public String quoted(String name)
    {
        return delegate().quoted(name);
    }

    @Override
    public String quoted(RemoteTableName remoteTableName)
    {
        return delegate().quoted(remoteTableName);
    }

    @Override
    public Map<String, Object> getTableProperties(JdbcIdentity identity, JdbcTableHandle tableHandle)
    {
        return delegate().getTableProperties(identity, tableHandle);
    }
}
