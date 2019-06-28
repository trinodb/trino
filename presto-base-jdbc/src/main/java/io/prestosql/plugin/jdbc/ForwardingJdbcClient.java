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

import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public abstract class ForwardingJdbcClient
        implements JdbcClient
{
    protected abstract JdbcClient getDelegate();

    @Override
    public boolean schemaExists(JdbcIdentity identity, String schema)
    {
        return getDelegate().schemaExists(identity, schema);
    }

    @Override
    public Set<String> getSchemaNames(JdbcIdentity identity)
    {
        return getDelegate().getSchemaNames(identity);
    }

    @Override
    public List<SchemaTableName> getTableNames(JdbcIdentity identity, Optional<String> schema)
    {
        return getDelegate().getTableNames(identity, schema);
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        return getDelegate().getTableHandle(identity, schemaTableName);
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return getDelegate().getColumns(session, tableHandle);
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        return getDelegate().toPrestoType(session, connection, typeHandle);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        return getDelegate().toWriteMapping(session, type);
    }

    @Override
    public ConnectorSplitSource getSplits(JdbcIdentity identity, JdbcTableHandle layoutHandle)
    {
        return getDelegate().getSplits(identity, layoutHandle);
    }

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcSplit split)
            throws SQLException
    {
        return getDelegate().getConnection(identity, split);
    }

    @Override
    public void abortReadConnection(Connection connection)
            throws SQLException
    {
        getDelegate().abortReadConnection(connection);
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        return getDelegate().buildSql(session, connection, split, tableHandle, columnHandles);
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return getDelegate().beginCreateTable(session, tableMetadata);
    }

    @Override
    public void commitCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        getDelegate().commitCreateTable(identity, handle);
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return getDelegate().beginInsertTable(session, tableMetadata);
    }

    @Override
    public void finishInsertTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        getDelegate().finishInsertTable(identity, handle);
    }

    @Override
    public void dropTable(JdbcIdentity identity, JdbcTableHandle jdbcTableHandle)
    {
        getDelegate().dropTable(identity, jdbcTableHandle);
    }

    @Override
    public void rollbackCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        getDelegate().rollbackCreateTable(identity, handle);
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle)
    {
        return getDelegate().buildInsertSql(handle);
    }

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcOutputTableHandle handle)
            throws SQLException
    {
        return getDelegate().getConnection(identity, handle);
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        return getDelegate().getPreparedStatement(connection, sql);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        return getDelegate().getTableStatistics(session, handle, tupleDomain);
    }

    @Override
    public boolean supportsLimit()
    {
        return getDelegate().supportsLimit();
    }

    @Override
    public boolean isLimitGuaranteed()
    {
        return getDelegate().isLimitGuaranteed();
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        getDelegate().addColumn(session, handle, column);
    }

    @Override
    public void dropColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        getDelegate().dropColumn(identity, handle, column);
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        getDelegate().renameColumn(identity, handle, jdbcColumn, newColumnName);
    }

    @Override
    public void renameTable(JdbcIdentity identity, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        getDelegate().renameTable(identity, handle, newTableName);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        getDelegate().createTable(session, tableMetadata);
    }
}
