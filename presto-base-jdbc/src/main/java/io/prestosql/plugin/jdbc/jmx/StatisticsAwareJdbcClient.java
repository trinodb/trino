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
package io.prestosql.plugin.jdbc.jmx;

import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcExpression;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.RemoteTableName;
import io.prestosql.plugin.jdbc.WriteMapping;
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
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class StatisticsAwareJdbcClient
        implements JdbcClient
{
    private final JdbcClientStats stats = new JdbcClientStats();
    private final JdbcClient delegate;

    public StatisticsAwareJdbcClient(JdbcClient delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    private JdbcClient delegate()
    {
        return delegate;
    }

    @Managed
    @Flatten
    public JdbcClientStats getStats()
    {
        return stats;
    }

    @Override
    public boolean schemaExists(JdbcIdentity identity, String schema)
    {
        return stats.getSchemaExists().wrap(() -> delegate().schemaExists(identity, schema));
    }

    @Override
    public Set<String> getSchemaNames(JdbcIdentity identity)
    {
        return stats.getGetSchemaNames().wrap(() -> delegate().getSchemaNames(identity));
    }

    @Override
    public List<SchemaTableName> getTableNames(JdbcIdentity identity, Optional<String> schema)
    {
        return stats.getGetTableNames().wrap(() -> delegate().getTableNames(identity, schema));
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        return stats.getGetTableHandle().wrap(() -> delegate().getTableHandle(identity, schemaTableName));
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return stats.getGetColumns().wrap(() -> delegate().getColumns(session, tableHandle));
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        return stats.getToPrestoType().wrap(() -> delegate().toPrestoType(session, connection, typeHandle));
    }

    @Override
    public List<ColumnMapping> getColumnMappings(ConnectorSession session, List<JdbcTypeHandle> typeHandles)
    {
        return stats.getGetColumnMappings().wrap(() -> delegate.getColumnMappings(session, typeHandles));
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        return stats.getToWriteMapping().wrap(() -> delegate().toWriteMapping(session, type));
    }

    @Override
    public boolean supportsGroupingSets()
    {
        return delegate().supportsGroupingSets();
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return stats.getImplementAggregation().wrap(() -> delegate().implementAggregation(session, aggregate, assignments));
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcTableHandle layoutHandle)
    {
        return stats.getGetSplits().wrap(() -> delegate().getSplits(session, layoutHandle));
    }

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcSplit split)
            throws SQLException
    {
        return stats.getGetConnectionWithSplit().wrap(() -> delegate().getConnection(identity, split));
    }

    @Override
    public void abortReadConnection(Connection connection)
            throws SQLException
    {
        stats.getAbortReadConnection().wrap(() -> delegate().abortReadConnection(connection));
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        return stats.getBuildSql().wrap(() -> delegate().buildSql(session, connection, split, tableHandle, columnHandles));
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        stats.getAddColumn().wrap(() -> delegate().addColumn(session, handle, column));
    }

    @Override
    public void dropColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        stats.getDropColumn().wrap(() -> delegate().dropColumn(identity, handle, column));
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        stats.getRenameColumn().wrap(() -> delegate().renameColumn(identity, handle, jdbcColumn, newColumnName));
    }

    @Override
    public void renameTable(JdbcIdentity identity, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        stats.getRenameTable().wrap(() -> delegate().renameTable(identity, handle, newTableName));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        stats.getCreateTable().wrap(() -> delegate().createTable(session, tableMetadata));
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return stats.getBeginCreateTable().wrap(() -> delegate().beginCreateTable(session, tableMetadata));
    }

    @Override
    public void commitCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        stats.getCommitCreateTable().wrap(() -> delegate().commitCreateTable(identity, handle));
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        return stats.getBeginInsertTable().wrap(() -> delegate().beginInsertTable(session, tableHandle, columns));
    }

    @Override
    public void finishInsertTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        stats.getFinishInsertTable().wrap(() -> delegate().finishInsertTable(identity, handle));
    }

    @Override
    public void dropTable(JdbcIdentity identity, JdbcTableHandle jdbcTableHandle)
    {
        stats.getDropTable().wrap(() -> delegate().dropTable(identity, jdbcTableHandle));
    }

    @Override
    public void rollbackCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        stats.getRollbackCreateTable().wrap(() -> delegate().rollbackCreateTable(identity, handle));
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle)
    {
        return stats.getBuildInsertSql().wrap(() -> delegate().buildInsertSql(handle));
    }

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcOutputTableHandle handle)
            throws SQLException
    {
        return stats.getGetConnectionWithHandle().wrap(() -> delegate().getConnection(identity, handle));
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        return stats.getGetPreparedStatement().wrap(() -> delegate().getPreparedStatement(connection, sql));
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        return stats.getGetTableStatistics().wrap(() -> delegate().getTableStatistics(session, handle, tupleDomain));
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
    public void createSchema(JdbcIdentity identity, String schemaName)
    {
        stats.getCreateSchema().wrap(() -> delegate().createSchema(identity, schemaName));
    }

    @Override
    public void dropSchema(JdbcIdentity identity, String schemaName)
    {
        stats.getDropSchema().wrap(() -> delegate().dropSchema(identity, schemaName));
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
