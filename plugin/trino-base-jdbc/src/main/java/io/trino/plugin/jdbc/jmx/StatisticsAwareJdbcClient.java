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
package io.trino.plugin.jdbc.jmx;

import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.Type;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
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
    public boolean schemaExists(ConnectorSession session, String schema)
    {
        return stats.getSchemaExists().wrap(() -> delegate().schemaExists(session, schema));
    }

    @Override
    public Set<String> getSchemaNames(ConnectorSession session)
    {
        return stats.getGetSchemaNames().wrap(() -> delegate().getSchemaNames(session));
    }

    @Override
    public List<SchemaTableName> getTableNames(ConnectorSession session, Optional<String> schema)
    {
        return stats.getGetTableNames().wrap(() -> delegate().getTableNames(session, schema));
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return stats.getGetTableHandle().wrap(() -> delegate().getTableHandle(session, schemaTableName));
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return stats.getGetColumns().wrap(() -> delegate().getColumns(session, tableHandle));
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        return stats.getToPrestoType().wrap(() -> delegate().toColumnMapping(session, connection, typeHandle));
    }

    @Override
    public List<ColumnMapping> toColumnMappings(ConnectorSession session, List<JdbcTypeHandle> typeHandles)
    {
        return stats.getGetColumnMappings().wrap(() -> delegate.toColumnMappings(session, typeHandles));
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        return stats.getToWriteMapping().wrap(() -> delegate().toWriteMapping(session, type));
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        return delegate().supportsAggregationPushdown(session, table, aggregates, assignments, groupingSets);
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
    public Connection getConnection(ConnectorSession session, JdbcSplit split)
            throws SQLException
    {
        return stats.getGetConnectionWithSplit().wrap(() -> delegate().getConnection(session, split));
    }

    @Override
    public void abortReadConnection(Connection connection, ResultSet resultSet)
            throws SQLException
    {
        stats.getAbortReadConnection().wrap(() -> delegate().abortReadConnection(connection, resultSet));
    }

    @Override
    public PreparedQuery prepareQuery(
            ConnectorSession session,
            JdbcTableHandle table,
            Optional<List<List<JdbcColumnHandle>>> groupingSets,
            List<JdbcColumnHandle> columns,
            Map<String, String> columnExpressions)
    {
        return stats.getPrepareQuery().wrap(() -> delegate().prepareQuery(session, table, groupingSets, columns, columnExpressions));
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        return stats.getBuildSql().wrap(() -> delegate().buildSql(session, connection, split, tableHandle, columnHandles));
    }

    @Override
    public Optional<PreparedQuery> implementJoin(ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> joinConditions,
            Map<JdbcColumnHandle, String> rightAssignments,
            Map<JdbcColumnHandle, String> leftAssignments,
            JoinStatistics statistics)
    {
        return stats.getImplementJoin().wrap(() -> delegate().implementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics));
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        stats.getSetColumnComment().wrap(() -> delegate().setColumnComment(session, handle, column, comment));
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        stats.getAddColumn().wrap(() -> delegate().addColumn(session, handle, column));
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        stats.getDropColumn().wrap(() -> delegate().dropColumn(session, handle, column));
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        stats.getRenameColumn().wrap(() -> delegate().renameColumn(session, handle, jdbcColumn, newColumnName));
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        stats.getRenameTable().wrap(() -> delegate().renameTable(session, handle, newTableName));
    }

    @Override
    public void setTableProperties(ConnectorSession session, JdbcTableHandle handle, Map<String, Optional<Object>> properties)
    {
        stats.getSetTableProperties().wrap(() -> delegate().setTableProperties(session, handle, properties));
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
    public void commitCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        stats.getCommitCreateTable().wrap(() -> delegate().commitCreateTable(session, handle));
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        return stats.getBeginInsertTable().wrap(() -> delegate().beginInsertTable(session, tableHandle, columns));
    }

    @Override
    public void finishInsertTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        stats.getFinishInsertTable().wrap(() -> delegate().finishInsertTable(session, handle));
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle jdbcTableHandle)
    {
        stats.getDropTable().wrap(() -> delegate().dropTable(session, jdbcTableHandle));
    }

    @Override
    public void rollbackCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        stats.getRollbackCreateTable().wrap(() -> delegate().rollbackCreateTable(session, handle));
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters)
    {
        return stats.getBuildInsertSql().wrap(() -> delegate().buildInsertSql(handle, columnWriters));
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcOutputTableHandle handle)
            throws SQLException
    {
        return stats.getGetConnectionWithHandle().wrap(() -> delegate().getConnection(session, handle));
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
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return delegate().supportsTopN(session, handle, sortOrder);
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return delegate().isTopNGuaranteed(session);
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
    public void createSchema(ConnectorSession session, String schemaName)
    {
        stats.getCreateSchema().wrap(() -> delegate().createSchema(session, schemaName));
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        stats.getDropSchema().wrap(() -> delegate().dropSchema(session, schemaName));
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        stats.getRenameSchema().wrap(() -> delegate().renameSchema(session, schemaName, newSchemaName));
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
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return delegate().getTableProperties(session, tableHandle);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> getTableScanRedirection(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return stats.getGetTableScanRedirection().wrap(() -> delegate().getTableScanRedirection(session, tableHandle));
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        return stats.getDelete().wrap(() -> delegate().delete(session, handle));
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        stats.getTruncateTable().wrap(() -> delegate().truncateTable(session, handle));
    }
}
