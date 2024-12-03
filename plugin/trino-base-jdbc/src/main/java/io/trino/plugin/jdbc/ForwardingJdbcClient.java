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

import io.trino.plugin.jdbc.JdbcProcedureHandle.ProcedureQuery;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.Type;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
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
    public boolean schemaExists(ConnectorSession session, String schema)
    {
        return delegate().schemaExists(session, schema);
    }

    @Override
    public Set<String> getSchemaNames(ConnectorSession session)
    {
        return delegate().getSchemaNames(session);
    }

    @Override
    public List<SchemaTableName> getTableNames(ConnectorSession session, Optional<String> schema)
    {
        return delegate().getTableNames(session, schema);
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return delegate().getTableHandle(session, schemaTableName);
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, PreparedQuery preparedQuery)
    {
        return delegate().getTableHandle(session, preparedQuery);
    }

    @Override
    public JdbcProcedureHandle getProcedureHandle(ConnectorSession session, ProcedureQuery procedureQuery)
    {
        return delegate().getProcedureHandle(session, procedureQuery);
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, SchemaTableName schemaTableName, RemoteTableName remoteTableName)
    {
        return delegate().getColumns(session, schemaTableName, remoteTableName);
    }

    @Override
    public Iterator<RelationColumnsMetadata> getAllTableColumns(ConnectorSession session, Optional<String> schema)
    {
        return delegate().getAllTableColumns(session, schema);
    }

    @Override
    public List<RelationCommentMetadata> getAllTableComments(ConnectorSession session, Optional<String> schema)
    {
        return delegate().getAllTableComments(session, schema);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        return delegate().toColumnMapping(session, connection, typeHandle);
    }

    @Override
    public List<ColumnMapping> toColumnMappings(ConnectorSession session, List<JdbcTypeHandle> typeHandles)
    {
        return delegate().toColumnMappings(session, typeHandles);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        return delegate().toWriteMapping(session, type);
    }

    @Override
    public Optional<Type> getSupportedType(ConnectorSession session, Type type)
    {
        return delegate().getSupportedType(session, type);
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        return delegate().supportsAggregationPushdown(session, table, aggregates, assignments, groupingSets);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return delegate().implementAggregation(session, aggregate, assignments);
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return delegate().convertPredicate(session, expression, assignments);
    }

    @Override
    public Optional<JdbcExpression> convertProjection(ConnectorSession session, JdbcTableHandle handle, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return delegate().convertProjection(session, handle, expression, assignments);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcTableHandle layoutHandle)
    {
        return delegate().getSplits(session, layoutHandle);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcProcedureHandle procedureHandle)
    {
        return delegate().getSplits(session, procedureHandle);
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcSplit split, JdbcTableHandle tableHandle)
            throws SQLException
    {
        return delegate().getConnection(session, split, tableHandle);
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcSplit split, JdbcProcedureHandle procedureHandle)
            throws SQLException
    {
        return delegate().getConnection(session, split, procedureHandle);
    }

    @Override
    public void abortReadConnection(Connection connection, ResultSet resultSet)
            throws SQLException
    {
        delegate().abortReadConnection(connection, resultSet);
    }

    @Override
    public PreparedQuery prepareQuery(
            ConnectorSession session,
            JdbcTableHandle table,
            Optional<List<List<JdbcColumnHandle>>> groupingSets,
            List<JdbcColumnHandle> columns,
            Map<String, ParameterizedExpression> columnExpressions)
    {
        return delegate().prepareQuery(session, table, groupingSets, columns, columnExpressions);
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        return delegate().buildSql(session, connection, split, tableHandle, columnHandles);
    }

    @Override
    public CallableStatement buildProcedure(ConnectorSession session, Connection connection, JdbcSplit split, JdbcProcedureHandle procedureHandle)
            throws SQLException
    {
        return delegate().buildProcedure(session, connection, split, procedureHandle);
    }

    @Override
    public Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            Map<JdbcColumnHandle, String> leftProjections,
            PreparedQuery rightSource,
            Map<JdbcColumnHandle, String> rightProjections,
            List<ParameterizedExpression> joinConditions,
            JoinStatistics statistics)
    {
        return delegate().implementJoin(session, joinType, leftSource, leftProjections, rightSource, rightProjections, joinConditions, statistics);
    }

    @Override
    public Optional<PreparedQuery> legacyImplementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> joinConditions,
            Map<JdbcColumnHandle, String> rightAssignments,
            Map<JdbcColumnHandle, String> leftAssignments,
            JoinStatistics statistics)
    {
        return delegate().legacyImplementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics);
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return delegate().beginCreateTable(session, tableMetadata);
    }

    @Override
    public void commitCreateTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        delegate().commitCreateTable(session, handle, pageSinkIds);
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        return delegate().beginInsertTable(session, tableHandle, columns);
    }

    @Override
    public void finishInsertTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        delegate().finishInsertTable(session, handle, pageSinkIds);
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle jdbcTableHandle)
    {
        delegate().dropTable(session, jdbcTableHandle);
    }

    @Override
    public void rollbackCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        delegate().rollbackCreateTable(session, handle);
    }

    @Override
    public boolean supportsRetries()
    {
        return delegate().supportsRetries();
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters)
    {
        return delegate().buildInsertSql(handle, columnWriters);
    }

    @Override
    public Connection getConnection(ConnectorSession session)
            throws SQLException
    {
        return delegate().getConnection(session);
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcOutputTableHandle handle)
            throws SQLException
    {
        return delegate().getConnection(session, handle);
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql, Optional<Integer> columnCount)
            throws SQLException
    {
        return delegate().getPreparedStatement(connection, sql, columnCount);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
    {
        return delegate().getTableStatistics(session, handle);
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
    public Optional<String> getTableComment(ResultSet resultSet)
            throws SQLException
    {
        return delegate().getTableComment(resultSet);
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        delegate().setTableComment(session, handle, comment);
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        delegate().setColumnComment(session, handle, column, comment);
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        delegate().addColumn(session, handle, column);
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        delegate().dropColumn(session, handle, column);
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        delegate().renameColumn(session, handle, jdbcColumn, newColumnName);
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        delegate().setColumnType(session, handle, column, type);
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        delegate().dropNotNullConstraint(session, handle, column);
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        delegate().renameTable(session, handle, newTableName);
    }

    @Override
    public void setTableProperties(ConnectorSession session, JdbcTableHandle handle, Map<String, Optional<Object>> properties)
    {
        delegate().setTableProperties(session, handle, properties);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        delegate().createTable(session, tableMetadata);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        delegate().createSchema(session, schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        delegate().dropSchema(session, schemaName, cascade);
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        delegate().renameSchema(session, schemaName, newSchemaName);
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
        return delegate().getTableScanRedirection(session, tableHandle);
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        return delegate().delete(session, handle);
    }

    @Override
    public OptionalLong update(ConnectorSession session, JdbcTableHandle handle)
    {
        return delegate().update(session, handle);
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        delegate().truncateTable(session, handle);
    }

    @Override
    public OptionalInt getMaxWriteParallelism(ConnectorSession session)
    {
        return delegate().getMaxWriteParallelism(session);
    }

    @Override
    public OptionalInt getMaxColumnNameLength(ConnectorSession session)
    {
        return delegate().getMaxColumnNameLength(session);
    }

    @Override
    public List<JdbcColumnHandle> getPrimaryKeys(ConnectorSession session, RemoteTableName remoteTableName)
    {
        return delegate().getPrimaryKeys(session, remoteTableName);
    }
}
