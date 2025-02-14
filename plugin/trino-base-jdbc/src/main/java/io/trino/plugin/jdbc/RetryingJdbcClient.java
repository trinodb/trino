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
import dev.failsafe.RetryPolicy;
import io.trino.plugin.jdbc.JdbcProcedureHandle.ProcedureQuery;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
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

import static io.trino.plugin.jdbc.RetryingModule.retry;
import static java.util.Objects.requireNonNull;

public class RetryingJdbcClient
        implements JdbcClient
{
    private final JdbcClient delegate;
    private final RetryPolicy<Object> policy;

    @Inject
    public RetryingJdbcClient(@ForRetrying JdbcClient delegate, RetryPolicy<Object> policy)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.policy = requireNonNull(policy, "policy is null");
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schema)
    {
        return retry(policy, () -> delegate.schemaExists(session, schema));
    }

    @Override
    public Set<String> getSchemaNames(ConnectorSession session)
    {
        return retry(policy, () -> delegate.getSchemaNames(session));
    }

    @Override
    public List<SchemaTableName> getTableNames(ConnectorSession session, Optional<String> schema)
    {
        return retry(policy, () -> delegate.getTableNames(session, schema));
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return retry(policy, () -> delegate.getTableHandle(session, schemaTableName));
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, PreparedQuery preparedQuery)
    {
        return retry(policy, () -> delegate.getTableHandle(session, preparedQuery));
    }

    @Override
    public JdbcProcedureHandle getProcedureHandle(ConnectorSession session, ProcedureQuery procedureQuery)
    {
        return retry(policy, () -> delegate.getProcedureHandle(session, procedureQuery));
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, SchemaTableName schemaTableName, RemoteTableName remoteTableName)
    {
        return retry(policy, () -> delegate.getColumns(session, schemaTableName, remoteTableName));
    }

    @Override
    public Iterator<RelationColumnsMetadata> getAllTableColumns(ConnectorSession session, Optional<String> schema)
    {
        return retry(policy, () -> delegate.getAllTableColumns(session, schema));
    }

    @Override
    public List<RelationCommentMetadata> getAllTableComments(ConnectorSession session, Optional<String> schema)
    {
        return retry(policy, () -> delegate.getAllTableComments(session, schema));
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        // no retrying as it could be not idempotent operation (connection could be not reusable after the first failure)
        return delegate.toColumnMapping(session, connection, typeHandle);
    }

    @Override
    public List<ColumnMapping> toColumnMappings(ConnectorSession session, List<JdbcTypeHandle> typeHandles)
    {
        return retry(policy, () -> delegate.toColumnMappings(session, typeHandles));
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        return retry(policy, () -> delegate.toWriteMapping(session, type));
    }

    @Override
    public Optional<Type> getSupportedType(ConnectorSession session, Type type)
    {
        // there should be no remote database interaction
        return delegate.getSupportedType(session, type);
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        // there should be no remote database interaction
        return delegate.supportsAggregationPushdown(session, table, aggregates, assignments, groupingSets);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // there should be no remote database interaction
        return delegate.implementAggregation(session, aggregate, assignments);
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        // there should be no remote database interaction
        return delegate.convertPredicate(session, expression, assignments);
    }

    @Override
    public Optional<JdbcExpression> convertProjection(ConnectorSession session, JdbcTableHandle handle, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        // there should be no remote database interaction
        return delegate.convertProjection(session, handle, expression, assignments);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return retry(policy, () -> delegate.getSplits(session, tableHandle));
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcProcedureHandle procedureHandle)
    {
        return retry(policy, () -> delegate.getSplits(session, procedureHandle));
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcSplit split, JdbcTableHandle tableHandle)
            throws SQLException
    {
        // retry already implemented by RetryingConnectionFactory
        return delegate.getConnection(session, split, tableHandle);
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcSplit split, JdbcProcedureHandle procedureHandle)
            throws SQLException
    {
        // retry already implemented by RetryingConnectionFactory
        return delegate.getConnection(session, split, procedureHandle);
    }

    @Override
    public void abortReadConnection(Connection connection, ResultSet resultSet)
            throws SQLException
    {
        // no retrying as it could be not idempotent operation (connection could be not reusable after the first failure)
        delegate.abortReadConnection(connection, resultSet);
    }

    @Override
    public PreparedQuery prepareQuery(ConnectorSession session, JdbcTableHandle table, Optional<List<List<JdbcColumnHandle>>> groupingSets, List<JdbcColumnHandle> columns, Map<String, ParameterizedExpression> columnExpressions)
    {
        // there should be no remote database interaction
        return delegate.prepareQuery(session, table, groupingSets, columns, columnExpressions);
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException
    {
        // no retrying as it could be not idempotent operation (connection could be not reusable after the first failure)
        return delegate.buildSql(session, connection, split, table, columns);
    }

    @Override
    public CallableStatement buildProcedure(ConnectorSession session, Connection connection, JdbcSplit split, JdbcProcedureHandle procedureHandle)
            throws SQLException
    {
        // no retrying as it could be not idempotent operation (connection could be not reusable after the first failure)
        return delegate.buildProcedure(session, connection, split, procedureHandle);
    }

    @Override
    public Optional<PreparedQuery> implementJoin(ConnectorSession session, JoinType joinType, PreparedQuery leftSource, Map<JdbcColumnHandle, String> leftProjections, PreparedQuery rightSource, Map<JdbcColumnHandle, String> rightProjections, List<ParameterizedExpression> joinConditions, JoinStatistics statistics)
    {
        // there should be no remote database interaction
        return delegate.implementJoin(session, joinType, leftSource, leftProjections, rightSource, rightProjections, joinConditions, statistics);
    }

    @Override
    public Optional<PreparedQuery> legacyImplementJoin(ConnectorSession session, JoinType joinType, PreparedQuery leftSource, PreparedQuery rightSource, List<JdbcJoinCondition> joinConditions, Map<JdbcColumnHandle, String> rightAssignments, Map<JdbcColumnHandle, String> leftAssignments, JoinStatistics statistics)
    {
        // there should be no remote database interaction
        return delegate.legacyImplementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics);
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        // there should be no remote database interaction
        return delegate.supportsTopN(session, handle, sortOrder);
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        // there should be no remote database interaction
        return delegate.isTopNGuaranteed(session);
    }

    @Override
    public boolean supportsLimit()
    {
        // there should be no remote database interaction
        return delegate.supportsLimit();
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        // there should be no remote database interaction
        return delegate.isLimitGuaranteed(session);
    }

    @Override
    public boolean supportsMerge()
    {
        // there should be no remote database interaction
        return delegate.supportsMerge();
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
            throws SQLException
    {
        // no retrying as it could be not idempotent operation
        return delegate.getTableComment(resultSet);
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        retry(policy, () -> delegate.setTableComment(session, handle, comment));
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        // no retrying as it could be not idempotent operation
        retry(policy, () -> delegate.setColumnComment(session, handle, column, comment));
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column, ColumnPosition position)
    {
        // no retrying as it could be not idempotent operation
        delegate.addColumn(session, handle, column, position);
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        // no retrying as it could be not idempotent operation
        delegate.dropColumn(session, handle, column);
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        // no retrying as it could be not idempotent operation
        delegate.renameColumn(session, handle, jdbcColumn, newColumnName);
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        // no retrying as it could be not idempotent operation
        delegate.setColumnType(session, handle, column, type);
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        // safe to retry, but retry needs to handle exceptions in case the constraint is already dropped
        delegate.dropNotNullConstraint(session, handle, column);
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        // no retrying as it could be not idempotent operation
        delegate.renameTable(session, handle, newTableName);
    }

    @Override
    public void setTableProperties(ConnectorSession session, JdbcTableHandle handle, Map<String, Optional<Object>> properties)
    {
        // no retrying as it could be not idempotent operation
        delegate.setTableProperties(session, handle, properties);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        // no retrying as it could be not idempotent operation
        delegate.createTable(session, tableMetadata);
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        // no retrying as it could be not idempotent operation
        return delegate.beginCreateTable(session, tableMetadata);
    }

    @Override
    public void commitCreateTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        // no retrying as it could be not idempotent operation
        delegate.commitCreateTable(session, handle, pageSinkIds);
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        // no retrying as it could be not idempotent operation
        return delegate.beginInsertTable(session, tableHandle, columns);
    }

    @Override
    public void finishInsertTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        // no retrying as it could be not idempotent operation
        delegate.finishInsertTable(session, handle, pageSinkIds);
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle jdbcTableHandle)
    {
        // no retrying as it could be not idempotent operation
        delegate.dropTable(session, jdbcTableHandle);
    }

    @Override
    public void rollbackCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        // no retrying as it could be not idempotent operation
        delegate.rollbackCreateTable(session, handle);
    }

    @Override
    public boolean supportsRetries()
    {
        // there should be no remote database interaction
        return delegate.supportsRetries();
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters)
    {
        // there should be no remote database interaction
        return delegate.buildInsertSql(handle, columnWriters);
    }

    @Override
    public Connection getConnection(ConnectorSession session)
            throws SQLException
    {
        // retry already implemented by RetryingConnectionFactory
        return delegate.getConnection(session);
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcOutputTableHandle handle)
            throws SQLException
    {
        // retry already implemented by RetryingConnectionFactory
        return delegate.getConnection(session, handle);
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql, Optional<Integer> columnCount)
            throws SQLException
    {
        // no retrying as it could be not idempotent operation (connection could be not reusable after the first failure)
        return delegate.getPreparedStatement(connection, sql, columnCount);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
    {
        return retry(policy, () -> delegate.getTableStatistics(session, handle));
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        // no retrying as it could be not idempotent operation
        delegate.createSchema(session, schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        // no retrying as it could be not idempotent operation
        delegate.dropSchema(session, schemaName, cascade);
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        // no retrying as it could be not idempotent operation
        delegate.renameSchema(session, schemaName, newSchemaName);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        // there should be no remote database interaction
        return delegate.getSystemTable(session, tableName);
    }

    @Override
    public String quoted(String name)
    {
        // there should be no remote database interaction
        return delegate.quoted(name);
    }

    @Override
    public String quoted(RemoteTableName remoteTableName)
    {
        // there should be no remote database interaction
        return delegate.quoted(remoteTableName);
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return retry(policy, () -> delegate.getTableProperties(session, tableHandle));
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> getTableScanRedirection(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return retry(policy, () -> delegate.getTableScanRedirection(session, tableHandle));
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        // no retrying as it could be not idempotent operation
        return delegate.delete(session, handle);
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        // no retrying as it could be not idempotent operation
        delegate.truncateTable(session, handle);
    }

    @Override
    public OptionalLong update(ConnectorSession session, JdbcTableHandle handle)
    {
        // no retrying as it could be not idempotent operation
        return delegate.update(session, handle);
    }

    @Override
    public OptionalInt getMaxWriteParallelism(ConnectorSession session)
    {
        return retry(policy, () -> delegate.getMaxWriteParallelism(session));
    }

    @Override
    public OptionalInt getMaxColumnNameLength(ConnectorSession session)
    {
        return retry(policy, () -> delegate.getMaxColumnNameLength(session));
    }

    @Override
    public List<JdbcColumnHandle> getPrimaryKeys(ConnectorSession session, RemoteTableName remoteTableName)
    {
        return retry(policy, () -> delegate.getPrimaryKeys(session, remoteTableName));
    }
}
