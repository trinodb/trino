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
import io.trino.spi.TrinoException;
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
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.Type;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public interface JdbcClient
{
    default boolean schemaExists(ConnectorSession session, String schema)
    {
        return getSchemaNames(session).contains(schema);
    }

    Set<String> getSchemaNames(ConnectorSession session);

    List<SchemaTableName> getTableNames(ConnectorSession session, Optional<String> schema);

    Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, SchemaTableName schemaTableName);

    JdbcTableHandle getTableHandle(ConnectorSession session, PreparedQuery preparedQuery);

    JdbcProcedureHandle getProcedureHandle(ConnectorSession session, ProcedureQuery procedureQuery);

    List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle);

    Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle);

    /**
     * Bulk variant of {@link #toColumnMapping(ConnectorSession, Connection, JdbcTypeHandle)}.
     */
    List<ColumnMapping> toColumnMappings(ConnectorSession session, List<JdbcTypeHandle> typeHandles);

    WriteMapping toWriteMapping(ConnectorSession session, Type type);

    default boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        return true;
    }

    default Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return Optional.empty();
    }

    default Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return Optional.empty();
    }

    ConnectorSplitSource getSplits(ConnectorSession session, JdbcTableHandle tableHandle);

    ConnectorSplitSource getSplits(ConnectorSession session, JdbcProcedureHandle procedureHandle);

    Connection getConnection(ConnectorSession session, JdbcSplit split, JdbcTableHandle tableHandle)
            throws SQLException;

    Connection getConnection(ConnectorSession session, JdbcSplit split, JdbcProcedureHandle procedureHandle)
            throws SQLException;

    default void abortReadConnection(Connection connection, ResultSet resultSet)
            throws SQLException
    {
        // most drivers do not need this
    }

    PreparedQuery prepareQuery(
            ConnectorSession session,
            JdbcTableHandle table,
            Optional<List<List<JdbcColumnHandle>>> groupingSets,
            List<JdbcColumnHandle> columns,
            Map<String, ParameterizedExpression> columnExpressions);

    PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException;

    CallableStatement buildProcedure(ConnectorSession session, Connection connection, JdbcSplit split, JdbcProcedureHandle procedureHandle)
            throws SQLException;

    Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> joinConditions,
            Map<JdbcColumnHandle, String> rightAssignments,
            Map<JdbcColumnHandle, String> leftAssignments,
            JoinStatistics statistics);

    boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder);

    /**
     * Reports whether result cardinality and ordering is guaranteed when {@link #supportsTopN(ConnectorSession, JdbcTableHandle, List)} returns true.
     */
    boolean isTopNGuaranteed(ConnectorSession session);

    boolean supportsLimit();

    boolean isLimitGuaranteed(ConnectorSession session);

    default Optional<String> getTableComment(ResultSet resultSet)
            throws SQLException
    {
        return Optional.ofNullable(resultSet.getString("REMARKS"));
    }

    default void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting table comments");
    }

    default void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column comments");
    }

    void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column);

    void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column);

    void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName);

    void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type);

    void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName);

    default void setTableProperties(ConnectorSession session, JdbcTableHandle handle, Map<String, Optional<Object>> properties)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting table properties");
    }

    void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata);

    JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata);

    void commitCreateTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds);

    JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns);

    void finishInsertTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds);

    void dropTable(ConnectorSession session, JdbcTableHandle jdbcTableHandle);

    void rollbackCreateTable(ConnectorSession session, JdbcOutputTableHandle handle);

    boolean supportsRetries();

    String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters);

    Connection getConnection(ConnectorSession session, JdbcOutputTableHandle handle)
            throws SQLException;

    PreparedStatement getPreparedStatement(Connection connection, String sql, Optional<Integer> columnCount)
            throws SQLException;

    /**
     * @deprecated Use {@link #getTableStatistics(ConnectorSession, JdbcTableHandle)}
     */
    @Deprecated
    TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain);

    default TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
    {
        return getTableStatistics(session, handle, TupleDomain.all());
    }

    void createSchema(ConnectorSession session, String schemaName);

    void dropSchema(ConnectorSession session, String schemaName);

    void renameSchema(ConnectorSession session, String schemaName, String newSchemaName);

    default Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return Optional.empty();
    }

    String quoted(String name);

    String quoted(RemoteTableName remoteTableName);

    Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle);

    default Optional<TableScanRedirectApplicationResult> getTableScanRedirection(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return Optional.empty();
    }

    OptionalLong delete(ConnectorSession session, JdbcTableHandle handle);

    void truncateTable(ConnectorSession session, JdbcTableHandle handle);
}
