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
package io.trino.plugin.hsqldb;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.TimestampTimeZoneDomain;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static io.trino.plugin.hsqldb.HsqlDbClient.DEFAULT_VARCHAR_LENGTH;
import static io.trino.plugin.hsqldb.HsqlDbClient.getTimePrecision;
import static io.trino.plugin.hsqldb.HsqlDbClient.getTimeWithTimeZonePrecision;
import static io.trino.plugin.hsqldb.HsqlDbConfig.HSQLDB_NO_COMMENT;
import static io.trino.plugin.hsqldb.HsqlDbHelper.fromRemoteIdentifier;
import static io.trino.plugin.hsqldb.HsqlDbHelper.getDefaultValue;
import static io.trino.plugin.hsqldb.HsqlDbHelper.toRemoteIdentifier;
import static io.trino.plugin.jdbc.BaseJdbcClient.varcharLiteral;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HsqlDbMetadata
        extends DefaultJdbcMetadata
{
    private final String viewOwnerPrivilege = "SELECT";
    private final JdbcClient hsqldbClient;

    public HsqlDbMetadata(JdbcClient hsqldbClient, TimestampTimeZoneDomain timestampTimeZoneDomain, Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        super(hsqldbClient, timestampTimeZoneDomain, false, jdbcQueryEventListeners);
        this.hsqldbClient = requireNonNull(hsqldbClient, "hsqldbClient is null");
    }

    @Override
    public void setDefaultValue(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle, String defaultValue)
    {
        JdbcTableHandle table = (JdbcTableHandle) tableHandle;
        JdbcColumnHandle column = (JdbcColumnHandle) columnHandle;
        try (Connection connection = getConnection(session)) {
            String sql = format("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s",
                    hsqldbClient.quoted(table.asPlainTable().getRemoteTableName()),
                    hsqldbClient.quoted(column.getColumnName()),
                    getDefaultValue(column.getColumnType(), defaultValue));
            executeUpdateStatement(connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void dropDefaultValue(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        JdbcTableHandle table = (JdbcTableHandle) tableHandle;
        JdbcColumnHandle column = (JdbcColumnHandle) columnHandle;
        try (Connection connection = getConnection(session)) {
            String sql = format("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT",
                    hsqldbClient.quoted(table.asPlainTable().getRemoteTableName()),
                    hsqldbClient.quoted(column.getColumnName()));
            executeUpdateStatement(connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        String[] types = {"VIEW"};
        ImmutableList.Builder<SchemaTableName> views = ImmutableList.builder();
        try (Connection connection = getConnection(session);
                ResultSet resultSet = connection.getMetaData().getTables(null, null, null, types)) {
            while (resultSet.next()) {
                views.add(getSchemaTableName(resultSet));
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
        return views.build();
    }

    private SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException
    {
        return new SchemaTableName(resultSet.getString("TABLE_SCHEM"),
                resultSet.getString("TABLE_NAME"));
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        Optional<ConnectorViewDefinition> viewDefinition;
        String sql = format("SELECT V.VIEW_DEFINITION, T.REMARKS, P.GRANTEE " +
                "FROM INFORMATION_SCHEMA.VIEWS V " +
                "INNER JOIN INFORMATION_SCHEMA.SYSTEM_TABLES T " +
                "ON V.TABLE_SCHEMA = T.TABLE_SCHEM AND V.TABLE_NAME = T.TABLE_NAME " +
                "LEFT JOIN INFORMATION_SCHEMA.TABLE_PRIVILEGES P " +
                "ON V.TABLE_SCHEMA = P.TABLE_SCHEMA AND V.TABLE_NAME = P.TABLE_NAME AND " +
                "P.PRIVILEGE_TYPE = '%s' AND P.GRANTEE IN (SELECT USER_NAME FROM INFORMATION_SCHEMA.SYSTEM_USERS) " +
                "WHERE V.TABLE_SCHEMA = ? AND V.TABLE_NAME = ?",
                viewOwnerPrivilege);

        try (Connection connection = getConnection(session);
                PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet resultSet = getViewDefinitionResultSet(statement, viewName)) {
            viewDefinition = getViewDefinition(connection, resultSet, viewName);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
        return viewDefinition;
    }

    private boolean isView(Connection connection, SchemaTableName viewName)
            throws SQLException
    {
        boolean isView;
        String sql = "SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";

        try (PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet resultSet = getViewDefinitionResultSet(statement, viewName)) {
            isView = resultSet.next();
        }
        return isView;
    }

    private ResultSet getViewDefinitionResultSet(PreparedStatement statement, SchemaTableName viewName)
            throws SQLException
    {
        statement.setString(1, toRemoteSchemaName(viewName));
        statement.setString(2, toRemoteTableName(viewName));
        return statement.executeQuery();
    }

    private Optional<ConnectorViewDefinition> getViewDefinition(Connection connection, ResultSet resultSet, SchemaTableName viewName)
            throws SQLException
    {
        if (resultSet.next()) {
            String definition = resultSet.getString("VIEW_DEFINITION");
            Optional<String> comment = getResultSetComment(resultSet);
            Optional<String> owner = Optional.ofNullable(resultSet.getString("GRANTEE"));
            return Optional.of(new ConnectorViewDefinition(
                    definition,
                    Optional.of("hsqldb"),
                    Optional.ofNullable(viewName.getSchemaName()),
                    getViewColumns(connection, viewName),
                    comment,
                    owner,
                    owner.isEmpty(),
                    ImmutableList.of()));
        }
        return Optional.empty();
    }

    private List<ConnectorViewDefinition.ViewColumn> getViewColumns(Connection connection, SchemaTableName viewName)
            throws SQLException
    {
        String schema = toRemoteIdentifier(viewName.getSchemaName());
        String view = toRemoteIdentifier(viewName.getTableName());
        ImmutableList.Builder<ConnectorViewDefinition.ViewColumn> viewColumns = ImmutableList.builder();
        try (ResultSet resultSet = connection.getMetaData().getColumns(null, schema, view, null)) {
            while (resultSet.next()) {
                viewColumns.add(getResultSetViewDefinition(resultSet));
            }
        }
        return viewColumns.build();
    }

    private ConnectorViewDefinition.ViewColumn getResultSetViewDefinition(ResultSet resultSet)
            throws SQLException
    {
        String columnName = fromRemoteIdentifier(resultSet.getString("COLUMN_NAME"));
        TypeId typeId = fromJdbcType(resultSet).getTypeId();
        Optional<String> comment = getResultSetComment(resultSet);
        return new ConnectorViewDefinition.ViewColumn(columnName, typeId, comment);
    }

    private Optional<String> getResultSetComment(ResultSet resultSet)
            throws SQLException
    {
        return Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        checkArgument(viewProperties.isEmpty(), "This connector does not support creating views with properties");
        checkSchemaExists(session, viewName.getSchemaName());

        try (Connection connection = getConnection(session)) {
            String schemaViewName = getQuotedSchemaTable(viewName);
            if (!replace || !isView(connection, viewName)) {
                createView(connection, schemaViewName, definition);
            }
            else {
                refreshView(connection, schemaViewName, definition);
            }
            Optional<String> comment = definition.getComment();
            if (comment.isPresent()) {
                setViewComment(connection, schemaViewName, comment);
            }
            if (!definition.isRunAsInvoker()) {
                String sql = format("GRANT %s ON TABLE %s TO %s",
                        viewOwnerPrivilege,
                        schemaViewName,
                        hsqldbClient.quoted(connection.getMetaData().getUserName()));
                executeUpdateStatement(connection, sql);
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private synchronized void checkSchemaExists(ConnectorSession session, String schemaName)
    {
        if (hsqldbClient.getSchemaNames(session).stream().noneMatch(schema -> schema.equals(schemaName))) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema %s not found", schemaName));
        }
    }

    @Override
    public boolean isView(ConnectorSession session, SchemaTableName viewName)
    {
        try (Connection connection = getConnection(session)) {
            return isView(connection, viewName);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        try (Connection connection = getConnection(session)) {
            String schemaViewName = getQuotedSchemaTable(viewName);
            setViewComment(connection, schemaViewName, comment);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private void createView(Connection connection, String schemaViewName, ConnectorViewDefinition definition)
            throws SQLException
    {
        String sql = format("CREATE VIEW IF NOT EXISTS %s AS %s",
                schemaViewName,
                definition.getOriginalSql());
        executeUpdateStatement(connection, sql);
    }

    private void setViewComment(Connection connection, String schemaViewName, Optional<String> comment)
            throws SQLException
    {
        String sql = format("COMMENT ON VIEW %s IS %s",
                schemaViewName,
                varcharLiteral(comment.orElse(HSQLDB_NO_COMMENT)));
        executeUpdateStatement(connection, sql);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        if (!Objects.equals(source.getSchemaName(), target.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming views across schemas");
        }

        try (Connection connection = getConnection(session);
                Statement statement = connection.createStatement()) {
            String sql = format("ALTER TABLE %s RENAME TO %s",
                    getQuotedSchemaTable(source),
                    getQuotedSchemaTable(target));
            statement.executeUpdate(sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void refreshView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition)
    {
        try (Connection connection = getConnection(session)) {
            String schemaViewName = getQuotedSchemaTable(viewName);
            refreshView(connection, schemaViewName, definition);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        try (Connection connection = getConnection(session)) {
            String sql = format(
                    "COMMENT ON COLUMN %s.%s IS %s",
                    getQuotedSchemaTable(viewName),
                    hsqldbClient.quoted(toRemoteIdentifier(columnName)),
                    varcharLiteral(comment.orElse(HSQLDB_NO_COMMENT)));
            executeUpdateStatement(connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private void refreshView(Connection connection, String schemaViewName, ConnectorViewDefinition definition)
            throws SQLException
    {
        String sql = format("ALTER VIEW %s AS %s",
                schemaViewName,
                definition.getOriginalSql());
        executeUpdateStatement(connection, sql);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        try (Connection connection = getConnection(session);
                Statement statement = connection.createStatement()) {
            String sql = format("DROP VIEW %s CASCADE",
                    getQuotedSchemaTable(viewName));
            statement.executeUpdate(sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private void executeUpdateStatement(Connection connection, String sql)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    private String getQuotedSchemaTable(SchemaTableName schemaTable)
    {
        String remoteSchema = hsqldbClient.quoted(toRemoteSchemaName(schemaTable));
        String remoteTable = hsqldbClient.quoted(toRemoteTableName(schemaTable));
        return remoteSchema + "." + remoteTable;
    }

    private String toRemoteSchemaName(SchemaTableName schemaTable)
    {
        return toRemoteIdentifier(schemaTable.getSchemaName());
    }

    private String toRemoteTableName(SchemaTableName schemaTable)
    {
        return toRemoteIdentifier(schemaTable.getTableName());
    }

    private Connection getConnection(ConnectorSession session)
            throws SQLException
    {
        Connection connection = hsqldbClient.getConnection(session);
        connection.setReadOnly(false);
        return connection;
    }

    private static Type fromJdbcType(ResultSet resultSet)
            throws SQLException
    {
        int type = resultSet.getInt("DATA_TYPE");
        int precision = resultSet.getInt("COLUMN_SIZE");
        int decimalDigits = resultSet.getInt("DECIMAL_DIGITS");
        return switch (type) {
            case Types.BOOLEAN -> BOOLEAN;
            case Types.TINYINT -> TINYINT;
            case Types.SMALLINT -> SMALLINT;
            case Types.INTEGER -> INTEGER;
            case Types.BIGINT -> BIGINT;

            case Types.DOUBLE -> DOUBLE;
            case Types.NUMERIC, Types.DECIMAL -> createDecimalType(min(precision, 38), max(decimalDigits, 0));

            case Types.CHAR -> createCharType(precision);
            case Types.VARCHAR, Types.LONGVARCHAR -> {
                // varchar columns get created as varchar(default_length) in HsqlDB
                if (precision == DEFAULT_VARCHAR_LENGTH) {
                    yield createUnboundedVarcharType();
                }
                yield createVarcharType(precision);
            }
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> VARBINARY;

            case Types.DATE -> DATE;
            case Types.TIME -> createTimeType(getTimePrecision(precision));
            case Types.TIME_WITH_TIMEZONE -> createTimeWithTimeZoneType(getTimeWithTimeZonePrecision(precision));

            default -> createUnboundedVarcharType();
        };
    }
}
