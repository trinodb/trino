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
package io.trino.plugin.firebird;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.mapping.RemoteIdentifiers;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.TimestampTimeZoneDomain;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.firebird.FirebirdClient.VARCHAR_UNBOUNDED_LENGTH;
import static io.trino.plugin.jdbc.BaseJdbcClient.varcharLiteral;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;

public class FirebirdMetadata
        extends DefaultJdbcMetadata
{
    public FirebirdMetadata(JdbcClient jdbcClient, TimestampTimeZoneDomain timestampTimeZoneDomain, Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        super(jdbcClient, timestampTimeZoneDomain, true, jdbcQueryEventListeners);
    }

    @Override
    public void setDefaultValue(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle, String defaultValue)
    {
        JdbcTableHandle table = (JdbcTableHandle) tableHandle;
        JdbcColumnHandle column = (JdbcColumnHandle) columnHandle;
        jdbcClient.execute(session, format(
                "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s",
                jdbcClient.quoted(jdbcClient.getRemoteTableName(table.asPlainTable().getRemoteTableName())),
                jdbcClient.quoted(column.getColumnName()),
                getDefaultValue(column.getColumnType(), defaultValue)));
    }

    @Override
    public void dropDefaultValue(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        JdbcTableHandle table = (JdbcTableHandle) tableHandle;
        JdbcColumnHandle column = (JdbcColumnHandle) columnHandle;
        jdbcClient.execute(session, format(
                "ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT",
                jdbcClient.quoted(jdbcClient.getRemoteTableName(table.asPlainTable().getRemoteTableName())),
                jdbcClient.quoted(column.getColumnName())));
    }

    private String getDefaultValue(Type columnType, String defaultValue)
    {
        return switch (columnType.getBaseName()) {
            case "varchar", "char" -> varcharLiteral(defaultValue);
            default -> defaultValue;
        };
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schema)
    {
        return listViews(session, schema, Optional.empty());
    }

    private List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName, Optional<String> viewName)
    {
        String[] types = {"VIEW"};
        ConnectorIdentity identity = session.getIdentity();
        ImmutableList.Builder<SchemaTableName> views = ImmutableList.builder();
        try (Connection connection = jdbcClient.getConnection(session)) {
            IdentifierMapping identifierMapping = jdbcClient.getIdentifierMapping();
            RemoteIdentifiers remoteIdentifiers = jdbcClient.getRemoteIdentifiers(connection);
            Optional<String> remoteSchema = jdbcClient.getRemoteSchemaName(schemaName
                    .map(schema -> identifierMapping.toRemoteSchemaName(remoteIdentifiers, identity, schema)));
            Optional<String> remoteView = remoteSchema.flatMap(schema -> viewName
                    .map(view -> identifierMapping.toRemoteTableName(remoteIdentifiers, identity, schema, view)));
            try (ResultSet resultSet = connection.getMetaData().getTables(null, remoteSchema.orElse(null), remoteView.orElse(null), types)) {
                while (resultSet.next()) {
                    views.add(getSchemaTableName(identifierMapping, resultSet));
                }
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
        return views.build();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        try (Connection connection = jdbcClient.getConnection(session)) {
            Optional<ConnectorViewDefinition> viewDefinition;
            RemoteTableName remoteTable = getRemoteTableName(session, connection, viewName);
            String sql = format(
                    "SELECT RDB$VIEW_SOURCE, RDB$DESCRIPTION FROM RDB$RELATIONS WHERE RDB$VIEW_SOURCE IS NOT NULL AND RDB$RELATION_NAME = ?%s",
                    remoteTable.getSchemaName().map(_ -> " AND RDB$SCHEMA_NAME = ?").orElse(""));

            int columnCount = remoteTable.getSchemaName().map(_ -> 2).orElse(1);
            try (PreparedStatement statement = jdbcClient.getPreparedStatement(connection, sql, Optional.of(columnCount));
                    ResultSet rs = getViewDefinitionResultSet(statement, remoteTable)) {
                viewDefinition = rs.next() ? getViewDefinition(connection, rs, viewName, remoteTable) : Optional.empty();
            }
            return viewDefinition;
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private RemoteTableName getRemoteTableName(ConnectorSession session, Connection connection, SchemaTableName viewName)
    {
        ConnectorIdentity identity = session.getIdentity();
        IdentifierMapping identifierMapping = jdbcClient.getIdentifierMapping();
        RemoteIdentifiers remoteIdentifiers = jdbcClient.getRemoteIdentifiers(connection);
        String remoteSchema = identifierMapping.toRemoteSchemaName(remoteIdentifiers, identity, viewName.getSchemaName());
        String remoteView = identifierMapping.toRemoteTableName(remoteIdentifiers, identity, remoteSchema, viewName.getTableName());
        return new RemoteTableName(Optional.empty(), jdbcClient.getRemoteSchemaName(Optional.of(remoteSchema)), remoteView);
    }

    private boolean isView(ConnectorSession session, Connection connection, SchemaTableName viewName)
            throws SQLException
    {
        boolean isView = false;
        RemoteTableName remoteTable = getRemoteTableName(session, connection, viewName);
        String sql = format(
                "SELECT 1 FROM RDB$RELATIONS WHERE RDB$VIEW_SOURCE IS NOT NULL AND RDB$RELATION_NAME = ?%s",
                remoteTable.getSchemaName().map(_ -> " AND RDB$SCHEMA_NAME = ?").orElse(""));

        int columnCount = remoteTable.getSchemaName().map(_ -> 2).orElse(1);
        try (PreparedStatement statement = jdbcClient.getPreparedStatement(connection, sql, Optional.of(columnCount));
                ResultSet resultSet = getViewDefinitionResultSet(statement, remoteTable)) {
            isView = resultSet.next();
        }
        catch (SQLException _) {
            // FIXME: If the table name is too long, it will generate an unexpected error. It might be a good idea to issue a warning.
        }
        return isView;
    }

    private ResultSet getViewDefinitionResultSet(PreparedStatement statement, RemoteTableName remoteTable)
            throws SQLException
    {
        statement.setString(1, remoteTable.getTableName());
        if (remoteTable.getSchemaName().isPresent()) {
            statement.setString(2, remoteTable.getSchemaName().get());
        }
        return statement.executeQuery();
    }

    private Optional<ConnectorViewDefinition> getViewDefinition(Connection connection, ResultSet resultSet, SchemaTableName viewName, RemoteTableName remoteTable)
            throws SQLException
    {
        Optional<String> definition = getViewDefinition(resultSet);
        if (definition.isPresent()) {
            return Optional.of(new ConnectorViewDefinition(
                    definition.get(),
                    Optional.of("firebird"),
                    Optional.ofNullable(viewName.getSchemaName()),
                    getViewColumns(connection, remoteTable),
                    getViewComment(resultSet),
                    Optional.empty(),
                    true,
                    ImmutableList.of()));
        }
        return Optional.empty();
    }

    private Optional<String> getViewDefinition(ResultSet resultSet)
            throws SQLException
    {
        return Optional.ofNullable(resultSet.getString("RDB$VIEW_SOURCE"));
    }

    private Optional<String> getViewComment(ResultSet resultSet)
            throws SQLException
    {
        return Optional.ofNullable(resultSet.getString("RDB$DESCRIPTION"));
    }

    private List<ConnectorViewDefinition.ViewColumn> getViewColumns(Connection connection, RemoteTableName view)
            throws SQLException
    {
        ImmutableList.Builder<ConnectorViewDefinition.ViewColumn> viewColumns = ImmutableList.builder();
        try (ResultSet resultSet = connection.getMetaData().getColumns(null, view.getSchemaName().orElse(null), view.getTableName(), null)) {
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                TypeId typeId = fromJdbcType(resultSet).getTypeId();
                Optional<String> comment = Optional.ofNullable(resultSet.getString("REMARKS"));
                viewColumns.add(new ConnectorViewDefinition.ViewColumn(columnName, typeId, comment));
            }
        }
        return viewColumns.build();
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        checkArgument(viewProperties.isEmpty(), "This connector does not support creating views with properties");
        checkSchemaExists(session, viewName.getSchemaName());

        try (Connection connection = jdbcClient.getConnection(session, false)) {
            boolean isView = isView(session, connection, viewName);
            if (isView && !replace) {
                throw new TrinoException(ALREADY_EXISTS, "View already exists: " + viewName);
            }

            String view = jdbcClient.quoted(getRemoteTableName(session, connection, viewName));
            ImmutableList.Builder<String> createViewBuilder = ImmutableList.builder();

            if (isView) {
                createViewBuilder.add(format(
                        "RECREATE VIEW %s AS %s",
                        view,
                        definition.getOriginalSql()));
            }
            else {
                createViewBuilder.add(format(
                        "CREATE VIEW %s AS %s",
                        view,
                        definition.getOriginalSql()));
            }

            Optional<String> comment = definition.getComment();
            if (comment.isPresent()) {
                createViewBuilder.add(viewCommentSql(view, comment));
            }

            for (String query : createViewBuilder.build()) {
                jdbcClient.execute(session, connection, query);
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private synchronized void checkSchemaExists(ConnectorSession session, String schemaName)
    {
        if (jdbcClient.getSchemaNames(session).stream().noneMatch(schema -> schema.equals(schemaName))) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema %s not found", schemaName));
        }
    }

    @Override
    public boolean isView(ConnectorSession session, SchemaTableName viewName)
    {
        try (Connection connection = jdbcClient.getConnection(session)) {
            return isView(session, connection, viewName);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        try (Connection connection = jdbcClient.getConnection(session, false)) {
            String view = jdbcClient.quoted(getRemoteTableName(session, connection, viewName));
            jdbcClient.execute(session, connection, viewCommentSql(view, comment));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        try (Connection connection = jdbcClient.getConnection(session, false)) {
            String view = jdbcClient.quoted(getRemoteTableName(session, connection, viewName));
            IdentifierMapping identifierMapping = jdbcClient.getIdentifierMapping();
            RemoteIdentifiers remoteIdentifiers = jdbcClient.getRemoteIdentifiers(connection);
            String column = jdbcClient.quoted(identifierMapping.toRemoteColumnName(remoteIdentifiers, columnName));
            jdbcClient.execute(session, connection, format(
                    "COMMENT ON COLUMN %s.%s IS %s",
                    view,
                    column,
                    comment.map(BaseJdbcClient::varcharLiteral).orElse("NULL")));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        try (Connection connection = jdbcClient.getConnection(session, false)) {
            String view = jdbcClient.quoted(getRemoteTableName(session, connection, viewName));
            jdbcClient.execute(session, connection, format(
                    "DROP VIEW %s",
                    view));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private SchemaTableName getSchemaTableName(IdentifierMapping identifierMapping, ResultSet resultSet)
            throws SQLException
    {
        String schema = identifierMapping.fromRemoteSchemaName(jdbcClient.getTableRemoteSchemaName(resultSet));
        String view = identifierMapping.fromRemoteTableName(schema, resultSet.getString("TABLE_NAME"));
        return new SchemaTableName(schema, view);
    }

    private String viewCommentSql(String view, Optional<String> comment)
    {
        return format(
                "COMMENT ON VIEW %s IS %s",
                view,
                comment.map(BaseJdbcClient::varcharLiteral).orElse("NULL"));
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
                // varchar columns get created as varchar(default_length) in Firebird
                if (precision == VARCHAR_UNBOUNDED_LENGTH) {
                    yield createUnboundedVarcharType();
                }
                yield createVarcharType(precision);
            }
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> VARBINARY;

            case Types.DATE -> DATE;

            default -> createUnboundedVarcharType();
        };
    }
}
