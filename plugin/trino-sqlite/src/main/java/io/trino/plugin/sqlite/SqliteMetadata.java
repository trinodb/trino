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
package io.trino.plugin.sqlite;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.TimestampTimeZoneDomain;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
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
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.sqlite.SqliteClient.MAIN_SCHEMA;
import static io.trino.plugin.sqlite.SqliteClient.SCHEMAS;
import static io.trino.plugin.sqlite.SqliteHelper.fromRemoteIdentifier;
import static io.trino.plugin.sqlite.SqliteHelper.fromRemoteSchemaName;
import static io.trino.plugin.sqlite.SqliteHelper.getSqliteDataType;
import static io.trino.plugin.sqlite.SqliteHelper.toRemoteIdentifier;
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
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SqliteMetadata
        extends DefaultJdbcMetadata
{
    private final JdbcClient sqliteClient;
    private final boolean useTypeAffinity;
    private final Map<String, String> customDataTypes;

    public SqliteMetadata(JdbcClient sqliteClient, SqliteConfig sqliteConfig, TimestampTimeZoneDomain timestampTimeZoneDomain, Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        super(sqliteClient, timestampTimeZoneDomain, false, jdbcQueryEventListeners);
        requireNonNull(sqliteConfig, "sqliteConfig is null");
        this.useTypeAffinity = sqliteConfig.getUseTypeAffinity();
        this.customDataTypes = sqliteConfig.getCustomDataTypes();
        this.sqliteClient = requireNonNull(sqliteClient, "sqliteClient is null");
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        String[] types = {"VIEW"};
        ImmutableList.Builder<SchemaTableName> views = ImmutableList.builder();
        try (Connection connection = getConnection(session);
                ResultSet resultSet = connection.getMetaData().getTables(null, null, null, types)) {
            while (resultSet.next()) {
                SchemaTableName schemaTableName = getSchemaTableName(resultSet);
                System.out.println("SqliteMetadata.listViews() schemaTableName: " + schemaTableName);
                views.add(schemaTableName);
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
        return new SchemaTableName(fromRemoteSchemaName(resultSet, MAIN_SCHEMA),
                resultSet.getString("TABLE_NAME"));
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        Optional<ConnectorViewDefinition> viewDefinition;
        String sql = "SELECT SUBSTR(sql, (INSTR(sql, ' AS SELECT') + 4)) sql FROM sqlite_schema WHERE type = 'view' AND name = ?";

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
        String sql = "SELECT sql FROM sqlite_schema WHERE type = 'view' AND name = ?";

        try (PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet resultSet = getViewDefinitionResultSet(statement, viewName)) {
            isView = resultSet.next();
        }
        return isView;
    }

    private ResultSet getViewDefinitionResultSet(PreparedStatement statement, SchemaTableName viewName)
            throws SQLException
    {
        statement.setString(1, toRemoteTableName(viewName));
        return statement.executeQuery();
    }

    private Optional<ConnectorViewDefinition> getViewDefinition(Connection connection, ResultSet resultSet, SchemaTableName viewName)
            throws SQLException
    {
        if (resultSet.next()) {
            String definition = resultSet.getString("sql");
            System.out.println("SqliteMetadata.getViewDefinition() definition: " + definition);
            return Optional.of(new ConnectorViewDefinition(
                    definition,
                    Optional.of("sqlite"),
                    Optional.ofNullable(viewName.getSchemaName()),
                    getViewColumns(connection, viewName),
                    Optional.empty(),
                    Optional.empty(),
                    true,
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
        return new ConnectorViewDefinition.ViewColumn(columnName, typeId, Optional.empty());
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        checkArgument(viewProperties.isEmpty(), "This connector does not support creating views with properties");
        checkSchemaExists(viewName.getSchemaName());

        try (Connection connection = getConnection(session)) {
            String schemaViewName = getQuotedSchemaTable(viewName);
            if (replace) {
                dropView(connection, schemaViewName);
            }
            createView(connection, schemaViewName, definition);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private synchronized void checkSchemaExists(String schemaName)
    {
        if (!SCHEMAS.contains(schemaName)) {
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

    private void createView(Connection connection, String schemaViewName, ConnectorViewDefinition definition)
            throws SQLException
    {
        String sql = format("CREATE VIEW IF NOT EXISTS %s AS %s",
                schemaViewName,
                definition.getOriginalSql());
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
                    sqliteClient.quoted(toRemoteTableName(target)));
            statement.executeUpdate(sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        try (Connection connection = getConnection(session)) {
            dropView(connection, getQuotedSchemaTable(viewName));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private void dropView(Connection connection, String schemaViewName)
            throws SQLException
    {
        String sql = "DROP VIEW IF EXISTS " + schemaViewName;
        executeUpdateStatement(connection, sql);
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
        String remoteSchema = sqliteClient.quoted(toRemoteSchemaName(schemaTable));
        String remoteTable = sqliteClient.quoted(toRemoteTableName(schemaTable));
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
        return sqliteClient.getConnection(session);
    }

    private Type fromJdbcType(ResultSet resultSet)
            throws SQLException
    {
        Optional<String> typeName = Optional.ofNullable(resultSet.getString("TYPE_NAME"));
        int dataType = getSqliteDataType(customDataTypes, useTypeAffinity, resultSet, typeName);
        int precision = resultSet.getInt("COLUMN_SIZE");
        int decimalDigits = resultSet.getInt("DECIMAL_DIGITS");
        return switch (dataType) {
            case Types.BOOLEAN -> BOOLEAN;
            case Types.TINYINT -> TINYINT;
            case Types.SMALLINT -> SMALLINT;
            case Types.INTEGER -> INTEGER;
            case Types.BIGINT -> BIGINT;

            case Types.DOUBLE -> DOUBLE;
            case Types.NUMERIC, Types.DECIMAL -> createDecimalType(min(precision, 38), max(decimalDigits, 0));

            case Types.CHAR -> createCharType(precision);
            case Types.VARCHAR, Types.LONGVARCHAR -> createVarcharType(precision);
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> VARBINARY;

            case Types.DATE -> DATE;

            default -> createUnboundedVarcharType();
        };
    }
}
