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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.mapping.RemoteIdentifiers;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcMergeTableHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.firebird.FirebirdTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;

public class FirebirdClient
        extends BaseJdbcClient
{
    private static final int MAX_SUPPORTED_DATE_TIME_PRECISION = 4;
    private static final List<String> AUTOINCREMENT_TYPES = ImmutableList.of("smallint", "int", "integer", "bigint");
    static final int VARCHAR_UNBOUNDED_LENGTH = 255;

    private final List<String> tableTypes;
    private final boolean supportsSchema;
    private final boolean includeSystemTables;
    private final String schemaName;

    @Inject
    public FirebirdClient(
            BaseJdbcConfig config,
            FirebirdConfig firebirdConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, false);

        ImmutableList.Builder<String> tableTypes = ImmutableList.builder();
        tableTypes.add("TABLE", "VIEW");
        if (firebirdConfig.isIncludeSystemTables()) {
            tableTypes.add("SYSTEM TABLE", "SYSTEM VIEW");
        }
        this.tableTypes = tableTypes.build();
        this.supportsSchema = firebirdConfig.getSupportsSchema();
        this.includeSystemTables = firebirdConfig.isIncludeSystemTables();
        this.schemaName = firebirdConfig.getSchemaName();
    }

    @Override
    public Connection getConnection(ConnectorSession session, boolean readOnly)
            throws SQLException
    {
        // FIXME: The method calls Connection.setReadOnly method, but Firebird
        // FIXME: does not support changing read-only status when autocommit is on
        // FIXME: and Trino seems to need a read-only connection for its tests.
        Connection connection = connectionFactory.openConnection(session);
        try {
            connection.setAutoCommit(false);
            connection.setReadOnly(readOnly);
            connection.setAutoCommit(true);
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    @Override
    public String getTableRemoteSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return supportsSchema ? resultSet.getString("TABLE_SCHEM") : schemaName;
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        return supportsSchema ? super.listSchemas(connection) : ImmutableList.of(schemaName);
    }

    @Override
    protected Optional<List<String>> getTableTypes()
    {
        return Optional.of(tableTypes);
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> remoteSchemaName, Optional<String> remoteTableName)
            throws SQLException
    {
        // this method is called by IdentifierMapping, so cannot use IdentifierMapping here as this would cause an endless loop
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> schemaName = supportsSchema ? remoteSchemaName : Optional.empty();
        return metadata.getTables(
                connection.getCatalog(),
                escapeObjectNameForMetadataQuery(schemaName, metadata.getSearchStringEscape()).orElse(null),
                escapeObjectNameForMetadataQuery(remoteTableName, metadata.getSearchStringEscape()).orElse(null),
                getTableTypes().map(types -> types.toArray(String[]::new)).orElse(null));
    }

    @Override
    protected Optional<String> getColumnDefaultValue(ResultSet resultSet, JdbcTypeHandle typeHandle)
            throws SQLException
    {
        return Optional.ofNullable(resultSet.getString("COLUMN_DEF"));
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        execute(session, getTableCommentSql(handle.asPlainTable().getRemoteTableName(), comment));
    }

    @Override
    protected boolean filterRemoteSchema(String schemaName)
    {
        if (!includeSystemTables) {
            return super.filterRemoteSchema(schemaName);
        }
        return true;
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column, ColumnPosition position)
    {
        verify(handle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s", handle);

        RemoteTableName remoteTableName = handle.asPlainTable().getRemoteTableName();
        OptionalInt columnPosition = switch (position) {
            case ColumnPosition.First _ -> OptionalInt.of(1);
            case ColumnPosition.After after -> getNextColumnPosition(session, remoteTableName, after);
            case ColumnPosition.Last _ -> OptionalInt.empty();
        };
        addColumn(session, remoteTableName, column, columnPosition);
    }

    private OptionalInt getNextColumnPosition(ConnectorSession session, RemoteTableName remoteTableName, ColumnPosition.After after)
    {
        String previousColumn = after.columnName();

        SchemaTableName schemaTableName = new SchemaTableName(getSchemaName(remoteTableName.getSchemaName()), remoteTableName.getTableName());
        List<String> columnNames = getColumns(session, schemaTableName, remoteTableName).stream()
                .map(handle -> handle.getColumnMetadata().getName())
                .toList();

        if (!columnNames.contains(previousColumn) || columnNames.getLast().equals(previousColumn)) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(columnNames.indexOf(previousColumn) + 2);
    }

    private void addColumn(ConnectorSession session, RemoteTableName table, ColumnMetadata column, OptionalInt position)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            addColumn(session, connection, table, column);
            if (position.isPresent()) {
                setColumnPosition(session, connection, table, column, position.orElseThrow());
            }
            if (column.getComment().isPresent()) {
                setColumnComment(session, connection, table, column, column.getComment());
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void addColumn(ConnectorSession session, Connection connection, RemoteTableName table, ColumnMetadata column)
            throws SQLException
    {
        String columnName = column.getName();
        verifyColumnName(connection.getMetaData(), columnName);
        String remoteColumnName = getIdentifierMapping().toRemoteColumnName(getRemoteIdentifiers(connection), columnName);
        execute(session, connection, format(
                "ALTER TABLE %s ADD %s",
                getTableName(table),
                getColumnDefinitionSql(session, column, remoteColumnName)));
    }

    private void setColumnPosition(ConnectorSession session, Connection connection, RemoteTableName table, ColumnMetadata column, int position)
            throws SQLException
    {
        String columnName = column.getName();
        verifyColumnName(connection.getMetaData(), columnName);
        String remoteColumnName = getIdentifierMapping().toRemoteColumnName(getRemoteIdentifiers(connection), columnName);
        execute(session, connection, format(
                "ALTER TABLE %s ALTER %s POSITION %s",
                getTableName(table),
                quoted(remoteColumnName),
                position));
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        execute(session, getColumnCommentSql(handle.asPlainTable().getRemoteTableName(), column.getColumnName(), comment));
    }

    private void setColumnComment(ConnectorSession session, Connection connection, RemoteTableName table, ColumnMetadata column, Optional<String> comment)
            throws SQLException
    {
        String columnName = column.getName();
        verifyColumnName(connection.getMetaData(), columnName);
        String remoteColumnName = getIdentifierMapping().toRemoteColumnName(getRemoteIdentifiers(connection), columnName);
        execute(session, connection, getColumnCommentSql(table, remoteColumnName, comment));
    }

    private String getColumnCommentSql(RemoteTableName table, String column, Optional<String> comment)
    {
        return format(
                "COMMENT ON COLUMN %s.%s IS %s",
                getTableName(table),
                quoted(column),
                comment.map(BaseJdbcClient::varcharLiteral).orElse("NULL"));
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            String remoteColumnName = getIdentifierMapping().toRemoteColumnName(getRemoteIdentifiers(connection), column.getColumnName());
            String sql = format(
                    "ALTER TABLE %s ALTER %s TYPE %s",
                    quoted(getRemoteTableName(handle.asPlainTable().getRemoteTableName())),
                    quoted(remoteColumnName),
                    toWriteMapping(session, type).getDataType());
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters)
    {
        boolean hasPageSinkIdColumn = handle.getPageSinkIdColumnName().isPresent();
        checkArgument(handle.getColumnNames().size() == columnWriters.size(), "handle and columnWriters mismatch: %s, %s", handle, columnWriters);
        return format(
                "INSERT INTO %s (%s%s) VALUES (%s%s)",
                quoted(
                        null,
                        getSchemaName(handle.getRemoteTableName().getSchemaName(), null),
                        handle.getTemporaryTableName().orElseGet(() -> handle.getRemoteTableName().getTableName())),
                handle.getColumnNames().stream()
                        .map(this::quoted)
                        .collect(joining(", ")),
                hasPageSinkIdColumn ? ", " + quoted(handle.getPageSinkIdColumnName().get()) : "",
                columnWriters.stream()
                        .map(WriteFunction::getBindExpression)
                        .collect(joining(",")),
                hasPageSinkIdColumn ? ", ?" : "");
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        if (!supportsSchema) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
        }
        super.createSchema(session, schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        if (!supportsSchema) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
        }
        super.dropSchema(session, schemaName, cascade);
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        if (!supportsSchema) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
        }
        super.renameSchema(session, schemaName, newSchemaName);
    }

    @Override
    protected void dropTable(ConnectorSession session, RemoteTableName remoteTableName, boolean temporaryTable)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            dropTable(session, connection, remoteTableName);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private void dropTable(ConnectorSession session, Connection connection, RemoteTableName remoteTableName)
            throws SQLException
    {
        execute(session, connection, "DROP TABLE " + getTableName(remoteTableName));
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalog, String schema, String table, String newSchema, String newTable)
    {
        if (schema == null) {
            schema = getIdentifierMapping().toRemoteSchemaName(getRemoteIdentifiers(connection), session.getIdentity(), schemaName);
        }

        if (!schema.equals(newSchema)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }

        SchemaTableName schemaTable = new SchemaTableName(schema, table);
        RemoteTableName remoteTable = getRemoteTableName(Optional.ofNullable(catalog), Optional.of(schema), table);
        RemoteTableName newRemoteTable = getRemoteTableName(Optional.ofNullable(catalog), Optional.of(newSchema), newTable);
        List<String> columnDefinition = getColumnDefinition(session, schemaTable, remoteTable);

        try {
            execute(session, connection, format(
                    "CREATE TABLE %s (%s)",
                    getTableName(newRemoteTable),
                    join(", ", columnDefinition)));

            Optional<String> comment = getTableComment(connection, remoteTable);
            if (comment.isPresent()) {
                execute(session, connection, getTableCommentSql(newRemoteTable, comment));
            }

            Set<String> primaryKeys = getPrimaryKeys(connection, remoteTable);
            if (!primaryKeys.isEmpty()) {
                execute(session, connection, getAddPrimaryKeySql(newRemoteTable, primaryKeys));
            }

            execute(session, connection, format(
                    "INSERT INTO %s SELECT * FROM %s",
                    getTableName(newRemoteTable),
                    getTableName(remoteTable)));

            dropTable(session, connection, remoteTable);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private Optional<String> getTableComment(Connection connection, RemoteTableName remoteTable)
            throws SQLException
    {
        Optional<String> comment;
        String schema = remoteTable.getSchemaName().orElse(null);
        try (ResultSet rs = connection.getMetaData().getTables(connection.getCatalog(), schema, remoteTable.getTableName(), null)) {
            comment = rs.next() ? getTableComment(rs) : Optional.empty();
        }
        return comment;
    }

    @Override
    protected void renameColumn(ConnectorSession session, Connection connection, RemoteTableName remoteTableName, String remoteColumnName, String newRemoteColumnName)
            throws SQLException
    {
        execute(session, connection, format(
                "ALTER TABLE %s ALTER %s TO %s",
                quoted(getRemoteTableName(remoteTableName)),
                quoted(remoteColumnName),
                quoted(newRemoteColumnName)));
    }

    private List<String> getColumnDefinition(ConnectorSession session, SchemaTableName schemaTableName, RemoteTableName remoteTableName)
    {
        return getColumnDefinition(session, schemaTableName, remoteTableName, ImmutableList.of());
    }

    private List<String> getColumnDefinition(ConnectorSession session, SchemaTableName schemaTableName, RemoteTableName remoteTableName, List<String> columns)
    {
        ImmutableList.Builder<String> columnDefinition = ImmutableList.builder();
        getColumns(session, schemaTableName, remoteTableName).stream()
                .filter(column -> columns.isEmpty() || columns.contains(column.getColumnName()))
                .forEach(column -> columnDefinition.add(getColumnDefinitionSql(session, column.getColumnMetadata(), column.getColumnName())));
        return columnDefinition.build();
    }

    @Override
    protected String getColumnDefinitionSql(ConnectorSession session, ColumnMetadata column, String columnName)
    {
        StringBuilder sb = new StringBuilder()
                .append(quoted(columnName))
                .append(" ")
                .append(toWriteMapping(session, column.getType()).getDataType());

        Optional<String> defaultValue = column.getDefaultValue();
        if (defaultValue.isPresent()) {
            sb.append(" DEFAULT ");
            sb.append(defaultValue.get());
        }

        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }

        if (FirebirdColumnProperties.isAutoIncrement(column.getProperties())) {
            checkAutoIncrementColumn(column.getType().getBaseName(), columnName);
            sb.append(" GENERATED BY DEFAULT AS IDENTITY");
        }

        return sb.toString();
    }

    private void checkAutoIncrementColumn(String columnType, String columnName)
    {
        checkArgument(AUTOINCREMENT_TYPES.contains(columnType), "Unsupported type %s for autoincrement properties on column %s", columnType, columnName);
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support truncating tables");
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        verify(handle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s", handle);
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            String remoteColumnName = getIdentifierMapping().toRemoteColumnName(getRemoteIdentifiers(connection), column.getColumnName());
            String sql = format(
                    "ALTER TABLE %s DROP %s",
                    quoted(getRemoteTableName(handle.asPlainTable().getRemoteTableName())),
                    quoted(remoteColumnName));
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        RemoteTableName remoteTableName = new RemoteTableName(Optional.empty(), getRemoteSchemaName(schemaName), tableName);
        List<String> columnDefinition = getColumnDefinition(session, schemaTableName, remoteTableName, columnNames);

        try {
            execute(session, connection, format(
                    "CREATE TABLE %s (%s)",
                    quoted(null, getSchemaName(schemaName), newTableName),
                    String.join(", ", columnDefinition)));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        return true;
    }

    @Override
    protected ResultSet getAllTableColumns(Connection connection, Optional<String> remoteSchemaName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getColumns(
                connection.getCatalog(),
                escapeObjectNameForMetadataQuery(remoteSchemaName, metadata.getSearchStringEscape()).orElse(null),
                null,
                null);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        return switch (typeHandle.jdbcType()) {
            case Types.BOOLEAN -> Optional.of(booleanColumnMapping());
            case Types.SMALLINT -> Optional.of(smallintColumnMapping());
            case Types.INTEGER -> Optional.of(integerColumnMapping());
            case Types.BIGINT -> Optional.of(bigintColumnMapping());
            case Types.DOUBLE, Types.FLOAT -> Optional.of(doubleColumnMapping());
            case Types.DECIMAL, Types.NUMERIC -> {
                int decimalDigits = typeHandle.requiredDecimalDigits();
                int columnSize = typeHandle.requiredColumnSize();
                yield Optional.of(decimalColumnMapping(createDecimalType(columnSize, decimalDigits)));
            }
            case Types.CHAR -> Optional.of(defaultCharColumnMapping(typeHandle.requiredColumnSize(), true));
            case Types.VARCHAR -> Optional.of(defaultVarcharColumnMapping(typeHandle.requiredColumnSize(), true));
            case Types.DATE -> Optional.of(dateColumnMapping());
            default -> {
                if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
                    yield mapToUnboundedVarchar(typeHandle);
                }
                yield Optional.empty();
            }
        };
    }

    private static ColumnMapping dateColumnMapping()
    {
        return ColumnMapping.longMapping(
                DATE,
                dateReadFunctionUsingSqlDate(),
                dateWriteFunctionUsingSqlDate());
    }

    private static LongReadFunction dateReadFunctionUsingSqlDate()
    {
        return (resultSet, columnIndex) -> {
            Date date = resultSet.getDate(columnIndex);
            return date.toLocalDate().toEpochDay();
        };
    }

    private static LongWriteFunction dateWriteFunctionUsingSqlDate()
    {
        return LongWriteFunction.of(Types.DATE, (statement, index, value) -> {
            LocalDate localDate = LocalDate.ofEpochDay(value);
            statement.setDate(index, Date.valueOf(localDate));
        });
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }

        if (type == TINYINT) {
            return WriteMapping.longMapping("smallint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }

        if (type == REAL) {
            return WriteMapping.longMapping("float", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }
        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        if (type instanceof CharType charType) {
            String dataType = format("char(%s)", charType.getLength());
            return WriteMapping.sliceMapping(dataType, charWriteFunction());
        }
        if (type instanceof VarcharType varcharType) {
            String dataType = format("varchar(%s)", varcharType.isUnbounded() ? VARCHAR_UNBOUNDED_LENGTH : varcharType.getBoundedLength());
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type == VARBINARY) {
            return WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction());
        }
        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
        }

        if (type instanceof TimeType timeType) {
            if (timeType.getPrecision() <= MAX_SUPPORTED_DATE_TIME_PRECISION) {
                return WriteMapping.longMapping(format("time(%s)", timeType.getPrecision()), timeWriteFunction(timeType.getPrecision()));
            }
            return WriteMapping.longMapping(format("time(%s)", MAX_SUPPORTED_DATE_TIME_PRECISION), timeWriteFunction(MAX_SUPPORTED_DATE_TIME_PRECISION));
        }
        if (type instanceof TimeWithTimeZoneType timeWithZoneType) {
            if (timeWithZoneType.getPrecision() <= MAX_SUPPORTED_DATE_TIME_PRECISION) {
                return WriteMapping.longMapping(format("time(%s) with time zone", timeWithZoneType.getPrecision()), timeWithTimeZoneWriteFunction());
            }
            return WriteMapping.longMapping(format("time(%s) with time zone", MAX_SUPPORTED_DATE_TIME_PRECISION), timeWithTimeZoneWriteFunction());
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> format("%s FETCH FIRST %s ROWS ONLY", sql, limit));
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream()
                    .map(sortItem -> {
                        String ordering = sortItem.sortOrder().isAscending() ? "ASC" : "DESC";
                        String nullsHandling = sortItem.sortOrder().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                        return format("%s %s %s", quoted(sortItem.column().getColumnName()), ordering, nullsHandling);
                    })
                    .collect(joining(", "));
            return format("%s ORDER BY %s FETCH FIRST %d ROWS ONLY", query, orderBy, limit);
        });
    }

    @Override
    public boolean supportsMerge()
    {
        return true;
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return true;
    }

    @Override
    protected JdbcOutputTableHandle createTable(
            ConnectorSession session,
            Connection connection,
            ConnectorTableMetadata tableMetadata,
            RemoteIdentifiers remoteIdentifiers,
            String catalog,
            String remoteSchema,
            String remoteTable,
            String remoteTargetTableName,
            Optional<ColumnMetadata> pageSinkIdColumn)
            throws SQLException
    {
        JdbcOutputTableHandle handle = super.createTable(session, connection, tableMetadata, remoteIdentifiers, catalog, remoteSchema, remoteTable, remoteTargetTableName, pageSinkIdColumn);
        RemoteTableName remoteTableName = new RemoteTableName(Optional.ofNullable(catalog), Optional.ofNullable(remoteSchema), remoteTargetTableName);
        if (tableMetadata.getComment().isPresent()) {
            execute(session, connection, getTableCommentSql(remoteTableName, tableMetadata.getComment()));
        }
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            if (column.getComment().isPresent()) {
                String remoteColumnName = getIdentifierMapping().toRemoteColumnName(getRemoteIdentifiers(connection), column.getName());
                execute(session, connection, getColumnCommentSql(remoteTableName, remoteColumnName, column.getComment()));
            }
        }
        return handle;
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columnNames, List<String> columnDefinitions, ConnectorTableMetadata tableMetadata)
    {
        Map<String, Object> tableProperties = tableMetadata.getProperties();

        ImmutableList.Builder<String> createTableBuilder = ImmutableList.builder();
        createTableBuilder.add(format(
                "CREATE TABLE %s (%s)",
                getTableName(remoteTableName),
                join(", ", columnDefinitions)));

        Optional<List<String>> primaryKeys = FirebirdTableProperties.getPrimaryKey(tableProperties);
        if (primaryKeys.isPresent() && !primaryKeys.get().isEmpty()) {
            verifyPrimaryKey(remoteTableName, primaryKeys.get(), tableMetadata.getColumns());
            for (String primaryKey : primaryKeys.get()) {
                createTableBuilder.add(format(
                        "ALTER TABLE %s ALTER %s SET NOT NULL",
                        getTableName(remoteTableName),
                        quoted(primaryKey)));
            }
            createTableBuilder.add(getAddPrimaryKeySql(remoteTableName, primaryKeys.get()));
        }

        if (tableMetadata.getComment().isPresent()) {
            createTableBuilder.add(getTableCommentSql(remoteTableName, tableMetadata.getComment()));
        }

        List<ColumnMetadata> columns = tableMetadata.getColumns();
        columns.stream()
                .filter(column -> column.getComment().isPresent())
                .map(column -> getColumnCommentSql(remoteTableName, columnNames.get(columns.indexOf(column)), column.getComment()))
                .forEach(createTableBuilder::add);

        return createTableBuilder.build();
    }

    private String getAddPrimaryKeySql(RemoteTableName table, Collection<String> primaryKeys)
    {
        return format(
                "ALTER TABLE %s ADD CONSTRAINT PK_%s PRIMARY KEY (%s)",
                getTableName(table),
                getPrimaryKeyName(table),
                primaryKeys.stream().map(this::quoted).collect(joining(", ")));
    }

    private String getTableCommentSql(RemoteTableName table, Optional<String> comment)
    {
        return format(
                "COMMENT ON TABLE %s IS %s",
                getTableName(table),
                comment.map(BaseJdbcClient::varcharLiteral).orElse("NULL"));
    }

    @Override
    public void finishMerge(ConnectorSession session, JdbcMergeTableHandle handle, Set<Long> pageSinkIds)
    {
        // When the connector retry mode is NO_RETRIES but isNonTransactionalInsert is false
        // the insert of merge still will first create temporary table
        finishInsertTable(session, handle.getOutputTableHandle(), pageSinkIds);
    }

    private void verifyPrimaryKey(RemoteTableName remoteTableName, List<String> primaryKeys, List<ColumnMetadata> columns)
    {
        Set<String> columnNames = columns.stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableSet());
        for (String primaryKey : primaryKeys) {
            if (!columnNames.contains(primaryKey.toLowerCase(ENGLISH))) {
                throw new TrinoException(INVALID_TABLE_PROPERTY,
                        format("Column '%s' specified in property '%s' doesn't exist in table '%s'",
                                primaryKey,
                                PRIMARY_KEY_PROPERTY,
                                remoteTableName.getTableName()));
            }
        }
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        List<String> primaryKeys = getPrimaryKeys(session, tableHandle.getRequiredNamedRelation().getRemoteTableName()).stream()
                .map(JdbcColumnHandle::getColumnName)
                .collect(toImmutableList());
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        if (!primaryKeys.isEmpty()) {
            properties.put(PRIMARY_KEY_PROPERTY, primaryKeys);
        }
        return properties.buildOrThrow();
    }

    @Override
    public List<JdbcColumnHandle> getPrimaryKeys(ConnectorSession session, RemoteTableName remoteTableName)
    {
        Set<String> primaryKeys;
        try (Connection connection = connectionFactory.openConnection(session)) {
            primaryKeys = getPrimaryKeys(connection, remoteTableName);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
        if (primaryKeys.isEmpty()) {
            return ImmutableList.of();
        }
        SchemaTableName schemaTableName = new SchemaTableName(getSchemaName(remoteTableName.getSchemaName()), remoteTableName.getTableName());
        return getColumns(session, schemaTableName, remoteTableName).stream()
                .filter(column -> primaryKeys.contains(column.getColumnName()))
                .collect(toImmutableList());
    }

    private Set<String> getPrimaryKeys(Connection connection, RemoteTableName remoteTableName)
            throws SQLException
    {
        Set<String> primaryKeys = new HashSet<>();
        String schema = getSchemaName(remoteTableName.getSchemaName(), null);
        try (ResultSet rs = connection.getMetaData().getPrimaryKeys(connection.getCatalog(), schema, remoteTableName.getTableName())) {
            while (rs.next()) {
                primaryKeys.add(rs.getString("COLUMN_NAME"));
            }
        }
        return primaryKeys;
    }

    public static LongWriteFunction timeWithTimeZoneWriteFunction()
    {
        return LongWriteFunction.of(Types.TIME_WITH_TIMEZONE, (statement, index, packedTime) -> {
            long nanosOfDay = unpackTimeNanos(packedTime);
            verify(nanosOfDay < NANOSECONDS_PER_DAY, "Invalid value of nanosOfDay: %s", nanosOfDay);
            ZoneOffset offset = ZoneOffset.ofTotalSeconds(unpackOffsetMinutes(packedTime) * 60);
            statement.setObject(index, OffsetTime.of(LocalTime.ofNanoOfDay(nanosOfDay), offset));
        });
    }

    @Override
    public RemoteTableName getRemoteTableName(RemoteTableName remoteTableName)
    {
        return getRemoteTableName(remoteTableName.getCatalogName(), remoteTableName.getSchemaName(), remoteTableName.getTableName());
    }

    @Override
    protected RemoteTableName getRemoteTableName(Optional<String> catalog, Optional<String> schema, String table)
    {
        return new RemoteTableName(Optional.empty(), getRemoteSchemaName(schema), table);
    }

    private String getTableName(RemoteTableName remoteTableName)
    {
        StringBuilder sb = new StringBuilder();
        if (supportsSchema && remoteTableName.getSchemaName().isPresent()) {
            sb.append(quoted(remoteTableName.getSchemaName().get()))
                    .append(".");
        }
        sb.append(quoted(remoteTableName.getTableName()));
        return sb.toString();
    }

    private String getPrimaryKeyName(RemoteTableName remoteTableName)
    {
        StringBuilder sb = new StringBuilder();
        if (supportsSchema && remoteTableName.getSchemaName().isPresent()) {
            sb.append(remoteTableName.getSchemaName().get()).append("_");
        }
        sb.append(remoteTableName.getTableName());
        return sb.toString();
    }

    private String getSchemaName(String schema)
    {
        return getSchemaName(Optional.of(schema), null);
    }

    private String getSchemaName(Optional<String> schema)
    {
        return getSchemaName(schema, schemaName);
    }

    private String getSchemaName(Optional<String> schema, String value)
    {
        if (supportsSchema && schema.isPresent()) {
            return schema.get();
        }
        return value;
    }

    private Optional<String> getRemoteSchemaName(String schema)
    {
        return getRemoteSchemaName(Optional.of(schema));
    }

    @Override
    public Optional<String> getRemoteSchemaName(Optional<String> schema)
    {
        if (supportsSchema) {
            return schema;
        }
        return Optional.empty();
    }
}
