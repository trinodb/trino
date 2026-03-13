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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcMergeTableHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Types;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hsqldb.HsqlDbConfig.HSQLDB_NO_COMMENT;
import static io.trino.plugin.hsqldb.HsqlDbHelper.getDefaultValue;
import static io.trino.plugin.hsqldb.HsqlDbHelper.toRemoteIdentifier;
import static io.trino.plugin.hsqldb.HsqlDbTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.isNonTransactionalMerge;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingLocalDate;
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
import static io.trino.plugin.jdbc.StandardColumnMappings.timeColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;

public class HsqlDbClient
        extends BaseJdbcClient
{
    private static final int MAX_SUPPORTED_DATE_TIME_PRECISION = 9;

    // HsqlDB driver returns width of time types instead of precision.
    private static final int ZERO_PRECISION_TIME_COLUMN_SIZE = 8;
    private static final int ZERO_PRECISION_TIME_WITH_TIME_ZONE_COLUMN_SIZE = 14;

    private static final List<String> AUTOINCREMENT_TYPES = ImmutableList.of("tinyint", "smallint", "int", "integer", "bigint");
    static final int DEFAULT_VARCHAR_LENGTH = 2_000_000_000;

    private final List<String> tableTypes;
    private final HsqlDbTableType defaultTableType;
    private final boolean includeSystemTables;

    @Inject
    public HsqlDbClient(
            BaseJdbcConfig config,
            HsqlDbConfig hsqldbConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier remoteQueryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, remoteQueryModifier, false);
        ImmutableList.Builder<String> tableTypes = ImmutableList.builder();
        tableTypes.add("TABLE", "VIEW");
        if (hsqldbConfig.isIncludeSystemTables()) {
            tableTypes.add("SYSTEM TABLE", "SYSTEM VIEW");
        }
        this.tableTypes = tableTypes.build();
        this.defaultTableType = hsqldbConfig.getDefaultTableType();
        this.includeSystemTables = hsqldbConfig.isIncludeSystemTables();
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
    protected Optional<List<String>> getTableTypes()
    {
        return Optional.of(tableTypes);
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
            throws SQLException
    {
        return Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        String sql = format(
                "COMMENT ON COLUMN %s.%s IS %s",
                quoted(handle.asPlainTable().getRemoteTableName()),
                quoted(column.getColumnName()),
                varcharLiteral(comment.orElse(HSQLDB_NO_COMMENT)));
        execute(session, sql);
    }

    @Override
    protected Optional<String> getColumnDefaultValue(ResultSet resultSet)
            throws SQLException
    {
        String defaultValue = emptyToNull(resultSet.getString("COLUMN_DEF"));
        if (defaultValue != null && defaultValue.startsWith("'") && defaultValue.endsWith("'")) {
            defaultValue = defaultValue.substring(1, defaultValue.length() - 2).replace("''", "'");
        }
        return Optional.ofNullable(defaultValue);
    }

    @Override
    protected ResultSet getAllTableColumns(Connection connection, Optional<String> remoteSchemaName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getColumns(
                metadata.getConnection().getCatalog(),
                escapeObjectNameForMetadataQuery(remoteSchemaName, metadata.getSearchStringEscape()).orElse(null),
                null,
                null);
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        System.out.println("HsqlDbClient.setTableComment() 1");
        execute(session, buildTableCommentSql(handle.asPlainTable().getRemoteTableName(), comment));
    }

    private String buildTableCommentSql(RemoteTableName remoteTableName, Optional<String> comment)
    {
        return format(
                "COMMENT ON TABLE %s IS %s",
                quoted(remoteTableName),
                varcharLiteral(comment.orElse(HSQLDB_NO_COMMENT)));
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        switch (typeHandle.jdbcType()) {
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());

            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());
            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());
            case Types.INTEGER:
                return Optional.of(integerColumnMapping());
            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC:
            case Types.DECIMAL:
                int precision = typeHandle.requiredColumnSize();
                int decimalDigits = typeHandle.requiredDecimalDigits();
                return Optional.of(decimalColumnMapping(createDecimalType(min(precision, 38), max(decimalDigits, 0))));

            case Types.CHAR:
                return Optional.of(defaultCharColumnMapping(typeHandle.requiredColumnSize(), true));

            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                // varchar columns get created as varchar(default_length) in HsqlDB
                if (typeHandle.requiredColumnSize() == DEFAULT_VARCHAR_LENGTH) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType(), true));
                }
                return Optional.of(defaultVarcharColumnMapping(typeHandle.requiredColumnSize(), true));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(varbinaryColumnMapping());

            case Types.DATE:
                return Optional.of(dateColumnMappingUsingLocalDate());

            case Types.TIME:
                TimeType timeType = createTimeType(getTimePrecision(typeHandle.requiredColumnSize()));
                return Optional.of(timeColumnMapping(timeType));

            case Types.TIME_WITH_TIMEZONE:
                int timePrecision = getTimeWithTimeZonePrecision(typeHandle.requiredColumnSize());
                return Optional.of(timeWithTimeZoneColumnMapping(timePrecision));
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }

        if (type == TINYINT) {
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
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
            String dataType = varcharType.isUnbounded() ? "varchar(32768)" : format("varchar(%s)", varcharType.getBoundedLength());
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
    protected void renameColumn(ConnectorSession session, Connection connection, RemoteTableName remoteTableName, String remoteColumnName, String newRemoteColumnName)
            throws SQLException
    {
        try {
            String sql = format(
                    "ALTER TABLE %s ALTER COLUMN %s RENAME TO %s",
                    quoted(remoteTableName.getCatalogName().orElse(null), remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName()),
                    quoted(remoteColumnName),
                    quoted(newRemoteColumnName));
            execute(session, connection, sql);
        }
        catch (SQLSyntaxErrorException syntaxError) {
            throw syntaxError;
        }
    }

    @Override
    protected String getColumnDefinitionSql(ConnectorSession session, ColumnMetadata column, String columnName)
    {
        if (column.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with column comment");
        }

        StringBuilder sb = new StringBuilder()
                .append(quoted(columnName))
                .append(" ")
                .append(toWriteMapping(session, column.getType()).getDataType());
        Optional<String> defaultValue = column.getDefaultValue();
        if (defaultValue.isPresent()) {
            sb.append(" DEFAULT ");
            sb.append(getDefaultValue(column.getType(), defaultValue.get()));
        }
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        if (HsqlDbColumnProperties.isAutoIncrement(column.getProperties())) {
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
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column, ColumnPosition position)
    {
        verify(handle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s", handle);

        RemoteTableName remoteTableName = handle.asPlainTable().getRemoteTableName();
        switch (position) {
            case ColumnPosition.First _ -> addColumn(session, remoteTableName, column, getFirstColumnName(session, remoteTableName));
            case ColumnPosition.After after -> addColumn(session, remoteTableName, column, getNextColumnName(session, remoteTableName, after));
            case ColumnPosition.Last _ -> addColumn(session, remoteTableName, column, Optional.empty());
        }
    }

    private Optional<String> getFirstColumnName(ConnectorSession session, RemoteTableName remoteTableName)
    {
        return getColumns(session, remoteTableName.getSchemaTableName(), remoteTableName).stream()
                .map(JdbcColumnHandle::getColumnName)
                .findFirst();
    }

    private Optional<String> getNextColumnName(ConnectorSession session, RemoteTableName remoteTableName, ColumnPosition.After position)
    {
        String previousColumn = toRemoteIdentifier(position.columnName());
        System.out.println("HsqlDbClient.getNextColumnName() previousColumn: " + previousColumn);
        List<String> columnNames = getColumns(session, remoteTableName.getSchemaTableName(), remoteTableName).stream()
                .map(e -> e.getColumnName())
                .toList();

        if (!columnNames.contains(previousColumn) || columnNames.getLast().equals(previousColumn)) {
            return Optional.empty();
        }
        return Optional.ofNullable(columnNames.get(columnNames.indexOf(previousColumn) + 1));
    }

    private void addColumn(ConnectorSession session, RemoteTableName table, ColumnMetadata column, Optional<String> nextColumn)
    {
        if (column.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns with comments");
        }

        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            addColumn(session, connection, table, column, nextColumn);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected void addColumn(ConnectorSession session, Connection connection, RemoteTableName table, ColumnMetadata column, Optional<String> nextColumn)
            throws SQLException
    {
        String columnName = column.getName();
        verifyColumnName(connection.getMetaData(), columnName);
        String remoteColumnName = getIdentifierMapping().toRemoteColumnName(getRemoteIdentifiers(connection), columnName);
        String sql = format(
                "ALTER TABLE %s ADD %s%s",
                quoted(table),
                getColumnDefinitionSql(session, column, remoteColumnName),
                nextColumn.map(c -> " BEFORE " + quoted(c)).orElse(""));
        System.out.println("HsqlDbClient.addColumn() sql: " + sql);
        execute(session, connection, sql);
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            String remoteColumnName = getIdentifierMapping().toRemoteColumnName(getRemoteIdentifiers(connection), column.getColumnName());
            String sql = format(
                    "ALTER TABLE %s ALTER COLUMN %s %s",
                    quoted(handle.asPlainTable().getRemoteTableName()),
                    quoted(remoteColumnName),
                    toWriteMapping(session, type).getDataType());
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String sql = format("CREATE TABLE %s AS (SELECT %s FROM %s) WITH NO DATA",
                quoted(catalogName, schemaName, newTableName),
                columnNames.stream()
                        .map(this::quoted)
                        .collect(joining(", ")),
                quoted(catalogName, schemaName, tableName));
        try {
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        Map<String, Object> tableProperties = tableMetadata.getProperties();

        ImmutableList.Builder<String> createTableBuilder = ImmutableList.builder();
        createTableBuilder.add(format("CREATE %s TABLE %s (%s)",
                getTableType(tableProperties),
                quoted(remoteTableName),
                join(", ", columns)));

        Optional<List<String>> primaryKeys = HsqlDbTableProperties.getPrimaryKey(tableProperties);
        if (primaryKeys.isPresent() && !primaryKeys.get().isEmpty()) {
            verifyPrimaryKey(remoteTableName, primaryKeys.get(), tableMetadata.getColumns());
            createTableBuilder.add(format("ALTER TABLE %s ADD PRIMARY KEY (%s)",
                    quoted(remoteTableName),
                    primaryKeys.get().stream().map(this::quoted).collect(joining(", "))));
        }

        Optional<String> tableComment = tableMetadata.getComment();
        if (tableComment.isPresent()) {
            System.out.println("HsqlDbClient.createTableSqls() has comment");
            createTableBuilder.add(buildTableCommentSql(remoteTableName, tableComment));
        }
        return createTableBuilder.build();
    }

    private String getTableType(Map<String, Object> tableProperties)
    {
        Optional<String> definedTableType = HsqlDbTableProperties.getTableType(tableProperties);
        definedTableType.ifPresent(HsqlDbConfig::checkTableTypeValue);
        return definedTableType.orElse(defaultTableType.name());
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
        List<JdbcColumnHandle> columns = getColumns(session, remoteTableName.getSchemaTableName(), remoteTableName);
        try (Connection connection = connectionFactory.openConnection(session);
                ResultSet resultSet = getPrimaryKeyResultSet(connection, remoteTableName)) {
            Set<String> primaryKeys = new HashSet<>();
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                primaryKeys.add(columnName);
            }
            if (primaryKeys.isEmpty()) {
                return ImmutableList.of();
            }
            return columns.stream()
                    .filter(column -> primaryKeys.contains(column.getColumnName()))
                    .collect(toImmutableList());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private ResultSet getPrimaryKeyResultSet(Connection connection, RemoteTableName remoteTableName)
            throws SQLException
    {
        return connection.getMetaData().getPrimaryKeys(remoteTableName.getCatalogName().orElse(null),
                remoteTableName.getSchemaName().orElse(null),
                remoteTableName.getTableName());
    }

    @Override
    protected void renameTable(ConnectorSession session, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        if (!schemaName.equalsIgnoreCase(newTable.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }
        super.renameTable(session, catalogName, schemaName, tableName, newTable);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean supportsMerge()
    {
        return true;
    }

    @Override
    public JdbcMergeTableHandle beginMerge(
            ConnectorSession session,
            JdbcTableHandle handle,
            Map<Integer, Collection<ColumnHandle>> updateColumnHandles,
            Consumer<Runnable> rollbackActionCollector,
            RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support MERGE with fault-tolerant execution");
        }

        if (!isNonTransactionalMerge(session)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support MERGE with transactional execution");
        }

        return super.beginMerge(session, handle, updateColumnHandles, rollbackActionCollector, retryMode);
    }

    @Override
    public void finishMerge(ConnectorSession session, JdbcMergeTableHandle handle, Set<Long> pageSinkIds)
    {
        // When the connector retry mode is NO_RETRIES but isNonTransactionalInsert is false
        // the insert of merge still will first create temporary table
        finishInsertTable(session, handle.getOutputTableHandle(), pageSinkIds);
    }

    protected static int getTimePrecision(int timeColumnSize)
    {
        return getTimePrecision(timeColumnSize, ZERO_PRECISION_TIME_COLUMN_SIZE);
    }

    static int getTimeWithTimeZonePrecision(int timeColumnSize)
    {
        return getTimePrecision(timeColumnSize, ZERO_PRECISION_TIME_WITH_TIME_ZONE_COLUMN_SIZE);
    }

    private static int getTimePrecision(int timeColumnSize, int zeroPrecisionColumnSize)
    {
        if (timeColumnSize == zeroPrecisionColumnSize) {
            return 0;
        }
        int timePrecision = timeColumnSize - zeroPrecisionColumnSize - 1;
        verify(1 <= timePrecision && timePrecision <= MAX_SUPPORTED_DATE_TIME_PRECISION, "Unexpected time precision %s calculated from time column size %s", timePrecision, timeColumnSize);
        return timePrecision;
    }

    private static ColumnMapping timeWithTimeZoneColumnMapping(int precision)
    {
        // HsqlDB supports timestamp with time zone precision up to nanoseconds
        checkArgument(precision <= MAX_SUPPORTED_DATE_TIME_PRECISION, "unsupported precision value %s", precision);
        return ColumnMapping.longMapping(
                createTimeWithTimeZoneType(precision),
                timeWithTimeZoneReadFunction(),
                timeWithTimeZoneWriteFunction());
    }

    public static LongReadFunction timeWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            OffsetTime time = resultSet.getObject(columnIndex, OffsetTime.class);
            long nanosOfDay = time.toLocalTime().toNanoOfDay();
            verify(nanosOfDay < NANOSECONDS_PER_DAY, "Invalid value of nanosOfDay: %s", nanosOfDay);
            int offset = time.getOffset().getTotalSeconds() / 60;
            return packTimeWithTimeZone(nanosOfDay, offset);
        };
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
}
