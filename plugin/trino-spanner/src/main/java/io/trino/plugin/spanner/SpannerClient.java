package io.trino.plugin.spanner;

import com.google.cloud.Timestamp;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.CaseSensitivity;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.StandardColumnMappings;
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateReadFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.util.stream.Collectors.joining;

public class SpannerClient
        extends BaseJdbcClient
{
    // Maps to Spanner's default empty schema
    public static final String DEFAULT_SCHEMA = "default";
    private final SpannerConfig config;
    private final IdentifierMapping identifierMapping;

    public SpannerClient(BaseJdbcConfig config, SpannerConfig spannerConfig, JdbcStatisticsConfig statisticsConfig, ConnectionFactory connectionFactory, QueryBuilder queryBuilder, TypeManager typeManager, IdentifierMapping identifierMapping, RemoteQueryModifier queryModifier)
    {
        super("`", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);
        this.config = spannerConfig;
        this.identifierMapping = identifierMapping;
    }

    private static RemoteTableName getRemoteTable(ResultSet resultSet)
            throws SQLException
    {
        String schema = resultSet.getString("TABLE_SCHEM");
        if (schema != null && schema.equals("")) {
            schema = null;
        }
        return new RemoteTableName(
                Optional.ofNullable(null),
                Optional.ofNullable(schema),
                resultSet.getString("TABLE_NAME"));
    }

    private static ColumnMetadata getPageSinkIdColumn(List<String> otherColumnNames)
    {
        // While it's unlikely this column name will collide with client table columns,
        // guarantee it will not by appending a deterministic suffix to it.
        String baseColumnName = "trino_page_sink_id";
        String columnName = baseColumnName;
        int suffix = 1;
        while (otherColumnNames.contains(columnName)) {
            columnName = baseColumnName + "_" + suffix;
            suffix++;
        }
        return new ColumnMetadata(columnName, BigintType.BIGINT);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        int jdbcType = typeHandle.getJdbcType();
        System.out.println("Column mapping for type " + typeHandle);
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        switch (jdbcType) {
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.TINYINT:
            case Types.BIGINT:
                return Optional.of(StandardColumnMappings.bigintColumnMapping());
            case Types.NUMERIC:
            case Types.DECIMAL:
                return Optional.of(StandardColumnMappings.decimalColumnMapping(DecimalType.createDecimalType(9, 38)));
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), false));
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(ColumnMapping.sliceMapping(VARBINARY, varbinaryReadFunction(), varbinaryWriteFunction(), FULL_PUSHDOWN));
            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(
                        DATE,
                        dateReadFunctionUsingLocalDate(),
                        spannerDateWriteFunctionUsingLocalDate()));
            case Types.TIMESTAMP:
                return Optional.of(ColumnMapping.longMapping(
                        TimestampType.TIMESTAMP_MILLIS,
                        (resultSet, columnIndex) -> {
                            java.sql.Timestamp timestamp = resultSet.getTimestamp(columnIndex);
                            return timestamp.toInstant().toEpochMilli() * 1000;
                        },
                        (statement, index, value) -> statement.setTimestamp(index, new java.sql.Timestamp(value / 1000))));
            case Types.BOOLEAN:
                return Optional.of(StandardColumnMappings.booleanColumnMapping());
            default:
                throw new TrinoException(SpannerErrorCode.SPANNER_ERROR_CODE, "Spanner type mapper cannot build type mapping for JDBC type " + typeHandle.getJdbcType());
        }
    }

    private LongWriteFunction spannerDateWriteFunctionUsingLocalDate()
    {
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return "CAST(? AS DATE)";
            }

            @Override
            public void set(PreparedStatement statement, int index, long epochDay)
                    throws SQLException
            {
                statement.setString(index, LocalDate.ofEpochDay(epochDay).format(ISO_DATE));
            }
        };
    }

    private LongWriteFunction spannerTimestampWriteFunction()
    {
        return new LongWriteFunction()
        {
            @Override
            public void set(PreparedStatement statement, int index, long value)
                    throws SQLException
            {
                Timestamp timestamp = Timestamp.parseTimestamp(Instant.ofEpochMilli(value).toString());
                statement.setObject(index, timestamp);
            }

            @Override
            public String getBindExpression()
            {
                return "CAST(? AS TIMESTAMP)";
            }
        };
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        //Spanner handles all types int and long types as INT64
        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT) {
            return WriteMapping.longMapping("INT64", bigintWriteFunction());
        }
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("BOOL", booleanWriteFunction());
        }
        if (type instanceof DecimalType) {
            return WriteMapping.objectMapping("NUMERIC", longDecimalWriteFunction(DecimalType.createDecimalType(9, 38)));
        }
        if (type == REAL || type == DOUBLE) {
            return WriteMapping.doubleMapping("FLOAT64", doubleWriteFunction());
        }
        if (type instanceof TimestampType) {
            return WriteMapping.objectMapping("TIMESTAMP",
                    tsWrite());
        }
        if (type instanceof VarcharType) {
            return WriteMapping.sliceMapping("STRING(MAX)", varcharWriteFunction());
        }
        if (type instanceof DateType) {
            return WriteMapping.sliceMapping("DATE", varcharWriteFunction());
        }

        throw new RuntimeException("Dont know type " + type);
    }

    private ObjectWriteFunction tsWrite()
    {
        return ObjectWriteFunction.of(
                String.class,
                (statement, index, value) -> statement.setTimestamp(index,
                        com.google.cloud.Timestamp.parseTimestamp(value).toSqlTimestamp()));
    }

    @Override
    protected Optional<List<String>> getTableTypes()
    {
        return Optional.of(Arrays.asList("BASE TABLE", "VIEW"));
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        Set<String> schemas = new HashSet<>(Collections.singleton(DEFAULT_SCHEMA));

        try {
            ResultSet resultSet = connection.getMetaData().getSchemas(null, null);
            while (resultSet.next()) {
                schemas.add(resultSet.getString(1));
            }
            return schemas;
        }
        catch (SQLException e) {
            return schemas;
        }
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schema)
    {
        if (schema.equalsIgnoreCase(DEFAULT_SCHEMA)) {
            return true;
        }
        else {
            try {
                Connection connection = connectionFactory.openConnection(session);
                ResultSet schemas = connection.getMetaData().getSchemas(null, null);
                boolean found = false;
                while (schemas.next()) {
                    if (schemas.getString(1).equalsIgnoreCase(schema)) {
                        found = true;
                        break;
                    }
                }
                return found;
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(SpannerErrorCode.SPANNER_ERROR_CODE, "Spanner connector does not support creating schemas");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(SpannerErrorCode.SPANNER_ERROR_CODE, "Spanner connector does not support dropping schemas");
    }

    @Override
    public List<SchemaTableName> getTableNames(ConnectorSession session, Optional<String> schema)
    {
        List<SchemaTableName> tables = new ArrayList<>();
        try {
            Connection connection = connectionFactory.openConnection(session);
            ResultSet resultSet = getTablesFromSpanner(connection);
            while (resultSet.next()) {
                tables.add(new SchemaTableName(DEFAULT_SCHEMA, resultSet.getString(1)));
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return tables;
    }

    private ResultSet getTablesFromSpanner(Connection connection)
            throws SQLException
    {
        return connection.createStatement().executeQuery("SELECT\n" +
                "  TABLE_NAME\n" +
                "FROM\n" +
                "  INFORMATION_SCHEMA.TABLES\n" +
                "WHERE\n" +
                "  TABLE_CATALOG = '' and TABLE_SCHEMA = ''");
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        Map<String, String> columnAndDataTypeMap = tableMetadata.getTableSchema().getColumns()
                .stream()
                .collect(Collectors.toMap(k -> k.getName().toUpperCase(Locale.ENGLISH),
                        v -> toWriteMapping(session, v.getType()).getDataType(),
                        (k, v) -> v, LinkedHashMap::new));

        Map<String, Object> properties = tableMetadata.getProperties();
        List<String> primaryKeys = SpannerTableProperties.getPrimaryKey(properties);
        List<String> notNullFields = SpannerTableProperties.getNotNullFields(properties);
        List<String> commitTimestampFields = SpannerTableProperties.getCommitTimestampFields(properties);
        Preconditions.checkArgument(primaryKeys != null && !primaryKeys.isEmpty(), "Primary key is required to create a table in spanner");
        Map<String, String> columns = new LinkedHashMap<>();
        columnAndDataTypeMap.forEach((column, dataType) -> {
            columns.put(column, join(" ", quoted(column), dataType));
            if (notNullFields.contains(column)) {
                String columnWithConstraint = String.format("%s NOT NULL", columns.get(column));
                columns.put(column, columnWithConstraint);
            }
            if (commitTimestampFields.contains(column)) {
                String columnWithConstraint = String.format("%s OPTIONS(allow_commit_timestamp=true)", columns.get(column));
                columns.put(column, columnWithConstraint);
            }
        });
        String interleaveTable = SpannerTableProperties.getInterleaveInParent(properties);
        boolean onDeleteCascade = SpannerTableProperties.getOnDeleteCascade(properties);
        String interleaveClause = "";
        String onDeleteClause = "";
        if (interleaveTable != null) {
            interleaveClause = String.format(", INTERLEAVE IN PARENT %s ", quoted(interleaveTable));
            onDeleteClause = onDeleteCascade ? " ON DELETE CASCADE " : " ON DELETE NO ACTION ";
        }
        String sql = format("CREATE TABLE %s (%s) PRIMARY KEY (%s) %s %s",
                quoted(tableMetadata.getTable().getTableName()), String.join(", ", columns.values()),
                quoted(join(", ", primaryKeys)),
                interleaveClause, onDeleteClause);
        execute(session, sql);
    }

    public boolean checkTableExists(ConnectorSession session, String tableName)
            throws SQLException
    {
        return checkTableExists(connectionFactory.openConnection(session), tableName);
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                schemaName.orElse(null),
                null,
                escapeObjectNameForMetadataQuery(tableName, metadata.getSearchStringEscape()).orElse(null),
                getTableTypes().map(types -> types.toArray(String[]::new)).orElse(null));
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return null;
    }

    @Override
    protected ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException
    {
        RemoteTableName remoteTableName = tableHandle.getRequiredNamedRelation().getRemoteTableName();
        String schema = remoteTableName.getSchemaName().orElse(DEFAULT_SCHEMA)
                .equalsIgnoreCase(DEFAULT_SCHEMA) ? null : escapeObjectNameForMetadataQuery(remoteTableName.getSchemaName(), metadata.getSearchStringEscape()).orElse(null);
        return metadata.getColumns(
                null,
                schema,
                escapeObjectNameForMetadataQuery(remoteTableName.getTableName(), metadata.getSearchStringEscape()),
                null);
    }

    @Override
    public boolean supportsRetries()
    {
        return false;
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters)
    {
        boolean hasPageSinkIdColumn = handle.getPageSinkIdColumnName().isPresent();
        checkArgument(handle.getColumnNames().size() == columnWriters.size(), "handle and columnWriters mismatch: %s, %s", handle, columnWriters);
        return format(
                "INSERT INTO %s (%s%s) VALUES (%s%s)",
                quoted(null, null, handle.getTemporaryTableName().orElseGet(handle::getTableName)),
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
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        SchemaTableName schemaTableName = tableHandle.asPlainTable().getSchemaTableName();
        ConnectorIdentity identity = session.getIdentity();

        verify(tableHandle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(tableHandle));
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            String remoteSchema = identifierMapping.toRemoteSchemaName(identity, connection, schemaTableName.getSchemaName());
            String remoteTable = identifierMapping.toRemoteTableName(identity, connection, remoteSchema, schemaTableName.getTableName());

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<JdbcTypeHandle> jdbcColumnTypes = ImmutableList.builder();
            for (JdbcColumnHandle column : columns) {
                columnNames.add(column.getColumnName());
                columnTypes.add(column.getColumnType());
                jdbcColumnTypes.add(column.getJdbcTypeHandle());
            }
            return new JdbcOutputTableHandle(
                    null,
                    DEFAULT_SCHEMA,
                    remoteTable,
                    columnNames.build(),
                    columnTypes.build(),
                    Optional.of(jdbcColumnTypes.build()),
                    Optional.empty(),
                    Optional.empty());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private boolean shouldUseFaultTolerantExecution(ConnectorSession session)
    {
        //Does not support fault-tolerant exec
        return false;
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        if (tableHandle.getColumns().isPresent()) {
            return tableHandle.getColumns().get();
        }
        checkArgument(tableHandle.isNamedRelation(), "Cannot get columns for %s", tableHandle);
        verify(tableHandle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(tableHandle));
        SchemaTableName schemaTableName = tableHandle.getRequiredNamedRelation().getSchemaTableName();
        RemoteTableName remoteTableName = tableHandle.getRequiredNamedRelation().getRemoteTableName();
        try (Connection connection = connectionFactory.openConnection(session);
                ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
            Map<String, CaseSensitivity> caseSensitivityMapping = getCaseSensitivityForColumns(session, connection, tableHandle);
            int allColumns = 0;
            List<JdbcColumnHandle> columns = new ArrayList<>();
            while (resultSet.next()) {
                // skip if table doesn't match expected
                RemoteTableName remoteTable = getRemoteTable(resultSet);
                if (!(Objects.equals(remoteTableName, remoteTable))) {
                    continue;
                }
                allColumns++;
                String columnName = resultSet.getString("COLUMN_NAME");
                JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                        getInteger(resultSet, "DATA_TYPE").orElseThrow(() -> new IllegalStateException("DATA_TYPE is null")),
                        Optional.ofNullable(resultSet.getString("TYPE_NAME")),
                        getInteger(resultSet, "COLUMN_SIZE"),
                        getInteger(resultSet, "DECIMAL_DIGITS"),
                        Optional.empty(),
                        Optional.ofNullable(caseSensitivityMapping.get(columnName)));
                Optional<ColumnMapping> columnMapping = toColumnMapping(session, connection, typeHandle);
                //log.debug("Mapping data type of '%s' column '%s': %s mapped to %s", schemaTableName, columnName, typeHandle, columnMapping);
                boolean nullable = (resultSet.getInt("NULLABLE") != columnNoNulls);
                // Note: some databases (e.g. SQL Server) do not return column remarks/comment here.
                Optional<String> comment = Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
                // skip unsupported column types
                columnMapping.ifPresent(mapping -> columns.add(JdbcColumnHandle.builder()
                        .setColumnName(columnName)
                        .setJdbcTypeHandle(typeHandle)
                        .setColumnType(mapping.getType())
                        .setNullable(nullable)
                        .setComment(comment)
                        .build()));
                if (columnMapping.isEmpty()) {
                    UnsupportedTypeHandling unsupportedTypeHandling = getUnsupportedTypeHandling(session);
                    verify(
                            unsupportedTypeHandling == IGNORE,
                            "Unsupported type handling is set to %s, but toColumnMapping() returned empty for %s",
                            unsupportedTypeHandling,
                            typeHandle);
                }
            }
            if (columns.isEmpty()) {
                throw new TableNotFoundException(
                        schemaTableName,
                        format("Table '%s' has no supported columns (all %s columns are not supported)", schemaTableName, allColumns));
            }
            return ImmutableList.copyOf(columns);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
    {
        System.out.println("Called table statistics");
        return super.getTableStatistics(session, handle);
    }

    public boolean checkTableExists(Connection connection, String tableName)
            throws SQLException
    {
        ResultSet tablesFromSpanner = getTablesFromSpanner(connection);
        boolean exists = false;
        while (tablesFromSpanner.next()) {
            String table = tablesFromSpanner.getString(1);
            if (table.equalsIgnoreCase(tableName)) {
                exists = true;
                break;
            }
        }
        return exists;
    }

    @Override
    public void finishInsertTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        //Nothing to do after insert
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        boolean tableExists = false;
        try {
            tableExists = checkTableExists(session, schemaTableName.getTableName());
            if (tableExists) {
                return Optional.of(new JdbcTableHandle(new SchemaTableName(DEFAULT_SCHEMA, schemaTableName.getTableName()),
                        new RemoteTableName(Optional.empty(),
                                Optional.empty(), schemaTableName.getTableName()),
                        Optional.empty()));
            }
            else {
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new TrinoException(SpannerErrorCode.SPANNER_ERROR_CODE, e);
        }
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle handle)
    {
        System.out.println("Drop table ");
        SchemaTableName schemaTableName = handle.getRequiredNamedRelation().getSchemaTableName();
        try (Connection connection = connectionFactory.openConnection(session)) {
            String format = format("DROP TABLE %s", schemaTableName.getTableName());
            connection.createStatement().executeUpdate(format);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
       // System.out.println("PROPS WAS CALLED ");
        return new HashMap<>();
    }

    public enum SpannerErrorCode
            implements ErrorCodeSupplier
    {
        SPANNER_ERROR_CODE(1, INTERNAL_ERROR);

        private final ErrorCode errorCode;

        SpannerErrorCode(int code, ErrorType type)
        {
            errorCode = new ErrorCode(code + 0x0506_0000, name(), type);
        }

        @Override
        public ErrorCode toErrorCode()
        {
            return errorCode;
        }
    }
}
