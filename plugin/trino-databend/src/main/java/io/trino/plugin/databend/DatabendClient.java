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
package io.trino.plugin.databend;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Shorts;
import com.google.inject.Inject;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.CaseSensitivity;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.StandardColumnMappings;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.weakref.jmx.$internal.guava.base.Enums;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.databend.DatabendTableProperties.ENGINE_PROPERTY;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_INSENSITIVE;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.weakref.jmx.$internal.guava.base.Preconditions.checkArgument;
import static org.weakref.jmx.$internal.guava.base.Strings.emptyToNull;

public final class DatabendClient
        extends BaseJdbcClient
{
    @Inject
    public DatabendClient(
            BaseJdbcConfig config,
            DatabendConfig databendConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, false);
        requireNonNull(databendConfig, "databendConfig is null");
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream().map(sortItem -> {
                String ordering = sortItem.sortOrder().isAscending() ? "ASC" : "DESC";
                String nullsHandling = sortItem.sortOrder().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                return format("%s %s %s", quoted(sortItem.column().getColumnName()), ordering, nullsHandling);
            }).collect(joining(", "));
            return format("%s ORDER BY %s LIMIT %d", query, orderBy, limit);
        });
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
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
        if (joinType == JoinType.FULL_OUTER) {
            return Optional.empty();
        }
        return implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.implementJoin(session, joinType, leftSource, leftProjections, rightSource, rightProjections, joinConditions, statistics));
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        if (tableName.isPresent()) {
            // Databend maps their "database" to SQL catalogs and does not have schemas
            return metadata.getTables(schemaName.orElse(null), null, tableName.get(), getTableTypes().map(types -> types.toArray(String[]::new)).orElse(null));
        }
        return metadata.getTables(schemaName.orElse(null), null, tableName.orElse(null), getTableTypes().map(types -> types.toArray(String[]::new)).orElse(null));
    }

    @Override
    protected ResultSet getColumns(RemoteTableName remoteTableName, DatabaseMetaData metadata)
            throws SQLException
    {
        // Databend exposes databases via JDBC catalogs and does not use schemas
        Optional<String> database = remoteTableName.getSchemaName();
        if (database.isEmpty()) {
            database = remoteTableName.getCatalogName();
        }
        return metadata.getColumns(
                database.orElse(null),
                null,
                escapeObjectNameForMetadataQuery(remoteTableName.getTableName(), metadata.getSearchStringEscape()),
                null);
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, SchemaTableName schemaTableName, RemoteTableName remoteTableName)
    {
        Map<String, Boolean> remoteNullability = getRemoteNullability(session, remoteTableName);
        return super.getColumns(session, schemaTableName, remoteTableName).stream()
                .map(column -> applyRemoteNullability(column, remoteNullability))
                .collect(toImmutableList());
    }

    private Map<String, Boolean> getRemoteNullability(ConnectorSession session, RemoteTableName remoteTableName)
    {
        Optional<String> database = remoteTableName.getSchemaName();
        if (database.isEmpty()) {
            database = remoteTableName.getCatalogName();
        }
        if (database.isEmpty()) {
            return Map.of();
        }
        try (Connection connection = connectionFactory.openConnection(session);
                PreparedStatement statement = connection.prepareStatement("""
                        SELECT column_name, is_nullable
                        FROM information_schema.columns
                        WHERE table_catalog = ?
                          AND table_name = ?
                        """)) {
            statement.setString(1, database.get());
            statement.setString(2, remoteTableName.getTableName());
            try (ResultSet resultSet = statement.executeQuery()) {
                ImmutableMap.Builder<String, Boolean> builder = ImmutableMap.builder();
                while (resultSet.next()) {
                    builder.put(
                            resultSet.getString("column_name").toLowerCase(ENGLISH),
                            "YES".equalsIgnoreCase(resultSet.getString("is_nullable")));
                }
                return builder.buildOrThrow();
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to fetch column nullability", e);
        }
    }

    private JdbcColumnHandle applyRemoteNullability(JdbcColumnHandle column, Map<String, Boolean> remoteNullability)
    {
        Boolean remoteNullable = remoteNullability.get(column.getColumnName().toLowerCase(ENGLISH));
        if (remoteNullable == null || remoteNullable == column.isNullable()) {
            return column;
        }
        return JdbcColumnHandle.builderFrom(column)
                .setNullable(remoteNullable)
                .build();
    }

    @Override
    protected String quoted(@Nullable String catalog, @Nullable String schema, String table)
    {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(schema)) {
            sb.append(quoted(schema)).append(".");
        }
        else if (!isNullOrEmpty(catalog)) {
            sb.append(quoted(catalog)).append(".");
        }
        sb.append(quoted(table));
        return sb.toString();
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String tableCopyFormat = "CREATE TABLE %s AS SELECT * FROM %s";
        String sql = format(tableCopyFormat, quoted(catalogName, schemaName, newTableName), quoted(catalogName, schemaName, tableName));
        try {
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CAT");
                if (filterSchema(schemaName)) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
            throws SQLException
    {
        // Empty remarks means that the table doesn't have a comment in Databend
        return Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        ImmutableList.Builder<String> tableOptions = ImmutableList.builder();
        Map<String, Object> tableProperties = tableMetadata.getProperties();
        if (tableMetadata.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with table comment");
        }
        DatabendEngineType engine = DatabendTableProperties.getEngine(tableProperties);
        tableOptions.add("ENGINE = " + engine.getEngineType());

        formatProperty(DatabendTableProperties.getOrderBy(tableProperties)).ifPresent(value -> tableOptions.add("ORDER BY " + value));

        return ImmutableList.of(format("CREATE TABLE %s (%s) %s", quoted(remoteTableName), join(", ", columns), join(" ", tableOptions.build())));
    }

    /**
     * format property to match Databend create table statement
     *
     * @param prop property will be formatted
     * @return formatted property
     */
    private Optional<String> formatProperty(List<String> prop)
    {
        if (prop == null || prop.isEmpty()) {
            return Optional.empty();
        }
        if (prop.size() == 1) {
            // only one column
            return Optional.of(quoted(prop.getFirst()));
        }
        // include more than one column
        return Optional.of(prop.stream().map(this::quoted).collect(joining(",", "(", ")")));
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            PreparedStatement statement = connection.prepareStatement("SELECT engine " + "FROM system.tables " + "WHERE database = ? AND name = ?");
            statement.setString(1, tableHandle.asPlainTable().getRemoteTableName().getCatalogName().orElse(null));
            statement.setString(2, tableHandle.asPlainTable().getRemoteTableName().getTableName());

            try (ResultSet resultSet = statement.executeQuery()) {
                ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
                while (resultSet.next()) {
                    String engine = resultSet.getString("engine");
                    if (!isNullOrEmpty(engine)) {
                        Optional<DatabendEngineType> engineType = Enums.getIfPresent(DatabendEngineType.class, engine.toUpperCase(ENGLISH)).toJavaUtil();
                        engineType.ifPresent(type -> properties.put(ENGINE_PROPERTY, type));
                    }
                }
                return properties.buildOrThrow();
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, PreparedQuery preparedQuery)
    {
        try {
            return super.getTableHandle(session, preparedQuery);
        }
        catch (TrinoException e) {
            if (e.getErrorCode().equals(NOT_SUPPORTED.toErrorCode()) &&
                    e.getMessage().startsWith("Query not supported: ResultSetMetaData not available for query")) {
                throw new TrinoException(JDBC_ERROR, "Failed to get table handle for prepared query. " + e.getMessage(), e);
            }
            throw e;
        }
    }

    @Override
    public void setTableProperties(ConnectorSession session, JdbcTableHandle handle, Map<String, Optional<Object>> nullableProperties)
    {
        checkArgument(nullableProperties.values().stream().noneMatch(Optional::isEmpty), "Setting a property to null is not supported");

        if (nullableProperties.isEmpty()) {
            return;
        }

        throw new TrinoException(NOT_SUPPORTED, "Setting table properties is not supported for Databend tables");
    }

    @Override
    protected String getColumnDefinitionSql(ConnectorSession session, ColumnMetadata column, String columnName)
    {
        if (column.getComment() != null) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with column comment");
        }
        StringBuilder sb = new StringBuilder()
                .append(quoted(columnName))
                .append(" ");
        if (column.isNullable()) {
            // set column nullable property explicitly
            sb.append("Nullable(").append(toWriteMapping(session, column.getType()).getDataType()).append(")");
        }
        else {
            // By default, the clickhouse column is not allowed to be null
            sb.append(toWriteMapping(session, column.getType()).getDataType());
        }
        return sb.toString();
    }

    @Override
    protected void createSchema(ConnectorSession session, Connection connection, String remoteSchemaName)
            throws SQLException
    {
        execute(session, connection, "CREATE DATABASE " + quoted(remoteSchemaName));
    }

    @Override
    protected void dropSchema(ConnectorSession session, Connection connection, String remoteSchemaName, boolean cascade)
            throws SQLException
    {
        if (!cascade) {
            try (ResultSet tables = getTables(connection, Optional.of(remoteSchemaName), Optional.empty())) {
                if (tables.next()) {
                    throw new TrinoException(SCHEMA_NOT_EMPTY, String.format("Cannot drop non-empty schema '%s'", remoteSchemaName));
                }
            }
        }
        execute(session, connection, "DROP DATABASE " + quoted(remoteSchemaName));
    }

    @Override
    protected void renameSchema(ConnectorSession session, Connection connection, String remoteSchemaName, String newRemoteSchemaName)
            throws SQLException
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column, ColumnPosition position)
    {
        if (column.getComment() != null) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns with comments");
        }
        if (!(position instanceof ColumnPosition.Last)) {
            if (position instanceof ColumnPosition.First) {
                throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns with FIRST clause");
            }
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns with AFTER clause");
        }
        try (Connection connection = connectionFactory.openConnection(session)) {
            String remoteColumnName = getIdentifierMapping().toRemoteColumnName(getRemoteIdentifiers(connection), column.getName());
            String sql = format("ALTER TABLE %s ADD COLUMN %s", quoted(handle.asPlainTable().getRemoteTableName()), getColumnDefinitionSql(session, column, remoteColumnName));
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping a not null constraint");
    }

    @Override
    protected Optional<List<String>> getTableTypes()
    {
        return Optional.empty();
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName)
            throws SQLException
    {
        if (!Objects.equals(remoteSchemaName, newRemoteSchemaName)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }
        execute(session, connection, format("RENAME TABLE %s TO %s", quoted(catalogName, remoteSchemaName, remoteTableName), quoted(catalogName, newRemoteSchemaName, newRemoteTableName)));
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
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.jdbcTypeName().orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        switch (jdbcTypeName.toLowerCase(ENGLISH)) {
            case "uint8":
                return Optional.of(ColumnMapping.longMapping(SMALLINT, ResultSet::getShort, uInt8WriteFunction()));
            case "uint16":
                return Optional.of(ColumnMapping.longMapping(INTEGER, ResultSet::getInt, uInt16WriteFunction()));
            case "uint32":
                return Optional.of(ColumnMapping.longMapping(BIGINT, ResultSet::getLong, uInt32WriteFunction()));
            case "uint64":
                return Optional.of(decimalColumnMapping(createDecimalType(20)));
            case "string":
                return Optional.of(varcharColumnMapping(typeHandle.requiredColumnSize(), typeHandle.caseSensitivity()));
            default:
        }

        switch (typeHandle.jdbcType()) {
            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.BOOLEAN:
            case Types.BIT:
                return Optional.of(StandardColumnMappings.booleanColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.FLOAT:
                return Optional.of(ColumnMapping.longMapping(REAL, (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)), realWriteFunction(), DISABLE_PUSHDOWN));

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.VARCHAR:
                if (jdbcTypeName.equals("varchar")) {
                    return Optional.of(varcharColumnMapping(typeHandle.requiredColumnSize(), typeHandle.caseSensitivity()));
                }
                // Some other Databend types (ARRAY, VARIANT, etc.) are also mapped to Types.VARCHAR, but they're unsupported.
                break;

            case Types.DECIMAL:
                int decimalDigits = typeHandle.requiredDecimalDigits();
                int precision = typeHandle.requiredColumnSize();

                ColumnMapping decimalColumnMapping;
                if (getDecimalRounding(session) == ALLOW_OVERFLOW && precision > Decimals.MAX_PRECISION) {
                    int scale = Math.min(decimalDigits, getDecimalDefaultScale(session));
                    decimalColumnMapping = decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session));
                }
                else {
                    decimalColumnMapping = decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0)));
                }
                return Optional.of(ColumnMapping.mapping(decimalColumnMapping.getType(), decimalColumnMapping.getReadFunction(), decimalColumnMapping.getWriteFunction(),
                        DISABLE_PUSHDOWN)); // To avoid potential data loss or precision issues during pushdown operations

            case Types.DATE:
                return Optional.of(dateColumnMappingUsingLocalDate());
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    private static ColumnMapping varcharColumnMapping(int varcharLength, Optional<CaseSensitivity> caseSensitivity)
    {
        VarcharType varcharType = varcharLength <= VarcharType.MAX_LENGTH ? createVarcharType(varcharLength) : createUnboundedVarcharType();
        return StandardColumnMappings.varcharColumnMapping(varcharType, caseSensitivity.orElse(CASE_INSENSITIVE) == CASE_SENSITIVE);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("Boolean", booleanWriteFunction());
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("Int8", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("Int16", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("Int32", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("Int64", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("Float32", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("Float64", doubleWriteFunction());
        }
        if (type instanceof DecimalType decimalType) {
            String dataType = format("Decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        if (type instanceof CharType || type instanceof VarcharType) {
            // The String type replaces the types VARCHAR, BLOB, CLOB, and others from other DBMSs.
            return WriteMapping.sliceMapping("String", varcharWriteFunction());
        }
        if (type instanceof VarbinaryType) {
            // Strings of an arbitrary length. The length is not limited
            return WriteMapping.sliceMapping("String", varbinaryWriteFunction());
        }
        if (type == DATE) {
            return WriteMapping.longMapping("Date", dateWriteFunctionUsingLocalDate());
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type);
    }

    private static LongWriteFunction uInt8WriteFunction()
    {
        return (statement, index, value) -> {
            statement.setShort(index, Shorts.checkedCast(value));
        };
    }

    private static LongWriteFunction uInt16WriteFunction()
    {
        return (statement, index, value) -> {
            statement.setInt(index, toIntExact(value));
        };
    }

    private static LongWriteFunction uInt32WriteFunction()
    {
        return PreparedStatement::setLong;
    }

    private static ColumnMapping dateColumnMappingUsingLocalDate()
    {
        return ColumnMapping.longMapping(DATE, databendDateReadFunction(), dateWriteFunctionUsingLocalDate());
    }

    private static LongReadFunction databendDateReadFunction()
    {
        return new LongReadFunction() {
            @Override
            public boolean isNull(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                resultSet.getObject(columnIndex);
                return resultSet.wasNull();
            }

            @Override
            public long readLong(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                Object value = resultSet.getObject(columnIndex);
                if (value == null) {
                    throw new TrinoException(JDBC_ERROR, "Driver returned null LocalDate for a non-null value");
                }
                if (value instanceof LocalDate localDate) {
                    return localDate.toEpochDay();
                }
                if (value instanceof Date date) {
                    return date.toLocalDate().toEpochDay();
                }
                return LocalDate.parse(value.toString()).toEpochDay();
            }
        };
    }

    private static LongWriteFunction dateWriteFunctionUsingLocalDate()
    {
        return (statement, index, value) -> {
            LocalDate date = LocalDate.ofEpochDay(value);
            statement.setObject(index, date);
        };
    }
}
