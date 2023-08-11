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
package io.trino.plugin.ignite;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

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
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.plugin.ignite.IgniteTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.plugin.jdbc.ColumnMapping.longMapping;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.UNBOUNDED_LENGTH;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;

public class IgniteClient
        extends BaseJdbcClient
{
    private static final String IGNITE_SCHEMA = "PUBLIC";

    private static final String IGNITE_DUMMY_ID = "dummy_id";
    private static final Splitter SPLITTER = Splitter.on("\"").omitEmptyStrings().trimResults();
    private static final LocalDate MIN_DATE = LocalDate.parse("1970-01-01");
    private static final LocalDate MAX_DATE = LocalDate.parse("9999-12-31");

    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;

    @Inject
    public IgniteClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("`", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, false);

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .map("$like(value: varchar, pattern: varchar): boolean").to("value LIKE pattern")
                .map("$like(value: varchar, pattern: varchar, escape: varchar(1)): boolean").to("value LIKE pattern ESCAPE escape")
                .build();
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax(true))
                        .add(new ImplementSum(IgniteClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgBigint())
                        .add(new ImplementCountDistinct(bigintTypeHandle, true))
                        .build());
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        return ImmutableSet.of(IGNITE_SCHEMA);
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                null, // no catalogs in Ignite
                // TODO: https://github.com/trinodb/trino/issues/8552 support user custom schemas.
                escapeObjectNameForMetadataQuery(schemaName, metadata.getSearchStringEscape()).orElse(IGNITE_SCHEMA),
                escapeObjectNameForMetadataQuery(tableName, metadata.getSearchStringEscape()).orElse(null),
                new String[] {"TABLE", "VIEW"});
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        switch (typeHandle.getJdbcType()) {
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

            case Types.FLOAT:
                return Optional.of(realColumnMapping());

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.DECIMAL:
                int decimalDigits = typeHandle.getRequiredDecimalDigits();
                int precision = typeHandle.getRequiredColumnSize();
                if (getDecimalRounding(session) == ALLOW_OVERFLOW && precision > Decimals.MAX_PRECISION) {
                    int scale = min(decimalDigits, getDecimalDefaultScale(session));
                    return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
                }
                precision = precision + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));

            case Types.VARCHAR:
                return Optional.of(varcharColumnMapping(typeHandle.getColumnSize()));

            case Types.DATE:
                return Optional.of(longMapping(DATE, dateReadFunction(), dateWriteFunction()));

            case Types.BINARY:
                return Optional.of(varbinaryColumnMapping());
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
            return WriteMapping.longMapping("int", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double", doubleWriteFunction());
        }
        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        if (type instanceof CharType) {
            return WriteMapping.sliceMapping("varchar(" + ((CharType) type).getLength() + ")", varcharWriteFunction());
        }
        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar";
            }
            else {
                dataType = "varchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type instanceof DateType) {
            return WriteMapping.longMapping("date", dateWriteFunction());
        }
        if (type instanceof VarbinaryType) {
            return WriteMapping.sliceMapping("binary", varbinaryWriteFunction());
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(
                new JdbcTypeHandle(
                        Types.DECIMAL,
                        Optional.of("Decimal"),
                        Optional.of(decimalType.getPrecision()),
                        Optional.of(decimalType.getScale()),
                        Optional.empty(),
                        Optional.empty()));
    }

    private static ColumnMapping varcharColumnMapping(Optional<Integer> columnSize)
    {
        VarcharType varcharType = columnSize
                // default added column in Ignite will have Integer.MAX_VALUE length which not allow in Trino, will consider the column as varchar
                .filter(size -> size != UNBOUNDED_LENGTH)
                .map(VarcharType::createVarcharType)
                .orElse(createUnboundedVarcharType());
        return ColumnMapping.sliceMapping(varcharType, varcharReadFunction(varcharType), varcharWriteFunction(), FULL_PUSHDOWN);
    }

    private static LongReadFunction dateReadFunction()
    {
        return (resultSet, columnIndex) -> {
            long localMillis = resultSet.getDate(columnIndex).getTime();
            long days = MILLISECONDS.toDays(localMillis);
            checkDateValue(days);
            return days;
        };
    }

    private static LongWriteFunction dateWriteFunction()
    {
        return (statement, index, value) -> {
            checkDateValue(value);
            long millis = DAYS.toMillis(value);
            statement.setDate(index, new Date(millis));
        };
    }

    private static void checkDateValue(long epochDay)
    {
        if (epochDay < MIN_DATE.toEpochDay() || epochDay > MAX_DATE.toEpochDay()) {
            throw new TrinoException(INVALID_ARGUMENTS, format("Date must be between %s and %s in Ignite: %s", MIN_DATE, MAX_DATE, LocalDate.ofEpochDay(epochDay)));
        }
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        if (tableMetadata.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with table comment");
        }

        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schema = schemaTableName.getSchemaName();
        if (!getSchemaNames(session).contains(schema)) {
            throw new SchemaNotFoundException(schema);
        }

        int expectedSize = tableMetadata.getColumns().size();
        ImmutableList.Builder<String> columns = ImmutableList.builderWithExpectedSize(expectedSize);
        ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builderWithExpectedSize(expectedSize);
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builderWithExpectedSize(expectedSize);
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            columns.add(getColumnDefinitionSql(session, columnMetadata, columnMetadata.getName()));
            columnNamesBuilder.add(columnMetadata.getName());
            columnTypes.add(columnMetadata.getType());
        }

        List<String> columnNames = columnNamesBuilder.build();
        List<String> primaryKeys = IgniteTableProperties.getPrimaryKey(tableMetadata.getProperties());

        for (String primaryKey : primaryKeys) {
            if (!columnNames.contains(primaryKey)) {
                throw new TrinoException(INVALID_TABLE_PROPERTY,
                        format("Column '%s' specified in property '%s' doesn't exist in table", primaryKey, PRIMARY_KEY_PROPERTY));
            }
        }

        String sql = buildCreateSql(schemaTableName, columns.build(), primaryKeys);

        try (Connection connection = connectionFactory.openConnection(session)) {
            execute(session, connection, sql);

            return new IgniteOutputTableHandle(
                    schemaTableName.getSchemaName(),
                    schemaTableName.getTableName(),
                    columnNames,
                    columnTypes.build(),
                    Optional.empty(),
                    primaryKeys.isEmpty() ? Optional.of(IGNITE_DUMMY_ID) : Optional.empty());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private String buildCreateSql(SchemaTableName schemaTableName, List<String> columns, List<String> primaryKeys)
    {
        ImmutableList.Builder<String> columnDefinitions = ImmutableList.builder();
        columnDefinitions.addAll(columns);

        checkArgument(primaryKeys.size() < columns.size(), "Ignite table must have at least one non PRIMARY KEY column.");
        if (primaryKeys.isEmpty()) {
            columnDefinitions.add(quoted(IGNITE_DUMMY_ID) + " VARCHAR NOT NULL");
            primaryKeys = ImmutableList.of(IGNITE_DUMMY_ID);
        }
        columnDefinitions.add("PRIMARY KEY (" + join(", ", primaryKeys.stream().map(this::quoted).collect(joining(", "))) + ")");

        String remoteTableName = quoted(null, schemaTableName.getSchemaName(), schemaTableName.getTableName());
        return format("CREATE TABLE %s (%s) ", remoteTableName, join(", ", columnDefinitions.build()));
    }

    @Override
    protected String quoted(@Nullable String catalog, @Nullable String schema, String table)
    {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(schema)) {
            sb.append(quoted(schema)).append(".");
        }
        sb.append(quoted(table));
        return sb.toString();
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
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream()
                    .map(sortItem -> {
                        String ordering = sortItem.getSortOrder().isAscending() ? "ASC" : "DESC";
                        String nullsHandling = sortItem.getSortOrder().isNullsFirst() ? "IS NULL DESC" : "IS NULL ASC";
                        String columnName = quoted(sortItem.getColumn().getColumnName());

                        return format("%s %s, %1$s %s", columnName, nullsHandling, ordering);
                    })
                    .collect(joining(", "));

            return format("%s ORDER BY %s OFFSET 0 ROWS FETCH NEXT %s ROWS ONLY", query, orderBy, limit);
        });
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            return getTableProperties(connection, tableHandle);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    public Map<String, Object> getTableProperties(Connection connection, JdbcTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        SchemaTableName schemaTableName = tableHandle.asPlainTable().getSchemaTableName();
        String schemaName = requireNonNull(schemaTableName.getSchemaName(), "Ignite schema name can not be null").toUpperCase(ENGLISH);
        String tableName = requireNonNull(schemaTableName.getTableName(), "Ignite table name can not be null").toUpperCase(ENGLISH);
        // Get primary keys from 'sys.indexes' because DatabaseMetaData.getPrimaryKeys doesn't work well while table being concurrent modified
        String sql = "SELECT COLUMNS FROM sys.indexes WHERE SCHEMA_NAME = ? AND TABLE_NAME = ? AND IS_PK LIMIT 1";

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, schemaName);
            preparedStatement.setString(2, tableName);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (!resultSet.next()) {
                    return ImmutableMap.of();
                }
                List<String> primaryKeys = extractColumnNamesFromOrderingScheme(resultSet.getString("COLUMNS"));
                checkArgument(!primaryKeys.isEmpty(), "Ignite table should has at least one primary key");
                properties.put(PRIMARY_KEY_PROPERTY, primaryKeys);
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
        return properties.buildOrThrow();
    }

    // extract result from format: "ID" ASC, "CITY_ID" ASC
    private List<String> extractColumnNamesFromOrderingScheme(String row)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        if (isNullOrEmpty(row)) {
            return builder.build();
        }

        List<String> fields = SPLITTER.splitToList(row);
        for (int i = 0; i < fields.size(); i += 2) {
            String field = fields.get(i);
            checkArgument(!isNullOrEmpty(field), "Ignite column name is empty");
            builder.add(field.toLowerCase(ENGLISH));
        }

        return builder.build();
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters)
    {
        IgniteOutputTableHandle outputHandle = (IgniteOutputTableHandle) handle;
        String params = columnWriters.stream()
                .map(WriteFunction::getBindExpression)
                .collect(joining(","));
        String columns = handle.getColumnNames().stream().map(this::quoted).collect(joining(", "));
        if (outputHandle.dummyIdColumn().isPresent()) {
            String nextId = "CAST(UUID() AS VARCHAR), ";
            params = nextId + params;
            columns = quoted(outputHandle.dummyIdColumn().get()) + ", " + columns;
        }
        return format(
                "INSERT INTO %s (%s) VALUES (%s)",
                quoted(null, handle.getSchemaName(), handle.getTableName()),
                columns,
                params);
    }

    @Override
    public Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> joinConditions,
            Map<JdbcColumnHandle, String> rightAssignments,
            Map<JdbcColumnHandle, String> leftAssignments,
            JoinStatistics statistics)
    {
        // Ignite does not support FULL JOIN
        if (joinType == JoinType.FULL_OUTER) {
            return Optional.empty();
        }

        return super.implementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics);
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        return true;
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support truncating tables");
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }
}
