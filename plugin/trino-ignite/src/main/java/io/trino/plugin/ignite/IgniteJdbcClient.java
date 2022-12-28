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
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.plugin.ignite.IgniteTableProperties.AFFINITY_KEY_PROPERTY;
import static io.trino.plugin.ignite.IgniteTableProperties.BACKUPS_PROPERTY;
import static io.trino.plugin.ignite.IgniteTableProperties.CACHE_GROUP_PROPERTY;
import static io.trino.plugin.ignite.IgniteTableProperties.CACHE_NAME_PROPERTY;
import static io.trino.plugin.ignite.IgniteTableProperties.DATA_REGION_PROPERTY;
import static io.trino.plugin.ignite.IgniteTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.plugin.ignite.IgniteTableProperties.TEMPLATE_PROPERTY;
import static io.trino.plugin.ignite.IgniteTableProperties.WRITE_SYNCHRONIZATION_MODE_PROPERTY;
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
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
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
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
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

public class IgniteJdbcClient
        extends BaseJdbcClient
{
    private static final String IGNITE_SCHEMA = "PUBLIC";
    private static final Splitter SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

    private final ConnectorExpressionRewriter<String> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, String> aggregateFunctionRewriter;

    @Inject
    public IgniteJdbcClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super(config, "`", connectionFactory, queryBuilder, identifierMapping, queryModifier);

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .build();
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, String>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax(true))
                        .add(new ImplementSum(IgniteJdbcClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgBigint())
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
                IGNITE_SCHEMA,
                tableName.orElse(null),
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
            case Types.CHAR:
                return Optional.of(charColumnMapping(typeHandle.getColumnSize()));
            case Types.VARCHAR:
                return Optional.of(varcharColumnMapping(typeHandle.getColumnSize()));
            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(
                        DATE,
                        dateReadFunction(),
                        dateWriteFunction()));
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

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        if (type instanceof CharType) {
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction());
        }
        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
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

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.DECIMAL, Optional.of("Decimal"),
                Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    public static ColumnMapping varcharColumnMapping(Optional<Integer> columnSize)
    {
        VarcharType varcharType = columnSize
                // default added column in Ignite will has Integer.MAX_VALUE length which not allow in Trino, will consider the column as varchar
                .filter(size -> size != UNBOUNDED_LENGTH)
                .map(VarcharType::createVarcharType)
                .orElse(createUnboundedVarcharType());
        return ColumnMapping.sliceMapping(varcharType, varcharReadFunction(varcharType), varcharWriteFunction(), FULL_PUSHDOWN);
    }

    private static ColumnMapping charColumnMapping(Optional<Integer> charLength)
    {
        if (charLength.isEmpty() || charLength.get() > CharType.MAX_LENGTH) {
            return varcharColumnMapping(charLength);
        }
        CharType charType = createCharType(charLength.get());
        return ColumnMapping.sliceMapping(
                charType,
                charReadFunction(charType),
                charWriteFunction());
    }

    public static LongReadFunction dateReadFunction()
    {
        return (resultSet, columnIndex) -> {
            long localMillis = resultSet.getDate(columnIndex).getTime();
            return MILLISECONDS.toDays(localMillis);
        };
    }

    public static LongWriteFunction dateWriteFunction()
    {
        return (statement, index, value) -> {
            long millis = DAYS.toMillis(value);
            statement.setDate(index, new Date(millis));
        };
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

    @Override
    protected String createTableSql(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        if (tableMetadata.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with table comment");
        }
        return createTableSql(quoted(remoteTableName), columns, tableMetadata.getProperties());
    }

    private String createTableSql(String remoteTableName, List<String> columns, Map<String, Object> tableProperties)
    {
        ImmutableList.Builder<String> tableOptions = ImmutableList.builder();
        ImmutableList.Builder<String> columnDefinitions = ImmutableList.builder();
        columnDefinitions.addAll(columns);

        List<String> primaryKeys = IgniteTableProperties.getPrimaryKey(tableProperties);
        checkArgument(primaryKeys != null && !primaryKeys.isEmpty(), "No primary key defined for create table");
        checkArgument(primaryKeys.size() < columns.size(), "Ignite table must have at least one non PRIMARY KEY column.");
        columnDefinitions.add("PRIMARY KEY (" + join(", ", primaryKeys.stream().map(this::quoted).collect(joining(", "))) + ")");

        for (Map.Entry<String, Object> propertyEntry : tableProperties.entrySet()) {
            String propertyKey = propertyEntry.getKey();
            switch (propertyKey) {
                case PRIMARY_KEY_PROPERTY:
                    break;
                case AFFINITY_KEY_PROPERTY:
                    String affinityKey = IgniteTableProperties.getAffinityKey(tableProperties);
                    if (!isNullOrEmpty(affinityKey)) {
                        checkArgument(ImmutableSet.copyOf(primaryKeys).contains(affinityKey), "Affinity key should be one of the primary key");
                        tableOptions.add(propertyKey + " = " + affinityKey);
                    }
                    break;
                case BACKUPS_PROPERTY:
                case TEMPLATE_PROPERTY:
                case CACHE_GROUP_PROPERTY:
                case CACHE_NAME_PROPERTY:
                case DATA_REGION_PROPERTY:
                case WRITE_SYNCHRONIZATION_MODE_PROPERTY:
                    tableOptions.add(propertyKey + " = " + propertyEntry.getValue());
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + propertyKey);
            }
        }
        List<String> withProperties = tableOptions.build();

        String createTableSql = format("CREATE TABLE %s (%s) ", remoteTableName, join(", ", columnDefinitions.build()));
        // If the table properties all are default value
        if (withProperties.isEmpty()) {
            return createTableSql;
        }
        return createTableSql + format(" WITH \" %s \"", join(", ", withProperties));
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
                        return format("%s, %s", format("%s %s", columnName, nullsHandling), format("%s %s", columnName, ordering));
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
        String tableName = requireNonNull(schemaTableName.getTableName(), "Ignite table name can not be null").toUpperCase(ENGLISH);
        String sql = format("SELECT idx.CACHE_ID, " +
                "che.CACHE_MODE AS TEMPLATE, " +
                "che.WRITE_SYNCHRONIZATION_MODE, " +
                "che.ATOMICITY_MODE AS ATOMICITY, " +
                "che.DATA_REGION_NAME AS DATA_REGION, " +
                "che.CACHE_GROUP_NAME AS CACHE_GROUP, " +
                "che.CACHE_NAME, " +
                "che.BACKUPS, " +
                "(SELECT COLUMNS FROM sys.indexes WHERE SCHEMA_NAME = 'PUBLIC' AND TABLE_NAME = '%s' AND INDEX_NAME = '_key_PK') AS PKS," +
                "(SELECT COLUMNS FROM sys.indexes WHERE SCHEMA_NAME = 'PUBLIC' AND TABLE_NAME = '%1$s' AND INDEX_NAME = 'AFFINITY_KEY') AS AFK FROM sys.indexes as idx " +
                "JOIN sys.caches che ON idx.CACHE_ID = che.CACHE_ID WHERE idx.SCHEMA_NAME = 'PUBLIC' AND idx.TABLE_NAME = '%1$s' LIMIT 1", tableName);

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
                ResultSet resultSet = preparedStatement.executeQuery()) {
            if (!resultSet.next()) {
                return ImmutableMap.of();
            }
            List<String> primaryKeys = extractColumnNamesFromOrderingScheme(resultSet.getString("PKS"));
            checkArgument(!primaryKeys.isEmpty(), "Ignite table should has at least one primary key");
            properties.put(PRIMARY_KEY_PROPERTY, primaryKeys);

            for (String property : IgniteTableProperties.WITH_PROPERTIES) {
                switch (property) {
                    case AFFINITY_KEY_PROPERTY:
                        List<String> affinityKeys = extractColumnNamesFromOrderingScheme(resultSet.getString("AFK"));
                        if (!affinityKeys.isEmpty()) {
                            String affinityKey = affinityKeys.get(0);
                            checkArgument(ImmutableSet.copyOf(primaryKeys).contains(affinityKey), "Table affinity key should be one of the primary key");
                            properties.put(property, affinityKey);
                        }
                        break;
                    case BACKUPS_PROPERTY:
                        int backups = resultSet.getInt(property.toUpperCase(ENGLISH));
                        properties.put(property, backups);
                        break;
                    case TEMPLATE_PROPERTY:
                        Optional.ofNullable(resultSet.getString(property.toUpperCase(ENGLISH)))
                                .map(IgniteTableProperties.IgniteTemplateType::valueOf)
                                .ifPresent(value -> properties.put(property, value));
                        break;
                    case WRITE_SYNCHRONIZATION_MODE_PROPERTY:
                        Optional.ofNullable(resultSet.getString(property.toUpperCase(ENGLISH)))
                                .map(IgniteTableProperties.IgniteWriteSyncMode::valueOf)
                                .ifPresent(value -> properties.put(property, value));
                        break;
                    case CACHE_NAME_PROPERTY:
                    case CACHE_GROUP_PROPERTY:
                    case DATA_REGION_PROPERTY:
                        Optional.ofNullable(resultSet.getString(property.toUpperCase(ENGLISH)))
                                .ifPresent(value -> properties.put(property, value));
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + property);
                }
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
        for (String key : SPLITTER.split(row)) {
            int left = key.indexOf("\"") + 1;
            int right = key.lastIndexOf("\"");
            builder.add(key.substring(left, right).toLowerCase(ENGLISH));
        }
        return builder.build();
    }

    /**
     * Ignite table must follow the rules:
     * 1. At least has one primary key
     * 2. At least has one non-primary, or non-affinity key column.
     * While Trino process transactional insert, will use this method create a temporary table, but the columns might
     * be the part of the table's. Therefore, we override the method to rebuild the creation temporary table sql to make
     * sure temporary table create successfully.
     */
    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        JdbcTableHandle tableHandle = getTableHandle(session, schemaTableName).orElseThrow();
        List<JdbcColumnHandle> columns = getColumns(session, tableHandle);
        copyTableSchema(session, connection, newTableName, columns, tableHandle);
    }

    protected void copyTableSchema(ConnectorSession session, Connection connection, String newTableName, List<JdbcColumnHandle> columns, JdbcTableHandle tableHandle)
    {
        ImmutableList.Builder<String> columnList = ImmutableList.builder();
        for (JdbcColumnHandle column : columns) {
            columnList.add(getColumnDefinitionSql(null, column.getColumnMetadata(), column.getColumnName()));
        }
        // when create temporary table, the table may be only contain primary keys, it's not allow in Ignite.
        // so we append a dummy column to make create temporary table won't fail because of this.
        columnList.add("`for_trino_ignore` varchar(1)");
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        Map<String, Object> tableProperties = getTableProperties(connection, tableHandle);
        // create temporary table only need the primary key
        properties.put(PRIMARY_KEY_PROPERTY, IgniteTableProperties.getPrimaryKey(tableProperties));

        String createTableSql = createTableSql(newTableName, columnList.build(), properties.buildOrThrow());
        try {
            execute(session, connection, createTableSql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support modifying table rows");
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support truncating tables");
    }
}
