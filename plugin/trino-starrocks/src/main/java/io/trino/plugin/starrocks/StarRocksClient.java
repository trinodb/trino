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
package io.trino.plugin.starrocks;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
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
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
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
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
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
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.starrocks.StarRocksTableProperties.DISTRIBUTION_DESC;
import static io.trino.plugin.starrocks.StarRocksTableProperties.REPLICATION_NUM;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.join;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.util.stream.Collectors.joining;

public class StarRocksClient
        extends BaseJdbcClient
{
    private static final JsonCodec<Map<String, String>> PROPERTIES_CODEC = JsonCodec.mapJsonCodec(String.class, String.class);
    private static final Splitter SPLITTER = Splitter.on("`").omitEmptyStrings().trimResults();
    private static final LocalDate MIN_SUPPORTED_DATE = LocalDate.parse("0000-01-01");
    private static final LocalDate MAX_SUPPORTED_DATE = LocalDate.parse("9999-12-31");

    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;
    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;

    @Inject
    public StarRocksClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("`", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, false);

        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .build();

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, false))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementSum(StarRocksClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgBigint())
                        .add(new ImplementAvgDecimal())
                        .build());
    }

    @Override
    protected void dropSchema(ConnectorSession session, Connection connection, String remoteSchemaName, boolean cascade)
            throws SQLException
    {
        if (cascade) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas with CASCADE option");
        }
        execute(session, connection, "DROP DATABASE " + quoted(remoteSchemaName));
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        // Remote database can be case insensitive.
        return preventTextualTypeAggregationPushdown(groupingSets);
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        // for StarRocks, we need to list catalogs instead of schemas
        try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CAT");
                // skip internal schemas
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
    public Optional<String> getTableComment(ResultSet resultSet)
    {
        // Don't return a comment until the connector supports creating tables with comment
        return Optional.empty();
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return resultSet.getString("TABLE_CAT");
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        if (tableMetadata.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with table comment");
        }

        Map<String, Object> tableProperties = tableMetadata.getProperties();
        StarRocksEngineType engine = StarRocksTableProperties.getEngine(tableProperties);
        List<String> distributionDesc = StarRocksTableProperties.getDistributionDesc(tableProperties);
        // OLAP engine type table requires distribution keys
        if (engine == StarRocksEngineType.OLAP && distributionDesc.isEmpty()) {
            List<ColumnMetadata> columnsMetadata = tableMetadata.getColumns();
            checkArgument(!columnsMetadata.isEmpty(), "Can not get column metadata");
            distributionDesc = ImmutableList.of(columnsMetadata.iterator().next().getName());
        }

        String createTableSql = "CREATE TABLE %s (%s) \n ENGINE = %s".formatted(
                quoted(remoteTableName),
                join(", ", columns),
                quoted(engine.getEngineType()));

        if (!distributionDesc.isEmpty()) {
            verifyDistributionDescExistsInTable(tableMetadata, distributionDesc);
            createTableSql += distributionDesc.stream()
                    .map(this::quoted)
                    .collect(joining(", ", "\n DISTRIBUTED BY HASH (", ")"));
        }

        createTableSql += buildCreateTableProperties(engine, tableProperties);

        return ImmutableList.of(createTableSql);
    }

    private void verifyDistributionDescExistsInTable(ConnectorTableMetadata tableMetadata, List<String> distributionDesc)
    {
        Set<String> columnNames = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableSet());
        for (String distributionKey : distributionDesc) {
            if (!columnNames.contains(distributionKey)) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, "Column '%s' specified in property '%s' doesn't exist in table".formatted(distributionKey, DISTRIBUTION_DESC));
            }
        }
    }

    private String buildCreateTableProperties(StarRocksEngineType engineType, Map<String, Object> tableProperties)
    {
        String tablePropertiesFormat = "\n PROPERTIES (%s)";
        ImmutableList.Builder<String> tablePropertiesBuilder = ImmutableList.builder();
        if (engineType == StarRocksEngineType.OLAP) {
            int replicationNum = StarRocksTableProperties.getReplicationNum(tableProperties);
            checkArgument(replicationNum > 0, "Replication number must greater than 0");
            tablePropertiesBuilder.add("\"%s\" = \"%d\"".formatted(REPLICATION_NUM, replicationNum));

            return tablePropertiesFormat.formatted(join(", ", tablePropertiesBuilder.build()));
        }

        return "";
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
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

            case Types.REAL:
                // Disable pushdown because floating-point values are approximate and not stored as exact values,
                // attempts to treat them as exact in comparisons may lead to problems
                return Optional.of(ColumnMapping.longMapping(
                        REAL,
                        (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)),
                        realWriteFunction(),
                        DISABLE_PUSHDOWN));

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.DECIMAL:
                int decimalDigits = typeHandle.getDecimalDigits().orElseThrow(() -> new IllegalStateException("decimal digits not present"));
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

            case Types.CHAR:
                return Optional.of(defaultCharColumnMapping(typeHandle.getRequiredColumnSize(), false));

            case Types.VARCHAR:
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), false));

            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(
                        DATE,
                        starRocksDateReadFunctionUsingLocalDate(),
                        starRocksDateWriteFunctionUsingLocalDate()));
        }

        return Optional.empty();
    }

    public static LongReadFunction starRocksDateReadFunctionUsingLocalDate()
    {
        return new LongReadFunction()
        {
            @Override
            public boolean isNull(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                resultSet.getObject(columnIndex, LocalDate.class);
                return resultSet.wasNull();
            }

            @Override
            public long readLong(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                LocalDate value = resultSet.getObject(columnIndex, LocalDate.class);
                if (value == null) {
                    throw new TrinoException(JDBC_ERROR, "Driver returned null LocalDate for a non-null value");
                }

                long days = value.toEpochDay();
                checkDateValue(days);
                return days;
            }
        };
    }

    private LongWriteFunction starRocksDateWriteFunctionUsingLocalDate()
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
                checkDateValue(epochDay);
                statement.setString(index, LocalDate.ofEpochDay(epochDay).format(ISO_DATE));
            }
        };
    }

    private static void checkDateValue(long epochDay)
    {
        if (epochDay < MIN_SUPPORTED_DATE.toEpochDay() || epochDay > MAX_SUPPORTED_DATE.toEpochDay()) {
            throw new TrinoException(INVALID_ARGUMENTS, "Date must be between %s and %s in StarRocks: %s".formatted(MIN_SUPPORTED_DATE, MAX_SUPPORTED_DATE, LocalDate.ofEpochDay(epochDay)));
        }
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
            return WriteMapping.doubleMapping("double", doubleWriteFunction());
        }
        if (type instanceof DecimalType decimalType) {
            String dataType = "decimal(%s, %s)".formatted(decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        if (type == DATE) {
            return WriteMapping.longMapping("date", starRocksDateWriteFunctionUsingLocalDate());
        }
        if (type instanceof CharType charType) {
            if (charType.getLength() <= 255) {
                return WriteMapping.sliceMapping("char(" + charType.getLength() + ")", charWriteFunction());
            }
        }
        if (type instanceof VarcharType varcharType) {
            if (!varcharType.isUnbounded() && varcharType.getBoundedLength() <= 1048576) {
                return WriteMapping.sliceMapping("varchar(" + varcharType.getBoundedLength() + ")", varcharWriteFunction());
            }
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        if (!tableHandle.isNamedRelation()) {
            return ImmutableMap.of();
        }

        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            RemoteTableName remoteTableName = tableHandle.getRequiredNamedRelation().getRemoteTableName();
            return handle.createQuery("SELECT TABLE_ENGINE, DISTRIBUTE_KEY, PROPERTIES FROM information_schema.tables_config " +
                            "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table_name")
                    .bind("schema", remoteTableName.getCatalogName().orElseThrow())
                    .bind("table_name", remoteTableName.getTableName())
                    .map((rs, ctx) -> {
                        ImmutableMap.Builder<String, Object> tablePropertiesBuilder = ImmutableMap.builder();
                        String engine = rs.getString("TABLE_ENGINE");
                        checkState(!rs.wasNull(), "ENGINE is null");
                        tablePropertiesBuilder.put("engine", StarRocksEngineType.valueOf(engine));

                        String distributionDesc = rs.getString("DISTRIBUTE_KEY");
                        if (!isNullOrEmpty(distributionDesc)) {
                            tablePropertiesBuilder.put("distribution_desc", extractDistributionKeys(distributionDesc));
                        }
                        Map<String, String> properties = PROPERTIES_CODEC.fromJson(rs.getString("PROPERTIES"));
                        if (properties.containsKey(REPLICATION_NUM)) {
                            tablePropertiesBuilder.put(REPLICATION_NUM, Integer.parseInt(properties.get(REPLICATION_NUM)));
                        }
                        return tablePropertiesBuilder.buildOrThrow();
                    })
                    .findOne()
                    .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Can not get table properties " + remoteTableName));
        }
        catch (SQLException exception) {
            throw new TrinoException(JDBC_ERROR, exception);
        }
    }

    private List<String> extractDistributionKeys(String distributionDesc)
    {
        List<String> distributionDescKeys = SPLITTER.splitToList(distributionDesc);
        ImmutableList.Builder<String> distributionDescKeysBuilder = ImmutableList.builder();
        for (int i = 0; i < distributionDescKeys.size(); i += 2) {
            distributionDescKeysBuilder.add(distributionDescKeys.get(i));
        }
        return distributionDescKeysBuilder.build();
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String sql = "CREATE TABLE %s LIKE %s".formatted(
                quoted(catalogName, schemaName, newTableName),
                quoted(catalogName, schemaName, tableName));
        try {
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        // StarRocks doesn't support specifying the catalog name in a rename. By setting the
        // catalogName parameter to null, it will be omitted in the ALTER TABLE statement.
        RemoteTableName remoteTableName = handle.asPlainTable().getRemoteTableName();
        verify(remoteTableName.getSchemaName().isEmpty());
        renameTable(session, null, remoteTableName.getCatalogName().orElse(null), remoteTableName.getTableName(), newTableName);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(
                new JdbcTypeHandle(
                        Types.DECIMAL,
                        Optional.of("decimal"),
                        Optional.of(decimalType.getPrecision()),
                        Optional.of(decimalType.getScale()),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName)
            throws SQLException
    {
        if (remoteSchemaName != null && !remoteSchemaName.equals(newRemoteSchemaName)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }

        connection.setCatalog(remoteSchemaName);
        execute(session, connection, "ALTER TABLE %s RENAME %s".formatted(remoteTableName, newRemoteTableName));
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE);
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support truncating tables");
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
                    .flatMap(sortItem -> {
                        String ordering = sortItem.getSortOrder().isAscending() ? "ASC" : "DESC";
                        String columnSorting = "%s %s".formatted(quoted(sortItem.getColumn().getColumnName()), ordering);

                        switch (sortItem.getSortOrder()) {
                            case ASC_NULLS_FIRST:
                            case DESC_NULLS_LAST:
                                return Stream.of(columnSorting);

                            case ASC_NULLS_LAST:
                                return Stream.of(
                                        "ISNULL(%s) ASC".formatted(quoted(sortItem.getColumn().getColumnName())),
                                        columnSorting);
                            case DESC_NULLS_FIRST:
                                return Stream.of(
                                        "ISNULL(%s) DESC".formatted(quoted(sortItem.getColumn().getColumnName())),
                                        columnSorting);
                        }
                        throw new UnsupportedOperationException("Unsupported sort order: " + sortItem.getSortOrder());
                    })
                    .collect(joining(", "));
            return "%s ORDER BY %s LIMIT %s".formatted(query, orderBy, limit);
        });
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        execute(session, "ALTER DATABASE %s RENAME %s".formatted(schemaName, newSchemaName));
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns");
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping a not null constraint");
    }

    @Override
    public OptionalLong update(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE);
    }
}
