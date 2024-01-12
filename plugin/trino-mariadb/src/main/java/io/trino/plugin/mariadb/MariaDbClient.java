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
package io.trino.plugin.mariadb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
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
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementStddevPop;
import io.trino.plugin.jdbc.aggregation.ImplementStddevSamp;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.aggregation.ImplementVariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementVarianceSamp;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateReadFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longTimestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryReadFunction;
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
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Map.entry;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class MariaDbClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(MariaDbClient.class);

    private static final int MAX_SUPPORTED_DATE_TIME_PRECISION = 6;
    // MariaDB driver returns width of time types instead of precision.
    private static final int ZERO_PRECISION_TIME_COLUMN_SIZE = 10;
    private static final int ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE = 19;

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd");

    // An empty character means that the table doesn't have a comment in MariaDB
    private static final String NO_COMMENT = "";

    // MariaDB Error Codes https://mariadb.com/kb/en/mariadb-error-codes/
    private static final int PARSE_ERROR = 1064;

    private final boolean statisticsEnabled;
    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;

    @Inject
    public MariaDbClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("`", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, false);

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                // No "real" on the list; pushdown on REAL is disabled also in toColumnMapping
                .withTypeClass("numeric_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint", "decimal", "double"))
                .map("$equal(left: numeric_type, right: numeric_type)").to("left = right")
                .map("$not_equal(left: numeric_type, right: numeric_type)").to("left <> right")
                // .map("$is_distinct_from(left: numeric_type, right: numeric_type)").to("left IS DISTINCT FROM right")
                .map("$less_than(left: numeric_type, right: numeric_type)").to("left < right")
                .map("$less_than_or_equal(left: numeric_type, right: numeric_type)").to("left <= right")
                .map("$greater_than(left: numeric_type, right: numeric_type)").to("left > right")
                .map("$greater_than_or_equal(left: numeric_type, right: numeric_type)").to("left >= right")
                .build();
        this.statisticsEnabled = statisticsConfig.isEnabled();
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementSum(MariaDbClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgBigint())
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())
                        .build());
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex ConnectorExpressions
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

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        // for MariaDB, we need to list catalogs instead of schemas
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
    protected boolean filterSchema(String schemaName)
    {
        // MariaDB has 'mysql' schema
        if (schemaName.equalsIgnoreCase("mysql")
                || schemaName.equalsIgnoreCase("performance_schema")) {
            return false;
        }
        return super.filterSchema(schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    @Override
    protected void dropSchema(ConnectorSession session, Connection connection, String remoteSchemaName, boolean cascade)
            throws SQLException
    {
        // MariaDB always deletes all tables inside the database https://mariadb.com/kb/en/drop-database/
        if (!cascade) {
            try (ResultSet tables = getTables(connection, Optional.of(remoteSchemaName), Optional.empty())) {
                if (tables.next()) {
                    throw new TrinoException(SCHEMA_NOT_EMPTY, "Cannot drop non-empty schema '%s'".formatted(remoteSchemaName));
                }
            }
        }
        execute(session, connection, "DROP SCHEMA " + quoted(remoteSchemaName));
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        // MariaDB maps their "database" to SQL catalogs and does not have schemas
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
        // MariaDB uses catalogs instead of schemas
        return resultSet.getString("TABLE_CAT");
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
            throws SQLException
    {
        // Empty remarks means that the table doesn't have a comment in MariaDB
        return Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        String sql = format(
                "ALTER TABLE %s COMMENT = %s",
                quoted(handle.asPlainTable().getRemoteTableName()),
                mariaDbVarcharLiteral(comment.orElse(NO_COMMENT))); // An empty character removes the existing comment in MariaDB
        execute(session, sql);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        Optional<ColumnMapping> unsignedMapping = getUnsignedMapping(typeHandle);
        if (unsignedMapping.isPresent()) {
            return unsignedMapping;
        }

        switch (typeHandle.getJdbcType()) {
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
            case Types.NUMERIC:
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
            case Types.CHAR:
                return Optional.of(defaultCharColumnMapping(typeHandle.getRequiredColumnSize(), false));
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), false));
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(ColumnMapping.sliceMapping(VARBINARY, varbinaryReadFunction(), varbinaryWriteFunction(), FULL_PUSHDOWN));
            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(
                        DATE,
                        dateReadFunctionUsingLocalDate(),
                        dateWriteFunction()));
            case Types.TIME:
                TimeType timeType = createTimeType(getTimePrecision(typeHandle.getRequiredColumnSize()));
                return Optional.of(timeColumnMapping(timeType));
            case Types.TIMESTAMP:
                // This jdbcType maps both MariaDB TIMESTAMP and DATETIME types to Trino TIMESTAMP type
                TimestampType timestampType = createTimestampType(getTimestampPrecision(typeHandle.getRequiredColumnSize()));
                return Optional.of(timestampColumnMapping(timestampType));
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    private static int getTimestampPrecision(int timestampColumnSize)
    {
        if (timestampColumnSize == ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE) {
            return 0;
        }
        int timestampPrecision = timestampColumnSize - ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE - 1;
        verify(1 <= timestampPrecision && timestampPrecision <= MAX_SUPPORTED_DATE_TIME_PRECISION, "Unexpected timestamp precision %s calculated from timestamp column size %s", timestampPrecision, timestampColumnSize);
        return timestampPrecision;
    }

    private static int getTimePrecision(int timeColumnSize)
    {
        if (timeColumnSize == ZERO_PRECISION_TIME_COLUMN_SIZE) {
            return 0;
        }
        int timePrecision = timeColumnSize - ZERO_PRECISION_TIME_COLUMN_SIZE - 1;
        verify(1 <= timePrecision && timePrecision <= MAX_SUPPORTED_DATE_TIME_PRECISION, "Unexpected time precision %s calculated from time column size %s", timePrecision, timeColumnSize);
        return timePrecision;
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
            return WriteMapping.sliceMapping("char(" + charType.getLength() + ")", charWriteFunction());
        }
        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "longtext";
            }
            else if (varcharType.getBoundedLength() <= 255) {
                dataType = "tinytext";
            }
            else if (varcharType.getBoundedLength() <= 65535) {
                dataType = "text";
            }
            else if (varcharType.getBoundedLength() <= 16777215) {
                dataType = "mediumtext";
            }
            else {
                dataType = "longtext";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type == VARBINARY) {
            return WriteMapping.sliceMapping("mediumblob", varbinaryWriteFunction());
        }
        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunction());
        }
        if (type instanceof TimeType timeType) {
            if (timeType.getPrecision() <= MAX_SUPPORTED_DATE_TIME_PRECISION) {
                return WriteMapping.longMapping(format("time(%s)", timeType.getPrecision()), timeWriteFunction(timeType.getPrecision()));
            }
            return WriteMapping.longMapping(format("time(%s)", MAX_SUPPORTED_DATE_TIME_PRECISION), timeWriteFunction(MAX_SUPPORTED_DATE_TIME_PRECISION));
        }
        if (type instanceof TimestampType timestampType) {
            if (timestampType.getPrecision() <= MAX_SUPPORTED_DATE_TIME_PRECISION) {
                verify(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION);
                return WriteMapping.longMapping(format("timestamp(%s)", timestampType.getPrecision()), timestampWriteFunction(timestampType));
            }
            return WriteMapping.objectMapping(format("timestamp(%s)", MAX_SUPPORTED_DATE_TIME_PRECISION), longTimestampWriteFunction(timestampType, MAX_SUPPORTED_DATE_TIME_PRECISION));
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    protected void renameColumn(ConnectorSession session, Connection connection, RemoteTableName remoteTableName, String remoteColumnName, String newRemoteColumnName)
            throws SQLException
    {
        try {
            // MariaDB versions earlier than 10.5.2 do not support the RENAME COLUMN syntax
            // ALTER TABLE ... CHANGE statement exists in th old versions, but it requires providing all attributes of the column
            String sql = format(
                    "ALTER TABLE %s RENAME COLUMN %s TO %s",
                    quoted(remoteTableName.getCatalogName().orElse(null), remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName()),
                    quoted(remoteColumnName),
                    quoted(newRemoteColumnName));
            execute(session, connection, sql);
        }
        catch (SQLSyntaxErrorException syntaxError) {
            // Note: SQLSyntaxErrorException can be thrown also when column name is invalid
            if (syntaxError.getErrorCode() == PARSE_ERROR) {
                throw new TrinoException(NOT_SUPPORTED, "Rename column not supported for the MariaDB server version", syntaxError);
            }
            throw syntaxError;
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
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        // Copy all columns for enforcing NOT NULL option in the temp table
        String tableCopyFormat = "CREATE TABLE %s AS SELECT * FROM %s WHERE 0 = 1";
        String sql = format(
                tableCopyFormat,
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
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        checkArgument(tableMetadata.getProperties().isEmpty(), "Unsupported table properties: %s", tableMetadata.getProperties());
        return ImmutableList.of(format("CREATE TABLE %s (%s) COMMENT %s", quoted(remoteTableName), join(", ", columns), mariaDbVarcharLiteral(tableMetadata.getComment().orElse(NO_COMMENT))));
    }

    private static String mariaDbVarcharLiteral(String value)
    {
        requireNonNull(value, "value is null");
        return "'" + value.replace("'", "''").replace("\\", "\\\\") + "'";
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        // MariaDB doesn't support specifying the catalog name in a rename. By setting the
        // catalogName parameter to null, it will be omitted in the ALTER TABLE statement.
        RemoteTableName remoteTableName = handle.asPlainTable().getRemoteTableName();
        verify(remoteTableName.getSchemaName().isEmpty());
        renameTable(session, null, remoteTableName.getCatalogName().orElse(null), remoteTableName.getTableName(), newTableName);
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
        for (JdbcSortItem sortItem : sortOrder) {
            Type sortItemType = sortItem.getColumn().getColumnType();
            if (sortItemType instanceof CharType || sortItemType instanceof VarcharType) {
                // Remote database can be case insensitive.
                return false;
            }
        }
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream()
                    .flatMap(sortItem -> {
                        String ordering = sortItem.getSortOrder().isAscending() ? "ASC" : "DESC";
                        String columnSorting = format("%s %s", quoted(sortItem.getColumn().getColumnName()), ordering);

                        switch (sortItem.getSortOrder()) {
                            case ASC_NULLS_FIRST:
                                // In MariaDB ASC implies NULLS FIRST
                            case DESC_NULLS_LAST:
                                // In MariaDB DESC implies NULLS LAST
                                return Stream.of(columnSorting);

                            case ASC_NULLS_LAST:
                                return Stream.of(
                                        format("ISNULL(%s) ASC", quoted(sortItem.getColumn().getColumnName())),
                                        columnSorting);
                            case DESC_NULLS_FIRST:
                                return Stream.of(
                                        format("ISNULL(%s) DESC", quoted(sortItem.getColumn().getColumnName())),
                                        columnSorting);
                        }
                        throw new UnsupportedOperationException("Unsupported sort order: " + sortItem.getSortOrder());
                    })
                    .collect(joining(", "));
            return format("%s ORDER BY %s LIMIT %s", query, orderBy, limit);
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
            // Not supported in MariaDB
            return Optional.empty();
        }
        return super.implementJoin(session, joinType, leftSource, leftProjections, rightSource, rightProjections, joinConditions, statistics);
    }

    @Override
    public Optional<PreparedQuery> legacyImplementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> joinConditions,
            Map<JdbcColumnHandle, String> rightAssignments,
            Map<JdbcColumnHandle, String> leftAssignments,
            JoinStatistics statistics)
    {
        if (joinType == JoinType.FULL_OUTER) {
            // Not supported in MariaDB
            return Optional.empty();
        }
        return super.legacyImplementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics);
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        if (joinCondition.getOperator() == JoinCondition.Operator.IS_DISTINCT_FROM) {
            // Not supported in MariaDB
            return false;
        }

        // Remote database can be case insensitive.
        return Stream.of(joinCondition.getLeftColumn(), joinCondition.getRightColumn())
                .map(JdbcColumnHandle::getColumnType)
                .noneMatch(type -> type instanceof CharType || type instanceof VarcharType);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
    {
        if (!statisticsEnabled) {
            return TableStatistics.empty();
        }
        if (!handle.isNamedRelation()) {
            return TableStatistics.empty();
        }
        try {
            return readTableStatistics(session, handle);
        }
        catch (SQLException | RuntimeException e) {
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(JDBC_ERROR, "Failed fetching statistics for table: " + handle, e);
        }
    }

    private TableStatistics readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

        log.debug("Reading statistics for %s", table);
        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            StatisticsDao statisticsDao = new StatisticsDao(handle);

            Long rowCount = statisticsDao.getTableRowCount(table);
            Long indexMaxCardinality = statisticsDao.getTableMaxColumnIndexCardinality(table);
            log.debug("Estimated row count of table %s is %s, and max index cardinality is %s", table, rowCount, indexMaxCardinality);

            if (rowCount != null && rowCount == 0) {
                // MariaDB may report 0 row count until a table is analyzed for the first time.
                rowCount = null;
            }

            if (rowCount == null && indexMaxCardinality == null) {
                // Table not found, or is a view, or has no usable statistics
                return TableStatistics.empty();
            }
            rowCount = max(firstNonNull(rowCount, 0L), firstNonNull(indexMaxCardinality, 0L));

            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));

            // TODO statistics from ANALYZE TABLE (https://mariadb.com/kb/en/engine-independent-table-statistics/)
            // Map<String, AnalyzeColumnStatistics> columnStatistics = statisticsDao.getColumnStatistics(table);
            Map<String, AnalyzeColumnStatistics> columnStatistics = ImmutableMap.of();

            // TODO add support for histograms https://mariadb.com/kb/en/histogram-based-statistics/

            // statistics based on existing indexes
            Map<String, ColumnIndexStatistics> columnStatisticsFromIndexes = statisticsDao.getColumnIndexStatistics(table);

            if (columnStatistics.isEmpty() && columnStatisticsFromIndexes.isEmpty()) {
                log.debug("No column and index statistics read");
                // No more information to work on
                return tableStatistics.build();
            }

            for (JdbcColumnHandle column : getColumns(session, table)) {
                ColumnStatistics.Builder columnStatisticsBuilder = ColumnStatistics.builder();

                String columnName = column.getColumnName();
                AnalyzeColumnStatistics analyzeColumnStatistics = columnStatistics.get(columnName);
                if (analyzeColumnStatistics != null) {
                    log.debug("Reading column statistics for %s, %s from analayze's column statistics: %s", table, columnName, analyzeColumnStatistics);
                    columnStatisticsBuilder.setNullsFraction(Estimate.of(analyzeColumnStatistics.nullsRatio()));
                }

                ColumnIndexStatistics columnIndexStatistics = columnStatisticsFromIndexes.get(columnName);
                if (columnIndexStatistics != null) {
                    log.debug("Reading column statistics for %s, %s from index statistics: %s", table, columnName, columnIndexStatistics);
                    columnStatisticsBuilder.setDistinctValuesCount(Estimate.of(columnIndexStatistics.cardinality()));

                    if (!columnIndexStatistics.nullable()) {
                        double knownNullFraction = columnStatisticsBuilder.build().getNullsFraction().getValue();
                        if (knownNullFraction > 0) {
                            log.warn("Inconsistent statistics, null fraction for a column %s, %s, that is not nullable according to index statistics: %s", table, columnName, knownNullFraction);
                        }
                        columnStatisticsBuilder.setNullsFraction(Estimate.zero());
                    }

                    // row count from INFORMATION_SCHEMA.TABLES may be very inaccurate
                    rowCount = max(rowCount, columnIndexStatistics.cardinality());
                }

                tableStatistics.setColumnStatistics(column, columnStatisticsBuilder.build());
            }

            tableStatistics.setRowCount(Estimate.of(rowCount));
            return tableStatistics.build();
        }
    }

    private static LongWriteFunction dateWriteFunction()
    {
        return (statement, index, day) -> statement.setString(index, DATE_FORMATTER.format(LocalDate.ofEpochDay(day)));
    }

    private static Optional<ColumnMapping> getUnsignedMapping(JdbcTypeHandle typeHandle)
    {
        if (typeHandle.getJdbcTypeName().isEmpty()) {
            return Optional.empty();
        }

        String typeName = typeHandle.getJdbcTypeName().get();
        if (typeName.equalsIgnoreCase("tinyint unsigned")) {
            return Optional.of(smallintColumnMapping());
        }
        if (typeName.equalsIgnoreCase("smallint unsigned")) {
            return Optional.of(integerColumnMapping());
        }
        if (typeName.equalsIgnoreCase("int unsigned")) {
            return Optional.of(bigintColumnMapping());
        }
        if (typeName.equalsIgnoreCase("bigint unsigned")) {
            return Optional.of(decimalColumnMapping(createDecimalType(20)));
        }

        return Optional.empty();
    }

    private static class StatisticsDao
    {
        private final Handle handle;

        public StatisticsDao(Handle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        Long getTableRowCount(JdbcTableHandle table)
        {
            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            return handle.createQuery("""
                        SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table_name
                        AND TABLE_TYPE = 'BASE TABLE'
                        """)
                    .bind("schema", remoteTableName.getCatalogName().orElse(null))
                    .bind("table_name", remoteTableName.getTableName())
                    .mapTo(Long.class)
                    .findOne()
                    .orElse(null);
        }

        Long getTableMaxColumnIndexCardinality(JdbcTableHandle table)
        {
            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            return handle.createQuery("""
                        SELECT max(CARDINALITY) AS row_count FROM INFORMATION_SCHEMA.STATISTICS
                        WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table_name
                        """)
                    .bind("schema", remoteTableName.getCatalogName().orElse(null))
                    .bind("table_name", remoteTableName.getTableName())
                    .mapTo(Long.class)
                    .findOne()
                    .orElse(null);
        }

        Map<String, AnalyzeColumnStatistics> getColumnStatistics(JdbcTableHandle table)
        {
            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            return handle.createQuery("""
                                SELECT
                                    column_name,
                                    -- TODO min_value, max_value,
                                    nulls_ratio
                                FROM mysql.column_stats
                                WHERE db_name = :database AND TABLE_NAME = :table_name
                                AND nulls_ratio IS NOT NULL
                            """)
                    .bind("database", remoteTableName.getCatalogName().orElse(null))
                    .bind("table_name", remoteTableName.getTableName())
                    .map((rs, ctx) -> {
                        String columnName = rs.getString("column_name");
                        double nullsRatio = rs.getDouble("nulls_ratio");
                        return entry(columnName, new AnalyzeColumnStatistics(nullsRatio));
                    })
                    .collect(toImmutableMap(Entry::getKey, Entry::getValue));
        }

        Map<String, ColumnIndexStatistics> getColumnIndexStatistics(JdbcTableHandle table)
        {
            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            return handle.createQuery("""
                                SELECT
                                    COLUMN_NAME,
                                    MAX(NULLABLE) AS NULLABLE,
                                    MAX(CARDINALITY) AS CARDINALITY
                                FROM INFORMATION_SCHEMA.STATISTICS
                                WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table_name
                                AND SEQ_IN_INDEX = 1 -- first column in the index
                                AND SUB_PART IS NULL -- ignore cases where only a column prefix is indexed
                                AND CARDINALITY IS NOT NULL -- CARDINALITY might be null (https://stackoverflow.com/a/42242729/65458)
                                AND CARDINALITY != 0 -- CARDINALITY is initially 0 until analyzed
                                GROUP BY COLUMN_NAME -- there might be multiple indexes on a column
                            """)
                    .bind("schema", remoteTableName.getCatalogName().orElse(null))
                    .bind("table_name", remoteTableName.getTableName())
                    .map((rs, ctx) -> {
                        String columnName = rs.getString("COLUMN_NAME");

                        boolean nullable = rs.getString("NULLABLE").equalsIgnoreCase("YES");
                        checkState(!rs.wasNull(), "NULLABLE is null");

                        long cardinality = rs.getLong("CARDINALITY");
                        checkState(!rs.wasNull(), "CARDINALITY is null");

                        return entry(columnName, new ColumnIndexStatistics(nullable, cardinality));
                    })
                    .collect(toImmutableMap(Entry::getKey, Entry::getValue));
        }
    }

    private record AnalyzeColumnStatistics(double nullsRatio) {}

    private record ColumnIndexStatistics(boolean nullable, long cardinality) {}
}
