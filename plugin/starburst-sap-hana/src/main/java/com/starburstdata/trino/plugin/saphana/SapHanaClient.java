/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DoubleWriteFunction;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
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
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
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
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
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
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.SECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;

public class SapHanaClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(SapHanaClient.class);

    private static final JsonCodec<DataStatisticsContent> STATISTICS_CONTENT_JSON_CODEC = jsonCodec(DataStatisticsContent.class);

    private static final int SAP_HANA_CHAR_LENGTH_LIMIT = 2000;
    private static final int SAP_HANA_VARCHAR_LENGTH_LIMIT = 5000;
    static final int SAP_HANA_MAX_DECIMAL_PRECISION = 38;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("y-MM-dd[ G]");

    private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone(ZoneId.of("UTC"));

    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ParameterizedExpression> aggregateFunctionRewriter;
    private final boolean statisticsEnabled;

    @Inject
    public SapHanaClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, false);
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .withTypeClass("numeric_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint", "decimal", "real", "double"))
                .map("$equal(left: numeric_type, right: numeric_type)").to("left = right")
                .map("$not_equal(left: numeric_type, right: numeric_type)").to("left <> right")
                .map("$less_than(left: numeric_type, right: numeric_type)").to("left < right")
                .map("$less_than_or_equal(left: numeric_type, right: numeric_type)").to("left <= right")
                .map("$greater_than(left: numeric_type, right: numeric_type)").to("left > right")
                .map("$greater_than_or_equal(left: numeric_type, right: numeric_type)").to("left >= right")
                .add(new RewriteStringComparison())
                .build();

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.empty(), Optional.of(0), Optional.of(0), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, true))
                        .add(new ImplementMinMax(true))
                        .add(new ImplementSum(SapHanaClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgBigint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())
                        .build());
        this.statisticsEnabled = requireNonNull(statisticsConfig, "statisticsConfig is null").isEnabled();
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.DECIMAL, Optional.empty(), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
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

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String sql = format(
                "CREATE TABLE %s AS (SELECT %s FROM %s WHERE 0 = 1)",
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
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        if (column.getComment() != null) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns with comments");
        }

        try (Connection connection = this.connectionFactory.openConnection(session)) {
            String columnName = column.getName();
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                columnName = columnName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s ADD (%s)",
                    quoted(handle.asPlainTable().getRemoteTableName()),
                    this.getColumnDefinitionSql(session, column, columnName));
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        String sql = format(
                "ALTER TABLE %s DROP (%s)",
                quoted(handle.asPlainTable().getRemoteTableName()),
                quoted(column.getColumnName()));
        execute(session, sql);
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                newColumnName = newColumnName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "RENAME COLUMN %s.%s TO %s",
                    quoted(handle.asPlainTable().getRemoteTableName()),
                    quoted(jdbcColumn.getColumnName()),
                    quoted(newColumnName));
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
    protected void renameTable(ConnectorSession session, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            String newSchemaName = newTable.getSchemaName();
            String newTableName = newTable.getTableName();
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                newSchemaName = newSchemaName.toUpperCase(ENGLISH);
                newTableName = newTableName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "RENAME TABLE %s TO %s",
                    quoted(catalogName, schemaName, tableName),
                    quoted(catalogName, newSchemaName, newTableName));
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
    {
        // Don't return a comment until the connector supports creating tables with comment
        return Optional.empty();
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
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
        return implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.implementJoin(session, joinType, leftSource, leftProjections, rightSource, rightProjections, joinConditions, statistics));
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        return joinCondition.getOperator() != JoinCondition.Operator.IS_DISTINCT_FROM;
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

            case Types.REAL:
                return Optional.of(realColumnMapping());

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.DECIMAL:
                if (typeHandle.getDecimalDigits().isEmpty()) {
                    // SAP HANA's SMALLDECIMAL and DECIMAL fit this category
                    //
                    // If precision and scale are not specified, then DECIMAL becomes a floating-point decimal number.
                    // In this case, precision can vary within the range [1, 34] and scale within [-6,111, 6,176].
                    // It is reported as decimal(34,NULL) in JDBC (with type name "DECIMAL").
                    //
                    // The SMALLDECIMAL data type is a floating-point decimal number.
                    // The precision can vary within the range [1, 16] and scale within [-369, 368].
                    // It is reported as decimal(16,NULL) in JDBC (with type name "SMALLDECIMAL")
                    //
                    // It worth to underline, that comparing to other connectors, e.g. Postgresql or Oracle where DECIMAL are always fixed-point,
                    // in HANA, SMALLDECIMAL is always floating-point and DECIMAL in this situation becomes floating point, so it's safe (and ideal) to map them to a DOUBLE.
                    // point, so the best option to map them to a DOUBLE.
                    // Hovewer such mapping is not perfect because java's (and so Trino's) DOUBLE data type stores decimal values with 15-16 digits of precision and [-324, +308]
                    // scale which does not fully cover HANA's types
                    return Optional.of(doubleColumnMapping());
                }

                int precision = typeHandle.getRequiredColumnSize();
                int scale = typeHandle.getRequiredDecimalDigits();
                if (precision < 1 || precision > SAP_HANA_MAX_DECIMAL_PRECISION || scale < 0 || scale > precision) {
                    // SAP HANA supports precision [1, 38], and scale [0, precision]
                    log.warn("Unexpected decimal precision: %s", typeHandle);
                    return Optional.empty();
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, scale), UNNECESSARY));

            case Types.CHAR:
            case Types.NCHAR:
                verify(typeHandle.getRequiredColumnSize() < CharType.MAX_LENGTH, "Unexpected type: %s", typeHandle); // SAP HANA char is shorter than Presto's
                return Optional.of(charColumnMapping(createCharType(typeHandle.getRequiredColumnSize()), true));

            case Types.VARCHAR:
            case Types.NVARCHAR:
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), true));

            case Types.CLOB:
            case Types.NCLOB:
                VarcharType varcharType = createUnboundedVarcharType();
                return Optional.of(ColumnMapping.sliceMapping(
                        varcharType,
                        varcharReadFunction(varcharType),
                        varcharWriteFunction(),
                        DISABLE_PUSHDOWN));

            case Types.BLOB:
            case Types.VARBINARY:
                return Optional.of(varbinaryColumnMapping());

            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(
                        DATE,
                        (resultSet, index) -> LocalDate.parse(resultSet.getString(index), DATE_FORMATTER).toEpochDay(),
                        dateWriteFunctionUsingLocalDate()));

            case Types.TIME:
                return Optional.of(timeColumnMapping());

            case Types.TIMESTAMP:
                int timestampPrecision = typeHandle.getRequiredDecimalDigits();
                return Optional.of(timestampColumnMapping(timestampPrecision));
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    private ColumnMapping doubleColumnMapping()
    {
        return ColumnMapping.doubleMapping(
                DOUBLE,
                ResultSet::getDouble,
                new SapHanaDoubleWriteFunction());
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
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", new SapHanaDoubleWriteFunction());
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
            CharType charType = (CharType) type;
            if (charType.getLength() > SAP_HANA_CHAR_LENGTH_LIMIT) {
                return WriteMapping.sliceMapping("nclob", padSpacesWriteFunction(charType));
            }
            return WriteMapping.sliceMapping("char(" + charType.getLength() + ")", charWriteFunction());
        }

        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            // 5000 is the max length for nvarchar in SAP HANA
            if (varcharType.isUnbounded() || varcharType.getBoundedLength() > SAP_HANA_VARCHAR_LENGTH_LIMIT) {
                dataType = "clob"; // TODO NCLOB ?
            }
            else {
                dataType = "nvarchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (type == VARBINARY) {
            // SAP HANA `varbinary(n)` is limited to n=[1, 5000]
            return WriteMapping.sliceMapping("blob", varbinaryWriteFunction());
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
        }

        if (type instanceof TimeType) {
            // SAP HANA's TIME is not parametric
            return WriteMapping.longMapping("time", timeWriteFunction());
        }

        if (type instanceof TimestampType) {
            TimestampType timestampType = (TimestampType) type;
            if (timestampType.getPrecision() == 0) {
                return WriteMapping.longMapping("seconddate", seconddateWriteFunction());
            }

            if (timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION) {
                return WriteMapping.longMapping("timestamp", shortTimestampWriteFunction());
            }
            return WriteMapping.objectMapping("timestamp", longTimestampWriteFunction());
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    private static class SapHanaDoubleWriteFunction
            implements DoubleWriteFunction
    {
        @Override
        public String getBindExpression()
        {
            return "CAST(? AS DOUBLE)";
        }

        @Override
        public void set(PreparedStatement statement, int index, double value)
                throws SQLException
        {
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                // We want to fail with a meaningful error message since NaN and infinity are not supported by SAP HANA
                statement.setDouble(index, value);
            }
            else {
                // For all other values we avoid setDouble because SAP HANA JDBC driver tries to read them as a SAP HANA
                // DECIMAL without scale thus limiting the value to precision of 34.
                statement.setString(index, String.valueOf(value));
            }
        }
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
                        String nullsHandling = sortItem.getSortOrder().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                        return format("%s %s %s", quoted(sortItem.getColumn().getColumnName()), ordering, nullsHandling);
                    })
                    .collect(joining(", "));

            return format("%s ORDER BY %s LIMIT %d", query, orderBy, limit);
        });
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    private static ColumnMapping timeColumnMapping()
    {
        return ColumnMapping.longMapping(
                createTimeType(0), // SAP HANA's TIME does not support second fraction
                timeReadFunction(),
                timeWriteFunction());
    }

    private static LongReadFunction timeReadFunction()
    {
        return (resultSet, columnIndex) -> {
            Time time = resultSet.getTime(columnIndex, newUtcCalendar());

            long millis = time.getTime();

            verify(0 <= millis && millis < DAYS.toMillis(1), "Invalid millis value read: %s", millis);
            // SAP HANA's TIME is mapped to time(0)
            verify(millis % MILLISECONDS_PER_SECOND == 0, "Invalid millis value read: %s", millis);

            return millis * PICOSECONDS_PER_MILLISECOND;
        };
    }

    private static LongWriteFunction timeWriteFunction()
    {
        return (statement, index, picosOfDay) -> {
            // SAP HANA stores time with no second fraction
            // Round on Presto side so that rounding occurs consistently in INSERT and CTAS cases.
            long secondsOfDay = roundDiv(picosOfDay, PICOSECONDS_PER_SECOND);
            // Make it clear we wrap around from 23:59.59.5 to 00:00:00.
            secondsOfDay = secondsOfDay % SECONDS_PER_DAY;
            statement.setTime(index, new Time(secondsOfDay * MILLISECONDS_PER_SECOND), newUtcCalendar());
        };
    }

    private static ColumnMapping timestampColumnMapping(int precision)
    {
        TimestampType timestampType = createTimestampType(precision);

        if (precision <= 6) {
            return ColumnMapping.longMapping(
                    timestampType,
                    shortTimestampReadFunction(),
                    shortTimestampWriteFunction());
        }

        return ColumnMapping.objectMapping(
                timestampType,
                longTimestampReadFunction(),
                longTimestampWriteFunction());
    }

    private static LongReadFunction shortTimestampReadFunction()
    {
        ObjectReadFunction longTimestampReadFunction = longTimestampReadFunction();
        return (resultSet, columnIndex) -> {
            LongTimestamp timestamp = (LongTimestamp) longTimestampReadFunction.readObject(resultSet, columnIndex);
            verify(timestamp.getPicosOfMicro() == 0, "Unexpected picosOfMicro: %s", timestamp);
            return timestamp.getEpochMicros();
        };
    }

    private static ObjectReadFunction longTimestampReadFunction()
    {
        return ObjectReadFunction.of(LongTimestamp.class, (resultSet, columnIndex) -> {
            Timestamp timestamp = resultSet.getTimestamp(columnIndex, newUtcCalendar());

            long epochMillis = timestamp.getTime();
            int nanosOfSecond = timestamp.getNanos();
            int nanosOfMilli = nanosOfSecond % NANOSECONDS_PER_MILLISECOND;

            long epochMicros = epochMillis * MICROSECONDS_PER_MILLISECOND + nanosOfMilli / NANOSECONDS_PER_MICROSECOND;
            int picosOfMicro = nanosOfMilli % NANOSECONDS_PER_MICROSECOND * PICOSECONDS_PER_NANOSECOND;

            return new LongTimestamp(epochMicros, picosOfMicro);
        });
    }

    private static LongWriteFunction seconddateWriteFunction()
    {
        return (statement, index, epochMicros) -> {
            long epochSeconds = roundDiv(epochMicros, MICROSECONDS_PER_SECOND);
            Timestamp sqlTimestamp = new Timestamp(epochSeconds * MILLISECONDS_PER_SECOND);
            statement.setTimestamp(index, sqlTimestamp, newUtcCalendar());
        };
    }

    private static LongWriteFunction shortTimestampWriteFunction()
    {
        return (statement, index, epochMicros) -> {
            long epochSecond = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
            int nanosOfSecond = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;

            Timestamp sqlTimestamp = new Timestamp(epochSecond * MILLISECONDS_PER_SECOND);
            sqlTimestamp.setNanos(nanosOfSecond);
            statement.setTimestamp(index, sqlTimestamp, newUtcCalendar());
        };
    }

    private static ObjectWriteFunction longTimestampWriteFunction()
    {
        return ObjectWriteFunction.of(LongTimestamp.class, (statement, index, timestamp) -> {
            long epochSecond = floorDiv(timestamp.getEpochMicros(), MICROSECONDS_PER_SECOND);
            int nanosOfSecond = floorMod(timestamp.getEpochMicros(), MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND +
                    timestamp.getPicosOfMicro() / PICOSECONDS_PER_NANOSECOND;

            // Round on Presto side so that rounding occurs consistently in INSERT and CTAS cases.
            nanosOfSecond = toIntExact(round(nanosOfSecond, 9 /* value is in nanosecond */ - 7 /* max precision support by SAP HANA */));

            if (nanosOfSecond == NANOSECONDS_PER_SECOND) {
                epochSecond++;
                nanosOfSecond = 0;
            }

            Timestamp sqlTimestamp = new Timestamp(epochSecond * MILLISECONDS_PER_SECOND);
            sqlTimestamp.setNanos(nanosOfSecond);
            statement.setTimestamp(index, sqlTimestamp, newUtcCalendar());
        });
    }

    private static SliceWriteFunction padSpacesWriteFunction(CharType charType)
    {
        return (statement, index, value) -> statement.setString(index, Chars.padSpaces(value, charType).toStringUtf8());
    }

    // Note: allocating a new Calendar per row may turn out to be too expensive.
    private static Calendar newUtcCalendar()
    {
        Calendar calendar = new GregorianCalendar(UTC_TIME_ZONE, ENGLISH);
        calendar.setTime(new Date(0));
        return calendar;
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
        RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            String schemaName = remoteTableName.getSchemaName().orElseThrow();
            String tableName = remoteTableName.getTableName();

            StatisticsDao statisticsDao = new StatisticsDao(handle);
            Long rowCount = statisticsDao.getRowCount(schemaName, tableName);
            log.debug("Estimated row count of table %s is %s", table, rowCount);

            if (rowCount == null) {
                // Table not found, or is a view.
                return TableStatistics.empty();
            }

            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));

            if (rowCount == 0) {
                return tableStatistics.build();
            }

            Map<String, ColumnStatisticsResult> columnStatistics = statisticsDao.getColumnStatistics(schemaName, tableName, "SIMPLE", StatisticsDao::toColumnStatisticsResult).stream()
                    .collect(toImmutableMap(ColumnStatisticsResult::getColumnName, identity()));
            Map<String, ColumnStatisticsResult> columnStatisticsFromHistograms = statisticsDao.getColumnStatistics(schemaName, tableName, "HISTOGRAM", StatisticsDao::toColumnStatisticsResultFromHistogram).stream()
                    .collect(toImmutableMap(ColumnStatisticsResult::getColumnName, identity()));
            Map<String, ColumnStatisticsResult> columnStatisticsFromTopK = statisticsDao.getColumnStatistics(schemaName, tableName, "TOPK", StatisticsDao::toColumnStatisticsResult).stream()
                    .collect(toImmutableMap(ColumnStatisticsResult::getColumnName, identity()));

            if (columnStatistics.isEmpty() && columnStatisticsFromHistograms.isEmpty() && columnStatisticsFromTopK.isEmpty()) {
                // No more information to work on
                return tableStatistics.build();
            }

            for (JdbcColumnHandle column : getColumns(session, table)) {
                ColumnStatistics.Builder builder = ColumnStatistics.builder();

                ColumnStatisticsResult columnStatisticsResult = Stream
                        .of(columnStatistics.get(column.getColumnName()),
                                columnStatisticsFromHistograms.get(column.getColumnName()),
                                columnStatisticsFromTopK.get(column.getColumnName()))
                        .filter(Objects::nonNull)
                        .findFirst()
                        .orElse(null);

                if (columnStatisticsResult != null) {
                    builder.setDistinctValuesCount(columnStatisticsResult.getDistinctValuesCount().map(Estimate::of).orElseGet(Estimate::unknown));
                    Estimate nullsFraction = columnStatisticsResult.getNullsFraction().map(Estimate::of).orElseGet(Estimate::unknown);
                    builder.setNullsFraction(nullsFraction);
                    // set range statistics only for numeric columns
                    if (isNumericType(column.getColumnType()) && (columnStatisticsResult.getMin().isPresent() || columnStatisticsResult.getMax().isPresent())) {
                        builder.setRange(new DoubleRange(
                                columnStatisticsResult.getMin().map(BigDecimal::new).map(BigDecimal::doubleValue).orElse(Double.NEGATIVE_INFINITY),
                                columnStatisticsResult.getMax().map(BigDecimal::new).map(BigDecimal::doubleValue).orElse(Double.POSITIVE_INFINITY)));
                    }

                    // SAP HANA returns incorrect NDV if all values are NULL, so we correct the NDV if nulls fraction is 1.0
                    if (nullsFraction.equals(Estimate.of(1.0f))) {
                        builder.setDistinctValuesCount(Estimate.zero());
                    }

                    tableStatistics.setColumnStatistics(column, builder.build());
                }
            }

            return tableStatistics.build();
        }
    }

    private static boolean isNumericType(Type type)
    {
        return type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT || type == REAL || type == DOUBLE || type instanceof DecimalType;
    }

    private static class StatisticsDao
    {
        private static final String STATS_QUERY = "SELECT DATA_SOURCE_COLUMN_NAMES AS COLUMN_NAME, DATA_STATISTICS_CONTENT AS STATISTICS " +
                "FROM SYS.M_DATA_STATISTICS " +
                "WHERE DATA_STATISTICS_TYPE = :statistics_type " +
                "  AND DATA_SOURCE_SCHEMA_NAME = :schema " +
                "  AND DATA_SOURCE_OBJECT_NAME = :table_name";

        private final Handle handle;

        public StatisticsDao(Handle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        @Nullable
        Long getRowCount(String schema, String tableName)
        {
            Optional<Long> rowCount = handle.createQuery("" +
                    "SELECT RECORD_COUNT " +
                    "FROM SYS.M_TABLES " +
                    "WHERE SCHEMA_NAME = :schema " +
                    "  AND TABLE_NAME = :table_name")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .mapTo(Long.class)
                    .findOne();
            return rowCount.orElse(null);
        }

        List<ColumnStatisticsResult> getColumnStatistics(String schema, String tableName, String statisticsType, BiFunction<String, String, ColumnStatisticsResult> statsJsonToColumnStatisticsResult)
        {
            return handle.createQuery(STATS_QUERY)
                    .bind("statistics_type", statisticsType)
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .map((rs, ctx) -> {
                        String columnName = requireNonNull(rs.getString("COLUMN_NAME"), "COLUMN_NAME is null");
                        String statsJson = rs.getString("STATISTICS");

                        return statsJsonToColumnStatisticsResult.apply(columnName, statsJson);
                    })
                    .list();
        }

        private static ColumnStatisticsResult toColumnStatisticsResult(String columnName, String statsJson)
        {
            Optional<DataStatisticsContent> stats = Optional.empty();
            try {
                stats = Optional.of(STATISTICS_CONTENT_JSON_CODEC.fromJson(statsJson));
            }
            catch (RuntimeException e) {
                log.warn(e, "Failed to parse column statistics histogram: %s", statsJson);
            }

            if (stats.isPresent() && stats.get().lastRefreshProperties.isPresent()) {
                LastRefreshProperties props = stats.get().lastRefreshProperties.get();
                Optional<Float> nullFraction = calculateNullFraction(props.nullCount, props.count);
                return new ColumnStatisticsResult(columnName, props.distinctCount, nullFraction, props.minValue, props.maxValue);
            }

            return new ColumnStatisticsResult(columnName, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }

        private static ColumnStatisticsResult toColumnStatisticsResultFromHistogram(String columnName, String statsJson)
        {
            Optional<DataStatisticsContent> stats = Optional.empty();
            try {
                stats = Optional.of(STATISTICS_CONTENT_JSON_CODEC.fromJson(statsJson));
            }
            catch (RuntimeException e) {
                log.warn(e, "Failed to parse column statistics histogram: %s", statsJson);
            }

            Optional<Float> nullFraction = Optional.empty();
            Optional<Long> distinctCount = Optional.empty();
            Optional<String> min = Optional.empty();
            Optional<String> max = Optional.empty();

            if (stats.isPresent() && stats.get().lastRefreshProperties.isPresent()) {
                LastRefreshProperties props = stats.get().lastRefreshProperties.get();
                nullFraction = calculateNullFraction(props.nullCount, props.count);
                distinctCount = props.distinctCount;
            }

            if (stats.isPresent() && stats.get().statisticsContent.isPresent()) {
                StatisticsContent content = stats.get().statisticsContent.get();
                min = content.histogram.flatMap(histogram -> histogram.minValue);
                max = content.histogram.flatMap(histogram -> histogram.buckets.stream()
                        .map(bucket -> {
                            try {
                                return bucket.maxValue.map(BigDecimal::new).orElse(null);
                            }
                            catch (NumberFormatException ignored) {
                            }
                            return null;
                        })
                        .filter(Objects::nonNull)
                        .max(BigDecimal::compareTo)
                        .map(BigDecimal::toPlainString));
            }

            return new ColumnStatisticsResult(columnName, distinctCount, nullFraction, min, max);
        }

        private static Optional<Float> calculateNullFraction(Optional<Long> nullCount, Optional<Long> rowCount)
        {
            if (nullCount.isEmpty() || rowCount.isEmpty() || rowCount.get() == 0) {
                return Optional.empty();
            }

            // avoid (inexact) division so that an all nulls column can be detected by comparing against 1.0f
            if (nullCount.get().equals(rowCount.get())) {
                return Optional.of(1.0f);
            }

            return Optional.of((float) nullCount.get() / rowCount.get());
        }
    }

    public static class DataStatisticsContent
    {
        private final Optional<LastRefreshProperties> lastRefreshProperties;
        private final Optional<StatisticsContent> statisticsContent;

        @JsonCreator
        public DataStatisticsContent(
                @JsonProperty("LastRefreshProperties") Optional<LastRefreshProperties> lastRefreshProperties,
                @JsonProperty("StatisticsContent") Optional<StatisticsContent> statisticsContent)
        {
            this.lastRefreshProperties = requireNonNull(lastRefreshProperties, "lastRefreshProperties is null");
            this.statisticsContent = requireNonNull(statisticsContent, "statisticsContent is null");
        }
    }

    /**
     * Summarised statistics common for all statistics types.
     * See https://help.sap.com/viewer/4fe29514fd584807ac9f2a04f6754767/2.0.01/en-US/4f74378472cb46a6bbff3582b1863bac.html.
     */
    public static class LastRefreshProperties
    {
        private final Optional<Long> distinctCount;
        private final Optional<Long> nullCount;
        private final Optional<String> minValue;
        private final Optional<String> maxValue;
        private final Optional<Long> count;

        @JsonCreator
        public LastRefreshProperties(
                @JsonProperty("DISTINCT COUNT") Optional<String> distinctCount,
                @JsonProperty("NULL COUNT") Optional<String> nullCount,
                @JsonProperty("MIN VALUE") Optional<String> minValue,
                @JsonProperty("MAX VALUE") Optional<String> maxValue,
                @JsonProperty("COUNT") Optional<String> count,
                @JsonProperty("MIN MAX IS VALID") Optional<String> minMaxIsValid)
        {
            requireNonNull(distinctCount, "distinctCount is null");
            requireNonNull(nullCount, "nullCount is null");
            requireNonNull(minValue, "minValue is null");
            requireNonNull(maxValue, "maxValue is null");
            requireNonNull(count, "count is null");
            requireNonNull(minMaxIsValid, "minMaxIsValid is null");

            this.distinctCount = distinctCount.map(Long::valueOf);
            this.nullCount = nullCount.map(Long::valueOf);
            this.count = count.map(Long::valueOf);

            boolean isValid = minMaxIsValid.isPresent() && minMaxIsValid.get().equals("1");
            if (isValid) {
                this.minValue = minValue;
                this.maxValue = maxValue;
            }
            else {
                this.minValue = Optional.empty();
                this.maxValue = Optional.empty();
            }
        }
    }

    public static class StatisticsContent
    {
        private final Optional<Histogram> histogram;

        @JsonCreator
        public StatisticsContent(
                @JsonProperty("Histogram") Optional<Histogram> histogram)
        {
            this.histogram = requireNonNull(histogram, "histogram is null");
        }
    }

    public static class Histogram
    {
        private final Optional<String> minValue;
        private final List<Bucket> buckets;

        @JsonCreator
        public Histogram(
                @JsonProperty("MIN_VALUE") Optional<String> minValue,
                @JsonProperty("buckets") List<Bucket> buckets)
        {
            this.minValue = requireNonNull(minValue, "minValue is null");
            this.buckets = requireNonNull(buckets, "buckets is null");
        }
    }

    public static class Bucket
    {
        private final Optional<String> maxValue;

        @JsonCreator
        public Bucket(
                @JsonProperty("MAX_VALUE") Optional<String> maxValue)
        {
            this.maxValue = requireNonNull(maxValue, "maxValue is null");
        }
    }

    private static class ColumnStatisticsResult
    {
        private final String columnName;
        private final Optional<Long> distinctValuesCount;
        private final Optional<Float> nullsFraction;
        private final Optional<String> min;
        private final Optional<String> max;

        public ColumnStatisticsResult(
                String columnName,
                Optional<Long> distinctValuesCount,
                Optional<Float> nullsFraction,
                Optional<String> min,
                Optional<String> max)
        {
            this.columnName = requireNonNull(columnName, "columnName is null");
            this.distinctValuesCount = requireNonNull(distinctValuesCount, "distinctValuesCount is null");
            this.nullsFraction = requireNonNull(nullsFraction, "nullsFraction is null");
            this.min = requireNonNull(min, "min is null");
            this.max = requireNonNull(max, "max is null");
        }

        public String getColumnName()
        {
            return columnName;
        }

        public Optional<Long> getDistinctValuesCount()
        {
            return distinctValuesCount;
        }

        public Optional<Float> getNullsFraction()
        {
            return nullsFraction;
        }

        public Optional<String> getMin()
        {
            return min;
        }

        public Optional<String> getMax()
        {
            return max;
        }
    }

    @Override
    protected Optional<List<String>> getTableTypes()
    {
        return Optional.of(ImmutableList.of("TABLE", "VIEW", "CALC VIEW", "JOIN VIEW", "OLAP VIEW"));
    }
}
