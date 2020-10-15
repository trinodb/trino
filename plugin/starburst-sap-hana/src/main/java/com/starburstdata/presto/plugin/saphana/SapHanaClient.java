/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcExpression;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.LongReadFunction;
import io.prestosql.plugin.jdbc.LongWriteFunction;
import io.prestosql.plugin.jdbc.ObjectReadFunction;
import io.prestosql.plugin.jdbc.ObjectWriteFunction;
import io.prestosql.plugin.jdbc.SliceWriteFunction;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRewriter;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRule;
import io.prestosql.plugin.jdbc.expression.ImplementAvgDecimal;
import io.prestosql.plugin.jdbc.expression.ImplementAvgFloatingPoint;
import io.prestosql.plugin.jdbc.expression.ImplementCount;
import io.prestosql.plugin.jdbc.expression.ImplementCountAll;
import io.prestosql.plugin.jdbc.expression.ImplementMinMax;
import io.prestosql.plugin.jdbc.expression.ImplementSum;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.Chars;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.BiFunction;

import static com.google.common.base.Verify.verify;
import static io.prestosql.plugin.jdbc.ColumnMapping.DISABLE_PUSHDOWN;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.charColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.dateColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.dateWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.createTimeType;
import static io.prestosql.spi.type.TimestampType.createTimestampType;
import static io.prestosql.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.prestosql.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.prestosql.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.prestosql.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.prestosql.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.prestosql.spi.type.Timestamps.NANOSECONDS_PER_SECOND;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static io.prestosql.spi.type.Timestamps.SECONDS_PER_DAY;
import static io.prestosql.spi.type.Timestamps.round;
import static io.prestosql.spi.type.Timestamps.roundDiv;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.stream.Collectors.joining;

public class SapHanaClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(SapHanaClient.class);

    private static final int SAP_HANA_CHAR_LENGTH_LIMIT = 2000;
    private static final int SAP_HANA_VARCHAR_LENGTH_LIMIT = 5000;

    private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");

    private final AggregateFunctionRewriter aggregateFunctionRewriter;

    @Inject
    public SapHanaClient(BaseJdbcConfig baseJdbcConfig, ConnectionFactory connectionFactory)
    {
        super(baseJdbcConfig, "\"", connectionFactory);

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.empty(), 0, 0, Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter(
                this::quoted,
                ImmutableSet.<AggregateFunctionRule>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax())
                        .add(new ImplementSum(SapHanaClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgBigint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementStddev())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVariance())
                        .add(new ImplementVariancePop())
                        .build());
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.DECIMAL, Optional.empty(), decimalType.getPrecision(), decimalType.getScale(), Optional.empty()));
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    protected void copyTableSchema(Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String sql = format(
                "CREATE TABLE %s AS (SELECT %s FROM %s WHERE 0 = 1)",
                quoted(catalogName, schemaName, newTableName),
                columnNames.stream()
                        .map(this::quoted)
                        .collect(joining(", ")),
                quoted(catalogName, schemaName, tableName));
        execute(connection, sql);
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        try (Connection connection = this.connectionFactory.openConnection(JdbcIdentity.from(session))) {
            String columnName = column.getName();
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                columnName = columnName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s ADD (%s)",
                    quoted(handle.getRemoteTableName()),
                    this.getColumnDefinitionSql(session, column, columnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void dropColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        String sql = format(
                "ALTER TABLE %s DROP (%s)",
                quoted(handle.getRemoteTableName()),
                column.getColumnName());
        execute(identity, sql);
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                newColumnName = newColumnName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "RENAME COLUMN %s.%s TO %s",
                    quoted(handle.getRemoteTableName()),
                    jdbcColumn.getColumnName(),
                    newColumnName);
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
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
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        int columnSize = typeHandle.getColumnSize();
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
                    // e.g.
                    // In SAP HANA's `decimal` if precision and scale are not specified, then DECIMAL becomes a floating-point decimal number.
                    // In this case, precision and scale can vary within the range of 1 to 34 for precision and -6,111 to 6,176 for scale, depending on the stored value.
                    // However, this is reported as decimal(34,NULL) in JDBC.

                    // Similarly for `smalldecimal``, which is reported as decimal(16,NULL) in JDBC (with type name "SMALLDECIMAL")

                    return Optional.of(doubleColumnMapping());
                }

                int precision = columnSize;
                int scale = typeHandle.getDecimalDigits().orElseThrow(() -> new IllegalStateException("decimal digits not present"));
                if (precision < 1 || precision > Decimals.MAX_PRECISION || scale < 0 || scale > precision) {
                    // SAP HANA supports precision [1, 38], and scale [0, precision]
                    log.warn("Unexpected decimal precision: %s", typeHandle);
                    return Optional.empty();
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, scale), UNNECESSARY));

            case Types.CHAR:
            case Types.NCHAR:
                verify(columnSize < CharType.MAX_LENGTH, "Unexpected type: %s", typeHandle); // SAP HANA char is shorter than Presto's
                return Optional.of(charColumnMapping(createCharType(columnSize)));

            case Types.VARCHAR:
            case Types.NVARCHAR:
                if (columnSize > VarcharType.MAX_LENGTH) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
                }
                return Optional.of(varcharColumnMapping(createVarcharType(columnSize)));

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
                return Optional.of(dateColumnMapping());

            case Types.TIME:
                return Optional.of(timeColumnMapping());

            case Types.TIMESTAMP:
                int timestampPrecision = typeHandle.getDecimalDigits().orElseThrow(() -> new IllegalStateException("decimal digits not present"));
                return Optional.of(timestampColumnMapping(timestampPrecision));
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
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.sliceMapping(dataType, longDecimalWriteFunction(decimalType));
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
            return WriteMapping.longMapping("date", dateWriteFunction());
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

        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
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

    private static ColumnMapping timeColumnMapping()
    {
        return ColumnMapping.longMapping(
                createTimeType(0), // SAP HANA's TIME does not support second fraction
                timeReadFunction(),
                timeWriteFunction(),
                DISABLE_PUSHDOWN); // TODO enable, add a test
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
}
