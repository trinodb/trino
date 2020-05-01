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
package io.prestosql.plugin.jdbc;

import com.google.common.base.CharMatcher;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.VarcharType;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.plugin.jdbc.ColumnMapping.DISABLE_PUSHDOWN;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.Decimals.decodeUnscaledValue;
import static io.prestosql.spi.type.Decimals.encodeScaledValue;
import static io.prestosql.spi.type.Decimals.encodeShortScaledValue;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class StandardColumnMappings
{
    private StandardColumnMappings() {}

    public static ColumnMapping booleanColumnMapping()
    {
        return ColumnMapping.booleanMapping(BOOLEAN, ResultSet::getBoolean, booleanWriteFunction());
    }

    public static BooleanWriteFunction booleanWriteFunction()
    {
        return PreparedStatement::setBoolean;
    }

    public static ColumnMapping tinyintColumnMapping()
    {
        return ColumnMapping.longMapping(TINYINT, ResultSet::getByte, tinyintWriteFunction());
    }

    public static LongWriteFunction tinyintWriteFunction()
    {
        return (statement, index, value) -> statement.setByte(index, SignedBytes.checkedCast(value));
    }

    public static ColumnMapping smallintColumnMapping()
    {
        return ColumnMapping.longMapping(SMALLINT, ResultSet::getShort, smallintWriteFunction());
    }

    public static LongWriteFunction smallintWriteFunction()
    {
        return (statement, index, value) -> statement.setShort(index, Shorts.checkedCast(value));
    }

    public static ColumnMapping integerColumnMapping()
    {
        return ColumnMapping.longMapping(INTEGER, ResultSet::getInt, integerWriteFunction());
    }

    public static LongWriteFunction integerWriteFunction()
    {
        return (statement, index, value) -> statement.setInt(index, toIntExact(value));
    }

    public static ColumnMapping bigintColumnMapping()
    {
        return ColumnMapping.longMapping(BIGINT, ResultSet::getLong, bigintWriteFunction());
    }

    public static LongWriteFunction bigintWriteFunction()
    {
        return PreparedStatement::setLong;
    }

    public static ColumnMapping realColumnMapping()
    {
        return ColumnMapping.longMapping(REAL, (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)), realWriteFunction());
    }

    public static LongWriteFunction realWriteFunction()
    {
        return (statement, index, value) -> statement.setFloat(index, intBitsToFloat(toIntExact(value)));
    }

    public static ColumnMapping doubleColumnMapping()
    {
        return ColumnMapping.doubleMapping(DOUBLE, ResultSet::getDouble, doubleWriteFunction());
    }

    public static DoubleWriteFunction doubleWriteFunction()
    {
        return PreparedStatement::setDouble;
    }

    public static ColumnMapping decimalColumnMapping(DecimalType decimalType, RoundingMode roundingMode)
    {
        // JDBC driver can return BigDecimal with lower scale than column's scale when there are trailing zeroes
        int scale = decimalType.getScale();
        if (decimalType.isShort()) {
            return ColumnMapping.longMapping(
                    decimalType,
                    (resultSet, columnIndex) -> encodeShortScaledValue(resultSet.getBigDecimal(columnIndex), scale),
                    shortDecimalWriteFunction(decimalType));
        }
        return ColumnMapping.sliceMapping(
                decimalType,
                (resultSet, columnIndex) -> encodeScaledValue(resultSet.getBigDecimal(columnIndex).setScale(scale, roundingMode)),
                longDecimalWriteFunction(decimalType));
    }

    public static LongWriteFunction shortDecimalWriteFunction(DecimalType decimalType)
    {
        requireNonNull(decimalType, "decimalType is null");
        checkArgument(decimalType.isShort());
        return (statement, index, value) -> {
            BigInteger unscaledValue = BigInteger.valueOf(value);
            BigDecimal bigDecimal = new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
            statement.setBigDecimal(index, bigDecimal);
        };
    }

    public static SliceWriteFunction longDecimalWriteFunction(DecimalType decimalType)
    {
        requireNonNull(decimalType, "decimalType is null");
        checkArgument(!decimalType.isShort());
        return (statement, index, value) -> {
            BigInteger unscaledValue = decodeUnscaledValue(value);
            BigDecimal bigDecimal = new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
            statement.setBigDecimal(index, bigDecimal);
        };
    }

    public static ColumnMapping charColumnMapping(CharType charType)
    {
        requireNonNull(charType, "charType is null");
        return ColumnMapping.sliceMapping(charType, charReadFunction(), charWriteFunction());
    }

    public static SliceReadFunction charReadFunction()
    {
        return (resultSet, columnIndex) -> utf8Slice(CharMatcher.is(' ').trimTrailingFrom(resultSet.getString(columnIndex)));
    }

    public static SliceWriteFunction charWriteFunction()
    {
        return (statement, index, value) -> {
            statement.setString(index, value.toStringUtf8());
        };
    }

    public static ColumnMapping varcharColumnMapping(VarcharType varcharType)
    {
        return ColumnMapping.sliceMapping(varcharType, varcharReadFunction(), varcharWriteFunction());
    }

    public static SliceReadFunction varcharReadFunction()
    {
        return (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex));
    }

    public static SliceWriteFunction varcharWriteFunction()
    {
        return (statement, index, value) -> statement.setString(index, value.toStringUtf8());
    }

    public static ColumnMapping varbinaryColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                VARBINARY,
                (resultSet, columnIndex) -> wrappedBuffer(resultSet.getBytes(columnIndex)),
                varbinaryWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    public static SliceWriteFunction varbinaryWriteFunction()
    {
        return (statement, index, value) -> statement.setBytes(index, value.getBytes());
    }

    public static ColumnMapping dateColumnMapping()
    {
        return ColumnMapping.longMapping(
                DATE,
                (resultSet, columnIndex) -> {
                    /*
                     * JDBC returns a date using a timestamp at midnight in the JVM timezone, or earliest time after that if there was no midnight.
                     * This works correctly for all dates and zones except when the missing local times 'gap' is 24h. I.e. this fails when JVM time
                     * zone is Pacific/Apia and date to be returned is 2011-12-30.
                     *
                     * `return resultSet.getObject(columnIndex, LocalDate.class).toEpochDay()` avoids these problems but
                     * is currently known not to work with Redshift (old Postgres connector) and SQL Server.
                     */
                    long localMillis = resultSet.getDate(columnIndex).getTime();
                    // Convert it to a ~midnight in UTC.
                    long utcMillis = ISOChronology.getInstance().getZone().getMillisKeepLocal(DateTimeZone.UTC, localMillis);
                    // convert to days
                    return MILLISECONDS.toDays(utcMillis);
                },
                dateWriteFunction());
    }

    public static LongWriteFunction dateWriteFunction()
    {
        return (statement, index, value) -> {
            // convert to midnight in default time zone
            long millis = DAYS.toMillis(value);
            statement.setDate(index, new Date(DateTimeZone.UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millis)));
        };
    }

    /**
     * @deprecated This method uses {@link java.sql.Time} and the class cannot represent time value when JVM zone had
     * forward offset change (a 'gap') at given time on 1970-01-01. If driver only supports {@link LocalTime}, use
     * {@link #timeColumnMapping} instead.
     */
    public static ColumnMapping timeColumnMappingUsingSqlTime(ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            ZoneId sessionZone = ZoneId.of(session.getTimeZoneKey().getId());
            return ColumnMapping.longMapping(
                    TIME,
                    (resultSet, columnIndex) -> {
                        Time time = resultSet.getTime(columnIndex);
                        return toPrestoLegacyTimestamp(toLocalTime(time).atDate(LocalDate.ofEpochDay(0)), sessionZone);
                    },
                    timeWriteFunctionUsingSqlTime(session));
        }

        return ColumnMapping.longMapping(
                TIME,
                (resultSet, columnIndex) -> {
                    Time time = resultSet.getTime(columnIndex);
                    return NANOSECONDS.toMillis(toLocalTime(time).toNanoOfDay());
                },
                timeWriteFunctionUsingSqlTime(session));
    }

    private static LocalTime toLocalTime(Time sqlTime)
    {
        // Time.toLocalTime() does not preserve second fraction
        return sqlTime.toLocalTime()
                // TODO is the conversion correct if sqlTime.getTime() < 0?
                .withNano(toIntExact(MILLISECONDS.toNanos(sqlTime.getTime() % 1000)));
    }

    /**
     * @deprecated This method uses {@link java.sql.Time} and the class cannot represent time value when JVM zone had
     * forward offset change (a 'gap') at given time on 1970-01-01. If driver only supports {@link LocalTime}, use
     * {@link #timeWriteFunction} instead.
     */
    public static LongWriteFunction timeWriteFunctionUsingSqlTime(ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            ZoneId sessionZone = ZoneId.of(session.getTimeZoneKey().getId());
            return (statement, index, value) -> statement.setTime(index, toSqlTime(fromPrestoLegacyTimestamp(value, sessionZone).toLocalTime()));
        }
        return (statement, index, value) -> statement.setTime(index, toSqlTime(fromPrestoTimestamp(value).toLocalTime()));
    }

    private static Time toSqlTime(LocalTime localTime)
    {
        // Time.valueOf does not preserve second fraction
        return new Time(Time.valueOf(localTime).getTime() + NANOSECONDS.toMillis(localTime.getNano()));
    }

    public static ColumnMapping timeColumnMapping(ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            ZoneId sessionZone = ZoneId.of(session.getTimeZoneKey().getId());
            return ColumnMapping.longMapping(
                    TIME,
                    (resultSet, columnIndex) -> {
                        LocalTime time = resultSet.getObject(columnIndex, LocalTime.class);
                        return toPrestoLegacyTimestamp(time.atDate(LocalDate.ofEpochDay(0)), sessionZone);
                    },
                    timeWriteFunction(session));
        }

        return ColumnMapping.longMapping(
                TIME,
                (resultSet, columnIndex) -> {
                    LocalTime time = resultSet.getObject(columnIndex, LocalTime.class);
                    return NANOSECONDS.toMillis(time.toNanoOfDay());
                },
                timeWriteFunction(session));
    }

    public static LongWriteFunction timeWriteFunction(ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            ZoneId sessionZone = ZoneId.of(session.getTimeZoneKey().getId());
            return (statement, index, value) -> statement.setObject(index, fromPrestoLegacyTimestamp(value, sessionZone).toLocalTime());
        }
        return (statement, index, value) -> statement.setObject(index, fromPrestoTimestamp(value).toLocalTime());
    }

    /**
     * @deprecated This method uses {@link java.sql.Timestamp} and the class cannot represent date-time value when JVM zone had
     * forward offset change (a 'gap'). This includes regular DST changes (e.g. Europe/Warsaw) and one-time policy changes
     * (Asia/Kathmandu's shift by 15 minutes on January 1, 1986, 00:00:00). If driver only supports {@link LocalDateTime}, use
     * {@link #timestampColumnMapping} instead.
     */
    @Deprecated
    public static ColumnMapping timestampColumnMappingUsingSqlTimestamp(ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            ZoneId sessionZone = ZoneId.of(session.getTimeZoneKey().getId());
            return ColumnMapping.longMapping(
                    TIMESTAMP,
                    (resultSet, columnIndex) -> {
                        Timestamp timestamp = resultSet.getTimestamp(columnIndex);
                        return toPrestoLegacyTimestamp(timestamp.toLocalDateTime(), sessionZone);
                    },
                    timestampWriteFunctionUsingSqlTimestamp(session));
        }

        return ColumnMapping.longMapping(
                TIMESTAMP,
                (resultSet, columnIndex) -> {
                    Timestamp timestamp = resultSet.getTimestamp(columnIndex);
                    return toPrestoTimestamp(timestamp.toLocalDateTime());
                },
                timestampWriteFunctionUsingSqlTimestamp(session));
    }

    public static ColumnMapping timestampColumnMapping(ConnectorSession session)
    {
        return ColumnMapping.longMapping(
                TIMESTAMP,
                timestampReadFunction(session),
                timestampWriteFunction(session));
    }

    public static LongReadFunction timestampReadFunction(ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            ZoneId sessionZone = ZoneId.of(session.getTimeZoneKey().getId());
            return (resultSet, columnIndex) -> toPrestoLegacyTimestamp(resultSet.getObject(columnIndex, LocalDateTime.class), sessionZone);
        }
        return (resultSet, columnIndex) -> toPrestoTimestamp(resultSet.getObject(columnIndex, LocalDateTime.class));
    }

    /**
     * @deprecated This method uses {@link java.sql.Timestamp} and the class cannot represent date-time value when JVM zone had
     * forward offset change (a 'gap'). This includes regular DST changes (e.g. Europe/Warsaw) and one-time policy changes
     * (Asia/Kathmandu's shift by 15 minutes on January 1, 1986, 00:00:00). If driver only supports {@link LocalDateTime}, use
     * {@link #timestampWriteFunction} instead.
     */
    @Deprecated
    public static LongWriteFunction timestampWriteFunctionUsingSqlTimestamp(ConnectorSession connectorSession)
    {
        if (connectorSession.isLegacyTimestamp()) {
            ZoneId sessionZone = ZoneId.of(connectorSession.getTimeZoneKey().getId());
            return (statement, index, value) -> statement.setTimestamp(index, Timestamp.valueOf(fromPrestoLegacyTimestamp(value, sessionZone)));
        }
        return (statement, index, value) -> statement.setTimestamp(index, Timestamp.valueOf(fromPrestoTimestamp(value)));
    }

    public static LongWriteFunction timestampWriteFunction(ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            ZoneId sessionZone = ZoneId.of(session.getTimeZoneKey().getId());
            return (statement, index, value) -> statement.setObject(index, fromPrestoLegacyTimestamp(value, sessionZone));
        }
        return (statement, index, value) -> {
            statement.setObject(index, fromPrestoTimestamp(value));
        };
    }

    /**
     * @deprecated applicable in legacy timestamp semantics only
     */
    @Deprecated
    public static long toPrestoLegacyTimestamp(LocalDateTime localDateTime, ZoneId sessionZone)
    {
        return localDateTime.atZone(sessionZone).toInstant().toEpochMilli();
    }

    public static long toPrestoTimestamp(LocalDateTime localDateTime)
    {
        return localDateTime.atZone(UTC).toInstant().toEpochMilli();
    }

    /**
     * @deprecated applicable in legacy timestamp semantics only
     */
    @Deprecated
    public static LocalDateTime fromPrestoLegacyTimestamp(long value, ZoneId sessionZone)
    {
        return Instant.ofEpochMilli(value).atZone(sessionZone).toLocalDateTime();
    }

    public static LocalDateTime fromPrestoTimestamp(long value)
    {
        return Instant.ofEpochMilli(value).atZone(UTC).toLocalDateTime();
    }

    public static Optional<ColumnMapping> jdbcTypeToPrestoType(ConnectorSession session, JdbcTypeHandle type)
    {
        int columnSize = type.getColumnSize();
        switch (type.getJdbcType()) {
            case Types.BIT:
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

            case Types.FLOAT:
            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalDigits = type.getDecimalDigits();
                int precision = columnSize + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    return Optional.empty();
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0)), UNNECESSARY));

            case Types.CHAR:
            case Types.NCHAR:
                // TODO this is wrong, we're going to construct malformed Slice representation if source > charLength
                int charLength = min(columnSize, CharType.MAX_LENGTH);
                return Optional.of(charColumnMapping(createCharType(charLength)));

            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                if (columnSize > VarcharType.MAX_LENGTH) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
                }
                return Optional.of(varcharColumnMapping(createVarcharType(columnSize)));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(varbinaryColumnMapping());

            case Types.DATE:
                return Optional.of(dateColumnMapping());

            case Types.TIME:
                // TODO default to `timeColumnMapping`
                return Optional.of(timeColumnMappingUsingSqlTime(session));

            case Types.TIMESTAMP:
                // TODO default to `timestampColumnMapping`
                return Optional.of(timestampColumnMappingUsingSqlTimestamp(session));
        }
        return Optional.empty();
    }
}
