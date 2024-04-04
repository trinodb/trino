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
package io.trino.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.math.IntMath;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Verify.verify;
import static io.trino.type.DateTimes.NANOSECONDS_PER_MILLISECOND;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseTestJdbcResultSet
{
    protected abstract Connection createConnection()
            throws SQLException;

    protected abstract int getTestedServerVersion();

    @Test
    public void testDuplicateColumnLabels()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            try (ResultSet rs = connectedStatement.getStatement().executeQuery("SELECT 123 x, 456 x")) {
                ResultSetMetaData metadata = rs.getMetaData();
                assertThat(metadata.getColumnCount()).isEqualTo(2);
                assertThat(metadata.getColumnName(1)).isEqualTo("x");
                assertThat(metadata.getColumnName(2)).isEqualTo("x");

                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(123L);
                assertThat(rs.getLong(2)).isEqualTo(456L);
                assertThat(rs.getLong("x")).isEqualTo(123L);
            }
        }
    }

    @Test
    public void testPrimitiveTypes()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            checkRepresentation(connectedStatement.getStatement(), "123", Types.INTEGER, 123);
            checkRepresentation(connectedStatement.getStatement(), "12300000000", Types.BIGINT, 12300000000L);
            checkRepresentation(connectedStatement.getStatement(), "REAL '123.45'", Types.REAL, 123.45f);
            checkRepresentation(connectedStatement.getStatement(), "1e-1", Types.DOUBLE, 0.1);
            checkRepresentation(connectedStatement.getStatement(), "1.0E0 / 0.0E0", Types.DOUBLE, Double.POSITIVE_INFINITY);
            checkRepresentation(connectedStatement.getStatement(), "0.0E0 / 0.0E0", Types.DOUBLE, Double.NaN);
            checkRepresentation(connectedStatement.getStatement(), "true", Types.BOOLEAN, true);
            checkRepresentation(connectedStatement.getStatement(), "'hello'", Types.VARCHAR, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo("hello");
                assertThat(rs.getAsciiStream(column)).hasBinaryContent("hello".getBytes(StandardCharsets.US_ASCII));
                assertThatThrownBy(() -> rs.getBinaryStream(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Value is not a byte array: hello");
            });
            checkRepresentation(connectedStatement.getStatement(), "CAST(NULL AS VARCHAR)", Types.VARCHAR, (rs, column) -> {
                assertThat(rs.getAsciiStream(column)).isNull();
                assertThat(rs.getBinaryStream(column)).isNull();
            });
            checkRepresentation(connectedStatement.getStatement(), "cast('foo' as char(5))", Types.CHAR, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo("foo  ");
                assertThat(rs.getAsciiStream(column)).hasBinaryContent("foo  ".getBytes(StandardCharsets.US_ASCII));
            });
            checkRepresentation(connectedStatement.getStatement(), "VARCHAR '123'", Types.VARCHAR,
                    (rs, column) -> assertThat(rs.getLong(column)).isEqualTo(123L));

            checkRepresentation(connectedStatement.getStatement(), "VARCHAR ''", Types.VARCHAR, (rs, column) -> {
                assertThatThrownBy(() -> rs.getLong(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Value is not a number: ");

                assertThatThrownBy(() -> rs.getDouble(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Value is not a number: ");

                assertThatThrownBy(() -> rs.getBoolean(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Value is not a boolean: ");

                assertThat(rs.getAsciiStream(column)).isEmpty();
            });

            checkRepresentation(connectedStatement.getStatement(), "VARCHAR '123e-1'", Types.VARCHAR, (rs, column) -> {
                assertThat(rs.getDouble(column)).isEqualTo(12.3);
                assertThat(rs.getLong(column)).isEqualTo(12);
                assertThat(rs.getFloat(column)).isEqualTo(12.3f);
                assertThat(rs.getAsciiStream(column)).hasBinaryContent("123e-1".getBytes(StandardCharsets.US_ASCII));
            });

            checkRepresentation(connectedStatement.getStatement(), "DOUBLE '123.456'", Types.DOUBLE, (rs, column) -> {
                assertThat(rs.getDouble(column)).isEqualTo(123.456);
                assertThat(rs.getLong(column)).isEqualTo(123);
                assertThat(rs.getFloat(column)).isEqualTo(123.456f);
                assertThatThrownBy(() -> rs.getAsciiStream(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Value is not a string: 123.456");
            });

            checkRepresentation(connectedStatement.getStatement(), "VARCHAR '123'", Types.VARCHAR, (rs, column) -> {
                assertThat(rs.getDouble(column)).isEqualTo(123.0);
                assertThat(rs.getLong(column)).isEqualTo(123);
                assertThat(rs.getFloat(column)).isEqualTo(123f);
                assertThat(rs.getAsciiStream(column)).hasBinaryContent("123".getBytes(StandardCharsets.US_ASCII));
            });
        }
    }

    @Test
    public void testDecimal()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            checkRepresentation(connectedStatement.getStatement(), "0.1", Types.DECIMAL, new BigDecimal("0.1"));
            checkRepresentation(connectedStatement.getStatement(), "DECIMAL '0.12'", Types.DECIMAL, (rs, column) -> {
                assertThat(rs.getBigDecimal(column)).isEqualTo(new BigDecimal("0.12"));
                assertThat(rs.getDouble(column)).isEqualTo(0.12);
                assertThat(rs.getLong(column)).isEqualTo(0);
                assertThat(rs.getFloat(column)).isEqualTo(0.12f);
            });

            long outsideOfDoubleExactRange = 9223372036854775774L;
            //noinspection ConstantConditions
            verify((long) (double) outsideOfDoubleExactRange - outsideOfDoubleExactRange != 0, "outsideOfDoubleExactRange should not be exact-representable as a double");
            checkRepresentation(connectedStatement.getStatement(), format("DECIMAL '%s'",
                    outsideOfDoubleExactRange), Types.DECIMAL,
                    (rs, column) -> {
                        assertThat(rs.getObject(column)).isEqualTo(new BigDecimal("9223372036854775774"));
                        assertThat(rs.getBigDecimal(column)).isEqualTo(new BigDecimal("9223372036854775774"));
                        assertThat(rs.getLong(column)).isEqualTo(9223372036854775774L);
                        assertThat(rs.getDouble(column)).isEqualTo(9.223372036854776E18);
                        assertThat(rs.getString(column)).isEqualTo("9223372036854775774");
                    });

            checkRepresentation(connectedStatement.getStatement(), "VARCHAR ''", Types.VARCHAR, (rs, column) -> {
                assertThatThrownBy(() -> rs.getBigDecimal(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Value is not a number: ");
            });

            checkRepresentation(connectedStatement.getStatement(), "VARCHAR '123a'", Types.VARCHAR, (rs, column) -> {
                assertThatThrownBy(() -> rs.getBigDecimal(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Value is not a number: 123a");
            });

            checkRepresentation(connectedStatement.getStatement(), "VARCHAR '123e-1'", Types.VARCHAR,
                    (rs, column) -> assertThat(rs.getBigDecimal(column)).isEqualTo(new BigDecimal("12.3")));
        }
    }

    @Test
    public void testVarbinary()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            checkRepresentation(connectedStatement.getStatement(), "X'12345678'", Types.VARBINARY, (rs, column) -> {
                byte[] bytes = {0x12, 0x34, 0x56, 0x78};
                assertThat(rs.getObject(column)).isEqualTo(bytes);
                assertThat(rs.getObject(column, byte[].class)).isEqualTo(bytes);
                assertThat(rs.getBytes(column)).isEqualTo(bytes);

                assertThatThrownBy(() -> rs.getLong(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessageStartingWith("Value is not a number: [B@");
                assertThatThrownBy(() -> rs.getBigDecimal(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessageStartingWith("Value is not a number: [B@");

                assertThat(rs.getString(column)).isEqualTo("0x12345678");
                assertThat(rs.getBinaryStream(column)).hasBinaryContent(bytes);
                assertThatThrownBy(() -> rs.getAsciiStream(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessageStartingWith("Value is not a string: [B@");
            });
            checkRepresentation(connectedStatement.getStatement(), "CAST(NULL AS VARBINARY)", Types.VARBINARY, (rs, column) -> {
                assertThat(rs.getAsciiStream(column)).isNull();
                assertThat(rs.getBinaryStream(column)).isNull();
            });

            checkRepresentation(connectedStatement.getStatement(), "X''", Types.VARBINARY, (rs, column) -> {
                assertThat(rs.getBinaryStream(column)).isEmpty();
                assertThatThrownBy(() -> rs.getAsciiStream(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessageStartingWith("Value is not a string: [B@");
            });
        }
    }

    @Test
    public void testDate()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            checkRepresentation(connectedStatement.getStatement(), "DATE '2018-02-13'", Types.DATE, (rs, column) -> {
                LocalDate localDate = LocalDate.of(2018, 2, 13);
                Date sqlDate = Date.valueOf(localDate);

                assertThat(rs.getObject(column)).isEqualTo(sqlDate);
                assertThat(rs.getObject(column, Date.class)).isEqualTo(sqlDate);
                assertThat(rs.getObject(column, LocalDate.class)).isEqualTo(localDate);

                assertThat(rs.getDate(column)).isEqualTo(sqlDate);
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Expected column to be a time type but is date");
                assertThatThrownBy(() -> rs.getTimestamp(column))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Expected column to be a timestamp type but is date");

                assertThat(rs.getString(column)).isEqualTo(localDate.toString());
            });

            // distant past, but apparently not an uncommon value in practice
            checkRepresentation(connectedStatement.getStatement(), "DATE '0001-01-01'", Types.DATE, (rs, column) -> {
                LocalDate localDate = LocalDate.of(1, 1, 1);
                Date sqlDate = Date.valueOf(localDate);

                assertThat(rs.getObject(column)).isEqualTo(sqlDate);
                assertThat(rs.getObject(column, Date.class)).isEqualTo(sqlDate);
                assertThat(rs.getObject(column, LocalDate.class)).isEqualTo(localDate);

                assertThat(rs.getDate(column)).isEqualTo(sqlDate);
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Expected column to be a time type but is date");
                assertThatThrownBy(() -> rs.getTimestamp(column))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Expected column to be a timestamp type but is date");

                assertThat(rs.getString(column)).isEqualTo(localDate.toString());
            });

            // date which midnight does not exist in test JVM zone
            checkRepresentation(connectedStatement.getStatement(), "DATE '1970-01-01'", Types.DATE, (rs, column) -> {
                LocalDate localDate = LocalDate.of(1970, 1, 1);
                Date sqlDate = Date.valueOf(localDate);

                assertThat(rs.getObject(column)).isEqualTo(sqlDate);
                assertThat(rs.getObject(column, Date.class)).isEqualTo(sqlDate);
                assertThat(rs.getObject(column, LocalDate.class)).isEqualTo(localDate);
                assertThat(rs.getDate(column)).isEqualTo(sqlDate);
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Expected column to be a time type but is date");
                assertThatThrownBy(() -> rs.getTimestamp(column))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Expected column to be a timestamp type but is date");

                assertThat(rs.getString(column)).isEqualTo(localDate.toString());
            });

            // the Julian-Gregorian calendar "default cut-over"
            checkRepresentation(connectedStatement.getStatement(), "DATE '1582-10-04'", Types.DATE, (rs, column) -> {
                LocalDate localDate = LocalDate.of(1582, 10, 4);
                Date sqlDate = Date.valueOf(localDate);

                assertThat(rs.getObject(column)).isEqualTo(sqlDate);
                assertThat(rs.getObject(column, Date.class)).isEqualTo(sqlDate);
                assertThat(rs.getObject(column, LocalDate.class)).isEqualTo(localDate);

                assertThat(rs.getDate(column)).isEqualTo(sqlDate);
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Expected column to be a time type but is date");
                assertThatThrownBy(() -> rs.getTimestamp(column))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Expected column to be a timestamp type but is date");

                assertThat(rs.getString(column)).isEqualTo(localDate.toString());
            });

            // after the Julian-Gregorian calendar "default cut-over", but before the Gregorian calendar start
            checkRepresentation(connectedStatement.getStatement(), "DATE '1582-10-10'", Types.DATE, (rs, column) -> {
                LocalDate localDate = LocalDate.of(1582, 10, 10);
                Date sqlDate = Date.valueOf(localDate);

                assertThat(rs.getObject(column)).isEqualTo(sqlDate);
                assertThat(rs.getObject(column, Date.class)).isEqualTo(sqlDate);

                // There are no days between 1582-10-05 and 1582-10-14
                assertThat(rs.getObject(column, LocalDate.class)).isEqualTo(LocalDate.of(1582, 10, 20));

                assertThat(rs.getDate(column)).isEqualTo(sqlDate);
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Expected column to be a time type but is date");
                assertThatThrownBy(() -> rs.getTimestamp(column))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Expected column to be a timestamp type but is date");

                assertThat(rs.getString(column)).isEqualTo(localDate.toString());
            });

            // the Gregorian calendar start
            checkRepresentation(connectedStatement.getStatement(), "DATE '1582-10-15'", Types.DATE, (rs, column) -> {
                LocalDate localDate = LocalDate.of(1582, 10, 15);
                Date sqlDate = Date.valueOf(localDate);

                assertThat(rs.getObject(column)).isEqualTo(sqlDate);
                assertThat(rs.getObject(column, Date.class)).isEqualTo(sqlDate);
                assertThat(rs.getObject(column, LocalDate.class)).isEqualTo(localDate);

                assertThat(rs.getDate(column)).isEqualTo(sqlDate);
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Expected column to be a time type but is date");
                assertThatThrownBy(() -> rs.getTimestamp(column))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Expected column to be a timestamp type but is date");

                assertThat(rs.getString(column)).isEqualTo(localDate.toString());
            });
        }
    }

    @Test
    public void testTime()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            checkRepresentation(connectedStatement.getStatement(), "TIME '09:39:05.000'", Types.TIME, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(toSqlTime(LocalTime.of(9, 39, 5)));
                assertThat(rs.getObject(column, Time.class)).isEqualTo(toSqlTime(LocalTime.of(9, 39, 5)));
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 09:39:05.000");
                assertThat(rs.getTime(column)).isEqualTo(Time.valueOf(LocalTime.of(9, 39, 5)));
                assertThatThrownBy(() -> rs.getTimestamp(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a timestamp type but is time(3)");
            });

            checkRepresentation(connectedStatement.getStatement(), "TIME '00:39:05'", Types.TIME, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(toSqlTime(LocalTime.of(0, 39, 5)));
                assertThat(rs.getObject(column, Time.class)).isEqualTo(toSqlTime(LocalTime.of(0, 39, 5)));
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 00:39:05");
                assertThat(rs.getTime(column)).isEqualTo(Time.valueOf(LocalTime.of(0, 39, 5)));
                assertThatThrownBy(() -> rs.getTimestamp(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a timestamp type but is time(0)");
            });

            // second fraction could be overflowing to next millisecond
            checkRepresentation(connectedStatement.getStatement(), "TIME '10:11:12.1235'", Types.TIME, (rs, column) -> {
                // TODO (https://github.com/trinodb/trino/issues/6205) maybe should round to 124 ms instead
                assertThat(rs.getObject(column)).isEqualTo(toSqlTime(LocalTime.of(10, 11, 12, 123_000_000)));
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 10:11:12.1235");
                assertThat(rs.getTime(column)).isEqualTo(toSqlTime(LocalTime.of(10, 11, 12, 123_000_000)));
                assertThatThrownBy(() -> rs.getTimestamp(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a timestamp type but is time(4)");
            });

            // second fraction could be overflowing to next nanosecond, second, minute and hour
            checkRepresentation(connectedStatement.getStatement(), "TIME '10:59:59.999999999999'", Types.TIME, (rs, column) -> {
                // TODO (https://github.com/trinodb/trino/issues/6205) maybe result should be 11:00:00
                assertThat(rs.getObject(column)).isEqualTo(toSqlTime(LocalTime.of(10, 59, 59, 999_000_000)));
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 10:59:59.999999999999");
                // TODO (https://github.com/trinodb/trino/issues/6205) maybe result should be 11:00:00
                assertThat(rs.getTime(column)).isEqualTo(toSqlTime(LocalTime.of(10, 59, 59, 999_000_000)));
                assertThatThrownBy(() -> rs.getTimestamp(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a timestamp type but is time(12)");
            });

            // second fraction could be overflowing to next day
            checkRepresentation(connectedStatement.getStatement(), "TIME '23:59:59.999999999999'", Types.TIME, (rs, column) -> {
                // TODO (https://github.com/trinodb/trino/issues/6205) maybe result should be 01:00:00 (shifted from 00:00:00 as test JVM has gap in 1970-01-01)
                assertThat(rs.getObject(column)).isEqualTo(toSqlTime(LocalTime.of(23, 59, 59, 999_000_000)));
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 23:59:59.999999999999");
                // TODO (https://github.com/trinodb/trino/issues/6205) maybe result should be 01:00:00 (shifted from 00:00:00 as test JVM has gap in 1970-01-01)
                assertThat(rs.getTime(column)).isEqualTo(toSqlTime(LocalTime.of(23, 59, 59, 999_000_000)));
                assertThatThrownBy(() -> rs.getTimestamp(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a timestamp type but is time(12)");
            });
        }
    }

    @Test
    public void testTimeWithTimeZone()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            checkRepresentation(connectedStatement.getStatement(), "TIME '09:39:07 +01:00'", Types.TIME_WITH_TIMEZONE, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Time.valueOf(LocalTime.of(1, 39, 7))); // TODO this should represent TIME '09:39:07 +01:00'
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 09:39:07+01:00");
                assertThat(rs.getTime(column)).isEqualTo(Time.valueOf(LocalTime.of(1, 39, 7))); // TODO this should fail, or represent TIME '09:39:07'
                // TODO (https://github.com/trinodb/trino/issues/5317) placement of precision parameter
                assertThatThrownBy(() -> rs.getTimestamp(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a timestamp type but is time with time zone(0)");
            });

            checkRepresentation(connectedStatement.getStatement(), "TIME '01:39:07 +01:00'", Types.TIME_WITH_TIMEZONE, (rs, column) -> {
                Time someBogusValue = new Time(
                        Time.valueOf(
                                LocalTime.of(16, 39, 7)).getTime() /* 16:39:07 = 01:39:07 - +01:00 shift + Bahia_Banderas's shift (-8) (modulo 24h which we "un-modulo" below) */
                                - DAYS.toMillis(1) /* because we use currently 'shifted' representation, not possible to create just using LocalTime */
                                + HOURS.toMillis(1) /* because there was offset shift on 1970-01-01 in America/Bahia_Banderas */);
                assertThat(rs.getObject(column)).isEqualTo(someBogusValue); // TODO this should represent TIME '01:39:07 +01:00'
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 01:39:07+01:00");
                assertThat(rs.getTime(column)).isEqualTo(someBogusValue); // TODO this should fail, or represent TIME '01:39:07'
                // TODO (https://github.com/trinodb/trino/issues/5317) placement of precision parameter
                assertThatThrownBy(() -> rs.getTimestamp(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a timestamp type but is time with time zone(0)");
            });

            checkRepresentation(connectedStatement.getStatement(), "TIME '00:39:07 +01:00'", Types.TIME_WITH_TIMEZONE, (rs, column) -> {
                Time someBogusValue = new Time(
                        Time.valueOf(
                                LocalTime.of(15, 39, 7)).getTime() /* 15:39:07 = 00:39:07 - +01:00 shift + Bahia_Banderas's shift (-8) (modulo 24h which we "un-modulo" below) */
                                - DAYS.toMillis(1) /* because we use currently 'shifted' representation, not possible to create just using LocalTime */
                                + HOURS.toMillis(1) /* because there was offset shift on 1970-01-01 in America/Bahia_Banderas */);
                assertThat(rs.getObject(column)).isEqualTo(someBogusValue); // TODO this should represent TIME '00:39:07 +01:00'
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 00:39:07+01:00");
                assertThat(rs.getTime(column)).isEqualTo(someBogusValue); // TODO this should fail, as there no java.sql.Time representation for TIME '00:39:07' in America/Bahia_Banderas
                // TODO (https://github.com/trinodb/trino/issues/5317) placement of precision parameter
                assertThatThrownBy(() -> rs.getTimestamp(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a timestamp type but is time with time zone(0)");
            });
        }
    }

    @Test
    public void testTimestamp()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '2018-02-13 13:14:15.123'", Types.TIMESTAMP, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 123_000_000)));
                assertThat(rs.getObject(column, Timestamp.class)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 123_000_000)));
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 2018-02-13 13:14:15.123");
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp(3)");
                assertThat(rs.getTimestamp(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 123_000_000)));
            });

            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '2018-02-13 13:14:15.111111111111'", Types.TIMESTAMP, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 111_111_111)));
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 2018-02-13 13:14:15.111111111111");
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp(12)");
                assertThat(rs.getTimestamp(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 111_111_111)));
            });

            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '2018-02-13 13:14:15.555555555555'", Types.TIMESTAMP, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 555_555_556)));
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 2018-02-13 13:14:15.555555555555");
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp(12)");
                assertThat(rs.getTimestamp(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2018, 2, 13, 13, 14, 15, 555_555_556)));
            });

            // second fraction in nanoseconds overflowing to next second, minute, hour, day, month, year
            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '2019-12-31 23:59:59.999999999999'", Types.TIMESTAMP, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2020, 1, 1, 0, 0, 0, 0)));
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 2019-12-31 23:59:59.999999999999");
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp(12)");
                assertThat(rs.getTimestamp(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2020, 1, 1, 0, 0, 0, 0)));
            });

            // second fraction in nanoseconds overflowing to next second, minute, hour, day, month, year; before epoch
            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '1957-12-31 23:59:59.999999999999'", Types.TIMESTAMP, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(1958, 1, 1, 0, 0, 0, 0)));
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 1957-12-31 23:59:59.999999999999");
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp(12)");
                assertThat(rs.getTimestamp(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(1958, 1, 1, 0, 0, 0, 0)));
            });

            // distant past, but apparently not an uncommon value in practice
            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '0001-01-01 00:00:00'", Types.TIMESTAMP, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(1, 1, 1, 0, 0, 0)));
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 0001-01-01 00:00:00");
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp(0)");
                assertThat(rs.getTimestamp(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(1, 1, 1, 0, 0, 0)));
            });

            // the Julian-Gregorian calendar "default cut-over"
            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '1582-10-04 00:00:00'", Types.TIMESTAMP, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(1582, 10, 4, 0, 0, 0)));
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 1582-10-04 00:00:00");
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp(0)");
                assertThat(rs.getTimestamp(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(1582, 10, 4, 0, 0, 0)));
            });

            // after the Julian-Gregorian calendar "default cut-over", but before the Gregorian calendar start
            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '1582-10-10 00:00:00'", Types.TIMESTAMP, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(1582, 10, 10, 0, 0, 0)));
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 1582-10-10 00:00:00");
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp(0)");
                assertThat(rs.getTimestamp(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(1582, 10, 10, 0, 0, 0)));
            });

            // the Gregorian calendar start
            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '1582-10-15 00:00:00'", Types.TIMESTAMP, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(1582, 10, 15, 0, 0, 0)));
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 1582-10-15 00:00:00");
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp(0)");
                assertThat(rs.getTimestamp(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(1582, 10, 15, 0, 0, 0)));
            });

            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '1583-01-01 00:00:00'", Types.TIMESTAMP, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(1583, 1, 1, 0, 0, 0)));
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 1583-01-01 00:00:00");
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp(0)");
                assertThat(rs.getTimestamp(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(1583, 1, 1, 0, 0, 0)));
            });

            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '1970-01-01 00:14:15.123'", Types.TIMESTAMP, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 0, 14, 15, 123_000_000)));
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 1970-01-01 00:14:15.123");
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp(3)");
                assertThat(rs.getTimestamp(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 0, 14, 15, 123_000_000)));
            });

            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '123456-01-23 01:23:45.123456789'", Types.TIMESTAMP, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(123456, 1, 23, 1, 23, 45, 123_456_789)));
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: +123456-01-23 01:23:45.123456789");
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp(9)");
                assertThat(rs.getTimestamp(column)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(123456, 1, 23, 1, 23, 45, 123_456_789)));
            });
        }
    }

    @Test
    public void testTimestampWithTimeZone()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            // zero
            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '1970-01-01 00:00:00.000 +00:00'", Types.TIMESTAMP_WITH_TIMEZONE, (rs, column) -> {
                Timestamp timestampForPointInTime = Timestamp.from(Instant.EPOCH);
                assertThat(rs.getObject(column)).isEqualTo(timestampForPointInTime);
                assertThat(rs.getObject(column, ZonedDateTime.class)).isEqualTo(ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC")));
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 1970-01-01 00:00:00.000 UTC");
                // TODO (https://github.com/trinodb/trino/issues/5317) placement of precision parameter
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp with time zone(3)");
                assertThat(rs.getTimestamp(column)).isEqualTo(timestampForPointInTime);
            });

            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '2018-02-13 13:14:15.227 Europe/Warsaw'", Types.TIMESTAMP_WITH_TIMEZONE, (rs, column) -> {
                ZonedDateTime zonedDateTime = ZonedDateTime.of(2018, 2, 13, 13, 14, 15, 227_000_000, ZoneId.of("Europe/Warsaw"));
                Timestamp timestampForPointInTime = Timestamp.from(zonedDateTime.toInstant());
                assertThat(rs.getObject(column)).isEqualTo(timestampForPointInTime); // TODO this should represent TIMESTAMP '2018-02-13 13:14:15.227 Europe/Warsaw'
                assertThat(rs.getObject(column, ZonedDateTime.class)).isEqualTo(zonedDateTime);
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 2018-02-13 13:14:15.227 Europe/Warsaw");
                // TODO (https://github.com/trinodb/trino/issues/5317) placement of precision parameter
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp with time zone(3)");
                assertThat(rs.getTimestamp(column)).isEqualTo(timestampForPointInTime);
            });

            // second fraction in nanoseconds overflowing to next second, minute, hour, day, month, year
            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '2019-12-31 23:59:59.999999999999 Europe/Warsaw'", Types.TIMESTAMP_WITH_TIMEZONE, (rs, column) -> {
                ZonedDateTime zonedDateTime = ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneId.of("Europe/Warsaw"));
                Timestamp timestampForPointInTime = Timestamp.from(zonedDateTime.toInstant());
                assertThat(rs.getObject(column)).isEqualTo(timestampForPointInTime);  // TODO this should represent TIMESTAMP '2019-12-31 23:59:59.999999999999 Europe/Warsaw'
                assertThat(rs.getObject(column, ZonedDateTime.class)).isEqualTo(zonedDateTime);
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 2019-12-31 23:59:59.999999999999 Europe/Warsaw");
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp with time zone(12)"); // TODO (https://github.com/trinodb/trino/issues/5317) placement of precision parameter
                assertThat(rs.getTimestamp(column)).isEqualTo(timestampForPointInTime);
            });

            ZoneId jvmZone = ZoneId.systemDefault();
            checkRepresentation(
                    connectedStatement.getStatement(),
                    format("TIMESTAMP '2019-12-31 23:59:59.999999999999 %s'", jvmZone.getId()),
                    Types.TIMESTAMP_WITH_TIMEZONE,
                    (rs, column) -> {
                        ZonedDateTime zonedDateTime = ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, jvmZone);
                        Timestamp timestampForPointInTime = Timestamp.from(zonedDateTime.toInstant());
                        assertThat(rs.getObject(column)).isEqualTo(timestampForPointInTime);  // TODO this should represent TIMESTAMP '2019-12-31 23:59:59.999999999999 JVM ZONE'
                        assertThat(rs.getObject(column, ZonedDateTime.class)).isEqualTo(zonedDateTime);
                        assertThatThrownBy(() -> rs.getDate(column))
                                .isInstanceOf(SQLException.class)
                                .hasMessage("Expected value to be a date but is: 2019-12-31 23:59:59.999999999999 America/Bahia_Banderas");
                        assertThatThrownBy(() -> rs.getTime(column))
                                .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                                .hasMessage("Expected column to be a time type but is timestamp with time zone(12)"); // TODO (https://github.com/trinodb/trino/issues/5317) placement of precision parameter
                        assertThat(rs.getTimestamp(column)).isEqualTo(timestampForPointInTime);
                    });

            // before epoch
            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '1957-12-31 23:59:59.999999999999 Europe/Warsaw'", Types.TIMESTAMP_WITH_TIMEZONE, (rs, column) -> {
                ZonedDateTime zonedDateTime = ZonedDateTime.of(1958, 1, 1, 0, 0, 0, 0, ZoneId.of("Europe/Warsaw"));
                Timestamp timestampForPointInTime = Timestamp.from(zonedDateTime.toInstant());
                assertThat(rs.getObject(column)).isEqualTo(timestampForPointInTime);  // TODO this should represent TIMESTAMP '2019-12-31 23:59:59.999999999999 Europe/Warsaw'
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 1957-12-31 23:59:59.999999999999 Europe/Warsaw");
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp with time zone(12)"); // TODO (https://github.com/trinodb/trino/issues/5317) placement of precision parameter
                assertThat(rs.getTimestamp(column)).isEqualTo(timestampForPointInTime);
            });

            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '1970-01-01 09:14:15.227 Europe/Warsaw'", Types.TIMESTAMP_WITH_TIMEZONE, (rs, column) -> {
                ZonedDateTime zonedDateTime = ZonedDateTime.of(1970, 1, 1, 9, 14, 15, 227_000_000, ZoneId.of("Europe/Warsaw"));
                Timestamp timestampForPointInTime = Timestamp.from(zonedDateTime.toInstant());
                assertThat(rs.getObject(column)).isEqualTo(timestampForPointInTime); // TODO this should represent TIMESTAMP '1970-01-01 09:14:15.227 Europe/Warsaw'
                assertThat(rs.getObject(column, ZonedDateTime.class)).isEqualTo(zonedDateTime);
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 1970-01-01 09:14:15.227 Europe/Warsaw");
                // TODO (https://github.com/trinodb/trino/issues/5317) placement of precision parameter
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp with time zone(3)");
                assertThat(rs.getTimestamp(column)).isEqualTo(timestampForPointInTime);
            });

            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '1970-01-01 00:14:15.227 Europe/Warsaw'", Types.TIMESTAMP_WITH_TIMEZONE, (rs, column) -> {
                ZonedDateTime zonedDateTime = ZonedDateTime.of(1970, 1, 1, 0, 14, 15, 227_000_000, ZoneId.of("Europe/Warsaw"));
                Timestamp timestampForPointInTime = Timestamp.from(zonedDateTime.toInstant());
                assertThat(rs.getObject(column)).isEqualTo(timestampForPointInTime); // TODO this should represent TIMESTAMP '1970-01-01 00:14:15.227 Europe/Warsaw'
                assertThat(rs.getObject(column, ZonedDateTime.class)).isEqualTo(zonedDateTime);
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: 1970-01-01 00:14:15.227 Europe/Warsaw");
                // TODO (https://github.com/trinodb/trino/issues/5317) placement of precision parameter
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp with time zone(3)");
                assertThat(rs.getTimestamp(column)).isEqualTo(timestampForPointInTime);
            });

            // TODO https://github.com/trinodb/trino/issues/4363
//        checkRepresentation(statementWrapper.getStatement(), "TIMESTAMP '-12345-01-23 01:23:45.123456789 Europe/Warsaw'", Types.TIMESTAMP_WITH_TIMEZONE, (rs, column) -> {
//            ...
//        });

            checkRepresentation(connectedStatement.getStatement(), "TIMESTAMP '12345-01-23 01:23:45.123456789 Europe/Warsaw'", Types.TIMESTAMP_WITH_TIMEZONE, (rs, column) -> {
                ZonedDateTime zonedDateTime = ZonedDateTime.of(12345, 1, 23, 1, 23, 45, 123_456_789, ZoneId.of("Europe/Warsaw"));
                Timestamp timestampForPointInTime = Timestamp.from(zonedDateTime.toInstant());
                assertThat(rs.getObject(column)).isEqualTo(timestampForPointInTime); // TODO this should contain the zone
                assertThat(rs.getObject(column, ZonedDateTime.class)).isEqualTo(zonedDateTime);
                assertThatThrownBy(() -> rs.getDate(column))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Expected value to be a date but is: +12345-01-23 01:23:45.123456789 Europe/Warsaw");
                assertThatThrownBy(() -> rs.getTime(column))
                        .isInstanceOf(IllegalArgumentException.class) // TODO (https://github.com/trinodb/trino/issues/5315) SQLException
                        .hasMessage("Expected column to be a time type but is timestamp with time zone(9)"); // TODO (https://github.com/trinodb/trino/issues/5317) placement of precision parameter
                assertThat(rs.getTimestamp(column)).isEqualTo(timestampForPointInTime);
            });
        }
    }

    @Test
    public void testIpAddress()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            checkRepresentation(connectedStatement.getStatement(), "IPADDRESS '1.2.3.4'", Types.JAVA_OBJECT, "1.2.3.4");
        }
    }

    @Test
    public void testUuid()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            checkRepresentation(connectedStatement.getStatement(), "UUID '0397e63b-2b78-4b7b-9c87-e085fa225dd8'", Types.JAVA_OBJECT, "0397e63b-2b78-4b7b-9c87-e085fa225dd8");
        }
    }

    @Test
    public void testArray()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            // no NULL elements
            checkRepresentation(connectedStatement.getStatement(), "ARRAY[1, 2]", Types.ARRAY, (rs, column) -> {
                Array array = rs.getArray(column);
                assertThat(array.getArray()).isEqualTo(new Object[] {1, 2});
                assertThat(array.getBaseType()).isEqualTo(Types.INTEGER);
                assertThat(array.getBaseTypeName()).isEqualTo("integer");

                array = (Array) rs.getObject(column); // TODO (https://github.com/trinodb/trino/issues/6049) subject to change
                assertThat(array.getArray()).isEqualTo(new Object[] {1, 2});
                assertThat(array.getBaseType()).isEqualTo(Types.INTEGER);
                assertThat(array.getBaseTypeName()).isEqualTo("integer");

                assertThat(rs.getObject(column, List.class)).isEqualTo(ImmutableList.of(1, 2));
            });

            checkArrayRepresentation(connectedStatement.getStatement(), "1", Types.INTEGER, "integer");
            checkArrayRepresentation(connectedStatement.getStatement(), "BIGINT '1'", Types.BIGINT, "bigint");
            checkArrayRepresentation(connectedStatement.getStatement(), "REAL '42.123'", Types.REAL, "real");
            checkArrayRepresentation(connectedStatement.getStatement(), "DOUBLE '42.123'", Types.DOUBLE, "double");
            checkArrayRepresentation(connectedStatement.getStatement(), "42.123", Types.DECIMAL, "decimal(5,3)");

            checkArrayRepresentation(connectedStatement.getStatement(), "TIMESTAMP '2017-01-02 09:00:00.123'", Types.TIMESTAMP, "timestamp(3)");
            checkArrayRepresentation(connectedStatement.getStatement(), "TIMESTAMP '2017-01-02 09:00:00.123456789'", Types.TIMESTAMP, "timestamp(9)");

            checkArrayRepresentation(connectedStatement.getStatement(), "TIMESTAMP '2017-01-02 09:00:00.123 Europe/Warsaw'", Types.TIMESTAMP_WITH_TIMEZONE, "timestamp with time zone(3)");
            checkArrayRepresentation(connectedStatement.getStatement(), "TIMESTAMP '2017-01-02 09:00:00.123456789 Europe/Warsaw'", Types.TIMESTAMP_WITH_TIMEZONE, "timestamp with time zone(9)");

            // array or array
            checkRepresentation(connectedStatement.getStatement(), "ARRAY[NULL, ARRAY[NULL, BIGINT '1', 2]]", Types.ARRAY, (rs, column) -> {
                Array array = rs.getArray(column);
                assertThat(array.getArray()).isEqualTo(new Object[] {null, asList(null, 1L, 2L)});
                assertThat(array.getBaseType()).isEqualTo(Types.ARRAY);
                assertThat(array.getBaseTypeName()).isEqualTo("array(bigint)");

                array = (Array) rs.getObject(column); // TODO (https://github.com/trinodb/trino/issues/6049) subject to change
                assertThat(array.getArray()).isEqualTo(new Object[] {null, asList(null, 1L, 2L)});
                assertThat(array.getBaseType()).isEqualTo(Types.ARRAY);
                assertThat(array.getBaseTypeName()).isEqualTo("array(bigint)");

                assertThat(rs.getObject(column, List.class)).isEqualTo(asList(null, asList(null, 1L, 2L)));
            });

            // array of map
            checkRepresentation(connectedStatement.getStatement(), "ARRAY[map(ARRAY['k1', 'k2'], ARRAY[42, NULL])]", Types.ARRAY, (rs, column) -> {
                Map<String, Integer> element = new HashMap<>();
                element.put("k1", 42);
                element.put("k2", null);

                Array array = rs.getArray(column);
                assertThat(array.getArray()).isEqualTo(new Object[] {element});
                assertThat(array.getBaseType()).isEqualTo(Types.JAVA_OBJECT);
                assertThat(array.getBaseTypeName()).isEqualTo("map(varchar(2),integer)");

                array = (Array) rs.getObject(column);
                assertThat(array.getArray()).isEqualTo(new Object[] {element});
                assertThat(array.getBaseType()).isEqualTo(Types.JAVA_OBJECT);
                assertThat(array.getBaseTypeName()).isEqualTo("map(varchar(2),integer)");

                assertThat(rs.getObject(column, List.class)).isEqualTo(ImmutableList.of(element));
            });

            // array of row
            checkRepresentation(connectedStatement.getStatement(), "ARRAY[CAST(ROW(42, 'Trino') AS row(a_bigint bigint, a_varchar varchar(17)))]", Types.ARRAY, (rs, column) -> {
                Row element = Row.builder()
                        .addField("a_bigint", 42L)
                        .addField("a_varchar", "Trino")
                        .build();

                Array array = rs.getArray(column);
                assertThat(array.getArray()).isEqualTo(new Object[] {element});
                assertThat(array.getBaseType()).isEqualTo(Types.JAVA_OBJECT);
                assertThat(array.getBaseTypeName()).isEqualTo("row(a_bigint bigint,a_varchar varchar(17))");

                array = (Array) rs.getObject(column);
                assertThat(array.getArray()).isEqualTo(new Object[] {element});
                assertThat(array.getBaseType()).isEqualTo(Types.JAVA_OBJECT);
                assertThat(array.getBaseTypeName()).isEqualTo("row(a_bigint bigint,a_varchar varchar(17))");

                assertThat(rs.getObject(column, List.class)).isEqualTo(ImmutableList.of(element));
            });
        }
    }

    private void checkArrayRepresentation(Statement statement, String elementExpression, int elementSqlType, String elementTypeName)
            throws Exception
    {
        Object element = getObjectRepresentation(statement.getConnection(), elementExpression);
        checkRepresentation(statement, format("ARRAY[NULL, %s]", elementExpression), Types.ARRAY, (rs, column) -> {
            Array array = rs.getArray(column);
            assertThat(array.getArray()).isEqualTo(new Object[] {null, element});
            assertThat(array.getBaseType()).isEqualTo(elementSqlType);
            assertThat(array.getBaseTypeName()).isEqualTo(elementTypeName);

            array = (Array) rs.getObject(column); // TODO (https://github.com/trinodb/trino/issues/6049) subject to change
            assertThat(array.getArray()).isEqualTo(new Object[] {null, element});
            assertThat(array.getBaseType()).isEqualTo(elementSqlType);
            assertThat(array.getBaseTypeName()).isEqualTo(elementTypeName);

            assertThat(rs.getObject(column, List.class)).isEqualTo(asList(null, element));
        });
    }

    @Test
    public void testMap()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            checkRepresentation(connectedStatement.getStatement(), "map(ARRAY['k1', 'k2'], ARRAY[BIGINT '42', -117])", Types.JAVA_OBJECT, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(ImmutableMap.of("k1", 42L, "k2", -117L));
                assertThat(rs.getObject(column, Map.class)).isEqualTo(ImmutableMap.of("k1", 42L, "k2", -117L));
            });

            // NULL value
            checkRepresentation(connectedStatement.getStatement(), "map(ARRAY['k1', 'k2'], ARRAY[42, NULL])", Types.JAVA_OBJECT, (rs, column) -> {
                Map<String, Integer> expected = new HashMap<>();
                expected.put("k1", 42);
                expected.put("k2", null);
                assertThat(rs.getObject(column)).isEqualTo(expected);
                assertThat(rs.getObject(column, Map.class)).isEqualTo(expected);
            });

            // map or row
            checkRepresentation(connectedStatement.getStatement(), "map(ARRAY['k1', 'k2'], ARRAY[CAST(ROW(42) AS row(a integer)), NULL])", Types.JAVA_OBJECT, (rs, column) -> {
                Map<String, Row> expected = new HashMap<>();
                expected.put("k1", Row.builder().addField("a", 42).build());
                expected.put("k2", null);
                assertThat(rs.getObject(column)).isEqualTo(expected);
                assertThat(rs.getObject(column, Map.class)).isEqualTo(expected);
            });
        }
    }

    @Test
    public void testRow()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            // named row
            checkRepresentation(connectedStatement.getStatement(), "CAST(ROW(42, 'Trino') AS row(a_bigint bigint, a_varchar varchar(17)))", Types.JAVA_OBJECT, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Row.builder()
                        .addField("a_bigint", 42L)
                        .addField("a_varchar", "Trino")
                        .build());
                assertThat(rs.getObject(column, Map.class)).isEqualTo(ImmutableMap.of("a_bigint", 42L, "a_varchar", "Trino"));
            });

            // partially named row
            checkRepresentation(connectedStatement.getStatement(), "CAST(ROW(42, 'Trino') AS row(a_bigint bigint, varchar(17)))", Types.JAVA_OBJECT, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Row.builder()
                        .addField("a_bigint", 42L)
                        .addUnnamedField("Trino")
                        .build());
                assertThat(rs.getObject(column, Map.class)).isEqualTo(ImmutableMap.of("a_bigint", 42L, "field1", "Trino"));
            });

            // anonymous row
            checkRepresentation(connectedStatement.getStatement(), "ROW(42, 'Trino')", Types.JAVA_OBJECT, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Row.builder()
                        .addUnnamedField(42)
                        .addUnnamedField("Trino")
                        .build());
                assertThat(rs.getObject(column, Map.class)).isEqualTo(ImmutableMap.of("field0", 42, "field1", "Trino"));
            });

            // name collision
            checkRepresentation(connectedStatement.getStatement(), "CAST(ROW(42, 'Trino') AS row(field1 integer, varchar(17)))", Types.JAVA_OBJECT, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Row.builder()
                        .addField("field1", 42)
                        .addUnnamedField("Trino")
                        .build());
                assertThatThrownBy(() -> rs.getObject(column, Map.class))
                        .isInstanceOf(SQLException.class)
                        .hasMessageMatching("Duplicate field name: field1");
            });

            // name collision with NULL value
            checkRepresentation(connectedStatement.getStatement(), "CAST(ROW(NULL, NULL) AS row(field1 integer, varchar(17)))", Types.JAVA_OBJECT, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Row.builder()
                        .addField("field1", null)
                        .addUnnamedField(null)
                        .build());
                assertThatThrownBy(() -> rs.getObject(column, Map.class))
                        .isInstanceOf(SQLException.class)
                        .hasMessageMatching("Duplicate field name: field1");
            });

            // row of row or row
            checkRepresentation(connectedStatement.getStatement(), "ROW(ROW(ROW(42)))", Types.JAVA_OBJECT, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Row.builder()
                        .addUnnamedField(Row.builder()
                                .addUnnamedField(Row.builder().addUnnamedField(42).build())
                                .build())
                        .build());
            });
            checkRepresentation(connectedStatement.getStatement(), "CAST(ROW(ROW(ROW(42))) AS row(a row(b row(c integer))))", Types.JAVA_OBJECT, (rs, column) -> {
                assertThat(rs.getObject(column)).isEqualTo(Row.builder()
                        .addField("a", Row.builder()
                                .addField("b", Row.builder().addField("c", 42).build())
                                .build())
                        .build());
            });

            // row of array of map of row
            checkRepresentation(
                    connectedStatement.getStatement(),
                    "CAST(" +
                            "   ROW(ARRAY[NULL, map(ARRAY['k1', 'k2'], ARRAY[NULL, ROW(42)])]) AS" +
                            "   row(\"outer\" array(map(varchar, row(leaf integer)))))",
                    Types.JAVA_OBJECT,
                    (rs, column) -> {
                        Map<String, Object> map = new HashMap<>();
                        map.put("k1", null);
                        map.put("k2", Row.builder().addField("leaf", 42).build());
                        map = unmodifiableMap(map);
                        List<Object> array = new ArrayList<>();
                        array.add(null);
                        array.add(map);
                        array = unmodifiableList(array);
                        assertThat(rs.getObject(column)).isEqualTo(Row.builder().addField("outer", array).build());
                        assertThat(rs.getObject(column, Map.class)).isEqualTo(ImmutableMap.of("outer", array));
                    });
        }
    }

    private void checkRepresentation(Statement statement, String expression, int expectedSqlType, Object expectedRepresentation)
            throws SQLException
    {
        checkRepresentation(statement, expression, expectedSqlType, (rs, column) -> {
            assertThat(rs.getObject(column)).isEqualTo(expectedRepresentation);
            assertThat(rs.getObject(column, expectedRepresentation.getClass())).isEqualTo(expectedRepresentation);
        });
    }

    private void checkRepresentation(Statement statement, String expression, int expectedSqlType, ResultAssertion assertion)
            throws SQLException
    {
        try (ResultSet rs = statement.executeQuery("SELECT " + expression)) {
            ResultSetMetaData metadata = rs.getMetaData();
            assertThat(metadata.getColumnCount()).isEqualTo(1);
            assertThat(metadata.getColumnType(1)).isEqualTo(expectedSqlType);
            assertThat(rs.next()).isTrue();
            assertion.accept(rs, 1);
            assertThat(rs.next()).isFalse();
        }
    }

    private Object getObjectRepresentation(Connection connection, String expression)
            throws SQLException
    {
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT " + expression)) {
            ResultSetMetaData metadata = rs.getMetaData();
            assertThat(metadata.getColumnCount()).isEqualTo(1);
            assertThat(rs.next()).isTrue();
            Object object = rs.getObject(1);
            assertThat(rs.next()).isFalse();
            return object;
        }
    }

    @Test
    public void testStatsExtraction()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            try (TrinoResultSet rs = (TrinoResultSet) connectedStatement.getStatement().executeQuery("SELECT 123 x, 456 x")) {
                assertThat(rs.getStats()).isNotNull();
                assertThat(rs.next()).isTrue();
                assertThat(rs.getStats()).isNotNull();
                assertThat(rs.next()).isFalse();
                assertThat(rs.getStats()).isNotNull();
            }
        }
    }

    @Test
    public void testMaxRowsUnset()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            assertMaxRowsLimit(connectedStatement.getStatement(), 0);
            assertMaxRowsResult(connectedStatement.getStatement(), 7);
        }
    }

    @Test
    public void testMaxRowsUnlimited()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            assertMaxRowsLimit(connectedStatement.getStatement(), 0);
            connectedStatement.getStatement().setMaxRows(0);
            assertMaxRowsLimit(connectedStatement.getStatement(), 0);
            assertMaxRowsResult(connectedStatement.getStatement(), 7);
        }
    }

    @Test
    public void testMaxRowsLimited()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            assertMaxRowsLimit(connectedStatement.getStatement(), 0);
            connectedStatement.getStatement().setMaxRows(4);
            assertMaxRowsLimit(connectedStatement.getStatement(), 4);
            assertMaxRowsResult(connectedStatement.getStatement(), 4);
        }
    }

    @Test
    public void testMaxRowsLimitLargerThanResult()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            assertMaxRowsLimit(connectedStatement.getStatement(), 0);
            connectedStatement.getStatement().setMaxRows(10);
            assertMaxRowsLimit(connectedStatement.getStatement(), 10);
            assertMaxRowsResult(connectedStatement.getStatement(), 7);
        }
    }

    @Test
    public void testLargeMaxRowsUnlimited()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            assertMaxRowsLimit(connectedStatement.getStatement(), 0);
            connectedStatement.getStatement().setLargeMaxRows(0);
            assertMaxRowsLimit(connectedStatement.getStatement(), 0);
            assertMaxRowsResult(connectedStatement.getStatement(), 7);
        }
    }

    @Test
    public void testLargeMaxRowsLimited()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            assertMaxRowsLimit(connectedStatement.getStatement(), 0);
            connectedStatement.getStatement().setLargeMaxRows(4);
            assertMaxRowsLimit(connectedStatement.getStatement(), 4);
            assertMaxRowsResult(connectedStatement.getStatement(), 4);
        }
    }

    @Test
    public void testLargeMaxRowsLimitLargerThanResult()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            long limit = Integer.MAX_VALUE * 10L;
            connectedStatement.getStatement().setLargeMaxRows(limit);
            assertThat(connectedStatement.getStatement().getLargeMaxRows()).isEqualTo(limit);
            assertMaxRowsResult(connectedStatement.getStatement(), 7);
        }
    }

    private void assertMaxRowsLimit(Statement statement, int expectedLimit)
            throws SQLException
    {
        assertThat(statement.getMaxRows()).isEqualTo(expectedLimit);
        assertThat(statement.getLargeMaxRows()).isEqualTo(expectedLimit);
    }

    private void assertMaxRowsResult(Statement statement, long expectedCount)
            throws SQLException
    {
        try (ResultSet rs = statement.executeQuery("SELECT * FROM (VALUES (1), (2), (3), (4), (5), (6), (7)) AS x (a)")) {
            assertThat(countRows(rs)).isEqualTo(expectedCount);
        }
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Max rows exceeds limit of 2147483647")
    public void testMaxRowsExceedsLimit()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            connectedStatement.getStatement().setLargeMaxRows(Integer.MAX_VALUE * 10L);
            connectedStatement.getStatement().getMaxRows();
        }
    }

    @Test
    public void testGetStatement()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            try (ResultSet rs = connectedStatement.getStatement().executeQuery("SELECT * FROM (VALUES (1), (2), (3))")) {
                assertThat(rs.getStatement()).isEqualTo(connectedStatement.getStatement());
            }
        }
    }

    @Test
    public void testGetRow()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            try (ResultSet rs = connectedStatement.getStatement().executeQuery("SELECT * FROM (VALUES (1), (2), (3))")) {
                assertThat(rs.getRow()).isEqualTo(0);
                int currentRow = 0;
                while (rs.next()) {
                    currentRow++;
                    assertThat(rs.getRow()).isEqualTo(currentRow);
                }
                assertThat(rs.getRow()).isEqualTo(0);
            }
        }
    }

    @Test
    public void testGetRowException()
            throws Exception
    {
        try (ConnectedStatement connectedStatement = newStatement()) {
            ResultSet rs = connectedStatement.getStatement().executeQuery("SELECT * FROM (VALUES (1), (2), (3))");
            rs.close();
            assertThatThrownBy(rs::getRow)
                    .isInstanceOf(SQLException.class)
                    .hasMessage("ResultSet is closed");
        }
    }

    private static long countRows(ResultSet rs)
            throws SQLException
    {
        long count = 0;
        while (rs.next()) {
            count++;
        }
        return count;
    }

    @FunctionalInterface
    private interface ResultAssertion
    {
        void accept(ResultSet rs, int column)
                throws SQLException;
    }

    protected ConnectedStatement newStatement()
    {
        return new ConnectedStatement();
    }

    protected class ConnectedStatement
            implements AutoCloseable
    {
        private final Connection connection;
        private final Statement statement;

        public ConnectedStatement()
        {
            try {
                this.connection = createConnection();
                this.statement = connection.createStatement();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close()
                throws SQLException
        {
            statement.close();
            connection.close();
        }

        public Statement getStatement()
        {
            return statement;
        }
    }

    static Time toSqlTime(LocalTime localTime)
    {
        // Time.valueOf does not preserve second fraction.
        // Expect no rounding, since this is used to create tests' expected values.
        // Also, java.sql.Time has millisecond precision.
        return new Time(Time.valueOf(localTime).getTime() + IntMath.divide(localTime.getNano(), NANOSECONDS_PER_MILLISECOND, UNNECESSARY));
    }
}
