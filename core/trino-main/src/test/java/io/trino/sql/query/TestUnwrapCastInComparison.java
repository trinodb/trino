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
package io.trino.sql.query;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalTime;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestUnwrapCastInComparison
{
    private static final List<String> COMPARISON_OPERATORS = asList("=", "<>", ">=", ">", "<=", "<", "IS DISTINCT FROM");

    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testTinyint()
    {
        for (Number from : asList(null, Byte.MIN_VALUE, 0, 1, Byte.MAX_VALUE)) {
            String fromType = "TINYINT";
            for (String operator : COMPARISON_OPERATORS) {
                for (Number to : asList(null, Byte.MIN_VALUE - 1, Byte.MIN_VALUE, 0, 1, Byte.MAX_VALUE, Byte.MAX_VALUE + 1)) {
                    validate(operator, fromType, from, "SMALLINT", to);
                }

                for (Number to : asList(null, Byte.MIN_VALUE - 1, Byte.MIN_VALUE, 0, 1, Byte.MAX_VALUE, Byte.MAX_VALUE + 1)) {
                    validate(operator, fromType, from, "INTEGER", to);
                }

                for (Number to : asList(null, Byte.MIN_VALUE - 1, Byte.MIN_VALUE, 0, 1, Byte.MAX_VALUE, Byte.MAX_VALUE + 1)) {
                    validate(operator, fromType, from, "BIGINT", to);
                }

                for (Number to : asList(null, Byte.MIN_VALUE - 1, Byte.MIN_VALUE, 0, 1, Byte.MAX_VALUE, Byte.MAX_VALUE + 1)) {
                    validate(operator, fromType, from, "REAL", to);
                }

                for (Number to : asList(null, Byte.MIN_VALUE - 1, Byte.MIN_VALUE, 0, 1, Byte.MAX_VALUE, Byte.MAX_VALUE + 1)) {
                    validate(operator, fromType, from, "DOUBLE", to);
                }
            }
        }
    }

    @Test
    public void testSmallint()
    {
        for (Number from : asList(null, Short.MIN_VALUE, 0, 1, Short.MAX_VALUE)) {
            String fromType = "SMALLINT";
            for (String operator : COMPARISON_OPERATORS) {
                for (Number to : asList(null, Short.MIN_VALUE - 1, Short.MIN_VALUE, 0, 1, Short.MAX_VALUE, Short.MAX_VALUE + 1)) {
                    validate(operator, fromType, from, "INTEGER", to);
                }

                for (Number to : asList(null, Short.MIN_VALUE - 1, Short.MIN_VALUE, 0, 1, Short.MAX_VALUE, Short.MAX_VALUE + 1)) {
                    validate(operator, fromType, from, "BIGINT", to);
                }

                for (Number to : asList(null, Short.MIN_VALUE - 1, Short.MIN_VALUE, 0, 1, Short.MAX_VALUE, Short.MAX_VALUE + 1)) {
                    validate(operator, fromType, from, "REAL", to);
                }

                for (Number to : asList(null, Short.MIN_VALUE - 1, Short.MIN_VALUE, 0, 1, Short.MAX_VALUE, Short.MAX_VALUE + 1)) {
                    validate(operator, fromType, from, "DOUBLE", to);
                }
            }
        }
    }

    @Test
    public void testInteger()
    {
        for (Number from : asList(null, Integer.MIN_VALUE, 0, 1, Integer.MAX_VALUE)) {
            String fromType = "INTEGER";
            for (String operator : COMPARISON_OPERATORS) {
                for (Number to : asList(null, Integer.MIN_VALUE - 1L, Integer.MIN_VALUE, 0, 1, Integer.MAX_VALUE, Integer.MAX_VALUE + 1L)) {
                    validate(operator, fromType, from, "BIGINT", to);
                }

                for (Number to : asList(null, Integer.MIN_VALUE - 1L, Integer.MIN_VALUE, 0, 0.1, 0.9, 1, Integer.MAX_VALUE, Integer.MAX_VALUE + 1L)) {
                    validate(operator, fromType, from, "DOUBLE", to);
                }

                for (Number to : asList(null, Integer.MIN_VALUE - 1L, Integer.MIN_VALUE, -1L << 23 + 1, 0, 0.1, 0.9, 1, 1L << 23 - 1, Integer.MAX_VALUE, Integer.MAX_VALUE + 1L)) {
                    validate(operator, fromType, from, "REAL", to);
                }
            }
        }
    }

    @Test
    public void testBigint()
    {
        for (Number from : asList(null, Long.MIN_VALUE, 0, 1, Long.MAX_VALUE)) {
            String fromType = "BIGINT";
            for (String operator : COMPARISON_OPERATORS) {
                for (Number to : asList(null, Long.MIN_VALUE, Long.MIN_VALUE + 1, -1L << 53 + 1, 0, 0.1, 0.9, 1, 1L << 53 - 1, Long.MAX_VALUE - 1, Long.MAX_VALUE)) {
                    validate(operator, fromType, from, "DOUBLE", to);
                }

                for (Number to : asList(null, Long.MIN_VALUE, Long.MIN_VALUE + 1, -1L << 23 + 1, 0, 0.1, 0.9, 1, 1L << 23 - 1, Long.MAX_VALUE - 1, Long.MAX_VALUE)) {
                    validate(operator, fromType, from, "REAL", to);
                }
            }
        }
    }

    @Test
    public void testReal()
    {
        String fromType = "REAL";
        String toType = "DOUBLE";

        for (String from : toLiteral(fromType, asList(null, Float.NEGATIVE_INFINITY, -Float.MAX_VALUE, 0, 0.1, 0.9, 1, Float.MAX_VALUE, Float.POSITIVE_INFINITY, Float.NaN))) {
            for (String operator : COMPARISON_OPERATORS) {
                for (String to : toLiteral(toType, asList(null, Double.NEGATIVE_INFINITY, Math.nextDown((double) -Float.MIN_VALUE), (double) -Float.MIN_VALUE, 0, 0.1, 0.9, 1, (double) Float.MAX_VALUE, Math.nextUp((double) Float.MAX_VALUE), Double.POSITIVE_INFINITY, Double.NaN))) {
                    validate(operator, fromType, from, toType, to);
                }
            }
        }
    }

    @Test
    public void testDecimal()
    {
        // decimal(15) -> double
        List<String> values = ImmutableList.of("-999999999999999", "999999999999999");
        for (String from : values) {
            for (String operator : COMPARISON_OPERATORS) {
                for (String to : values) {
                    validate(operator, "DECIMAL(15, 0)", from, "DOUBLE", Double.valueOf(to));
                }
            }
        }

        // decimal(16) -> double
        values = ImmutableList.of("-9999999999999999", "9999999999999999");
        for (String from : values) {
            for (String operator : COMPARISON_OPERATORS) {
                for (String to : values) {
                    validate(operator, "DECIMAL(16, 0)", from, "DOUBLE", Double.valueOf(to));
                }
            }
        }

        // decimal(7) -> real
        values = ImmutableList.of("-999999", "999999");
        for (String from : values) {
            for (String operator : COMPARISON_OPERATORS) {
                for (String to : values) {
                    validate(operator, "DECIMAL(7, 0)", from, "REAL", Double.valueOf(to));
                }
            }
        }

        // decimal(8) -> real
        values = ImmutableList.of("-9999999", "9999999");
        for (String from : values) {
            for (String operator : COMPARISON_OPERATORS) {
                for (String to : values) {
                    validate(operator, "DECIMAL(8, 0)", from, "REAL", Double.valueOf(to));
                }
            }
        }
    }

    @Test
    public void testVarchar()
    {
        for (String from : asList(null, "''", "'a'", "'b'")) {
            for (String operator : COMPARISON_OPERATORS) {
                for (String to : asList(null, "''", "'a'", "'aa'", "'b'", "'bb'")) {
                    validate(operator, "VARCHAR(1)", from, "VARCHAR(2)", to);
                }
            }
        }

        // type with no range
        for (String operator : COMPARISON_OPERATORS) {
            for (String to : asList("'" + "a".repeat(200) + "'", "'" + "b".repeat(200) + "'")) {
                validate(operator, "VARCHAR(200)", "'" + "a".repeat(200) + "'", "VARCHAR(300)", to);
            }
        }
    }

    @Test
    public void testCastTimestampToTimestampWithTimeZone()
    {
        // The values in this test are chosen for Pacific/Apia's DST changes
        Session session = Session.builder(assertions.getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("Pacific/Apia"))
                .build();

        for (String operator : COMPARISON_OPERATORS) {
            validate(session, operator, "timestamp(3)", "TIMESTAMP '2020-07-03 01:23:45.123'", "timestamp(3) with time zone", "TIMESTAMP '2020-07-03 01:23:45 Europe/Warsaw'");
            validate(session, operator, "timestamp(3)", "TIMESTAMP '2020-07-03 01:23:45.123'", "timestamp(3) with time zone", "TIMESTAMP '2020-07-03 01:23:45 UTC'");
            validate(session, operator, "timestamp(6)", "TIMESTAMP '2020-07-03 01:23:45.123456'", "timestamp(6) with time zone", "TIMESTAMP '2020-07-03 01:23:45 Europe/Warsaw'");
            validate(session, operator, "timestamp(6)", "TIMESTAMP '2020-07-03 01:23:45.123456'", "timestamp(6) with time zone", "TIMESTAMP '2020-07-03 01:23:45 UTC'");
            validate(session, operator, "timestamp(9)", "TIMESTAMP '2020-07-03 01:23:45.123456789'", "timestamp(9) with time zone", "TIMESTAMP '2020-07-03 01:23:45 Europe/Warsaw'");
            validate(session, operator, "timestamp(9)", "TIMESTAMP '2020-07-03 01:23:45.123456789'", "timestamp(9) with time zone", "TIMESTAMP '2020-07-03 01:23:45 UTC'");
            validate(session, operator, "timestamp(12)", "TIMESTAMP '2020-07-03 01:23:45.123456789123'", "timestamp(12) with time zone", "TIMESTAMP '2020-07-03 01:23:45 Europe/Warsaw'");
            validate(session, operator, "timestamp(12)", "TIMESTAMP '2020-07-03 01:23:45.123456789123'", "timestamp(12) with time zone", "TIMESTAMP '2020-07-03 01:23:45 UTC'");
        }

        // DST forward change (2017-09-24 03:00 -> 2017-09-24 04:00)
        List<LocalTime> fromLocalTimes = asList(
                LocalTime.parse("02:59:59.999999999"),
                LocalTime.parse("03:00:00"),
                LocalTime.parse("03:00:00.000000001"),
                LocalTime.parse("03:00:00.000000002"),
                LocalTime.parse("03:59:59.999999999"),
                LocalTime.parse("04:00:00"),
                LocalTime.parse("04:00:00.000000001"),
                LocalTime.parse("04:00:00.000000002"));

        List<LocalTime> toLocalTimes = asList(
                // 2017-09-24 02:59:00 Pacific/Apia is 2017-09-23T13:59:00Z
                LocalTime.parse("13:59:59.999999999"),
                // 2017-09-24 04:00:00 Pacific/Apia is 2017-09-23T14:00:00Z
                // 2017-09-24 03:00:00 gets interpreted as 2017-09-23T14:00:00Z too
                LocalTime.parse("14:00:00"),
                LocalTime.parse("14:00:00.000000001"),
                LocalTime.parse("14:00:00.000000002"),
                LocalTime.parse("14:59:59.999999999"),
                LocalTime.parse("15:00:00"),
                LocalTime.parse("15:00:00.000000001"),
                LocalTime.parse("15:00:00.000000002"));

        for (LocalTime fromLocalTime : fromLocalTimes) {
            for (LocalTime toLocalTime : toLocalTimes) {
                for (int timestampPrecision : asList(0, 3, 6, 9, 12)) {
                    for (String operator : COMPARISON_OPERATORS) {
                        validate(
                                session,
                                operator,
                                format("timestamp(%s)", timestampPrecision),
                                format("TIMESTAMP '2017-09-24 %s'", fromLocalTime),
                                format("timestamp(%s) with time zone", max(9, timestampPrecision)),
                                format("TIMESTAMP '2017-04-01 %s Z'", toLocalTime));
                    }
                }
            }
        }

        // DST backward change (2017-04-02 04:00 -> 2017-04-02 03:00)
        fromLocalTimes = asList(
                LocalTime.parse("02:59:59.999999999"),
                LocalTime.parse("03:00:00"),
                LocalTime.parse("03:00:00.000000001"),
                LocalTime.parse("03:00:00.000000002"),
                LocalTime.parse("03:59:59.999999999"),
                LocalTime.parse("04:00:00"),
                LocalTime.parse("04:00:00.000000001"),
                LocalTime.parse("04:00:00.000000002"));

        toLocalTimes = asList(
                // 2017-04-02 02:59:00 Pacific/Apia is 2017-04-01T12:59:00Z
                LocalTime.parse("12:59:59.999999999"),
                // 2017-04-02 03:00:00 Pacific/Apia is 2017-04-01T13:00:00Z
                LocalTime.parse("13:00:00"),
                LocalTime.parse("13:00:00.000000001"),
                LocalTime.parse("13:00:00.000000002"),
                LocalTime.parse("13:59:59.999999999"),
                // [2017-04-01T14:00:00Z - 2017-04-01T15:00:00Z) range is not addressable with TIMESTAMP to TIMESTAMP WITH TIME ZONE cast in Pacific/Apia zone
                LocalTime.parse("14:00:00"),
                LocalTime.parse("14:00:00.000000001"),
                LocalTime.parse("14:00:00.000000002"),
                LocalTime.parse("14:59:59.999999999"),
                // 2017-04-02 04:00:00 Pacific/Apia is 2017-04-01T15:00:00Z
                LocalTime.parse("15:00:00"),
                LocalTime.parse("15:00:00.000000001"),
                LocalTime.parse("15:00:00.000000002"));

        for (LocalTime fromLocalTime : fromLocalTimes) {
            for (LocalTime toLocalTime : toLocalTimes) {
                for (int timestampPrecision : asList(0, 3, 6, 9, 12)) {
                    for (String operator : COMPARISON_OPERATORS) {
                        validate(
                                session,
                                operator,
                                format("timestamp(%s)", timestampPrecision),
                                format("TIMESTAMP '2017-09-24 %s'", fromLocalTime),
                                format("timestamp(%s) with time zone", max(9, timestampPrecision)),
                                format("TIMESTAMP '2017-04-01 %s Z'", toLocalTime));
                    }
                }
            }
        }
    }

    private void validate(String operator, String fromType, Object fromValue, String toType, Object toValue)
    {
        validate(assertions.getDefaultSession(), operator, fromType, fromValue, toType, toValue);
    }

    private void validate(Session session, String operator, String fromType, Object fromValue, String toType, Object toValue)
    {
        String query = format(
                "SELECT (CAST(v AS %s) %s CAST(%s AS %s)) " +
                        "IS NOT DISTINCT FROM " +
                        "(CAST(%s AS %s) %s CAST(%s AS %s)) " +
                        "FROM (VALUES CAST(%s AS %s)) t(v)",
                toType, operator, toValue, toType,
                fromValue, toType, operator, toValue, toType,
                fromValue, fromType);

        boolean result = (boolean) assertions.execute(session, query)
                .getMaterializedRows()
                .get(0)
                .getField(0);

        assertTrue(result, "Query evaluated to false: " + query);
    }

    @Test
    public void testUnwrapTimestampToDate()
    {
        for (String from : asList(
                null,
                "1981-06-21 23:59:59.999",
                "1981-06-22 00:00:00.000",
                "1981-06-22 00:00:00.001",
                "1981-06-22 23:59:59.999",
                "1981-06-23 00:00:00.000",
                "1981-06-23 00:00:00.001")) {
            for (String operator : COMPARISON_OPERATORS) {
                for (String to : asList(
                        null,
                        "1981-06-21",
                        "1981-06-22",
                        "1981-06-23")) {
                    String fromLiteral = from == null ? "NULL" : format("TIMESTAMP '%s'", from);
                    String toLiteral = to == null ? "NULL" : format("DATE '%s'", to);
                    validate(operator, "timestamp(3)", fromLiteral, "date", toLiteral);
                    validateWithDateFunction(operator, "timestamp(3)", fromLiteral, toLiteral);
                }
            }
        }
    }

    private void validateWithDateFunction(String operator, String fromType, Object fromValue, Object toValue)
    {
        validateWithDateFunction(assertions.getDefaultSession(), operator, fromType, fromValue, toValue);
    }

    private void validateWithDateFunction(Session session, String operator, String fromType, Object fromValue, Object toValue)
    {
        String query = format(
                "SELECT (date(v) %s CAST(%s AS date)) " +
                        "IS NOT DISTINCT FROM " +
                        "(CAST(%s AS date) %s CAST(%s AS date)) " +
                        "FROM (VALUES CAST(%s AS %s)) t(v)",
                operator, toValue,
                fromValue, operator, toValue,
                fromValue, fromType);

        boolean result = (boolean) assertions.execute(session, query)
                .getMaterializedRows()
                .get(0)
                .getField(0);

        assertTrue(result, "Query evaluated to false: " + query);
    }

    private static List<String> toLiteral(String type, List<Number> values)
    {
        return values.stream()
                .map(value -> value == null ? "NULL" : type + "'" + value + "'")
                .collect(toImmutableList());
    }
}
