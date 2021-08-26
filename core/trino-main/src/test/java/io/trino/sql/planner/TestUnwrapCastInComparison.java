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
package io.trino.sql.planner;

import io.trino.Session;
import io.trino.spi.type.CharType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class TestUnwrapCastInComparison
        extends BasePlanTest
{
    @Test
    public void testEquals()
    {
        // representable
        testUnwrap("smallint", "a = DOUBLE '1'", "a = SMALLINT '1'");

        testUnwrap("bigint", "a = DOUBLE '1'", "a = BIGINT '1'");

        // non-representable
        testUnwrap("smallint", "a = DOUBLE '1.1'", "a IS NULL AND NULL");
        testUnwrap("smallint", "a = DOUBLE '1.9'", "a IS NULL AND NULL");

        testUnwrap("bigint", "a = DOUBLE '1.1'", "a IS NULL AND NULL");

        // below top of range
        testUnwrap("smallint", "a = DOUBLE '32766'", "a = SMALLINT '32766'");

        // round to top of range
        testUnwrap("smallint", "a = DOUBLE '32766.9'", "a IS NULL AND NULL");

        // top of range
        testUnwrap("smallint", "a = DOUBLE '32767'", "a = SMALLINT '32767'");

        // above range
        testUnwrap("smallint", "a = DOUBLE '32768.1'", "a IS NULL AND NULL");

        // above bottom of range
        testUnwrap("smallint", "a = DOUBLE '-32767'", "a = SMALLINT '-32767'");

        // round to bottom of range
        testUnwrap("smallint", "a = DOUBLE '-32767.9'", "a IS NULL AND NULL");

        // bottom of range
        testUnwrap("smallint", "a = DOUBLE '-32768'", "a = SMALLINT '-32768'");

        // below range
        testUnwrap("smallint", "a = DOUBLE '-32768.1'", "a IS NULL AND NULL");

        // -2^64 constant
        testUnwrap("bigint", "a = DOUBLE '-18446744073709551616'", "a IS NULL AND NULL");

        // shorter varchar and char
        testNoUnwrap("varchar(1)", "= CAST('abc' AS char(3))", "char(3)");
        // varchar and char, same length
        testUnwrap("varchar(3)", "a = CAST('abc' AS char(3))", "a = 'abc'");
        testNoUnwrap("varchar(3)", "= CAST('ab' AS char(3))", "char(3)");
        // longer varchar and char
        testUnwrap("varchar(10)", "a = CAST('abc' AS char(3))", "CAST(a AS char(10)) = CAST('abc' AS char(10))"); // actually unwrapping didn't happen
        // unbounded varchar and char
        testUnwrap("varchar", "a = CAST('abc' AS char(3))", "CAST(a AS char(65536)) = CAST('abc' AS char(65536))"); // actually unwrapping didn't happen
        // unbounded varchar and char of maximum length (could be unwrapped, but currently it is not)
        testNoUnwrap("varchar", format("= CAST('abc' AS char(%s))", CharType.MAX_LENGTH), "char(65536)");
    }

    @Test
    public void testNotEquals()
    {
        // representable
        testUnwrap("smallint", "a <> DOUBLE '1'", "a <> SMALLINT '1'");

        testUnwrap("bigint", "a <> DOUBLE '1'", "a <> BIGINT '1'");

        // non-representable
        testUnwrap("smallint", "a <> DOUBLE '1.1'", "NOT (a IS NULL) OR NULL");
        testUnwrap("smallint", "a <> DOUBLE '1.9'", "NOT (a IS NULL) OR NULL");

        testUnwrap("bigint", "a <> DOUBLE '1.1'", "NOT (a IS NULL) OR NULL");

        // below top of range
        testUnwrap("smallint", "a <> DOUBLE '32766'", "a <> SMALLINT '32766'");

        // round to top of range
        testUnwrap("smallint", "a <> DOUBLE '32766.9'", "NOT (a IS NULL) OR NULL");

        // top of range
        testUnwrap("smallint", "a <> DOUBLE '32767'", "a <> SMALLINT '32767'");

        // above range
        testUnwrap("smallint", "a <> DOUBLE '32768.1'", "NOT (a IS NULL) OR NULL");

        // 2^64 constant
        testUnwrap("bigint", "a <> DOUBLE '18446744073709551616'", "NOT (a IS NULL) OR NULL");

        // above bottom of range
        testUnwrap("smallint", "a <> DOUBLE '-32767'", "a <> SMALLINT '-32767'");

        // round to bottom of range
        testUnwrap("smallint", "a <> DOUBLE '-32767.9'", "NOT (a IS NULL) OR NULL");

        // bottom of range
        testUnwrap("smallint", "a <> DOUBLE '-32768'", "a <> SMALLINT '-32768'");

        // below range
        testUnwrap("smallint", "a <> DOUBLE '-32768.1'", "NOT (a IS NULL) OR NULL");
    }

    @Test
    public void testLessThan()
    {
        // representable
        testUnwrap("smallint", "a < DOUBLE '1'", "a < SMALLINT '1'");

        testUnwrap("bigint", "a < DOUBLE '1'", "a < BIGINT '1'");

        // non-representable
        testUnwrap("smallint", "a < DOUBLE '1.1'", "a <= SMALLINT '1'");

        testUnwrap("bigint", "a < DOUBLE '1.1'", "a <= BIGINT '1'");

        testUnwrap("smallint", "a < DOUBLE '1.9'", "a < SMALLINT '2'");

        // below top of range
        testUnwrap("smallint", "a < DOUBLE '32766'", "a < SMALLINT '32766'");

        // round to top of range
        testUnwrap("smallint", "a < DOUBLE '32766.9'", "a < SMALLINT '32767'");

        // top of range
        testUnwrap("smallint", "a < DOUBLE '32767'", "a <> SMALLINT '32767'");

        // above range
        testUnwrap("smallint", "a < DOUBLE '32768.1'", "NOT (a IS NULL) OR NULL");

        // above bottom of range
        testUnwrap("smallint", "a < DOUBLE '-32767'", "a < SMALLINT '-32767'");

        // round to bottom of range
        testUnwrap("smallint", "a < DOUBLE '-32767.9'", "a = SMALLINT '-32768'");

        // bottom of range
        testUnwrap("smallint", "a < DOUBLE '-32768'", "a IS NULL AND NULL");

        // below range
        testUnwrap("smallint", "a < DOUBLE '-32768.1'", "a IS NULL AND NULL");

        // -2^64 constant
        testUnwrap("bigint", "a < DOUBLE '-18446744073709551616'", "a IS NULL AND NULL");
    }

    @Test
    public void testLessThanOrEqual()
    {
        // representable
        testUnwrap("smallint", "a <= DOUBLE '1'", "a <= SMALLINT '1'");

        testUnwrap("bigint", "a <= DOUBLE '1'", "a <= BIGINT '1'");

        // non-representable
        testUnwrap("smallint", "a <= DOUBLE '1.1'", "a <= SMALLINT '1'");

        testUnwrap("bigint", "a <= DOUBLE '1.1'", "a <= BIGINT '1'");

        testUnwrap("smallint", "a <= DOUBLE '1.9'", "a < SMALLINT '2'");

        // below top of range
        testUnwrap("smallint", "a <= DOUBLE '32766'", "a <= SMALLINT '32766'");

        // round to top of range
        testUnwrap("smallint", "a <= DOUBLE '32766.9'", "a < SMALLINT '32767'");

        // top of range
        testUnwrap("smallint", "a <= DOUBLE '32767'", "NOT (a IS NULL) OR NULL");

        // above range
        testUnwrap("smallint", "a <= DOUBLE '32768.1'", "NOT (a IS NULL) OR NULL");

        // 2^64 constant
        testUnwrap("bigint", "a <= DOUBLE '18446744073709551616'", "NOT (a IS NULL) OR NULL");

        // above bottom of range
        testUnwrap("smallint", "a <= DOUBLE '-32767'", "a <= SMALLINT '-32767'");

        // round to bottom of range
        testUnwrap("smallint", "a <= DOUBLE '-32767.9'", "a = SMALLINT '-32768'");

        // bottom of range
        testUnwrap("smallint", "a <= DOUBLE '-32768'", "a = SMALLINT '-32768'");

        // below range
        testUnwrap("smallint", "a <= DOUBLE '-32768.1'", "a IS NULL AND NULL");
    }

    @Test
    public void testGreaterThan()
    {
        // representable
        testUnwrap("smallint", "a > DOUBLE '1'", "a > SMALLINT '1'");

        testUnwrap("bigint", "a > DOUBLE '1'", "a > BIGINT '1'");

        // non-representable
        testUnwrap("smallint", "a > DOUBLE '1.1'", "a > SMALLINT '1'");

        testUnwrap("smallint", "a > DOUBLE '1.9'", "a >= SMALLINT '2'");

        testUnwrap("bigint", "a > DOUBLE '1.9'", "a >= BIGINT '2'");

        // below top of range
        testUnwrap("smallint", "a > DOUBLE '32766'", "a > SMALLINT '32766'");

        // round to top of range
        testUnwrap("smallint", "a > DOUBLE '32766.9'", "a = SMALLINT '32767'");

        // top of range
        testUnwrap("smallint", "a > DOUBLE '32767'", "a IS NULL AND NULL");

        // above range
        testUnwrap("smallint", "a > DOUBLE '32768.1'", "a IS NULL AND NULL");

        // 2^64 constant
        testUnwrap("bigint", "a > DOUBLE '18446744073709551616'", "a IS NULL AND NULL");

        // above bottom of range
        testUnwrap("smallint", "a > DOUBLE '-32767'", "a > SMALLINT '-32767'");

        // round to bottom of range
        testUnwrap("smallint", "a > DOUBLE '-32767.9'", "a > SMALLINT '-32768'");

        // bottom of range
        testUnwrap("smallint", "a > DOUBLE '-32768'", "a <> SMALLINT '-32768'");

        // below range
        testUnwrap("smallint", "a > DOUBLE '-32768.1'", "NOT (a IS NULL) OR NULL");
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        // representable
        testUnwrap("smallint", "a >= DOUBLE '1'", "a >= SMALLINT '1'");

        testUnwrap("bigint", "a >= DOUBLE '1'", "a >= BIGINT '1'");

        // non-representable
        testUnwrap("smallint", "a >= DOUBLE '1.1'", "a > SMALLINT '1'");

        testUnwrap("bigint", "a >= DOUBLE '1.1'", "a > BIGINT '1'");

        testUnwrap("smallint", "a >= DOUBLE '1.9'", "a >= SMALLINT '2'");

        // below top of range
        testUnwrap("smallint", "a >= DOUBLE '32766'", "a >= SMALLINT '32766'");

        // round to top of range
        testUnwrap("smallint", "a >= DOUBLE '32766.9'", "a = SMALLINT '32767'");

        // top of range
        testUnwrap("smallint", "a >= DOUBLE '32767'", "a = SMALLINT '32767'");

        // above range
        testUnwrap("smallint", "a >= DOUBLE '32768.1'", "a IS NULL AND NULL");

        // above bottom of range
        testUnwrap("smallint", "a >= DOUBLE '-32767'", "a >= SMALLINT '-32767'");

        // round to bottom of range
        testUnwrap("smallint", "a >= DOUBLE '-32767.9'", "a > SMALLINT '-32768' ");

        // bottom of range
        testUnwrap("smallint", "a >= DOUBLE '-32768'", "NOT (a IS NULL) OR NULL");

        // below range
        testUnwrap("smallint", "a >= DOUBLE '-32768.1'", "NOT (a IS NULL) OR NULL");

        // -2^64 constant
        testUnwrap("bigint", "a >= DOUBLE '-18446744073709551616'", "NOT (a IS NULL) OR NULL");
    }

    @Test
    public void testDistinctFrom()
    {
        // representable
        testUnwrap("smallint", "a IS DISTINCT FROM DOUBLE '1'", "a IS DISTINCT FROM SMALLINT '1'");

        testUnwrap("bigint", "a IS DISTINCT FROM DOUBLE '1'", "a IS DISTINCT FROM BIGINT '1'");

        // non-representable
        testRemoveFilter("smallint", "a IS DISTINCT FROM DOUBLE '1.1'");

        testRemoveFilter("smallint", "a IS DISTINCT FROM DOUBLE '1.9'");

        testRemoveFilter("bigint", "a IS DISTINCT FROM DOUBLE '1.9'");

        // below top of range
        testUnwrap("smallint", "a IS DISTINCT FROM DOUBLE '32766'", "a IS DISTINCT FROM SMALLINT '32766'");

        // round to top of range
        testRemoveFilter("smallint", "a IS DISTINCT FROM DOUBLE '32766.9'");

        // top of range
        testUnwrap("smallint", "a IS DISTINCT FROM DOUBLE '32767'", "a IS DISTINCT FROM SMALLINT '32767'");

        // above range
        testRemoveFilter("smallint", "a IS DISTINCT FROM DOUBLE '32768.1'");

        // 2^64 constant
        testRemoveFilter("bigint", "a IS DISTINCT FROM DOUBLE '18446744073709551616'");

        // above bottom of range
        testUnwrap("smallint", "a IS DISTINCT FROM DOUBLE '-32767'", "a IS DISTINCT FROM SMALLINT '-32767'");

        // round to bottom of range
        testRemoveFilter("smallint", "a IS DISTINCT FROM DOUBLE '-32767.9'");

        // bottom of range
        testUnwrap("smallint", "a IS DISTINCT FROM DOUBLE '-32768'", "a IS DISTINCT FROM SMALLINT '-32768'");

        // below range
        testRemoveFilter("smallint", "a IS DISTINCT FROM DOUBLE '-32768.1'");
    }

    @Test
    public void testNull()
    {
        testUnwrap("smallint", "a = CAST(NULL AS DOUBLE)", "CAST(NULL AS BOOLEAN)");

        testUnwrap("bigint", "a = CAST(NULL AS DOUBLE)", "CAST(NULL AS BOOLEAN)");

        testUnwrap("smallint", "a <> CAST(NULL AS DOUBLE)", "CAST(NULL AS BOOLEAN)");

        testUnwrap("smallint", "a > CAST(NULL AS DOUBLE)", "CAST(NULL AS BOOLEAN)");

        testUnwrap("smallint", "a < CAST(NULL AS DOUBLE)", "CAST(NULL AS BOOLEAN)");

        testUnwrap("smallint", "a >= CAST(NULL AS DOUBLE)", "CAST(NULL AS BOOLEAN)");

        testUnwrap("smallint", "a <= CAST(NULL AS DOUBLE)", "CAST(NULL AS BOOLEAN)");

        testUnwrap("smallint", "a IS DISTINCT FROM CAST(NULL AS DOUBLE)", "NOT (CAST(a AS DOUBLE) IS NULL)");

        testUnwrap("bigint", "a IS DISTINCT FROM CAST(NULL AS DOUBLE)", "NOT (CAST(a AS DOUBLE) IS NULL)");
    }

    @Test
    public void testNaN()
    {
        testUnwrap("smallint", "a = nan()", "a IS NULL AND NULL");

        testUnwrap("bigint", "a = nan()", "a IS NULL AND NULL");

        testUnwrap("smallint", "a < nan()", "a IS NULL AND NULL");

        testUnwrap("smallint", "a <> nan()", "NOT (a IS NULL) OR NULL");

        testRemoveFilter("smallint", "a IS DISTINCT FROM nan()");

        testRemoveFilter("bigint", "a IS DISTINCT FROM nan()");

        testUnwrap("real", "a = nan()", "a IS NULL AND NULL");

        testUnwrap("real", "a < nan()", "a IS NULL AND NULL");

        testUnwrap("real", "a <> nan()", "NOT (a IS NULL) OR NULL");

        testUnwrap("real", "a IS DISTINCT FROM nan()", "a IS DISTINCT FROM CAST(nan() AS REAL)");
    }

    @Test
    public void smokeTests()
    {
        // smoke tests for various type combinations
        for (String type : asList("SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE")) {
            testUnwrap("tinyint", format("a = %s '1'", type), "a = TINYINT '1'");
        }

        for (String type : asList("INTEGER", "BIGINT", "REAL", "DOUBLE")) {
            testUnwrap("smallint", format("a = %s '1'", type), "a = SMALLINT '1'");
        }

        for (String type : asList("BIGINT", "DOUBLE")) {
            testUnwrap("integer", format("a = %s '1'", type), "a = 1");
        }

        testUnwrap("real", "a = DOUBLE '1'", "a = REAL '1.0'");
    }

    @Test
    public void testTermOrder()
    {
        // ensure the optimization works when the terms of the comparison are reversed
        // vs the canonical <expr> <op> <literal> form
        assertPlan("SELECT * FROM (VALUES REAL '1') t(a) WHERE DOUBLE '1' = a",
                output(
                        filter("A = REAL '1.0'",
                                values("A"))));
    }

    @Test
    public void testCastTimestampToTimestampWithTimeZone()
    {
        Session session = getQueryRunner().getDefaultSession();

        Session utcSession = withZone(session, TimeZoneKey.UTC_KEY);
        // east of Greenwich
        Session warsawSession = withZone(session, TimeZoneKey.getTimeZoneKey("Europe/Warsaw"));
        // west of Greenwich
        Session losAngelesSession = withZone(session, TimeZoneKey.getTimeZoneKey("America/Los_Angeles"));

        // same zone
        testUnwrap(utcSession, "timestamp(0)", "a > TIMESTAMP '2020-10-26 11:02:18 UTC'", "a > TIMESTAMP '2020-10-26 11:02:18'");
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-10-26 11:02:18 Europe/Warsaw'", "a > TIMESTAMP '2020-10-26 11:02:18'");
        testUnwrap(losAngelesSession, "timestamp(0)", "a > TIMESTAMP '2020-10-26 11:02:18 America/Los_Angeles'", "a > TIMESTAMP '2020-10-26 11:02:18'");

        // different zone
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-10-26 11:02:18 UTC'", "a > TIMESTAMP '2020-10-26 12:02:18'");
        testUnwrap(losAngelesSession, "timestamp(0)", "a > TIMESTAMP '2020-10-26 11:02:18 UTC'", "a > TIMESTAMP '2020-10-26 04:02:18'");

        // short timestamp, short timestamp with time zone being coerced to long timestamp with time zone
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-10-26 11:02:18.12 UTC'", "a > TIMESTAMP '2020-10-26 12:02:18.120000'");
        testUnwrap(losAngelesSession, "timestamp(6)", "a > TIMESTAMP '2020-10-26 11:02:18.12 UTC'", "a > TIMESTAMP '2020-10-26 04:02:18.120000'");

        // long timestamp, short timestamp with time zone being coerced to long timestamp with time zone
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-10-26 11:02:18.12 UTC'", "a > TIMESTAMP '2020-10-26 12:02:18.120000000'");
        testUnwrap(losAngelesSession, "timestamp(9)", "a > TIMESTAMP '2020-10-26 11:02:18.12 UTC'", "a > TIMESTAMP '2020-10-26 04:02:18.120000000'");

        // long timestamp, long timestamp with time zone
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-10-26 11:02:18.123456 UTC'", "a > TIMESTAMP '2020-10-26 12:02:18.123456000'");
        testUnwrap(losAngelesSession, "timestamp(9)", "a > TIMESTAMP '2020-10-26 11:02:18.123456 UTC'", "a > TIMESTAMP '2020-10-26 04:02:18.123456000'");

        // maximum precision
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-10-26 11:02:18.123456789321 UTC'", "a > TIMESTAMP '2020-10-26 12:02:18.123456789321'");
        testUnwrap(losAngelesSession, "timestamp(12)", "a > TIMESTAMP '2020-10-26 11:02:18.123456789321 UTC'", "a > TIMESTAMP '2020-10-26 04:02:18.123456789321'");

        // DST forward -- Warsaw changed clock 1h forward on 2020-03-29T01:00 UTC (2020-03-29T02:00 local time)
        // Note that in given session input TIMESTAMP values  2020-03-29 02:31 and 2020-03-29 03:31 produce the same value 2020-03-29 01:31 UTC (conversion is not monotonic)
        // last before
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-03-29 00:59:59 UTC'", "a > TIMESTAMP '2020-03-29 01:59:59'");
        testUnwrap(warsawSession, "timestamp(3)", "a > TIMESTAMP '2020-03-29 00:59:59.999 UTC'", "a > TIMESTAMP '2020-03-29 01:59:59.999'");
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-03-29 00:59:59.13 UTC'", "a > TIMESTAMP '2020-03-29 01:59:59.130000'");
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-03-29 00:59:59.999999 UTC'", "a > TIMESTAMP '2020-03-29 01:59:59.999999'");
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-03-29 00:59:59.999999999 UTC'", "a > TIMESTAMP '2020-03-29 01:59:59.999999999'");
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-03-29 00:59:59.999999999999 UTC'", "a > TIMESTAMP '2020-03-29 01:59:59.999999999999'");
        // first within
        testNoUnwrap(warsawSession, "timestamp(0)", "> TIMESTAMP '2020-03-29 01:00:00 UTC'", "timestamp(0) with time zone");
        testNoUnwrap(warsawSession, "timestamp(3)", "> TIMESTAMP '2020-03-29 01:00:00.000 UTC'", "timestamp(3) with time zone");
        testNoUnwrap(warsawSession, "timestamp(6)", "> TIMESTAMP '2020-03-29 01:00:00.000000 UTC'", "timestamp(6) with time zone");
        testNoUnwrap(warsawSession, "timestamp(9)", "> TIMESTAMP '2020-03-29 01:00:00.000000000 UTC'", "timestamp(9) with time zone");
        testNoUnwrap(warsawSession, "timestamp(12)", "> TIMESTAMP '2020-03-29 01:00:00.000000000000 UTC'", "timestamp(12) with time zone");
        // last within
        testNoUnwrap(warsawSession, "timestamp(0)", "> TIMESTAMP '2020-03-29 01:59:59 UTC'", "timestamp(0) with time zone");
        testNoUnwrap(warsawSession, "timestamp(3)", "> TIMESTAMP '2020-03-29 01:59:59.999 UTC'", "timestamp(3) with time zone");
        testNoUnwrap(warsawSession, "timestamp(6)", "> TIMESTAMP '2020-03-29 01:59:59.999999 UTC'", "timestamp(6) with time zone");
        testNoUnwrap(warsawSession, "timestamp(9)", "> TIMESTAMP '2020-03-29 01:59:59.999999999 UTC'", "timestamp(9) with time zone");
        testNoUnwrap(warsawSession, "timestamp(12)", "> TIMESTAMP '2020-03-29 01:59:59.999999999999 UTC'", "timestamp(12) with time zone");
        // first after
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-03-29 02:00:00 UTC'", "a > TIMESTAMP '2020-03-29 04:00:00'");
        testUnwrap(warsawSession, "timestamp(3)", "a > TIMESTAMP '2020-03-29 02:00:00.000 UTC'", "a > TIMESTAMP '2020-03-29 04:00:00.000'");
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-03-29 02:00:00.000000 UTC'", "a > TIMESTAMP '2020-03-29 04:00:00.000000'");
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-03-29 02:00:00.000000000 UTC'", "a > TIMESTAMP '2020-03-29 04:00:00.000000000'");
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-03-29 02:00:00.000000000000 UTC'", "a > TIMESTAMP '2020-03-29 04:00:00.000000000000'");

        // DST backward -- Warsaw changed clock 1h backward on 2020-10-25T01:00 UTC (2020-03-29T03:00 local time)
        // Note that in given session no input TIMESTAMP value can produce TIMESTAMP WITH TIME ZONE within [2020-10-25 00:00:00 UTC, 2020-10-25 01:00:00 UTC], so '>=' is OK
        // last before
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-10-25 00:59:59 UTC'", "a >= TIMESTAMP '2020-10-25 02:59:59'");
        testUnwrap(warsawSession, "timestamp(3)", "a > TIMESTAMP '2020-10-25 00:59:59.999 UTC'", "a >= TIMESTAMP '2020-10-25 02:59:59.999'");
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-10-25 00:59:59.999999 UTC'", "a >= TIMESTAMP '2020-10-25 02:59:59.999999'");
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-10-25 00:59:59.999999999 UTC'", "a >= TIMESTAMP '2020-10-25 02:59:59.999999999'");
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-10-25 00:59:59.999999999999 UTC'", "a >= TIMESTAMP '2020-10-25 02:59:59.999999999999'");
        // first within
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-10-25 01:00:00 UTC'", "a > TIMESTAMP '2020-10-25 02:00:00'");
        testUnwrap(warsawSession, "timestamp(3)", "a > TIMESTAMP '2020-10-25 01:00:00.000 UTC'", "a > TIMESTAMP '2020-10-25 02:00:00.000'");
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-10-25 01:00:00.000000 UTC'", "a > TIMESTAMP '2020-10-25 02:00:00.000000'");
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-10-25 01:00:00.000000000 UTC'", "a > TIMESTAMP '2020-10-25 02:00:00.000000000'");
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-10-25 01:00:00.000000000000 UTC'", "a > TIMESTAMP '2020-10-25 02:00:00.000000000000'");
        // last within
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-10-25 01:59:59 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59'");
        testUnwrap(warsawSession, "timestamp(3)", "a > TIMESTAMP '2020-10-25 01:59:59.999 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59.999'");
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-10-25 01:59:59.999999 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59.999999'");
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-10-25 01:59:59.999999999 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59.999999999'");
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-10-25 01:59:59.999999999999 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59.999999999999'");
        // first after
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-10-25 02:00:00 UTC'", "a > TIMESTAMP '2020-10-25 03:00:00'");
        testUnwrap(warsawSession, "timestamp(3)", "a > TIMESTAMP '2020-10-25 02:00:00.000 UTC'", "a > TIMESTAMP '2020-10-25 03:00:00.000'");
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-10-25 02:00:00.000000 UTC'", "a > TIMESTAMP '2020-10-25 03:00:00.000000'");
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-10-25 02:00:00.000000000 UTC'", "a > TIMESTAMP '2020-10-25 03:00:00.000000000'");
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-10-25 02:00:00.000000000000 UTC'", "a > TIMESTAMP '2020-10-25 03:00:00.000000000000'");
    }

    @Test
    public void testNoEffect()
    {
        // BIGINT->DOUBLE implicit cast is not injective if the double constant is >= 2^53 and <= double(2^63 - 1)
        testUnwrap("bigint", "a = DOUBLE '9007199254740992'", "CAST(a AS DOUBLE) = 9.007199254740992E15");

        testUnwrap("bigint", "a = DOUBLE '9223372036854775807'", "CAST(a AS DOUBLE) = 9.223372036854776E18");

        // BIGINT->DOUBLE implicit cast is not injective if the double constant is <= -2^53 and >= double(-2^63 + 1)
        testUnwrap("bigint", "a = DOUBLE '-9007199254740992'", "CAST(a AS DOUBLE) = -9.007199254740992E15");

        testUnwrap("bigint", "a = DOUBLE '-9223372036854775807'", "CAST(a AS DOUBLE) = -9.223372036854776E18");

        // BIGINT->REAL implicit cast is not injective if the real constant is >= 2^23 and <= real(2^63 - 1)
        testUnwrap("bigint", "a = REAL '8388608'", "CAST(a AS REAL) = REAL '8388608.0'");

        testUnwrap("bigint", "a = REAL '9223372036854775807'", "CAST(a AS REAL) = REAL '9.223372E18'");

        // BIGINT->REAL implicit cast is not injective if the real constant is <= -2^23 and >= real(-2^63 + 1)
        testUnwrap("bigint", "a = REAL '-8388608'", "CAST(a AS REAL) = REAL '-8388608.0'");

        testUnwrap("bigint", "a = REAL '-9223372036854775807'", "CAST(a AS REAL) = REAL '-9.223372E18'");

        // INTEGER->REAL implicit cast is not injective if the real constant is >= 2^23 and <= 2^31 - 1
        testUnwrap("integer", "a = REAL '8388608'", "CAST(a AS REAL) = REAL '8388608.0'");

        testUnwrap("integer", "a = REAL '2147483647'", "CAST(a AS REAL) = REAL '2.14748365E9'");

        // INTEGER->REAL implicit cast is not injective if the real constant is <= -2^23 and >= -2^31 + 1
        testUnwrap("integer", "a = REAL '-8388608'", "CAST(a AS REAL) = REAL '-8388608.0'");

        testUnwrap("integer", "a = REAL '-2147483647'", "CAST(a AS REAL) = REAL '-2.14748365E9'");

        // DECIMAL(p)->DOUBLE not injective for p > 15
        testUnwrap("decimal(16)", "a = DOUBLE '1'", "CAST(a AS DOUBLE) = 1E0");

        // DECIMAL(p)->REAL not injective for p > 7
        testUnwrap("decimal(8)", "a = REAL '1'", "CAST(a AS REAL) = REAL '1.0'");

        // no implicit cast between VARCHAR->INTEGER
        testUnwrap("varchar", "CAST(a AS INTEGER) = INTEGER '1'", "CAST(a AS INTEGER) = 1");

        // no implicit cast between DOUBLE->INTEGER
        testUnwrap("double", "CAST(a AS INTEGER) = INTEGER '1'", "CAST(a AS INTEGER) = 1");
    }

    private void testNoUnwrap(String inputType, String inputPredicate, String expectedCastType)
    {
        testNoUnwrap(getQueryRunner().getDefaultSession(), inputType, inputPredicate, expectedCastType);
    }

    private void testNoUnwrap(Session session, String inputType, String inputPredicate, String expectedCastType)
    {
        String sql = format("SELECT * FROM (VALUES CAST(NULL AS %s)) t(a) WHERE a %s", inputType, inputPredicate);
        try {
            assertPlan(sql,
                    session,
                    output(
                            filter(format("CAST(a AS %s) %s", expectedCastType, inputPredicate),
                                    values("a"))));
        }
        catch (Throwable e) {
            e.addSuppressed(new Exception("Query: " + sql));
            throw e;
        }
    }

    private void testRemoveFilter(String inputType, String inputPredicate)
    {
        String sql = format("SELECT * FROM (VALUES CAST(NULL AS %s)) t(a) WHERE %s", inputType, inputPredicate);
        try {
            assertPlan(sql,
                    output(
                            values("a")));
        }
        catch (Throwable e) {
            e.addSuppressed(new Exception("Query: " + sql));
            throw e;
        }
    }

    private void testUnwrap(String inputType, String inputPredicate, String expectedPredicate)
    {
        testUnwrap(getQueryRunner().getDefaultSession(), inputType, inputPredicate, expectedPredicate);
    }

    private void testUnwrap(Session session, String inputType, String inputPredicate, String expectedPredicate)
    {
        String sql = format("SELECT * FROM (VALUES CAST(NULL AS %s)) t(a) WHERE %s", inputType, inputPredicate);
        try {
            assertPlan(sql,
                    session,
                    output(
                            filter(expectedPredicate,
                                    values("a"))));
        }
        catch (Throwable e) {
            e.addSuppressed(new Exception("Query: " + sql));
            throw e;
        }
    }

    private static Session withZone(Session session, TimeZoneKey timeZoneKey)
    {
        return Session.builder(requireNonNull(session, "session is null"))
                .setTimeZoneKey(requireNonNull(timeZoneKey, "timeZoneKey is null"))
                .build();
    }
}
