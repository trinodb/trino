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
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestUnwrapDateTruncInComparison
        extends BasePlanTest
{
    @Test
    public void testDateTruncTimestampToTimestampWithTimeZone()
    {
        Session session = getQueryRunner().getDefaultSession();

        Session utcSession = withZone(session, TimeZoneKey.UTC_KEY);
        // east of Greenwich
        Session warsawSession = withZone(session, TimeZoneKey.getTimeZoneKey("Europe/Warsaw"));
        // west of Greenwich
        Session losAngelesSession = withZone(session, TimeZoneKey.getTimeZoneKey("America/Los_Angeles"));

        // same zone
        testUnwrap(utcSession, "timestamp(0)", "date_trunc('day', a) > TIMESTAMP '2020-10-26 11:02:18 UTC'", "a > TIMESTAMP '2020-10-26 23:59:59'");
        testUnwrap(warsawSession, "timestamp(0)", "date_trunc('day', a) > TIMESTAMP '2020-10-26 11:02:18 Europe/Warsaw'", "a > TIMESTAMP '2020-10-26 23:59:59'");
        testUnwrap(losAngelesSession, "timestamp(0)", "date_trunc('day', a) > TIMESTAMP '2020-10-26 11:02:18 America/Los_Angeles'", "a > TIMESTAMP '2020-10-26 23:59:59'");
        testUnwrap(utcSession, "timestamp(0)", "date_trunc('day', a) >= TIMESTAMP '2020-10-26 11:02:18 UTC'", "a > TIMESTAMP '2020-10-26 23:59:59'");
        testUnwrap(warsawSession, "timestamp(0)", "date_trunc('day', a) >= TIMESTAMP '2020-10-26 11:02:18 Europe/Warsaw'", "a > TIMESTAMP '2020-10-26 23:59:59'");
        testUnwrap(losAngelesSession, "timestamp(0)", "date_trunc('day', a) >= TIMESTAMP '2020-10-26 11:02:18 America/Los_Angeles'", "a > TIMESTAMP '2020-10-26 23:59:59'");

        // different zone
        testUnwrap(warsawSession, "timestamp(0)", "date_trunc('day', a) > TIMESTAMP '2020-10-26 11:02:18 UTC'", "a > TIMESTAMP '2020-10-26 23:59:59'");
        testUnwrap(losAngelesSession, "timestamp(0)", "date_trunc('day', a) > TIMESTAMP '2020-10-26 11:02:18 UTC'", "a > TIMESTAMP '2020-10-26 23:59:59'");

        // no effect
        testNoUnwrap(warsawSession, "timestamp(6)", "day", "BETWEEN TIMESTAMP '2020-10-26 11:02:18.123456 UTC' AND TIMESTAMP '2020-10-26 12:03:20.345678 UTC'", "timestamp(6) with time zone");

        // short timestamp, short timestamp with time zone being coerced to long timestamp with time zone
        testUnwrap(warsawSession, "timestamp(6)", "date_trunc('day', a) > TIMESTAMP '2020-10-26 11:02:18.12 UTC'", "a > TIMESTAMP '2020-10-26 23:59:59.999999'");
        testUnwrap(losAngelesSession, "timestamp(6)", "date_trunc('day', a) > TIMESTAMP '2020-10-26 11:02:18.12 UTC'", "a > TIMESTAMP '2020-10-26 23:59:59.999999'");

        // long timestamp, short timestamp with time zone being coerced to long timestamp with time zone
        testUnwrap(warsawSession, "timestamp(9)", "date_trunc('day', a) > TIMESTAMP '2020-10-26 11:02:18.12 UTC'", "a > TIMESTAMP '2020-10-26 23:59:59.999999999'");
        testUnwrap(losAngelesSession, "timestamp(9)", "date_trunc('day', a) > TIMESTAMP '2020-10-26 11:02:18.12 UTC'", "a > TIMESTAMP '2020-10-26 23:59:59.999999999'");

        // long timestamp, long timestamp with time zone
        testUnwrap(warsawSession, "timestamp(9)", "date_trunc('day', a) > TIMESTAMP '2020-10-26 11:02:18.123456 UTC'", "a > TIMESTAMP '2020-10-26 23:59:59.999999999'");
        testUnwrap(losAngelesSession, "timestamp(9)", "date_trunc('day', a) > TIMESTAMP '2020-10-26 11:02:18.123456 UTC'", "a > TIMESTAMP '2020-10-26 23:59:59.999999999'");

        // maximum precision
        testUnwrap(warsawSession, "timestamp(12)", "date_trunc('day', a) > TIMESTAMP '2020-10-26 11:02:18.123456789321 UTC'", "a > TIMESTAMP '2020-10-26 23:59:59.999999999999'");
        testUnwrap(losAngelesSession, "timestamp(12)", "date_trunc('day', a) > TIMESTAMP '2020-10-26 11:02:18.123456789321 UTC'", "a > TIMESTAMP '2020-10-26 23:59:59.999999999999'");

        // DST forward -- Warsaw changed clock 1h forward on 2020-03-29T01:00 UTC (2020-03-29T02:00 local time)
        // Note that in given session input TIMESTAMP values  2020-03-29 02:31 and 2020-03-29 03:31 produce the same value 2020-03-29 01:31 UTC (conversion is not monotonic)
        // last before
        testUnwrap(warsawSession, "timestamp(0)", "date_trunc('hour', a) > TIMESTAMP '2020-03-29 00:59:59 UTC'", "a > TIMESTAMP '2020-03-29 01:59:59'");
        testUnwrap(warsawSession, "timestamp(3)", "date_trunc('hour', a) > TIMESTAMP '2020-03-29 00:59:59.999 UTC'", "a > TIMESTAMP '2020-03-29 01:59:59.999'");
        testUnwrap(warsawSession, "timestamp(6)", "date_trunc('hour', a) > TIMESTAMP '2020-03-29 00:59:59.13 UTC'", "a > TIMESTAMP '2020-03-29 01:59:59.999999'");
        testUnwrap(warsawSession, "timestamp(6)", "date_trunc('hour', a) > TIMESTAMP '2020-03-29 00:59:59.999999 UTC'", "a > TIMESTAMP '2020-03-29 01:59:59.999999'");
        testUnwrap(warsawSession, "timestamp(9)", "date_trunc('hour', a) > TIMESTAMP '2020-03-29 00:59:59.999999999 UTC'", "a > TIMESTAMP '2020-03-29 01:59:59.999999999'");
        testUnwrap(warsawSession, "timestamp(12)", "date_trunc('hour', a) > TIMESTAMP '2020-03-29 00:59:59.999999999999 UTC'", "a > TIMESTAMP '2020-03-29 01:59:59.999999999999'");
        // first within
        testNoUnwrap(warsawSession, "timestamp(0)", "hour", "> TIMESTAMP '2020-03-29 01:00:00 UTC'", "timestamp(0) with time zone");
        testNoUnwrap(warsawSession, "timestamp(3)", "hour", "> TIMESTAMP '2020-03-29 01:00:00.000 UTC'", "timestamp(3) with time zone");
        testNoUnwrap(warsawSession, "timestamp(6)", "hour", "> TIMESTAMP '2020-03-29 01:00:00.000000 UTC'", "timestamp(6) with time zone");
        testNoUnwrap(warsawSession, "timestamp(9)", "hour", "> TIMESTAMP '2020-03-29 01:00:00.000000000 UTC'", "timestamp(9) with time zone");
        testNoUnwrap(warsawSession, "timestamp(12)", "hour", "> TIMESTAMP '2020-03-29 01:00:00.000000000000 UTC'", "timestamp(12) with time zone");
        // last within
        testNoUnwrap(warsawSession, "timestamp(0)", "hour", "> TIMESTAMP '2020-03-29 01:59:59 UTC'", "timestamp(0) with time zone");
        testNoUnwrap(warsawSession, "timestamp(3)", "hour", "> TIMESTAMP '2020-03-29 01:59:59.999 UTC'", "timestamp(3) with time zone");
        testNoUnwrap(warsawSession, "timestamp(6)", "hour", "> TIMESTAMP '2020-03-29 01:59:59.999999 UTC'", "timestamp(6) with time zone");
        testNoUnwrap(warsawSession, "timestamp(9)", "hour", "> TIMESTAMP '2020-03-29 01:59:59.999999999 UTC'", "timestamp(9) with time zone");
        testNoUnwrap(warsawSession, "timestamp(12)", "hour", "> TIMESTAMP '2020-03-29 01:59:59.999999999999 UTC'", "timestamp(12) with time zone");
        // first after
        testUnwrap(warsawSession, "timestamp(0)", "date_trunc('hour', a) > TIMESTAMP '2020-03-29 02:00:00 UTC'", "a > TIMESTAMP '2020-03-29 04:59:59'");
        testUnwrap(warsawSession, "timestamp(3)", "date_trunc('hour', a) > TIMESTAMP '2020-03-29 02:00:00.000 UTC'", "a > TIMESTAMP '2020-03-29 04:59:59.999'");
        testUnwrap(warsawSession, "timestamp(6)", "date_trunc('hour', a) > TIMESTAMP '2020-03-29 02:00:00.000000 UTC'", "a > TIMESTAMP '2020-03-29 04:59:59.999999'");
        testUnwrap(warsawSession, "timestamp(9)", "date_trunc('hour', a) > TIMESTAMP '2020-03-29 02:00:00.000000000 UTC'", "a > TIMESTAMP '2020-03-29 04:59:59.999999999'");
        testUnwrap(warsawSession, "timestamp(12)", "date_trunc('hour', a) > TIMESTAMP '2020-03-29 02:00:00.000000000000 UTC'", "a > TIMESTAMP '2020-03-29 04:59:59.999999999999'");

        // DST backward -- Warsaw changed clock 1h backward on 2020-10-25T01:00 UTC (2020-03-29T03:00 local time)
        // Note that in given session no input TIMESTAMP value can produce TIMESTAMP WITH TIME ZONE within [2020-10-25 00:00:00 UTC, 2020-10-25 01:00:00 UTC], so '>=' is OK
        // last before
        testUnwrap(warsawSession, "timestamp(0)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 00:59:59 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59'");
        testUnwrap(warsawSession, "timestamp(3)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 00:59:59.999 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59.999'");
        testUnwrap(warsawSession, "timestamp(6)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 00:59:59.999999 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59.999999'");
        testUnwrap(warsawSession, "timestamp(9)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 00:59:59.999999999 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59.999999999'");
        testUnwrap(warsawSession, "timestamp(12)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 00:59:59.999999999999 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59.999999999999'");
        // first within
        testUnwrap(warsawSession, "timestamp(0)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 01:00:00 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59'");
        testUnwrap(warsawSession, "timestamp(3)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 01:00:00.000 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59.999'");
        testUnwrap(warsawSession, "timestamp(6)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 01:00:00.000000 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59.999999'");
        testUnwrap(warsawSession, "timestamp(9)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 01:00:00.000000000 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59.999999999'");
        testUnwrap(warsawSession, "timestamp(12)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 01:00:00.000000000000 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59.999999999999'");
        // last within
        testUnwrap(warsawSession, "timestamp(0)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 01:59:59 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59'");
        testUnwrap(warsawSession, "timestamp(3)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 01:59:59.999 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59.999'");
        testUnwrap(warsawSession, "timestamp(6)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 01:59:59.999999 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59.999999'");
        testUnwrap(warsawSession, "timestamp(9)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 01:59:59.999999999 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59.999999999'");
        testUnwrap(warsawSession, "timestamp(12)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 01:59:59.999999999999 UTC'", "a > TIMESTAMP '2020-10-25 02:59:59.999999999999'");
        // first after
        testUnwrap(warsawSession, "timestamp(0)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 02:00:00 UTC'", "a > TIMESTAMP '2020-10-25 03:59:59'");
        testUnwrap(warsawSession, "timestamp(3)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 02:00:00.000 UTC'", "a > TIMESTAMP '2020-10-25 03:59:59.999'");
        testUnwrap(warsawSession, "timestamp(6)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 02:00:00.000000 UTC'", "a > TIMESTAMP '2020-10-25 03:59:59.999999'");
        testUnwrap(warsawSession, "timestamp(9)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 02:00:00.000000000 UTC'", "a > TIMESTAMP '2020-10-25 03:59:59.999999999'");
        testUnwrap(warsawSession, "timestamp(12)", "date_trunc('hour', a) > TIMESTAMP '2020-10-25 02:00:00.000000000000 UTC'", "a > TIMESTAMP '2020-10-25 03:59:59.999999999999'");
    }

    @Test
    public void testUnwrapDateTruncTimestampAsDay()
    {
        // equal
        testUnwrap("timestamp(3)", "date_trunc('day', a) = DATE '1981-06-22'", "a BETWEEN TIMESTAMP '1981-06-22 00:00:00.000' AND TIMESTAMP '1981-06-22 23:59:59.999'");
        testUnwrap("timestamp(6)", "date_trunc('day', a) = DATE '1981-06-22'", "a BETWEEN TIMESTAMP '1981-06-22 00:00:00.000000' AND TIMESTAMP '1981-06-22 23:59:59.999999'");
        testUnwrap("timestamp(9)", "date_trunc('day', a) = DATE '1981-06-22'", "a BETWEEN TIMESTAMP '1981-06-22 00:00:00.000000000' AND TIMESTAMP '1981-06-22 23:59:59.999999999'");
        testUnwrap("timestamp(12)", "date_trunc('day', a) = DATE '1981-06-22'", "a BETWEEN TIMESTAMP '1981-06-22 00:00:00.000000000000' AND TIMESTAMP '1981-06-22 23:59:59.999999999999'");

        // not equal
        testUnwrap("timestamp(3)", "date_trunc('day', a) <> DATE '1981-06-22'", "NOT(a BETWEEN TIMESTAMP '1981-06-22 00:00:00.000' AND TIMESTAMP '1981-06-22 23:59:59.999')");
        testUnwrap("timestamp(6)", "date_trunc('day', a) <> DATE '1981-06-22'", "NOT(a BETWEEN TIMESTAMP '1981-06-22 00:00:00.000000' AND TIMESTAMP '1981-06-22 23:59:59.999999')");
        testUnwrap("timestamp(9)", "date_trunc('day', a) <> DATE '1981-06-22'", "NOT(a BETWEEN TIMESTAMP '1981-06-22 00:00:00.000000000' AND TIMESTAMP '1981-06-22 23:59:59.999999999')");
        testUnwrap("timestamp(12)", "date_trunc('day', a) <> DATE '1981-06-22'", "NOT(a BETWEEN TIMESTAMP '1981-06-22 00:00:00.000000000000' AND TIMESTAMP '1981-06-22 23:59:59.999999999999')");

        // less than
        testUnwrap("timestamp(3)", "date_trunc('day', a) < DATE '1981-06-22'", "a < TIMESTAMP '1981-06-22 00:00:00.000'");
        testUnwrap("timestamp(6)", "date_trunc('day', a) < DATE '1981-06-22'", "a < TIMESTAMP '1981-06-22 00:00:00.000000'");
        testUnwrap("timestamp(9)", "date_trunc('day', a) < DATE '1981-06-22'", "a < TIMESTAMP '1981-06-22 00:00:00.000000000'");
        testUnwrap("timestamp(12)", "date_trunc('day', a) < DATE '1981-06-22'", "a < TIMESTAMP '1981-06-22 00:00:00.000000000000'");

        // less than or equal
        testUnwrap("timestamp(3)", "date_trunc('day', a) <= DATE '1981-06-22'", "a <= TIMESTAMP '1981-06-22 23:59:59.999'");
        testUnwrap("timestamp(6)", "date_trunc('day', a)<= DATE '1981-06-22'", "a <= TIMESTAMP '1981-06-22 23:59:59.999999'");
        testUnwrap("timestamp(9)", "date_trunc('day', a) <= DATE '1981-06-22'", "a <= TIMESTAMP '1981-06-22 23:59:59.999999999'");
        testUnwrap("timestamp(12)", "date_trunc('day', a) <= DATE '1981-06-22'", "a <= TIMESTAMP '1981-06-22 23:59:59.999999999999'");

        // greater than
        testUnwrap("timestamp(3)", "date_trunc('day', a) > DATE '1981-06-22'", "a > TIMESTAMP '1981-06-22 23:59:59.999'");
        testUnwrap("timestamp(6)", "date_trunc('day', a) > DATE '1981-06-22'", "a > TIMESTAMP '1981-06-22 23:59:59.999999'");
        testUnwrap("timestamp(9)", "date_trunc('day', a) > DATE '1981-06-22'", "a > TIMESTAMP '1981-06-22 23:59:59.999999999'");
        testUnwrap("timestamp(12)", "date_trunc('day', a) > DATE '1981-06-22'", "a > TIMESTAMP '1981-06-22 23:59:59.999999999999'");

        // greater than or equal
        testUnwrap("timestamp(3)", "date_trunc('day', a) >= DATE '1981-06-22'", "a >= TIMESTAMP '1981-06-22 00:00:00.000'");
        testUnwrap("timestamp(6)", "date_trunc('day', a) >= DATE '1981-06-22'", "a >= TIMESTAMP '1981-06-22 00:00:00.000000'");
        testUnwrap("timestamp(9)", "date_trunc('day', a) >= DATE '1981-06-22'", "a >= TIMESTAMP '1981-06-22 00:00:00.000000000'");
        testUnwrap("timestamp(12)", "date_trunc('day', a) >= DATE '1981-06-22'", "a >= TIMESTAMP '1981-06-22 00:00:00.000000000000'");

        // between
        testUnwrap("timestamp(3)", "date_trunc('day', a) BETWEEN DATE '1981-06-22' AND DATE '1981-07-23'", "a >= TIMESTAMP '1981-06-22 00:00:00.000' AND a <= TIMESTAMP '1981-07-23 23:59:59.999'");
        testUnwrap("timestamp(6)", "date_trunc('day', a) BETWEEN DATE '1981-06-22' AND DATE '1981-07-23'", "a >= TIMESTAMP '1981-06-22 00:00:00.000000' AND a <= TIMESTAMP '1981-07-23 23:59:59.999999'");
        testUnwrap("timestamp(9)", "date_trunc('day', a) BETWEEN DATE '1981-06-22' AND DATE '1981-07-23'", "a >= TIMESTAMP '1981-06-22 00:00:00.000000000' AND a <= TIMESTAMP '1981-07-23 23:59:59.999999999'");
        testUnwrap("timestamp(12)", "date_trunc('day', a) BETWEEN DATE '1981-06-22' AND DATE '1981-07-23'", "a >= TIMESTAMP '1981-06-22 00:00:00.000000000000' AND a <= TIMESTAMP '1981-07-23 23:59:59.999999999999'");

        // is distinct
        testUnwrap("timestamp(3)", "date_trunc('day', a) IS DISTINCT FROM DATE '1981-06-22'", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '1981-06-22 00:00:00.000' AND TIMESTAMP '1981-06-22 23:59:59.999')");
        testUnwrap("timestamp(6)", "date_trunc('day', a) IS DISTINCT FROM DATE '1981-06-22'", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '1981-06-22 00:00:00.000000' AND TIMESTAMP '1981-06-22 23:59:59.999999')");
        testUnwrap("timestamp(9)", "date_trunc('day', a) IS DISTINCT FROM DATE '1981-06-22'", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '1981-06-22 00:00:00.000000000' AND TIMESTAMP '1981-06-22 23:59:59.999999999')");
        testUnwrap("timestamp(12)", "date_trunc('day', a) IS DISTINCT FROM DATE '1981-06-22'", "a IS NULL OR  NOT (a BETWEEN TIMESTAMP '1981-06-22 00:00:00.000000000000' AND TIMESTAMP '1981-06-22 23:59:59.999999999999')");

        // is not distinct
        testUnwrap("timestamp(3)", "date_trunc('day', a) IS NOT DISTINCT FROM DATE '1981-06-22'", "(NOT a IS NULL) AND a BETWEEN TIMESTAMP '1981-06-22 00:00:00.000' AND TIMESTAMP '1981-06-22 23:59:59.999'");
        testUnwrap("timestamp(6)", "date_trunc('day', a) IS NOT DISTINCT FROM DATE '1981-06-22'", "(NOT a IS NULL) AND a BETWEEN TIMESTAMP '1981-06-22 00:00:00.000000' AND TIMESTAMP '1981-06-22 23:59:59.999999'");
        testUnwrap("timestamp(9)", "date_trunc('day', a) IS NOT DISTINCT FROM DATE '1981-06-22'", "(NOT a IS NULL) AND a BETWEEN TIMESTAMP '1981-06-22 00:00:00.000000000' AND TIMESTAMP '1981-06-22 23:59:59.999999999'");
        testUnwrap("timestamp(12)", "date_trunc('day', a) IS NOT DISTINCT FROM DATE '1981-06-22'", "(NOT a IS NULL) AND a BETWEEN TIMESTAMP '1981-06-22 00:00:00.000000000000' AND TIMESTAMP '1981-06-22 23:59:59.999999999999'");

        // null date literal
        testUnwrap("timestamp(3)", "date_trunc('day', a) = NULL", "CAST(NULL AS BOOLEAN)");
        testUnwrap("timestamp(3)", "date_trunc('day', a) < NULL", "CAST(NULL AS BOOLEAN)");
        testUnwrap("timestamp(3)", "date_trunc('day', a) <= NULL", "CAST(NULL AS BOOLEAN)");
        testUnwrap("timestamp(3)", "date_trunc('day', a) > NULL", "CAST(NULL AS BOOLEAN)");
        testUnwrap("timestamp(3)", "date_trunc('day', a) >= NULL", "CAST(NULL AS BOOLEAN)");
        testUnwrap("timestamp(3)", "date_trunc('day', a) IS DISTINCT FROM NULL", "NOT(date_trunc('day', a) IS NULL)");

        // non-optimized expression on the right
        testUnwrap("timestamp(3)", "date_trunc('day', a) = DATE '1981-06-22' + INTERVAL '2' DAY", "a BETWEEN TIMESTAMP '1981-06-24 00:00:00.000' AND TIMESTAMP '1981-06-24 23:59:59.999'");

        // cast on the right
        testUnwrap("timestamp(3)", "date_trunc('day', a) = CAST(a AS DATE)", "date_trunc('day', a) = CAST(CAST(a AS date) AS timestamp(3))");
    }

    private void testNoUnwrap(Session session, String inputType, String unit, String inputPredicate, String expectedCastType)
    {
        String sql = format("SELECT * FROM (VALUES CAST(NULL AS %s)) t(a) WHERE date_trunc('%s', a) %s", inputType, unit, inputPredicate);
        try {
            assertPlan(sql,
                    session,
                    output(
                            filter(format("CAST(date_trunc('%s', a) AS %s) %s", unit, expectedCastType, inputPredicate),
                                    values("a"))));
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
