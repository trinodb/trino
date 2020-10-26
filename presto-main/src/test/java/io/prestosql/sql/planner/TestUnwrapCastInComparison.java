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
package io.prestosql.sql.planner;

import io.prestosql.Session;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.filter;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.output;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
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
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '1'",
                anyTree(
                        filter("A = SMALLINT '1'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a = DOUBLE '1'",
                anyTree(
                        filter("A = BIGINT '1'",
                                values("A"))));

        // non-representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '1.1'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '1.9'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a = DOUBLE '1.1'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        // below top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '32766'",
                anyTree(
                        filter("A = SMALLINT '32766'",
                                values("A"))));

        // round to top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '32766.9'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        // top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '32767'",
                anyTree(
                        filter("A = SMALLINT '32767'",
                                values("A"))));

        // above range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '32768.1'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        // above bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '-32767'",
                anyTree(
                        filter("A = SMALLINT '-32767'",
                                values("A"))));

        // round to bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '-32767.9'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        // bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '-32768'",
                anyTree(
                        filter("A = SMALLINT '-32768'",
                                values("A"))));

        // below range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '-32768.1'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a = DOUBLE '-18446744073709551616'", // -2^64 constant
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));
    }

    @Test
    public void testNotEquals()
    {
        // representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '1'",
                anyTree(
                        filter("A <> SMALLINT '1'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a <> DOUBLE '1'",
                anyTree(
                        filter("A <> BIGINT '1'",
                                values("A"))));

        // non-representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '1.1'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '1.9'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a <> DOUBLE '1.1'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        // below top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '32766'",
                anyTree(
                        filter("A <> SMALLINT '32766'",
                                values("A"))));

        // round to top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '32766.9'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        // top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '32767'",
                anyTree(
                        filter("A <> SMALLINT '32767'",
                                values("A"))));

        // above range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '32768.1'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a <> DOUBLE '18446744073709551616'", // 2^64 constant
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        // above bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '-32767'",
                anyTree(
                        filter("A <> SMALLINT '-32767'",
                                values("A"))));

        // round to bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '-32767.9'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        // bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '-32768'",
                anyTree(
                        filter("A <> SMALLINT '-32768'",
                                values("A"))));

        // below range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '-32768.1'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));
    }

    @Test
    public void testLessThan()
    {
        // representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '1'",
                anyTree(
                        filter("A < SMALLINT '1'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a < DOUBLE '1'",
                anyTree(
                        filter("A < BIGINT '1'",
                                values("A"))));

        // non-representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '1.1'",
                anyTree(
                        filter("A <= SMALLINT '1'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a < DOUBLE '1.1'",
                anyTree(
                        filter("A <= BIGINT '1'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '1.9'",
                anyTree(
                        filter("A < SMALLINT '2'",
                                values("A"))));

        // below top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '32766'",
                anyTree(
                        filter("A < SMALLINT '32766'",
                                values("A"))));

        // round to top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '32766.9'",
                anyTree(
                        filter("A < SMALLINT '32767'",
                                values("A"))));

        // top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '32767'",
                anyTree(
                        filter("A <> SMALLINT '32767'",
                                values("A"))));

        // above range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '32768.1'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        // above bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '-32767'",
                anyTree(
                        filter("A < SMALLINT '-32767'",
                                values("A"))));

        // round to bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '-32767.9'",
                anyTree(
                        filter("A = SMALLINT '-32768'",
                                values("A"))));

        // bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '-32768'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        // below range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '-32768.1'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a < DOUBLE '-18446744073709551616'", // -2^64 constant
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));
    }

    @Test
    public void testLessThanOrEqual()
    {
        // representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '1'",
                anyTree(
                        filter("A <= SMALLINT '1'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a <= DOUBLE '1'",
                anyTree(
                        filter("A <= BIGINT '1'",
                                values("A"))));

        // non-representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '1.1'",
                anyTree(
                        filter("A <= SMALLINT '1'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a <= DOUBLE '1.1'",
                anyTree(
                        filter("A <= BIGINT '1'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '1.9'",
                anyTree(
                        filter("A < SMALLINT '2'",
                                values("A"))));

        // below top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '32766'",
                anyTree(
                        filter("A <= SMALLINT '32766'",
                                values("A"))));

        // round to top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '32766.9'",
                anyTree(
                        filter("A < SMALLINT '32767'",
                                values("A"))));

        // top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '32767'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        // above range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '32768.1'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a <= DOUBLE '18446744073709551616'", // 2^64 constant
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        // above bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '-32767'",
                anyTree(
                        filter("A <= SMALLINT '-32767'",
                                values("A"))));

        // round to bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '-32767.9'",
                anyTree(
                        filter("A = SMALLINT '-32768'",
                                values("A"))));

        // bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '-32768'",
                anyTree(
                        filter("A = SMALLINT '-32768'",
                                values("A"))));

        // below range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '-32768.1'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));
    }

    @Test
    public void testGreaterThan()
    {
        // representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '1'",
                anyTree(
                        filter("A > SMALLINT '1'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a > DOUBLE '1'",
                anyTree(
                        filter("A > BIGINT '1'",
                                values("A"))));

        // non-representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '1.1'",
                anyTree(
                        filter("A > SMALLINT '1'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '1.9'",
                anyTree(
                        filter("A >= SMALLINT '2'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a > DOUBLE '1.9'",
                anyTree(
                        filter("A >= BIGINT '2'",
                                values("A"))));

        // below top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '32766'",
                anyTree(
                        filter("A > SMALLINT '32766'",
                                values("A"))));

        // round to top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '32766.9'",
                anyTree(
                        filter("A = SMALLINT '32767'",
                                values("A"))));

        // top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '32767'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        // above range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '32768.1'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a > DOUBLE '18446744073709551616'", // 2^64 constant
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        // above bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '-32767'",
                anyTree(
                        filter("A > SMALLINT '-32767'",
                                values("A"))));

        // round to bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '-32767.9'",
                anyTree(
                        filter("A > SMALLINT '-32768'",
                                values("A"))));

        // bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '-32768'",
                anyTree(
                        filter("A <> SMALLINT '-32768'",
                                values("A"))));

        // below range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '-32768.1'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        // representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '1'",
                anyTree(
                        filter("A >= SMALLINT '1'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a >= DOUBLE '1'",
                anyTree(
                        filter("A >= BIGINT '1'",
                                values("A"))));

        // non-representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '1.1'",
                anyTree(
                        filter("A > SMALLINT '1'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a >= DOUBLE '1.1'",
                anyTree(
                        filter("A > BIGINT '1'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '1.9'",
                anyTree(
                        filter("A >= SMALLINT '2'",
                                values("A"))));

        // below top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '32766'",
                anyTree(
                        filter("A >= SMALLINT '32766'",
                                values("A"))));

        // round to top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '32766.9'",
                anyTree(
                        filter("A = SMALLINT '32767'",
                                values("A"))));

        // top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '32767'",
                anyTree(
                        filter("A = SMALLINT '32767'",
                                values("A"))));

        // above range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '32768.1'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        // above bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '-32767'",
                anyTree(
                        filter("A >= SMALLINT '-32767'",
                                values("A"))));

        // round to bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '-32767.9'",
                anyTree(
                        filter("A > SMALLINT '-32768' ",
                                values("A"))));

        // bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '-32768'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        // below range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '-32768.1'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '1') t(a) WHERE a >= DOUBLE '-18446744073709551616'", // -2^64 constant
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));
    }

    @Test
    public void testDistinctFrom()
    {
        // representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '1'",
                anyTree(
                        filter("A IS DISTINCT FROM SMALLINT '1'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '1'",
                anyTree(
                        filter("A IS DISTINCT FROM BIGINT '1'",
                                values("A"))));

        // non-representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '1.1'",
                output(
                        values("A")));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '1.9'",
                output(
                        values("A")));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '1.9'",
                output(
                        values("A")));

        // below top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '32766'",
                anyTree(
                        filter("A IS DISTINCT FROM SMALLINT '32766'",
                                values("A"))));

        // round to top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '32766.9'",
                output(
                        values("A")));

        // top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '32767'",
                anyTree(
                        filter("A IS DISTINCT FROM SMALLINT '32767'",
                                values("A"))));

        // above range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '32768.1'",
                output(
                        values("A")));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '1') t(a) WHERE a IS DISTINCT FROM DOUBLE '18446744073709551616'", // 2^64 constant
                output(
                        values("A")));

        // above bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '-32767'",
                anyTree(
                        filter("A IS DISTINCT FROM SMALLINT '-32767'",
                                values("A"))));

        // round to bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '-32767.9'",
                output(
                        values("A")));

        // bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '-32768'",
                anyTree(
                        filter("A IS DISTINCT FROM SMALLINT '-32768'",
                                values("A"))));

        // below range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '-32768.1'",
                output(
                        values("A")));
    }

    @Test
    public void testNull()
    {
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = CAST(NULL AS DOUBLE)",
                output(
                        filter("CAST(NULL AS BOOLEAN)",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a = CAST(NULL AS DOUBLE)",
                output(
                        filter("CAST(NULL AS BOOLEAN)",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> CAST(NULL AS DOUBLE)",
                output(
                        filter("CAST(NULL AS BOOLEAN)",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > CAST(NULL AS DOUBLE)",
                output(
                        filter("CAST(NULL AS BOOLEAN)",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < CAST(NULL AS DOUBLE)",
                output(
                        filter("CAST(NULL AS BOOLEAN)",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= CAST(NULL AS DOUBLE)",
                output(
                        filter("CAST(NULL AS BOOLEAN)",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= CAST(NULL AS DOUBLE)",
                output(
                        filter("CAST(NULL AS BOOLEAN)",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM CAST(NULL AS DOUBLE)",
                output(
                        filter("NOT (CAST(A AS DOUBLE) IS NULL)",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a IS DISTINCT FROM CAST(NULL AS DOUBLE)",
                output(
                        filter("NOT (CAST(A AS DOUBLE) IS NULL)",
                                values("A"))));
    }

    @Test
    public void testNaN()
    {
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = nan()",
                output(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a = nan()",
                output(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < nan()",
                output(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> nan()",
                output(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM nan()",
                output(
                        values("A")));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '0') t(a) WHERE a IS DISTINCT FROM nan()",
                output(
                        values("A")));

        assertPlan(
                "SELECT * FROM (VALUES REAL '0.0') t(a) WHERE a = nan()",
                output(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES REAL '0.0') t(a) WHERE a < nan()",
                output(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES REAL '0.0') t(a) WHERE a <> nan()",
                output(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES REAL '0.0') t(a) WHERE a IS DISTINCT FROM nan()",
                output(
                        filter("A IS DISTINCT FROM CAST(nan() AS REAL)",
                                values("A"))));
    }

    @Test
    public void smokeTests()
    {
        // smoke tests for various type combinations
        for (String type : asList("SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE")) {
            assertPlan(
                    format("SELECT * FROM (VALUES TINYINT '1') t(a) WHERE a = %s '1'", type),
                    anyTree(
                            filter("A = TINYINT '1'",
                                    values("A"))));
        }

        for (String type : asList("INTEGER", "BIGINT", "REAL", "DOUBLE")) {
            assertPlan(
                    format("SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = %s '1'", type),
                    anyTree(
                            filter("A = SMALLINT '1'",
                                    values("A"))));
        }

        for (String type : asList("BIGINT", "DOUBLE")) {
            assertPlan(
                    format("SELECT * FROM (VALUES INTEGER '1') t(a) WHERE a = %s '1'", type),
                    anyTree(
                            filter("A = 1",
                                    values("A"))));
        }

        assertPlan("SELECT * FROM (VALUES REAL '1') t(a) WHERE a = DOUBLE '1'",
                anyTree(
                        filter("A = REAL '1.0'",
                                values("A"))));
    }

    @Test
    public void testTermOrder()
    {
        // ensure the optimization works when the terms of the comparison are reversed
        // vs the canonical <expr> <op> <literal> form
        assertPlan("SELECT * FROM (VALUES REAL '1') t(a) WHERE DOUBLE '1' = a",
                anyTree(
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
        testUnwrap(utcSession, "timestamp(0)", "> TIMESTAMP '2020-10-26 11:02:18 UTC'", "> TIMESTAMP '2020-10-26 11:02:18'");
        testUnwrap(warsawSession, "timestamp(0)", "> TIMESTAMP '2020-10-26 11:02:18 Europe/Warsaw'", "> TIMESTAMP '2020-10-26 11:02:18'");
        testUnwrap(losAngelesSession, "timestamp(0)", "> TIMESTAMP '2020-10-26 11:02:18 America/Los_Angeles'", "> TIMESTAMP '2020-10-26 11:02:18'");

        // different zone
        testUnwrap(warsawSession, "timestamp(0)", "> TIMESTAMP '2020-10-26 11:02:18 UTC'", "> TIMESTAMP '2020-10-26 12:02:18'");
        testUnwrap(losAngelesSession, "timestamp(0)", "> TIMESTAMP '2020-10-26 11:02:18 UTC'", "> TIMESTAMP '2020-10-26 04:02:18'");

        // short timestamp, short timestamp with time zone being coerced to long timestamp with time zone
        testUnwrap(warsawSession, "timestamp(6)", "> TIMESTAMP '2020-10-26 11:02:18.12 UTC'", "> TIMESTAMP '2020-10-26 12:02:18.120000'");
        testUnwrap(losAngelesSession, "timestamp(6)", "> TIMESTAMP '2020-10-26 11:02:18.12 UTC'", "> TIMESTAMP '2020-10-26 04:02:18.120000'");

        // long timestamp, short timestamp with time zone being coerced to long timestamp with time zone
        testUnwrap(warsawSession, "timestamp(9)", "> TIMESTAMP '2020-10-26 11:02:18.12 UTC'", "> TIMESTAMP '2020-10-26 12:02:18.120000000'");
        testUnwrap(losAngelesSession, "timestamp(9)", "> TIMESTAMP '2020-10-26 11:02:18.12 UTC'", "> TIMESTAMP '2020-10-26 04:02:18.120000000'");

        // long timestamp, long timestamp with time zone
        testUnwrap(warsawSession, "timestamp(9)", "> TIMESTAMP '2020-10-26 11:02:18.123456 UTC'", "> TIMESTAMP '2020-10-26 12:02:18.123456000'");
        testUnwrap(losAngelesSession, "timestamp(9)", "> TIMESTAMP '2020-10-26 11:02:18.123456 UTC'", "> TIMESTAMP '2020-10-26 04:02:18.123456000'");

        // maximum precision
        testUnwrap(warsawSession, "timestamp(12)", "> TIMESTAMP '2020-10-26 11:02:18.123456789321 UTC'", "> TIMESTAMP '2020-10-26 12:02:18.123456789321'");
        testUnwrap(losAngelesSession, "timestamp(12)", "> TIMESTAMP '2020-10-26 11:02:18.123456789321 UTC'", "> TIMESTAMP '2020-10-26 04:02:18.123456789321'");

        // DST forward -- Warsaw changed clock 1h forward on 2020-03-29T01:00 UTC (2020-03-29T02:00 local time)
        // Note that in given session input TIMESTAMP values  2020-03-29 02:31 and 2020-03-29 03:31 produce the same value 2020-03-29 01:31 UTC (conversion is not monotonic)
        // last before
        testUnwrap(warsawSession, "timestamp(0)", "> TIMESTAMP '2020-03-29 00:59:59 UTC'", "> TIMESTAMP '2020-03-29 01:59:59'");
        testUnwrap(warsawSession, "timestamp(3)", "> TIMESTAMP '2020-03-29 00:59:59.999 UTC'", "> TIMESTAMP '2020-03-29 01:59:59.999'");
        testUnwrap(warsawSession, "timestamp(6)", "> TIMESTAMP '2020-03-29 00:59:59.13 UTC'", "> TIMESTAMP '2020-03-29 01:59:59.130000'");
        testUnwrap(warsawSession, "timestamp(6)", "> TIMESTAMP '2020-03-29 00:59:59.999999 UTC'", "> TIMESTAMP '2020-03-29 01:59:59.999999'");
        testUnwrap(warsawSession, "timestamp(9)", "> TIMESTAMP '2020-03-29 00:59:59.999999999 UTC'", "> TIMESTAMP '2020-03-29 01:59:59.999999999'");
        testUnwrap(warsawSession, "timestamp(12)", "> TIMESTAMP '2020-03-29 00:59:59.999999999999 UTC'", "> TIMESTAMP '2020-03-29 01:59:59.999999999999'");
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
        testUnwrap(warsawSession, "timestamp(0)", "> TIMESTAMP '2020-03-29 02:00:00 UTC'", "> TIMESTAMP '2020-03-29 04:00:00'");
        testUnwrap(warsawSession, "timestamp(3)", "> TIMESTAMP '2020-03-29 02:00:00.000 UTC'", "> TIMESTAMP '2020-03-29 04:00:00.000'");
        testUnwrap(warsawSession, "timestamp(6)", "> TIMESTAMP '2020-03-29 02:00:00.000000 UTC'", "> TIMESTAMP '2020-03-29 04:00:00.000000'");
        testUnwrap(warsawSession, "timestamp(9)", "> TIMESTAMP '2020-03-29 02:00:00.000000000 UTC'", "> TIMESTAMP '2020-03-29 04:00:00.000000000'");
        testUnwrap(warsawSession, "timestamp(12)", "> TIMESTAMP '2020-03-29 02:00:00.000000000000 UTC'", "> TIMESTAMP '2020-03-29 04:00:00.000000000000'");

        // DST backward -- Warsaw changed clock 1h backward on 2020-10-25T01:00 UTC (2020-03-29T03:00 local time)
        // Note that in given session no input TIMESTAMP value can produce TIMESTAMP WITH TIME ZONE within [2020-10-25 00:00:00 UTC, 2020-10-25 01:00:00 UTC], so '>=' is OK
        // last before
        testUnwrap(warsawSession, "timestamp(0)", "> TIMESTAMP '2020-10-25 00:59:59 UTC'", ">= TIMESTAMP '2020-10-25 02:59:59'");
        testUnwrap(warsawSession, "timestamp(3)", "> TIMESTAMP '2020-10-25 00:59:59.999 UTC'", ">= TIMESTAMP '2020-10-25 02:59:59.999'");
        testUnwrap(warsawSession, "timestamp(6)", "> TIMESTAMP '2020-10-25 00:59:59.999999 UTC'", ">= TIMESTAMP '2020-10-25 02:59:59.999999'");
        testUnwrap(warsawSession, "timestamp(9)", "> TIMESTAMP '2020-10-25 00:59:59.999999999 UTC'", ">= TIMESTAMP '2020-10-25 02:59:59.999999999'");
        testUnwrap(warsawSession, "timestamp(12)", "> TIMESTAMP '2020-10-25 00:59:59.999999999999 UTC'", ">= TIMESTAMP '2020-10-25 02:59:59.999999999999'");
        // first within
        testUnwrap(warsawSession, "timestamp(0)", "> TIMESTAMP '2020-10-25 01:00:00 UTC'", "> TIMESTAMP '2020-10-25 02:00:00'");
        testUnwrap(warsawSession, "timestamp(3)", "> TIMESTAMP '2020-10-25 01:00:00.000 UTC'", "> TIMESTAMP '2020-10-25 02:00:00.000'");
        testUnwrap(warsawSession, "timestamp(6)", "> TIMESTAMP '2020-10-25 01:00:00.000000 UTC'", "> TIMESTAMP '2020-10-25 02:00:00.000000'");
        testUnwrap(warsawSession, "timestamp(9)", "> TIMESTAMP '2020-10-25 01:00:00.000000000 UTC'", "> TIMESTAMP '2020-10-25 02:00:00.000000000'");
        testUnwrap(warsawSession, "timestamp(12)", "> TIMESTAMP '2020-10-25 01:00:00.000000000000 UTC'", "> TIMESTAMP '2020-10-25 02:00:00.000000000000'");
        // last within
        testUnwrap(warsawSession, "timestamp(0)", "> TIMESTAMP '2020-10-25 01:59:59 UTC'", "> TIMESTAMP '2020-10-25 02:59:59'");
        testUnwrap(warsawSession, "timestamp(3)", "> TIMESTAMP '2020-10-25 01:59:59.999 UTC'", "> TIMESTAMP '2020-10-25 02:59:59.999'");
        testUnwrap(warsawSession, "timestamp(6)", "> TIMESTAMP '2020-10-25 01:59:59.999999 UTC'", "> TIMESTAMP '2020-10-25 02:59:59.999999'");
        testUnwrap(warsawSession, "timestamp(9)", "> TIMESTAMP '2020-10-25 01:59:59.999999999 UTC'", "> TIMESTAMP '2020-10-25 02:59:59.999999999'");
        testUnwrap(warsawSession, "timestamp(12)", "> TIMESTAMP '2020-10-25 01:59:59.999999999999 UTC'", "> TIMESTAMP '2020-10-25 02:59:59.999999999999'");
        // first after
        testUnwrap(warsawSession, "timestamp(0)", "> TIMESTAMP '2020-10-25 02:00:00 UTC'", "> TIMESTAMP '2020-10-25 03:00:00'");
        testUnwrap(warsawSession, "timestamp(3)", "> TIMESTAMP '2020-10-25 02:00:00.000 UTC'", "> TIMESTAMP '2020-10-25 03:00:00.000'");
        testUnwrap(warsawSession, "timestamp(6)", "> TIMESTAMP '2020-10-25 02:00:00.000000 UTC'", "> TIMESTAMP '2020-10-25 03:00:00.000000'");
        testUnwrap(warsawSession, "timestamp(9)", "> TIMESTAMP '2020-10-25 02:00:00.000000000 UTC'", "> TIMESTAMP '2020-10-25 03:00:00.000000000'");
        testUnwrap(warsawSession, "timestamp(12)", "> TIMESTAMP '2020-10-25 02:00:00.000000000000 UTC'", "> TIMESTAMP '2020-10-25 03:00:00.000000000000'");
    }

    @Test
    public void testNoEffect()
    {
        // BIGINT->DOUBLE implicit cast is not injective if the double constant is >= 2^53 and <= double(2^63 - 1)
        assertPlan(
                "SELECT * FROM (VALUES BIGINT '1') t(a) WHERE a = DOUBLE '9007199254740992'",
                anyTree(
                        filter("CAST(A AS DOUBLE) = 9.007199254740992E15",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '1') t(a) WHERE a = DOUBLE '9223372036854775807'",
                anyTree(
                        filter("CAST(A AS DOUBLE) = 9.223372036854776E18",
                                values("A"))));

        // BIGINT->DOUBLE implicit cast is not injective if the double constant is <= -2^53 and >= double(-2^63 + 1)
        assertPlan(
                "SELECT * FROM (VALUES BIGINT '1') t(a) WHERE a = DOUBLE '-9007199254740992'",
                anyTree(
                        filter("CAST(A AS DOUBLE) = -9.007199254740992E15",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '1') t(a) WHERE a = DOUBLE '-9223372036854775807'",
                anyTree(
                        filter("CAST(A AS DOUBLE) = -9.223372036854776E18",
                                values("A"))));

        // BIGINT->REAL implicit cast is not injective if the real constant is >= 2^23 and <= real(2^63 - 1)
        assertPlan(
                "SELECT * FROM (VALUES BIGINT '1') t(a) WHERE a = REAL '8388608'",
                anyTree(
                        filter("CAST(A AS REAL) = REAL '8388608.0'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '1') t(a) WHERE a = REAL '9223372036854775807'",
                anyTree(
                        filter("CAST(A AS REAL) = REAL '9.223372E18'",
                                values("A"))));

        // BIGINT->REAL implicit cast is not injective if the real constant is <= -2^23 and >= real(-2^63 + 1)
        assertPlan(
                "SELECT * FROM (VALUES BIGINT '1') t(a) WHERE a = REAL '-8388608'",
                anyTree(
                        filter("CAST(A AS REAL) = REAL '-8388608.0'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES BIGINT '1') t(a) WHERE a = REAL '-9223372036854775807'",
                anyTree(
                        filter("CAST(A AS REAL) = REAL '-9.223372E18'",
                                values("A"))));

        // INTEGER->REAL implicit cast is not injective if the real constant is >= 2^23 and <= 2^31 - 1
        assertPlan(
                "SELECT * FROM (VALUES INTEGER '1') t(a) WHERE a = REAL '8388608'",
                anyTree(
                        filter("CAST(A AS REAL) = REAL '8388608.0'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES INTEGER '1') t(a) WHERE a = REAL '2147483647'",
                anyTree(
                        filter("CAST(A AS REAL) = REAL '2.14748365E9'",
                                values("A"))));

        // INTEGER->REAL implicit cast is not injective if the real constant is <= -2^23 and >= -2^31 + 1
        assertPlan(
                "SELECT * FROM (VALUES INTEGER '1') t(a) WHERE a = REAL '-8388608'",
                anyTree(
                        filter("CAST(A AS REAL) = REAL '-8388608.0'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES INTEGER '1') t(a) WHERE a = REAL '-2147483647'",
                anyTree(
                        filter("CAST(A AS REAL) = REAL '-2.14748365E9'",
                                values("A"))));

        // DECIMAL(p)->DOUBLE not injective for p > 15
        assertPlan(
                "SELECT * FROM (VALUES CAST('1' AS DECIMAL(16))) t(a) WHERE a = DOUBLE '1'",
                anyTree(
                        filter("CAST(A AS DOUBLE) = 1E0",
                                values("A"))));

        // DECIMAL(p)->REAL not injective for p > 7
        assertPlan(
                "SELECT * FROM (VALUES CAST('1' AS DECIMAL(8))) t(a) WHERE a = REAL '1'",
                anyTree(
                        filter("CAST(A AS REAL) = REAL '1.0'",
                                values("A"))));

        // no implicit cast between VARCHAR->INTEGER
        assertPlan(
                "SELECT * FROM (VALUES VARCHAR '1') t(a) WHERE CAST(a AS INTEGER) = INTEGER '1'",
                anyTree(
                        filter("CAST(A AS INTEGER) = 1",
                                values("A"))));

        // no implicit cast between DOUBLE->INTEGER
        assertPlan(
                "SELECT * FROM (VALUES DOUBLE '1') t(a) WHERE CAST(a AS INTEGER) = INTEGER '1'",
                anyTree(
                        filter("CAST(A AS INTEGER) = 1",
                                values("A"))));
    }

    private void testNoUnwrap(Session session, String inputType, String inputPredicate, String expectedCastType)
    {
        String sql = format("SELECT * FROM (VALUES CAST(NULL AS %s)) t(a) WHERE a %s", inputType, inputPredicate);
        try {
            assertPlan(sql,
                    session,
                    anyTree(
                            filter(format("CAST(A AS %s) %s", expectedCastType, inputPredicate),
                                    values("A"))));
        }
        catch (Throwable e) {
            e.addSuppressed(new Exception("Query: " + sql));
            throw e;
        }
    }

    private void testUnwrap(Session session, String inputType, String inputPredicate, String expectedPredicate)
    {
        String sql = format("SELECT * FROM (VALUES CAST(NULL AS %s)) t(a) WHERE a %s", inputType, inputPredicate);
        try {
            assertPlan(sql,
                    session,
                    anyTree(
                            filter("A " + expectedPredicate,
                                    values("A"))));
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
