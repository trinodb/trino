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
import io.trino.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public class TestRewriteYearFunctionToDateTrunc
        extends BasePlanTest
{
    @Test
    public void testEquals()
    {
        testUnwrap("date", "year(a) = -0001", "a BETWEEN DATE '-0001-01-01' AND DATE '-0001-12-31'");
        testUnwrap("date", "year(a) = 1960", "a BETWEEN DATE '1960-01-01' AND DATE '1960-12-31'");
        testUnwrap("date", "year(a) = 2022", "a BETWEEN DATE '2022-01-01' AND DATE '2022-12-31'");
        testUnwrap("date", "year(a) = 9999", "a BETWEEN DATE '9999-01-01' AND DATE '9999-12-31'");

        testUnwrap("timestamp", "year(a) = -0001", "a BETWEEN TIMESTAMP '-0001-01-01 00:00:00.000' AND TIMESTAMP '-0001-12-31 23:59:59.999'");
        testUnwrap("timestamp", "year(a) = 1960", "a BETWEEN TIMESTAMP '1960-01-01 00:00:00.000' AND TIMESTAMP '1960-12-31 23:59:59.999'");
        testUnwrap("timestamp", "year(a) = 2022", "a BETWEEN TIMESTAMP '2022-01-01 00:00:00.000' AND TIMESTAMP '2022-12-31 23:59:59.999'");
        testUnwrap("timestamp", "year(a) = 9999", "a BETWEEN TIMESTAMP '9999-01-01 00:00:00.000' AND TIMESTAMP '9999-12-31 23:59:59.999'");

        testUnwrap("timestamp(0)", "year(a) = 2022", "a BETWEEN TIMESTAMP '2022-01-01 00:00:00' AND TIMESTAMP '2022-12-31 23:59:59'");
        testUnwrap("timestamp(1)", "year(a) = 2022", "a BETWEEN TIMESTAMP '2022-01-01 00:00:00.0' AND TIMESTAMP '2022-12-31 23:59:59.9'");
        testUnwrap("timestamp(2)", "year(a) = 2022", "a BETWEEN TIMESTAMP '2022-01-01 00:00:00.00' AND TIMESTAMP '2022-12-31 23:59:59.99'");
        testUnwrap("timestamp(3)", "year(a) = 2022", "a BETWEEN TIMESTAMP '2022-01-01 00:00:00.000' AND TIMESTAMP '2022-12-31 23:59:59.999'");
        testUnwrap("timestamp(4)", "year(a) = 2022", "a BETWEEN TIMESTAMP '2022-01-01 00:00:00.0000' AND TIMESTAMP '2022-12-31 23:59:59.9999'");
        testUnwrap("timestamp(5)", "year(a) = 2022", "a BETWEEN TIMESTAMP '2022-01-01 00:00:00.00000' AND TIMESTAMP '2022-12-31 23:59:59.99999'");
        testUnwrap("timestamp(6)", "year(a) = 2022", "a BETWEEN TIMESTAMP '2022-01-01 00:00:00.000000' AND TIMESTAMP '2022-12-31 23:59:59.999999'");
        testUnwrap("timestamp(7)", "year(a) = 2022", "a BETWEEN TIMESTAMP '2022-01-01 00:00:00.0000000' AND TIMESTAMP '2022-12-31 23:59:59.9999999'");
        testUnwrap("timestamp(8)", "year(a) = 2022", "a BETWEEN TIMESTAMP '2022-01-01 00:00:00.00000000' AND TIMESTAMP '2022-12-31 23:59:59.99999999'");
        testUnwrap("timestamp(9)", "year(a) = 2022", "a BETWEEN TIMESTAMP '2022-01-01 00:00:00.000000000' AND TIMESTAMP '2022-12-31 23:59:59.999999999'");
        testUnwrap("timestamp(10)", "year(a) = 2022", "a BETWEEN TIMESTAMP '2022-01-01 00:00:00.0000000000' AND TIMESTAMP '2022-12-31 23:59:59.9999999999'");
        testUnwrap("timestamp(11)", "year(a) = 2022", "a BETWEEN TIMESTAMP '2022-01-01 00:00:00.00000000000' AND TIMESTAMP '2022-12-31 23:59:59.99999999999'");
        testUnwrap("timestamp(12)", "year(a) = 2022", "a BETWEEN TIMESTAMP '2022-01-01 00:00:00.000000000000' AND TIMESTAMP '2022-12-31 23:59:59.999999999999'");
    }

    @Test
    public void testNotEquals()
    {
        testUnwrap("date", "year(a) <> -0001", "NOT (a BETWEEN DATE '-0001-01-01' AND DATE '-0001-12-31')");
        testUnwrap("date", "year(a) <> 1960", "NOT (a BETWEEN DATE '1960-01-01' AND DATE '1960-12-31')");
        testUnwrap("date", "year(a) <> 2022", "NOT (a BETWEEN DATE '2022-01-01' AND DATE '2022-12-31')");
        testUnwrap("date", "year(a) <> 9999", "NOT (a BETWEEN DATE '9999-01-01' AND DATE '9999-12-31')");

        testUnwrap("timestamp", "year(a) <> -0001", "NOT (a BETWEEN TIMESTAMP '-0001-01-01 00:00:00.000' AND TIMESTAMP '-0001-12-31 23:59:59.999')");
        testUnwrap("timestamp", "year(a) <> 1960", "NOT (a BETWEEN TIMESTAMP '1960-01-01 00:00:00.000' AND TIMESTAMP '1960-12-31 23:59:59.999')");
        testUnwrap("timestamp", "year(a) <> 2022", "NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.000' AND TIMESTAMP '2022-12-31 23:59:59.999')");
        testUnwrap("timestamp", "year(a) <> 9999", "NOT (a BETWEEN TIMESTAMP '9999-01-01 00:00:00.000' AND TIMESTAMP '9999-12-31 23:59:59.999')");

        testUnwrap("timestamp(0)", "year(a) <> 2022", "NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00' AND TIMESTAMP '2022-12-31 23:59:59')");
        testUnwrap("timestamp(1)", "year(a) <> 2022", "NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.0' AND TIMESTAMP '2022-12-31 23:59:59.9')");
        testUnwrap("timestamp(2)", "year(a) <> 2022", "NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.00' AND TIMESTAMP '2022-12-31 23:59:59.99')");
        testUnwrap("timestamp(3)", "year(a) <> 2022", "NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.000' AND TIMESTAMP '2022-12-31 23:59:59.999')");
        testUnwrap("timestamp(4)", "year(a) <> 2022", "NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.0000' AND TIMESTAMP '2022-12-31 23:59:59.9999')");
        testUnwrap("timestamp(5)", "year(a) <> 2022", "NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.00000' AND TIMESTAMP '2022-12-31 23:59:59.99999')");
        testUnwrap("timestamp(6)", "year(a) <> 2022", "NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.000000' AND TIMESTAMP '2022-12-31 23:59:59.999999')");
        testUnwrap("timestamp(7)", "year(a) <> 2022", "NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.0000000' AND TIMESTAMP '2022-12-31 23:59:59.9999999')");
        testUnwrap("timestamp(8)", "year(a) <> 2022", "NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.00000000' AND TIMESTAMP '2022-12-31 23:59:59.99999999')");
        testUnwrap("timestamp(9)", "year(a) <> 2022", "NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.000000000' AND TIMESTAMP '2022-12-31 23:59:59.999999999')");
        testUnwrap("timestamp(10)", "year(a) <> 2022", "NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.0000000000' AND TIMESTAMP '2022-12-31 23:59:59.9999999999')");
        testUnwrap("timestamp(11)", "year(a) <> 2022", "NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.00000000000' AND TIMESTAMP '2022-12-31 23:59:59.99999999999')");
        testUnwrap("timestamp(12)", "year(a) <> 2022", "NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.000000000000' AND TIMESTAMP '2022-12-31 23:59:59.999999999999')");
    }

    @Test
    public void testLessThan()
    {
        testUnwrap("date", "year(a) < -0001", "a < DATE '-0001-01-01'");
        testUnwrap("date", "year(a) < 1960", "a < DATE '1960-01-01'");
        testUnwrap("date", "year(a) < 2022", "a < DATE '2022-01-01'");
        testUnwrap("date", "year(a) < 9999", "a < DATE '9999-01-01'");

        testUnwrap("timestamp", "year(a) < -0001", "a < TIMESTAMP '-0001-01-01 00:00:00.000'");
        testUnwrap("timestamp", "year(a) < 1960", "a < TIMESTAMP '1960-01-01 00:00:00.000'");
        testUnwrap("timestamp", "year(a) < 2022", "a < TIMESTAMP '2022-01-01 00:00:00.000'");
        testUnwrap("timestamp", "year(a) < 9999", "a < TIMESTAMP '9999-01-01 00:00:00.000'");

        testUnwrap("timestamp(0)", "year(a) < 2022", "a < TIMESTAMP '2022-01-01 00:00:00'");
        testUnwrap("timestamp(1)", "year(a) < 2022", "a < TIMESTAMP '2022-01-01 00:00:00.0'");
        testUnwrap("timestamp(2)", "year(a) < 2022", "a < TIMESTAMP '2022-01-01 00:00:00.00'");
        testUnwrap("timestamp(3)", "year(a) < 2022", "a < TIMESTAMP '2022-01-01 00:00:00.000'");
        testUnwrap("timestamp(4)", "year(a) < 2022", "a < TIMESTAMP '2022-01-01 00:00:00.0000'");
        testUnwrap("timestamp(5)", "year(a) < 2022", "a < TIMESTAMP '2022-01-01 00:00:00.00000'");
        testUnwrap("timestamp(6)", "year(a) < 2022", "a < TIMESTAMP '2022-01-01 00:00:00.000000'");
        testUnwrap("timestamp(7)", "year(a) < 2022", "a < TIMESTAMP '2022-01-01 00:00:00.0000000'");
        testUnwrap("timestamp(8)", "year(a) < 2022", "a < TIMESTAMP '2022-01-01 00:00:00.00000000'");
        testUnwrap("timestamp(9)", "year(a) < 2022", "a < TIMESTAMP '2022-01-01 00:00:00.000000000'");
        testUnwrap("timestamp(10)", "year(a) < 2022", "a < TIMESTAMP '2022-01-01 00:00:00.0000000000'");
        testUnwrap("timestamp(11)", "year(a) < 2022", "a < TIMESTAMP '2022-01-01 00:00:00.00000000000'");
        testUnwrap("timestamp(12)", "year(a) < 2022", "a < TIMESTAMP '2022-01-01 00:00:00.000000000000'");
    }

    @Test
    public void testLessThanOrEqual()
    {
        testUnwrap("date", "year(a) <= -0001", "a <= DATE '-0001-12-31'");
        testUnwrap("date", "year(a) <= 1960", "a <= DATE '1960-12-31'");
        testUnwrap("date", "year(a) <= 2022", "a <= DATE '2022-12-31'");
        testUnwrap("date", "year(a) <= 9999", "a <= DATE '9999-12-31'");

        testUnwrap("timestamp", "year(a) <= -0001", "a <= TIMESTAMP '-0001-12-31 23:59:59.999'");
        testUnwrap("timestamp", "year(a) <= 1960", "a <= TIMESTAMP '1960-12-31 23:59:59.999'");
        testUnwrap("timestamp", "year(a) <= 2022", "a <= TIMESTAMP '2022-12-31 23:59:59.999'");
        testUnwrap("timestamp", "year(a) <= 9999", "a <= TIMESTAMP '9999-12-31 23:59:59.999'");

        testUnwrap("timestamp(0)", "year(a) <= 2022", "a <= TIMESTAMP '2022-12-31 23:59:59'");
        testUnwrap("timestamp(1)", "year(a) <= 2022", "a <= TIMESTAMP '2022-12-31 23:59:59.9'");
        testUnwrap("timestamp(2)", "year(a) <= 2022", "a <= TIMESTAMP '2022-12-31 23:59:59.99'");
        testUnwrap("timestamp(3)", "year(a) <= 2022", "a <= TIMESTAMP '2022-12-31 23:59:59.999'");
        testUnwrap("timestamp(4)", "year(a) <= 2022", "a <= TIMESTAMP '2022-12-31 23:59:59.9999'");
        testUnwrap("timestamp(5)", "year(a) <= 2022", "a <= TIMESTAMP '2022-12-31 23:59:59.99999'");
        testUnwrap("timestamp(6)", "year(a) <= 2022", "a <= TIMESTAMP '2022-12-31 23:59:59.999999'");
        testUnwrap("timestamp(7)", "year(a) <= 2022", "a <= TIMESTAMP '2022-12-31 23:59:59.9999999'");
        testUnwrap("timestamp(8)", "year(a) <= 2022", "a <= TIMESTAMP '2022-12-31 23:59:59.99999999'");
        testUnwrap("timestamp(9)", "year(a) <= 2022", "a <= TIMESTAMP '2022-12-31 23:59:59.999999999'");
        testUnwrap("timestamp(10)", "year(a) <= 2022", "a <= TIMESTAMP '2022-12-31 23:59:59.9999999999'");
        testUnwrap("timestamp(11)", "year(a) <= 2022", "a <= TIMESTAMP '2022-12-31 23:59:59.99999999999'");
        testUnwrap("timestamp(12)", "year(a) <= 2022", "a <= TIMESTAMP '2022-12-31 23:59:59.999999999999'");
    }

    @Test
    public void testGreaterThan()
    {
        testUnwrap("date", "year(a) > -0001", "a > DATE '-0001-12-31'");
        testUnwrap("date", "year(a) > 1960", "a > DATE '1960-12-31'");
        testUnwrap("date", "year(a) > 2022", "a > DATE '2022-12-31'");
        testUnwrap("date", "year(a) > 9999", "a > DATE '9999-12-31'");

        testUnwrap("timestamp", "year(a) > -0001", "a > TIMESTAMP '-0001-12-31 23:59:59.999'");
        testUnwrap("timestamp", "year(a) > 1960", "a > TIMESTAMP '1960-12-31 23:59:59.999'");
        testUnwrap("timestamp", "year(a) > 2022", "a > TIMESTAMP '2022-12-31 23:59:59.999'");
        testUnwrap("timestamp", "year(a) > 9999", "a > TIMESTAMP '9999-12-31 23:59:59.999'");

        testUnwrap("timestamp(0)", "year(a) > 2022", "a > TIMESTAMP '2022-12-31 23:59:59'");
        testUnwrap("timestamp(1)", "year(a) > 2022", "a > TIMESTAMP '2022-12-31 23:59:59.9'");
        testUnwrap("timestamp(2)", "year(a) > 2022", "a > TIMESTAMP '2022-12-31 23:59:59.99'");
        testUnwrap("timestamp(3)", "year(a) > 2022", "a > TIMESTAMP '2022-12-31 23:59:59.999'");
        testUnwrap("timestamp(4)", "year(a) > 2022", "a > TIMESTAMP '2022-12-31 23:59:59.9999'");
        testUnwrap("timestamp(5)", "year(a) > 2022", "a > TIMESTAMP '2022-12-31 23:59:59.99999'");
        testUnwrap("timestamp(6)", "year(a) > 2022", "a > TIMESTAMP '2022-12-31 23:59:59.999999'");
        testUnwrap("timestamp(7)", "year(a) > 2022", "a > TIMESTAMP '2022-12-31 23:59:59.9999999'");
        testUnwrap("timestamp(8)", "year(a) > 2022", "a > TIMESTAMP '2022-12-31 23:59:59.99999999'");
        testUnwrap("timestamp(9)", "year(a) > 2022", "a > TIMESTAMP '2022-12-31 23:59:59.999999999'");
        testUnwrap("timestamp(10)", "year(a) > 2022", "a > TIMESTAMP '2022-12-31 23:59:59.9999999999'");
        testUnwrap("timestamp(11)", "year(a) > 2022", "a > TIMESTAMP '2022-12-31 23:59:59.99999999999'");
        testUnwrap("timestamp(12)", "year(a) > 2022", "a > TIMESTAMP '2022-12-31 23:59:59.999999999999'");
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        testUnwrap("date", "year(a) >= -0001", "a >= DATE '-0001-01-01'");
        testUnwrap("date", "year(a) >= 1960", "a >= DATE '1960-01-01'");
        testUnwrap("date", "year(a) >= 2022", "a >= DATE '2022-01-01'");
        testUnwrap("date", "year(a) >= 9999", "a >= DATE '9999-01-01'");

        testUnwrap("timestamp", "year(a) >= -0001", "a >= TIMESTAMP '-0001-01-01 00:00:00.000'");
        testUnwrap("timestamp", "year(a) >= 1960", "a >= TIMESTAMP '1960-01-01 00:00:00.000'");
        testUnwrap("timestamp", "year(a) >= 2022", "a >= TIMESTAMP '2022-01-01 00:00:00.000'");
        testUnwrap("timestamp", "year(a) >= 9999", "a >= TIMESTAMP '9999-01-01 00:00:00.000'");

        testUnwrap("timestamp(0)", "year(a) >= 2022", "a >= TIMESTAMP '2022-01-01 00:00:00'");
        testUnwrap("timestamp(1)", "year(a) >= 2022", "a >= TIMESTAMP '2022-01-01 00:00:00.0'");
        testUnwrap("timestamp(2)", "year(a) >= 2022", "a >= TIMESTAMP '2022-01-01 00:00:00.00'");
        testUnwrap("timestamp(3)", "year(a) >= 2022", "a >= TIMESTAMP '2022-01-01 00:00:00.000'");
        testUnwrap("timestamp(4)", "year(a) >= 2022", "a >= TIMESTAMP '2022-01-01 00:00:00.0000'");
        testUnwrap("timestamp(5)", "year(a) >= 2022", "a >= TIMESTAMP '2022-01-01 00:00:00.00000'");
        testUnwrap("timestamp(6)", "year(a) >= 2022", "a >= TIMESTAMP '2022-01-01 00:00:00.000000'");
        testUnwrap("timestamp(7)", "year(a) >= 2022", "a >= TIMESTAMP '2022-01-01 00:00:00.0000000'");
        testUnwrap("timestamp(8)", "year(a) >= 2022", "a >= TIMESTAMP '2022-01-01 00:00:00.00000000'");
        testUnwrap("timestamp(9)", "year(a) >= 2022", "a >= TIMESTAMP '2022-01-01 00:00:00.000000000'");
        testUnwrap("timestamp(10)", "year(a) >= 2022", "a >= TIMESTAMP '2022-01-01 00:00:00.0000000000'");
        testUnwrap("timestamp(11)", "year(a) >= 2022", "a >= TIMESTAMP '2022-01-01 00:00:00.00000000000'");
        testUnwrap("timestamp(12)", "year(a) >= 2022", "a >= TIMESTAMP '2022-01-01 00:00:00.000000000000'");
    }

    @Test
    public void testDistinctFrom()
    {
        testUnwrap("date", "year(a) IS DISTINCT FROM -0001", "a IS NULL OR NOT (a BETWEEN DATE '-0001-01-01' AND DATE '-0001-12-31')");
        testUnwrap("date", "year(a) IS DISTINCT FROM 1960", "a IS NULL OR NOT (a BETWEEN DATE '1960-01-01' AND DATE '1960-12-31')");
        testUnwrap("date", "year(a) IS DISTINCT FROM 2022", "a IS NULL OR NOT (a BETWEEN DATE '2022-01-01' AND DATE '2022-12-31')");
        testUnwrap("date", "year(a) IS DISTINCT FROM 9999", "a IS NULL OR NOT (a BETWEEN DATE '9999-01-01' AND DATE '9999-12-31')");

        testUnwrap("timestamp", "year(a) IS DISTINCT FROM -0001", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '-0001-01-01 00:00:00.000' AND TIMESTAMP '-0001-12-31 23:59:59.999')");
        testUnwrap("timestamp", "year(a) IS DISTINCT FROM 1960", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '1960-01-01 00:00:00.000' AND TIMESTAMP '1960-12-31 23:59:59.999')");
        testUnwrap("timestamp", "year(a) IS DISTINCT FROM 2022", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.000' AND TIMESTAMP '2022-12-31 23:59:59.999')");
        testUnwrap("timestamp", "year(a) IS DISTINCT FROM 9999", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '9999-01-01 00:00:00.000' AND TIMESTAMP '9999-12-31 23:59:59.999')");

        testUnwrap("timestamp(0)", "year(a) IS DISTINCT FROM 2022", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00' AND TIMESTAMP '2022-12-31 23:59:59')");
        testUnwrap("timestamp(1)", "year(a) IS DISTINCT FROM 2022", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.0' AND TIMESTAMP '2022-12-31 23:59:59.9')");
        testUnwrap("timestamp(2)", "year(a) IS DISTINCT FROM 2022", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.00' AND TIMESTAMP '2022-12-31 23:59:59.99')");
        testUnwrap("timestamp(3)", "year(a) IS DISTINCT FROM 2022", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.000' AND TIMESTAMP '2022-12-31 23:59:59.999')");
        testUnwrap("timestamp(4)", "year(a) IS DISTINCT FROM 2022", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.0000' AND TIMESTAMP '2022-12-31 23:59:59.9999')");
        testUnwrap("timestamp(5)", "year(a) IS DISTINCT FROM 2022", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.00000' AND TIMESTAMP '2022-12-31 23:59:59.99999')");
        testUnwrap("timestamp(6)", "year(a) IS DISTINCT FROM 2022", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.000000' AND TIMESTAMP '2022-12-31 23:59:59.999999')");
        testUnwrap("timestamp(7)", "year(a) IS DISTINCT FROM 2022", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.0000000' AND TIMESTAMP '2022-12-31 23:59:59.9999999')");
        testUnwrap("timestamp(8)", "year(a) IS DISTINCT FROM 2022", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.00000000' AND TIMESTAMP '2022-12-31 23:59:59.99999999')");
        testUnwrap("timestamp(9)", "year(a) IS DISTINCT FROM 2022", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.000000000' AND TIMESTAMP '2022-12-31 23:59:59.999999999')");
        testUnwrap("timestamp(10)", "year(a) IS DISTINCT FROM 2022", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.0000000000' AND TIMESTAMP '2022-12-31 23:59:59.9999999999')");
        testUnwrap("timestamp(11)", "year(a) IS DISTINCT FROM 2022", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.00000000000' AND TIMESTAMP '2022-12-31 23:59:59.99999999999')");
        testUnwrap("timestamp(12)", "year(a) IS DISTINCT FROM 2022", "a IS NULL OR NOT (a BETWEEN TIMESTAMP '2022-01-01 00:00:00.000000000000' AND TIMESTAMP '2022-12-31 23:59:59.999999999999')");
    }

    @Test
    public void testNull()
    {
        testUnwrap("date", "year(a) = CAST(NULL AS BIGINT)", "CAST(NULL AS BOOLEAN)");
        testUnwrap("timestamp", "year(a) = CAST(NULL AS BIGINT)", "CAST(NULL AS BOOLEAN)");
    }

    @Test
    public void testNaN()
    {
        testUnwrap("date", "year(a) = nan()", "year(a) IS NULL AND NULL");
        testUnwrap("timestamp", "year(a) = nan()", "year(a) IS NULL AND NULL");
    }

    @Test
    public void smokeTests()
    {
        // smoke tests for various type combinations
        for (String type : asList("SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE")) {
            testUnwrap("date", format("year(a) = %s '2022'", type), "a BETWEEN DATE '2022-01-01' AND DATE '2022-12-31'");
        }
    }

    @Test
    public void testTermOrder()
    {
        // ensure the optimization works when the terms of the comparison are reversed
        // vs the canonical <expr> <op> <literal> form
        assertPlan("SELECT * FROM (VALUES DATE '2022-01-01') t(a) WHERE 2022 = year(a)",
                output(
                        filter("A BETWEEN DATE '2022-01-01' AND DATE '2022-12-31'",
                                values("A"))));
    }

    @Test
    public void testLeapYear()
    {
        testUnwrap("date", "year (a) = 2024", "a BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'");
        testUnwrap("timestamp", "year(a) = 2024", "a BETWEEN TIMESTAMP '2024-01-01 00:00:00.000' AND TIMESTAMP '2024-12-31 23:59:59.999'");
    }

    private void testUnwrap(String inputType, String inputPredicate, String expectedPredicate)
    {
        testUnwrap(getQueryRunner().getDefaultSession(), inputType, inputPredicate, expectedPredicate);
    }

    private void testUnwrap(Session session, String inputType, String inputPredicate, String expectedPredicate)
    {
        String sql = format("SELECT * FROM (VALUES CAST(NULL AS %s)) t(a) WHERE %s OR rand() = 42", inputType, inputPredicate);
        try {
            assertPlan(sql,
                    session,
                    output(
                            filter(expectedPredicate + " OR rand() = 42e0",
                                    values("a"))));
        }
        catch (Throwable e) {
            e.addSuppressed(new Exception("Query: " + sql));
            throw e;
        }
    }
}
