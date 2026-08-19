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
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * Exercises {@code ReplaceDecimalAvgWithSumAndCount}: whenever a query computes both {@code sum(x)}
 * and {@code avg(x)} over a decimal column, avg is reconstructed from the reused sum and a new
 * count. These queries therefore run through the rewrite, and must return exactly what the built-in
 * decimal avg would.
 */
@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestDecimalAvgFromSumAndCount
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    void teardown()
    {
        assertions.close();
    }

    @Test
    void testReusesSumWithHalfUpRounding()
    {
        // 5.00 / 3 = 1.6666... rounds HALF_UP to 1.67
        assertThat(assertions.query(
                "SELECT avg(v), sum(v), count(v) " +
                        "FROM (VALUES DECIMAL '1.00', DECIMAL '2.00', DECIMAL '2.00') t(v)"))
                .matches("VALUES (DECIMAL '1.67', CAST(DECIMAL '5.00' AS decimal(38, 2)), BIGINT '3')");
    }

    @Test
    void testAvgIsReplacedInPlan()
    {
        // Guards that the rule is actually wired into the optimizer (registered and placed so it
        // fires): the aggregation over v must hold sum and count -- the count exists only because
        // avg was rewritten. Without the rewrite it would be sum and avg, with no count(v). A
        // result-based test cannot catch this, since avg computed the normal way returns the same
        // numbers.
        assertThat(assertions.query(
                "SELECT avg(v), sum(v) FROM (VALUES DECIMAL '1.00', DECIMAL '2.00') t(v)"))
                .matches(anyTree(
                        aggregation(
                                ImmutableMap.of(
                                        "sum_out", aggregationFunction("sum", ImmutableList.of("v")),
                                        "count_out", aggregationFunction("count", ImmutableList.of("v"))),
                                values("v"))));
    }

    @Test
    void testHalfUpTie()
    {
        // 0.03 / 2 = 0.015, the HALF_UP tie rounds up to 0.02
        assertThat(assertions.query(
                "SELECT avg(v), sum(v) FROM (VALUES DECIMAL '0.01', DECIMAL '0.02') t(v)"))
                .matches("VALUES (DECIMAL '0.02', CAST(DECIMAL '0.03' AS decimal(38, 2)))");
    }

    @Test
    void testNegativeHalfUpTie()
    {
        // -0.03 / 2 = -0.015, HALF_UP rounds away from zero to -0.02
        assertThat(assertions.query(
                "SELECT avg(v), sum(v) FROM (VALUES DECIMAL '-0.01', DECIMAL '-0.02') t(v)"))
                .matches("VALUES (DECIMAL '-0.02', CAST(DECIMAL '-0.03' AS decimal(38, 2)))");
    }

    @Test
    void testDividesByNonNullCountNotRowCount()
    {
        // A null value is summed and counted as absent: avg is 10.00, not 5.00
        assertThat(assertions.query(
                "SELECT avg(v), sum(v) FROM (VALUES DECIMAL '10.00', CAST(NULL AS decimal(4, 2))) t(v)"))
                .matches("VALUES (DECIMAL '10.00', CAST(DECIMAL '10.00' AS decimal(38, 2)))");
    }

    @Test
    void testDoesNotForceSumMaterializationWhenSumIsUnused()
    {
        // sum(x) overflows decimal(38, 0) but avg(x) does not. The outer predicate `true OR s > 0`
        // simplifies to TRUE, so sum is unused and gets pruned -- the query must succeed and return
        // just the average. Reconstructing avg from sum must not pin sum alive and reintroduce the
        // overflow.
        assertThat(assertions.query(
                """
                SELECT a FROM (
                  SELECT sum(v) s, avg(v) a
                  FROM (VALUES DECIMAL '99999999999999999999999999999999999999', DECIMAL '99999999999999999999999999999999999999') t(v)
                ) u WHERE true OR s > DECIMAL '0'
                """))
                .matches("VALUES DECIMAL '99999999999999999999999999999999999999'");
    }

    @Test
    void testAllNullGroupIsNull()
    {
        assertThat(assertions.query(
                "SELECT avg(v), sum(v), count(v) FROM (VALUES CAST(NULL AS decimal(10, 2))) t(v)"))
                .matches("VALUES (CAST(NULL AS decimal(10, 2)), CAST(NULL AS decimal(38, 2)), BIGINT '0')");
    }

    @Test
    void testReusesMaskForFilteredAverage()
    {
        // avg and sum share the FILTER predicate, so they share a mask and the rewrite fires. The
        // mask must reach the new count. GROUP BY is used deliberately: for a global aggregation
        // where every aggregate is filtered, ImplementFilteredAggregations pre-filters the rows
        // with a FilterNode, which would mask the count regardless. With a grouping set no such
        // filter is added, so the count's own mask is what restricts it. In group 1 the filter
        // drops 1.00, leaving 2.00 and 4.00: avg is 6.00 / 2 = 3.00, not 6.00 / 3.
        assertThat(assertions.query(
                """
                SELECT k, avg(v) FILTER (WHERE v > DECIMAL '1.00'), sum(v) FILTER (WHERE v > DECIMAL '1.00')
                FROM (VALUES (1, DECIMAL '1.00'), (1, DECIMAL '2.00'), (1, DECIMAL '4.00'), (2, DECIMAL '10.00')) t(k, v)
                GROUP BY k"""))
                .matches(
                        """
                        VALUES
                          (1, CAST(DECIMAL '3.00' AS decimal(4, 2)), CAST(DECIMAL '6.00' AS decimal(38, 2))),
                          (2, CAST(DECIMAL '10.00' AS decimal(4, 2)), CAST(DECIMAL '10.00' AS decimal(38, 2)))""");
    }

    @Test
    void testGroupBy()
    {
        assertThat(assertions.query(
                """
                SELECT k, avg(v), sum(v) FROM (VALUES (1, DECIMAL '1.00'), (1, DECIMAL '2.00'), (2, DECIMAL '10.00')) t(k, v)
                GROUP BY k"""))
                .matches("VALUES " +
                        "(1, CAST(DECIMAL '1.50' AS decimal(4, 2)), CAST(DECIMAL '3.00' AS decimal(38, 2))), " +
                        "(2, CAST(DECIMAL '10.00' AS decimal(4, 2)), CAST(DECIMAL '10.00' AS decimal(38, 2)))");
    }

    @Test
    void testAvoidsDoubleRoundingOfPlainDivide()
    {
        // 488999.99 / 200000 = 2.44499995. The correct HALF_UP-to-scale-2 average is 2.44. Trino's
        // decimal / operator instead lands at scale 6 (2.445000, its minimum divide scale for a
        // scale-2 dividend), and casting that back to scale 2 rounds a second time to 2.45. The
        // reconstruction rounds only once, to the dividend's scale, so it stays 2.44.
        assertThat(assertions.query(
                "SELECT \"$divide_round_to_scale\"(CAST(488999.99 AS decimal(38, 2)), BIGINT '200000')"))
                .matches("VALUES CAST(2.44 AS decimal(38, 2))");
        assertThat(assertions.query(
                "SELECT CAST(CAST(488999.99 AS decimal(38, 2)) / CAST(200000 AS bigint) AS decimal(38, 2))"))
                .matches("VALUES CAST(2.45 AS decimal(38, 2))");
    }

    @Test
    void testNegativeDivisor()
    {
        // A negative divisor is currently unsupported
        assertThat(assertions.query(
                "SELECT \"$divide_round_to_scale\"(CAST(12345.00 AS decimal(38, 2)), BIGINT '-3')"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("Negative divisor is not supported");
    }

    @Test
    void testZeroDivisor()
    {
        assertThat(assertions.query(
                "SELECT \"$divide_round_to_scale\"(CAST(12345.00 AS decimal(38, 2)), BIGINT '0')"))
                .failure()
                .hasErrorCode(DIVISION_BY_ZERO)
                .hasMessage("Division by zero");
    }

    @Test
    void testLargeSumReusedForAverage()
    {
        // sum needs the wide decimal(38, s); the average narrows back to the input precision
        assertThat(assertions.query(
                "SELECT avg(v), sum(v) FROM (VALUES DECIMAL '99999999.99', DECIMAL '99999999.99', DECIMAL '99999999.97') t(v)"))
                .matches("VALUES (DECIMAL '99999999.98', CAST(DECIMAL '299999999.95' AS decimal(38, 2)))");
    }

    @Test
    void testLongDecimalReusesSum()
    {
        // A long decimal (precision > 18, Int128-backed): the sum overflows a long, so the whole
        // summation and division runs through the Int128 path. 7.0000 / 3 = 2.33333... rounds
        // HALF_UP to scale 4 as 2.3333.
        assertThat(assertions.query(
                """
                SELECT avg(v), sum(v), count(v) FROM (VALUES
                CAST(10000000000000000000.0001 AS decimal(25, 4)),
                CAST(20000000000000000000.0002 AS decimal(25, 4)),
                CAST(40000000000000000000.0004 AS decimal(25, 4))) t(v)
                """))
                .matches(
                        """
                        VALUES (
                          CAST(23333333333333333333.3336 AS decimal(25, 4)),
                          CAST(70000000000000000000.0007 AS decimal(38, 4)),
                          BIGINT '3')""");
    }

    @Test
    void testSumOverflowStillFails()
    {
        // Built-in avg would survive this via the BigDecimal fallback in DecimalAverageAggregation,
        // but the query also computes sum(v), which overflows decimal(38, 0). The reconstruction
        // reuses that sum, so the query must still fail with NUMERIC_VALUE_OUT_OF_RANGE rather than
        // silently divide an overflowed sum.
        assertThat(assertions.query(
                """
                SELECT avg(v), sum(v) FROM (VALUES
                  DECIMAL '99999999999999999999999999999999999999',
                  DECIMAL '99999999999999999999999999999999999999') t(v)
                """))
                .failure().hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    void testLongDecimalHalfUpRounding()
    {
        // Int128 rounding at the widest scale: the exact average lands on a HALF_UP tie in the last
        // digit. 66666666666666666666.6666666667 / 2 = 33333333333333333333.33333333335, which
        // rounds up to ...334 at scale 10.
        assertThat(assertions.query(
                """
                SELECT avg(v), sum(v), count(v) FROM (VALUES
                  CAST(33333333333333333333.3333333333 AS decimal(38, 10)),
                  CAST(33333333333333333333.3333333334 AS decimal(38, 10))) t(v)
                """))
                .matches(
                        """
                        VALUES (
                          CAST(33333333333333333333.3333333334 AS decimal(38, 10)),
                          CAST(66666666666666666666.6666666667 AS decimal(38, 10)),
                          BIGINT '2')""");
    }
}
