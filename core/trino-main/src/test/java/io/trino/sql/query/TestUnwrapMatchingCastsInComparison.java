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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestUnwrapMatchingCastsInComparison
{
    private static final List<String> COMPARISON_OPERATORS = asList("=", "<>", ">=", ">", "<=", "<", "IS DISTINCT FROM", "IS NOT DISTINCT FROM");

    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testComparison()
    {
        List<String> tinyintValues = asList("-128", "0", "127", null);
        List<String> smallintValues = asList("-32768", "0", "32767", null);
        List<String> integerValues = asList("-2147483648", "0", "2147483647", null);
        List<String> bigintValues = asList("-9223372036854775808", "0", "9223372036854775807", null);
        List<String> shortDecimalValues = asList("-9999.9", "0.1", "9999.9", null);
        List<String> decimalValues = asList("-9999999999999999.99", "0.01", "9999999999999999.99", null);
        List<String> longDecimalValues = asList("-999999999999999999.99", "0.01", "999999999999999999.99", null);
        List<String> twoDigitValues = asList("-99", "0", "99", null);
        List<String> fourDigitValues = asList("-9999", "0", "9999", null);
        List<String> sevenDigitValues = asList("-9999999", "9999998", "9999999", null);
        List<String> nineDigitValues = asList("-999999999", "0", "999999999", null);
        List<String> fifteenDigitValues = asList("-999999999999999", "999999999999998", "999999999999999", null);
        List<String> eighteenDigitValues = asList("-999999999999999999", "0", "999999999999999999", null);
        List<String> realValues = asList("-infinity()", "-0E0", "0E0", "1.1E0", "nan()", null);
        validateComparisons("TINYINT", tinyintValues, "SMALLINT");
        validateComparisons("TINYINT", tinyintValues, "DECIMAL(3, 0)");
        validateComparisons("TINYINT", tinyintValues, "REAL");
        validateComparisons("SMALLINT", smallintValues, "INTEGER");
        validateComparisons("SMALLINT", smallintValues, "REAL");
        validateComparisons("INTEGER", integerValues, "BIGINT");
        validateComparisons("INTEGER", integerValues, "DECIMAL(10, 0)");
        validateComparisons("INTEGER", integerValues, "DOUBLE");
        validateComparisons("BIGINT", bigintValues, "DECIMAL(19, 0)");
        validateComparisons("BIGINT", bigintValues, "DECIMAL(38, 19)");
        validateComparisons("DECIMAL(5, 1)", shortDecimalValues, "DECIMAL(9, 3)");
        validateComparisons("DECIMAL(18, 2)", decimalValues, "DECIMAL(19, 2)");
        validateComparisons("DECIMAL(18, 2)", decimalValues, "DECIMAL(38, 22)");
        validateComparisons("DECIMAL(20, 2)", longDecimalValues, "DECIMAL(38, 2)");
        validateComparisons("DECIMAL(2, 0)", twoDigitValues, "TINYINT");
        validateComparisons("DECIMAL(4, 0)", fourDigitValues, "SMALLINT");
        validateComparisons("DECIMAL(7, 0)", sevenDigitValues, "REAL");
        validateComparisons("DECIMAL(9, 0)", nineDigitValues, "INTEGER");
        validateComparisons("DECIMAL(15, 0)", fifteenDigitValues, "DOUBLE");
        validateComparisons("DECIMAL(18, 0)", eighteenDigitValues, "BIGINT");
        validateComparisons("REAL", realValues, "DOUBLE");
    }

    @Test
    public void testBetween()
    {
        List<String> values = asList("-9999999999999999.99", "0.01", "9999999999999999.99", null);
        for (String value : values) {
            for (String min : values) {
                for (String max : values) {
                    validateBetween("DECIMAL(18, 2)", value, min, max, "DECIMAL(19, 2)");
                }
            }
        }
    }

    private void validateComparisons(String fromType, List<String> values, String toType)
    {
        for (String left : values) {
            for (String right : values) {
                validateComparison(fromType, left, right, toType);
            }
        }
    }

    private void validateComparison(String fromType, String leftValue, String rightValue, String toType)
    {
        String checks = COMPARISON_OPERATORS.stream()
                .map(operator -> format(
                        "(CAST(a AS %1$s) %2$s CAST(b AS %1$s)) " +
                                "IS NOT DISTINCT FROM " +
                                "(CAST(CAST(%3$s AS %4$s) AS %1$s) %2$s CAST(CAST(%5$s AS %4$s) AS %1$s))",
                        toType,
                        operator,
                        leftValue,
                        fromType,
                        rightValue))
                .collect(joining(", "));

        assertAllTrue(format(
                "SELECT %1$s FROM (VALUES CAST(ROW(%2$s, %3$s) AS ROW(%4$s, %4$s))) t(a, b)",
                checks,
                leftValue,
                rightValue,
                fromType));
    }

    private void validateBetween(String fromType, String value, String min, String max, String toType)
    {
        String bounds = format("CAST(CAST(%1$s AS %3$s) AS %4$s) AND CAST(CAST(%2$s AS %3$s) AS %4$s)", min, max, fromType, toType);
        String expected = format("(CAST(CAST(%1$s AS %2$s) AS %3$s) BETWEEN %4$s)", value, fromType, toType, bounds);
        String negatedExpected = format("(CAST(-CAST(%1$s AS %2$s) AS %3$s) BETWEEN %4$s)", value, fromType, toType, bounds);

        assertAllTrue(format(
                "SELECT " +
                        "(CAST(a AS %1$s) BETWEEN CAST(b AS %1$s) AND CAST(c AS %1$s)) IS NOT DISTINCT FROM %2$s, " +
                        "(CAST(a AS %1$s) BETWEEN CAST(b AS %1$s) AND max_bound) IS NOT DISTINCT FROM %2$s, " +
                        "(CAST(a AS %1$s) BETWEEN min_bound AND CAST(c AS %1$s)) IS NOT DISTINCT FROM %2$s, " +
                        "(CAST(-a AS %1$s) BETWEEN CAST(b AS %1$s) AND max_bound) IS NOT DISTINCT FROM %3$s " +
                        "FROM (VALUES CAST(ROW(%4$s, %5$s, %6$s, %5$s, %6$s) AS ROW(%7$s, %7$s, %7$s, %1$s, %1$s))) t(a, b, c, min_bound, max_bound)",
                toType,
                expected,
                negatedExpected,
                value,
                min,
                max,
                fromType));
    }

    private void assertAllTrue(String query)
    {
        assertThat(assertions.execute(query).getMaterializedRows().getFirst().getFields())
                .as("Query has a check that evaluated to false: " + query)
                .containsOnly(true);
    }
}
