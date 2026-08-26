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

import com.google.common.collect.ImmutableList;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.type.DateTimes;
import io.trino.util.DateTimeUtils;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static java.lang.String.format;

public class TestUnwrapDateTruncInComparison
        extends BasePlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", fromTypes());

    @Test
    public void testDateSupportedUnitsEqual()
    {
        testUnwrap(
                "date",
                "date_trunc('week', a) = DATE '2024-01-01'",
                dateRange("2024-01-01", "2024-01-07"));

        testUnwrap(
                "date",
                "date_trunc('month', a) = DATE '2024-04-01'",
                dateRange("2024-04-01", "2024-04-30"));

        testUnwrap(
                "date",
                "date_trunc('quarter', a) = DATE '2024-04-01'",
                dateRange("2024-04-01", "2024-06-30"));

        testUnwrap(
                "date",
                "date_trunc('year', a) = DATE '2024-01-01'",
                dateRange("2024-01-01", "2024-12-31"));

        testUnwrap(
                "date",
                "DATE '2024-04-01' = date_trunc('quarter', a)",
                dateRange("2024-04-01", "2024-06-30"));
    }

    @Test
    public void testTimestampSupportedUnitsEqual()
    {
        testUnwrap(
                "timestamp(3)",
                "date_trunc('day', a) = TIMESTAMP '2024-01-01 00:00:00.000'",
                timestampRange("2024-01-01 00:00:00.000", "2024-01-01 23:59:59.999"));

        testUnwrap(
                "timestamp(3)",
                "date_trunc('week', a) = TIMESTAMP '2024-01-01 00:00:00.000'",
                timestampRange("2024-01-01 00:00:00.000", "2024-01-07 23:59:59.999"));

        testUnwrap(
                "timestamp(3)",
                "date_trunc('month', a) = TIMESTAMP '2024-04-01 00:00:00.000'",
                timestampRange("2024-04-01 00:00:00.000", "2024-04-30 23:59:59.999"));

        testUnwrap(
                "timestamp(3)",
                "date_trunc('quarter', a) = TIMESTAMP '2024-04-01 00:00:00.000'",
                timestampRange("2024-04-01 00:00:00.000", "2024-06-30 23:59:59.999"));

        testUnwrap(
                "timestamp(3)",
                "date_trunc('year', a) = TIMESTAMP '2024-01-01 00:00:00.000'",
                timestampRange("2024-01-01 00:00:00.000", "2024-12-31 23:59:59.999"));
    }

    @Test
    public void testTimestampWeekAndQuarterComparisons()
    {
        testUnwrap(
                "timestamp(3)",
                "date_trunc('quarter', a) <= TIMESTAMP '2024-05-15 00:00:00.000'",
                comparison(LESS_THAN_OR_EQUAL, timestampReference(), timestampConstant("2024-06-30 23:59:59.999")));

        testUnwrap(
                "timestamp(3)",
                "date_trunc('week', a) <> TIMESTAMP '2024-01-01 00:00:00.000'",
                new Logical(OR, ImmutableList.of(
                        comparison(LESS_THAN, timestampReference(), timestampConstant("2024-01-01 00:00:00.000")),
                        comparison(LESS_THAN, timestampConstant("2024-01-07 23:59:59.999"), timestampReference()))));

        testUnwrap(
                "timestamp(3)",
                "date_trunc('quarter', a) IS NOT DISTINCT FROM TIMESTAMP '2024-04-01 00:00:00.000'",
                new Logical(AND, ImmutableList.of(
                        not(new IsNull(timestampReference())),
                        timestampRange("2024-04-01 00:00:00.000", "2024-06-30 23:59:59.999"))));

        testUnwrap(
                "timestamp(3)",
                "date_trunc('week', a) < TIMESTAMP '2024-01-01 00:00:00.000'",
                comparison(LESS_THAN, timestampReference(), timestampConstant("2024-01-01 00:00:00.000")));

        testUnwrap(
                "timestamp(9)",
                "date_trunc('quarter', a) <= TIMESTAMP '2024-05-15 00:00:00.000000000'",
                comparison(LESS_THAN_OR_EQUAL, timestampReference(9), timestampConstant(9, "2024-06-30 23:59:59.999999999")));
    }

    @Test
    public void testDateWeekAndQuarterComparisons()
    {
        testUnwrap(
                "date",
                "date_trunc('week', a) >= DATE '2024-01-03'",
                comparison(GREATER_THAN, dateReference(), dateConstant("2024-01-07")));

        testUnwrap(
                "date",
                "date_trunc('quarter', a) <= DATE '2024-05-15'",
                comparison(LESS_THAN_OR_EQUAL, dateReference(), dateConstant("2024-06-30")));

        testUnwrap(
                "date",
                "date_trunc('quarter', a) > DATE '2024-04-01'",
                comparison(GREATER_THAN, dateReference(), dateConstant("2024-06-30")));
    }

    @Test
    public void testNonBoundaryEqualityIsFalseIfInputIsNotNull()
    {
        testUnwrap("date",
                "date_trunc('week', a) = DATE '2024-01-03'",
                new Logical(AND, ImmutableList.of(
                        new IsNull(dateReference()),
                        new Constant(BOOLEAN, null))));

        testUnwrap("date",
                "date_trunc('quarter', a) = DATE '2024-05-15'",
                new Logical(AND, ImmutableList.of(
                        new IsNull(dateReference()),
                        new Constant(BOOLEAN, null))));
    }

    private void testUnwrap(String inputType, String inputPredicate, Expression expected)
    {
        Expression antiOptimization = comparison(EQUAL, new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 42.0));
        if (expected instanceof Logical logical && logical.operator() == AND) {
            expected = new Logical(AND, logical.terms().stream()
                    .flatMap(term -> term instanceof Logical nested && nested.operator() == AND ? nested.terms().stream() : Stream.of(term))
                    .collect(toImmutableList()));
        }
        if (expected instanceof Logical logical && logical.operator() == OR) {
            expected = new Logical(OR, ImmutableList.<Expression>builder()
                    .addAll(logical.terms())
                    .add(antiOptimization)
                    .build());
        }
        else {
            expected = new Logical(OR, ImmutableList.of(expected, antiOptimization));
        }

        assertPlan(format("SELECT * FROM (VALUES CAST(NULL AS %s)) t(a) WHERE (%s) OR rand() = 42", inputType, inputPredicate),
                output(
                        filter(
                                expected,
                                values("a"))));
    }

    private static Expression dateRange(String startInclusive, String endInclusive)
    {
        return new Logical(AND, ImmutableList.of(
                comparison(GREATER_THAN_OR_EQUAL, dateReference(), dateConstant(startInclusive)),
                comparison(LESS_THAN_OR_EQUAL, dateReference(), dateConstant(endInclusive))));
    }

    private static Expression timestampRange(String startInclusive, String endInclusive)
    {
        return new Logical(AND, ImmutableList.of(
                comparison(GREATER_THAN_OR_EQUAL, timestampReference(), timestampConstant(startInclusive)),
                comparison(LESS_THAN_OR_EQUAL, timestampReference(), timestampConstant(endInclusive))));
    }

    private static Reference dateReference()
    {
        return new Reference(DATE, "a");
    }

    private static Constant dateConstant(String date)
    {
        return new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice(date)));
    }

    private static Reference timestampReference()
    {
        return timestampReference(3);
    }

    private static Reference timestampReference(int precision)
    {
        return new Reference(createTimestampType(precision), "a");
    }

    private static Constant timestampConstant(String timestamp)
    {
        return timestampConstant(3, timestamp);
    }

    private static Constant timestampConstant(int precision, String timestamp)
    {
        return new Constant(createTimestampType(precision), DateTimes.parseTimestamp(precision, timestamp));
    }

    private static Expression not(Expression value)
    {
        return IrExpressions.not(FUNCTIONS.getMetadata(), getCharVarcharCoercion(TEST_SESSION), value);
    }
}
