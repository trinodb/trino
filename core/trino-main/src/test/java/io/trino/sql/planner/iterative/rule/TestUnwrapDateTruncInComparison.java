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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.util.DateTimeUtils;
import org.junit.jupiter.api.Test;

import static io.trino.operator.scalar.timestamp.VarcharToTimestampCast.castToShortTimestamp;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestUnwrapDateTruncInComparison
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    private static Call dateTrunc(Type type, String unit, String column)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("date_trunc", fromTypes(VARCHAR, type)),
                ImmutableList.of(new Constant(VARCHAR, Slices.utf8Slice(unit)), new Reference(type, column)));
    }

    private static Constant toDateConstant(String date)
    {
        return new Constant(DATE, (long) DateTimeUtils.parseDate(date));
    }

    private static Constant toTimeSecondsConstant(String time)
    {
        return new Constant(TIMESTAMP_SECONDS, castToShortTimestamp(0, time));
    }

    @Test
    public void testInvalidEquals()
    {
        // dateTrunc('week', a) = '2024-01-02' => a is null and cast(null as boolean)
        tester().assertThat(new UnwrapDateTruncInComparison(tester().getPlannerContext()).filterExpressionRewrite())
                .on(p -> p.filter(
                        new PlanNodeId("filter"),
                        new Comparison(Comparison.Operator.EQUAL,
                                dateTrunc(DATE, "week", "a"),
                                toDateConstant("2024-01-02")),
                        p.values(p.symbol("a", DATE))))
                .matches(
                        filter(
                                new Logical(Logical.Operator.AND, ImmutableList.of(
                                        new IsNull(new Reference(DATE, "a")),
                                        new Constant(BOOLEAN, null))),
                                values("a")));
    }

    @Test
    public void testDateToDayNotFire()
    {
        tester().assertThat(new UnwrapDateTruncInComparison(tester().getPlannerContext()).filterExpressionRewrite())
                .on(p -> p.filter(
                        new PlanNodeId("filter"),
                        new Comparison(Comparison.Operator.EQUAL,
                                dateTrunc(DATE, "day", "a"),
                                toDateConstant("2024-01-01")),
                        p.values(p.symbol("a", DATE))))
                .doesNotFire();
    }

    private void verifyDateEquals(String unit, String target, String start, String end)
    {
        tester().assertThat(new UnwrapDateTruncInComparison(tester().getPlannerContext()).filterExpressionRewrite())
                .on(p -> p.filter(
                        new PlanNodeId("filter"),
                        new Comparison(Comparison.Operator.EQUAL,
                                dateTrunc(DATE, unit, "a"),
                                toDateConstant(target)),
                        p.values(p.symbol("a", DATE))))
                .matches(
                        filter(
                                new Between(new Reference(DATE, "a"), toDateConstant(start), toDateConstant(end)),
                                values("a")));
    }

    @Test
    public void testDateEquals()
    {
        verifyDateEquals("week", "2024-01-01", "2024-01-01", "2024-01-07");
        verifyDateEquals("month", "2024-01-01", "2024-01-01", "2024-01-31");
        verifyDateEquals("year", "2024-01-01", "2024-01-01", "2024-12-31");
    }

    private void verifyDateCompare(String unit, Comparison.Operator compareOperator, String target, Comparison.Operator expectedOperator, String limit)
    {
        tester().assertThat(new UnwrapDateTruncInComparison(tester().getPlannerContext()).filterExpressionRewrite())
                .on(p -> p.filter(
                        new PlanNodeId("filter"),
                        new Comparison(compareOperator, dateTrunc(DATE, unit, "a"), toDateConstant(target)),
                        p.values(p.symbol("a", DATE))))
                .matches(
                        filter(
                                new Comparison(expectedOperator, new Reference(DATE, "a"), toDateConstant(limit)),
                                values("a")));
    }

    @Test
    public void testDateCompareGreaterThanOrEquals()
    {
        verifyDateCompare("week", GREATER_THAN, "2024-01-01", GREATER_THAN, "2024-01-07");
        verifyDateCompare("week", GREATER_THAN, "2024-01-07", GREATER_THAN, "2024-01-07");
        verifyDateCompare("week", GREATER_THAN_OR_EQUAL, "2024-01-01", GREATER_THAN_OR_EQUAL, "2024-01-01");
        verifyDateCompare("week", GREATER_THAN_OR_EQUAL, "2024-01-07", GREATER_THAN, "2024-01-07");
        verifyDateCompare("week", LESS_THAN, "2024-01-01", LESS_THAN, "2024-01-01");
        verifyDateCompare("week", LESS_THAN, "2024-01-07", LESS_THAN_OR_EQUAL, "2024-01-07");
        verifyDateCompare("week", LESS_THAN_OR_EQUAL, "2024-01-01", LESS_THAN_OR_EQUAL, "2024-01-07");
        verifyDateCompare("week", LESS_THAN_OR_EQUAL, "2024-01-07", LESS_THAN_OR_EQUAL, "2024-01-07");
        verifyDateCompare("month", GREATER_THAN, "2024-01-01", GREATER_THAN, "2024-01-31");
        verifyDateCompare("month", GREATER_THAN, "2024-01-31", GREATER_THAN, "2024-01-31");
        verifyDateCompare("month", GREATER_THAN_OR_EQUAL, "2024-01-01", GREATER_THAN_OR_EQUAL, "2024-01-01");
        verifyDateCompare("month", GREATER_THAN_OR_EQUAL, "2024-01-31", GREATER_THAN, "2024-01-31");
        verifyDateCompare("month", LESS_THAN, "2024-01-01", LESS_THAN, "2024-01-01");
        verifyDateCompare("month", LESS_THAN, "2024-01-31", LESS_THAN_OR_EQUAL, "2024-01-31");
        verifyDateCompare("month", LESS_THAN_OR_EQUAL, "2024-01-01", LESS_THAN_OR_EQUAL, "2024-01-31");
        verifyDateCompare("month", LESS_THAN_OR_EQUAL, "2024-01-31", LESS_THAN_OR_EQUAL, "2024-01-31");
        verifyDateCompare("year", GREATER_THAN, "2024-01-01", GREATER_THAN, "2024-12-31");
        verifyDateCompare("year", GREATER_THAN, "2024-12-31", GREATER_THAN, "2024-12-31");
        verifyDateCompare("year", GREATER_THAN_OR_EQUAL, "2024-01-01", GREATER_THAN_OR_EQUAL, "2024-01-01");
        verifyDateCompare("year", GREATER_THAN_OR_EQUAL, "2024-12-31", GREATER_THAN, "2024-12-31");
        verifyDateCompare("year", LESS_THAN, "2024-01-01", LESS_THAN, "2024-01-01");
        verifyDateCompare("year", LESS_THAN, "2024-12-31", LESS_THAN_OR_EQUAL, "2024-12-31");
        verifyDateCompare("year", LESS_THAN_OR_EQUAL, "2024-01-01", LESS_THAN_OR_EQUAL, "2024-12-31");
        verifyDateCompare("year", LESS_THAN_OR_EQUAL, "2024-12-31", LESS_THAN_OR_EQUAL, "2024-12-31");
    }

    public void verifyTimeEquals(String unit, String target, String start, String end)
    {
        tester().assertThat(new UnwrapDateTruncInComparison(tester().getPlannerContext()).filterExpressionRewrite())
                .on(p -> p.filter(
                        new PlanNodeId("filter"),
                        new Comparison(Comparison.Operator.EQUAL,
                                dateTrunc(TIMESTAMP_SECONDS, unit, "a"),
                                toTimeSecondsConstant(target)),
                        p.values(p.symbol("a", TIMESTAMP_SECONDS))))
                .matches(
                        filter(
                                new Between(new Reference(TIMESTAMP_SECONDS, "a"), toTimeSecondsConstant(start), toTimeSecondsConstant(end)),
                                values("a")));
    }

    @Test
    public void testTimeEquals()
    {
        verifyTimeEquals("hour", "2024-01-01 00:00:00", "2024-01-01 00:00:00", "2024-01-01 00:59:59");
        verifyTimeEquals("day", "2024-01-01 00:00:00", "2024-01-01 00:00:00", "2024-01-01 23:59:59");
        verifyTimeEquals("week", "2024-01-01 00:00:00", "2024-01-01 00:00:00", "2024-01-07 23:59:59");
        verifyTimeEquals("month", "2024-01-01 00:00:00", "2024-01-01 00:00:00", "2024-01-31 23:59:59");
        verifyTimeEquals("year", "2024-01-01 00:00:00", "2024-01-01 00:00:00", "2024-12-31 23:59:59");
    }

    private void verifyTimeCompare(String unit, Comparison.Operator compareOperator, String target, Comparison.Operator expectedOperator, String limit)
    {
        tester().assertThat(new UnwrapDateTruncInComparison(tester().getPlannerContext()).filterExpressionRewrite())
                .on(p -> p.filter(
                        new PlanNodeId("filter"),
                        new Comparison(compareOperator, dateTrunc(TIMESTAMP_SECONDS, unit, "a"), toTimeSecondsConstant(target)),
                        p.values(p.symbol("a", TIMESTAMP_SECONDS))))
                .matches(
                        filter(
                                new Comparison(expectedOperator, new Reference(TIMESTAMP_SECONDS, "a"), toTimeSecondsConstant(limit)),
                                values("a")));
    }

    @Test
    public void testTimeCompare()
    {
        verifyTimeCompare("hour", GREATER_THAN, "2024-01-01 00:00:00", GREATER_THAN, "2024-01-01 00:59:59");
        verifyTimeCompare("hour", GREATER_THAN, "2024-01-01 00:59:59", GREATER_THAN, "2024-01-01 00:59:59");
        verifyTimeCompare("hour", GREATER_THAN_OR_EQUAL, "2024-01-01 00:00:00", GREATER_THAN_OR_EQUAL, "2024-01-01 00:00:00");
        verifyTimeCompare("hour", GREATER_THAN_OR_EQUAL, "2024-01-01 00:59:59", GREATER_THAN, "2024-01-01 00:59:59");
        verifyTimeCompare("hour", LESS_THAN, "2024-01-01 00:00:00", LESS_THAN, "2024-01-01 00:00:00");
        verifyTimeCompare("hour", LESS_THAN, "2024-01-01 00:59:59", LESS_THAN_OR_EQUAL, "2024-01-01 00:59:59");
        verifyTimeCompare("hour", LESS_THAN_OR_EQUAL, "2024-01-01 00:00:00", LESS_THAN_OR_EQUAL, "2024-01-01 00:59:59");
        verifyTimeCompare("hour", LESS_THAN_OR_EQUAL, "2024-01-01 00:59:59", LESS_THAN_OR_EQUAL, "2024-01-01 00:59:59");

        verifyTimeCompare("day", GREATER_THAN, "2024-01-01 00:00:00", GREATER_THAN, "2024-01-01 23:59:59");
        verifyTimeCompare("day", GREATER_THAN, "2024-01-01 23:59:59", GREATER_THAN, "2024-01-01 23:59:59");
        verifyTimeCompare("day", GREATER_THAN_OR_EQUAL, "2024-01-01 00:00:00", GREATER_THAN_OR_EQUAL, "2024-01-01 00:00:00");
        verifyTimeCompare("day", GREATER_THAN_OR_EQUAL, "2024-01-01 23:59:59", GREATER_THAN, "2024-01-01 23:59:59");
        verifyTimeCompare("day", LESS_THAN, "2024-01-01 00:00:00", LESS_THAN, "2024-01-01 00:00:00");
        verifyTimeCompare("day", LESS_THAN, "2024-01-01 23:59:59", LESS_THAN_OR_EQUAL, "2024-01-01 23:59:59");
        verifyTimeCompare("day", LESS_THAN_OR_EQUAL, "2024-01-01 00:00:00", LESS_THAN_OR_EQUAL, "2024-01-01 23:59:59");
        verifyTimeCompare("day", LESS_THAN_OR_EQUAL, "2024-01-01 23:59:59", LESS_THAN_OR_EQUAL, "2024-01-01 23:59:59");

        verifyTimeCompare("week", GREATER_THAN, "2024-01-01 00:00:00", GREATER_THAN, "2024-01-07 23:59:59");
        verifyTimeCompare("week", GREATER_THAN, "2024-01-07 23:59:59", GREATER_THAN, "2024-01-07 23:59:59");
        verifyTimeCompare("week", GREATER_THAN_OR_EQUAL, "2024-01-01 00:00:00", GREATER_THAN_OR_EQUAL, "2024-01-01 00:00:00");
        verifyTimeCompare("week", GREATER_THAN_OR_EQUAL, "2024-01-07 23:59:59", GREATER_THAN, "2024-01-07 23:59:59");
        verifyTimeCompare("week", LESS_THAN, "2024-01-01 00:00:00", LESS_THAN, "2024-01-01 00:00:00");
        verifyTimeCompare("week", LESS_THAN, "2024-01-07 23:59:59", LESS_THAN_OR_EQUAL, "2024-01-07 23:59:59");
        verifyTimeCompare("week", LESS_THAN_OR_EQUAL, "2024-01-01 00:00:00", LESS_THAN_OR_EQUAL, "2024-01-07 23:59:59");
        verifyTimeCompare("week", LESS_THAN_OR_EQUAL, "2024-01-07 23:59:59", LESS_THAN_OR_EQUAL, "2024-01-07 23:59:59");

        verifyTimeCompare("month", GREATER_THAN, "2024-01-01 00:00:00", GREATER_THAN, "2024-01-31 23:59:59");
        verifyTimeCompare("month", GREATER_THAN, "2024-01-31 23:59:59", GREATER_THAN, "2024-01-31 23:59:59");
        verifyTimeCompare("month", GREATER_THAN_OR_EQUAL, "2024-01-01 00:00:00", GREATER_THAN_OR_EQUAL, "2024-01-01 00:00:00");
        verifyTimeCompare("month", GREATER_THAN_OR_EQUAL, "2024-01-31 23:59:59", GREATER_THAN, "2024-01-31 23:59:59");
        verifyTimeCompare("month", LESS_THAN, "2024-01-01 00:00:00", LESS_THAN, "2024-01-01 00:00:00");
        verifyTimeCompare("month", LESS_THAN, "2024-01-31 23:59:59", LESS_THAN_OR_EQUAL, "2024-01-31 23:59:59");
        verifyTimeCompare("month", LESS_THAN_OR_EQUAL, "2024-01-01 00:00:00", LESS_THAN_OR_EQUAL, "2024-01-31 23:59:59");
        verifyTimeCompare("month", LESS_THAN_OR_EQUAL, "2024-01-31 23:59:59", LESS_THAN_OR_EQUAL, "2024-01-31 23:59:59");

        verifyTimeCompare("year", GREATER_THAN, "2024-01-01 00:00:00", GREATER_THAN, "2024-12-31 23:59:59");
        verifyTimeCompare("year", GREATER_THAN, "2024-12-31 23:59:59", GREATER_THAN, "2024-12-31 23:59:59");
        verifyTimeCompare("year", GREATER_THAN_OR_EQUAL, "2024-01-01 00:00:00", GREATER_THAN_OR_EQUAL, "2024-01-01 00:00:00");
        verifyTimeCompare("year", GREATER_THAN_OR_EQUAL, "2024-12-31 23:59:59", GREATER_THAN, "2024-12-31 23:59:59");
        verifyTimeCompare("year", LESS_THAN, "2024-01-01 00:00:00", LESS_THAN, "2024-01-01 00:00:00");
        verifyTimeCompare("year", LESS_THAN, "2024-12-31 23:59:59", LESS_THAN_OR_EQUAL, "2024-12-31 23:59:59");
        verifyTimeCompare("year", LESS_THAN_OR_EQUAL, "2024-01-01 00:00:00", LESS_THAN_OR_EQUAL, "2024-12-31 23:59:59");
        verifyTimeCompare("year", LESS_THAN_OR_EQUAL, "2024-12-31 23:59:59", LESS_THAN_OR_EQUAL, "2024-12-31 23:59:59");
    }
}
