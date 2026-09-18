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
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.BasePlanTest;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.NOT_EQUAL;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static java.lang.String.format;

public class TestUnwrapAtTimeZoneInComparison
        extends BasePlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", fromTypes());

    private static final long EPOCH_MILLIS = LocalDateTime.of(2022, 5, 1, 10, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
    private static final String LITERAL_MILLIS = "TIMESTAMP '2022-05-01 10:00:00.000 UTC'";
    private static final String LITERAL_MICROS = "TIMESTAMP '2022-05-01 10:00:00.000000 UTC'";

    private static final Reference A_MILLIS = new Reference(TIMESTAMP_TZ_MILLIS, "a");
    private static final Constant VALUE_MILLIS = new Constant(TIMESTAMP_TZ_MILLIS, packDateTimeWithZone(EPOCH_MILLIS, getTimeZoneKey("UTC")));

    @Test
    public void testUnwrapComparisons()
    {
        testUnwrap("timestamp(3) with time zone", format("at_timezone(a, 'America/Los_Angeles') = %s", LITERAL_MILLIS), comparison(EQUAL, A_MILLIS, VALUE_MILLIS));
        testUnwrap("timestamp(3) with time zone", format("at_timezone(a, 'America/Los_Angeles') <> %s", LITERAL_MILLIS), comparison(NOT_EQUAL, A_MILLIS, VALUE_MILLIS));
        testUnwrap("timestamp(3) with time zone", format("at_timezone(a, 'America/Los_Angeles') < %s", LITERAL_MILLIS), comparison(LESS_THAN, A_MILLIS, VALUE_MILLIS));
        testUnwrap("timestamp(3) with time zone", format("at_timezone(a, 'America/Los_Angeles') <= %s", LITERAL_MILLIS), comparison(LESS_THAN_OR_EQUAL, A_MILLIS, VALUE_MILLIS));
        testUnwrap("timestamp(3) with time zone", format("at_timezone(a, 'America/Los_Angeles') > %s", LITERAL_MILLIS), comparison(GREATER_THAN, A_MILLIS, VALUE_MILLIS));
        testUnwrap("timestamp(3) with time zone", format("at_timezone(a, 'America/Los_Angeles') >= %s", LITERAL_MILLIS), comparison(GREATER_THAN_OR_EQUAL, A_MILLIS, VALUE_MILLIS));

        // constant on the left
        testUnwrap("timestamp(3) with time zone", format("%s < at_timezone(a, 'America/Los_Angeles')", LITERAL_MILLIS), comparison(LESS_THAN, VALUE_MILLIS, A_MILLIS));

        // fixed-offset zones are constants too
        testUnwrap("timestamp(3) with time zone", format("at_timezone(a, '+08:45') = %s", LITERAL_MILLIS), comparison(EQUAL, A_MILLIS, VALUE_MILLIS));

        // nested calls unwrap to the innermost argument
        testUnwrap("timestamp(3) with time zone", format("at_timezone(at_timezone(a, 'UTC'), 'Pacific/Apia') > %s", LITERAL_MILLIS), comparison(GREATER_THAN, A_MILLIS, VALUE_MILLIS));

        // long (non-packed) timestamp with time zone representation
        testUnwrap(
                "timestamp(6) with time zone",
                format("at_timezone(a, 'America/Los_Angeles') = %s", LITERAL_MICROS),
                comparison(
                        EQUAL,
                        new Reference(createTimestampWithTimeZoneType(6), "a"),
                        new Constant(createTimestampWithTimeZoneType(6), LongTimestampWithTimeZone.fromEpochMillisAndFraction(EPOCH_MILLIS, 0, getTimeZoneKey("UTC")))));
    }

    @Test
    public void testInvalidZoneIsNotUnwrapped()
    {
        // an invalid zone string fails the query at runtime; unwrapping would silently swallow the failure
        ResolvedFunction atTimezone = FUNCTIONS.resolveFunction("at_timezone", fromTypes(TIMESTAMP_TZ_MILLIS, createVarcharType(12)));
        testUnwrap(
                "timestamp(3) with time zone",
                format("at_timezone(a, 'Mars/Olympus') = %s", LITERAL_MILLIS),
                comparison(
                        EQUAL,
                        new Call(atTimezone, ImmutableList.of(A_MILLIS, new Constant(createVarcharType(12), utf8Slice("Mars/Olympus")))),
                        VALUE_MILLIS));

        // an empty zone id throws IllegalArgumentException from getTimeZoneKey rather than
        // TimeZoneNotSupportedException; the rule must bail, not fail planning
        ResolvedFunction atTimezoneEmptyZone = FUNCTIONS.resolveFunction("at_timezone", fromTypes(TIMESTAMP_TZ_MILLIS, createVarcharType(0)));
        testUnwrap(
                "timestamp(3) with time zone",
                format("at_timezone(a, '') = %s", LITERAL_MILLIS),
                comparison(
                        EQUAL,
                        new Call(atTimezoneEmptyZone, ImmutableList.of(A_MILLIS, new Constant(createVarcharType(0), utf8Slice("")))),
                        VALUE_MILLIS));
    }

    @Test
    public void testWithTimezoneIsNotUnwrapped()
    {
        // with_timezone reinterprets the wall time, changing the instant; it must never be unwrapped
        ResolvedFunction withTimezone = FUNCTIONS.resolveFunction("with_timezone", fromTypes(createTimestampType(3), createVarcharType(3)));
        testUnwrap(
                "timestamp(3)",
                format("with_timezone(a, 'UTC') = %s", LITERAL_MILLIS),
                comparison(
                        EQUAL,
                        new Call(withTimezone, ImmutableList.of(new Reference(createTimestampType(3), "a"), new Constant(createVarcharType(3), utf8Slice("UTC")))),
                        VALUE_MILLIS));
    }

    @Test
    public void testNonConstantZoneIsNotUnwrapped()
    {
        // a null zone makes the comparison null (row filtered); a bare comparison could wrongly match
        ResolvedFunction atTimezone = FUNCTIONS.resolveFunction("at_timezone", fromTypes(TIMESTAMP_TZ_MILLIS, createVarcharType(30)));
        assertPlan(
                format("SELECT * FROM (VALUES (CAST(NULL AS timestamp(3) with time zone), CAST(NULL AS varchar(30)))) t(a, z) WHERE at_timezone(a, z) = %s OR rand() = 42", LITERAL_MILLIS),
                getPlanTester().getDefaultSession(),
                output(
                        filter(
                                new Logical(OR, ImmutableList.of(
                                        comparison(
                                                EQUAL,
                                                new Call(atTimezone, ImmutableList.of(A_MILLIS, new Reference(createVarcharType(30), "z"))),
                                                VALUE_MILLIS),
                                        antiOptimization())),
                                values("a", "z"))));
    }

    private void testUnwrap(String inputType, String inputPredicate, Expression expected)
    {
        if (expected instanceof Logical logical && logical.operator() == OR) {
            expected = new Logical(OR, ImmutableList.<Expression>builder()
                    .addAll(logical.terms())
                    .add(antiOptimization())
                    .build());
        }
        else {
            expected = new Logical(OR, ImmutableList.of(expected, antiOptimization()));
        }

        String sql = format("SELECT * FROM (VALUES CAST(NULL AS %s)) t(a) WHERE %s OR rand() = 42", inputType, inputPredicate);
        try {
            assertPlan(
                    sql,
                    getPlanTester().getDefaultSession(),
                    output(
                            filter(
                                    expected,
                                    values("a"))));
        }
        catch (Throwable e) {
            e.addSuppressed(new Exception("Query: " + sql));
            throw e;
        }
    }

    private static Expression antiOptimization()
    {
        return comparison(EQUAL, new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 42.0));
    }
}
