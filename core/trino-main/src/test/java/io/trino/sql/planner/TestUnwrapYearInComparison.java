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
import io.trino.Session;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.LongTimestamp;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IrExpressions;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.type.DateTimes;
import io.trino.util.DateTimeUtils;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.PUSH_FILTER_INTO_VALUES_MAX_ROW_COUNT;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.ir.TestingIr.between;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.UnwrapYearInComparison.calculateRangeEndInclusive;
import static io.trino.sql.planner.iterative.rule.UnwrapYearInComparison.unwrapYear;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestUnwrapYearInComparison
        extends BasePlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", fromTypes());
    private static final ResolvedFunction FROM_UNIXTIME = FUNCTIONS.resolveFunction("from_unixtime", fromTypes(DOUBLE));
    private static final ResolvedFunction YEAR_DATE = FUNCTIONS.resolveFunction("year", fromTypes(DATE));
    private static final ResolvedFunction YEAR_TIMESTAMP_3 = FUNCTIONS.resolveFunction("year", fromTypes(createTimestampType(3)));

    @Test
    public void testEquals()
    {
        testUnwrap("date", "year(a) = -0001", between(new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("-0001-01-01"))), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("-0001-12-31")))));
        testUnwrap("date", "year(a) = 1960", between(new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("1960-01-01"))), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("1960-12-31")))));
        testUnwrap("date", "year(a) = 2022", between(new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("2022-01-01"))), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("2022-12-31")))));
        testUnwrap("date", "year(a) = 9999", between(new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("9999-01-01"))), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("9999-12-31")))));

        testUnwrap("timestamp", "year(a) = -0001", between(new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "-0001-01-01 00:00:00.000")), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "-0001-12-31 23:59:59.999"))));
        testUnwrap("timestamp", "year(a) = 1960", between(new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1960-01-01 00:00:00.000")), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1960-12-31 23:59:59.999"))));
        testUnwrap("timestamp", "year(a) = 2022", between(new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-01-01 00:00:00.000")), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-12-31 23:59:59.999"))));
        testUnwrap("timestamp", "year(a) = 9999", between(new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "9999-01-01 00:00:00.000")), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "9999-12-31 23:59:59.999"))));

        testUnwrap("timestamp(0)", "year(a) = 2022", between(new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2022-01-01 00:00:00")), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2022-12-31 23:59:59"))));
        testUnwrap("timestamp(1)", "year(a) = 2022", between(new Reference(createTimestampType(1), "a"), new Constant(createTimestampType(1), DateTimes.parseTimestamp(1, "2022-01-01 00:00:00.0")), new Constant(createTimestampType(1), DateTimes.parseTimestamp(1, "2022-12-31 23:59:59.9"))));
        testUnwrap("timestamp(2)", "year(a) = 2022", between(new Reference(createTimestampType(2), "a"), new Constant(createTimestampType(2), DateTimes.parseTimestamp(2, "2022-01-01 00:00:00.00")), new Constant(createTimestampType(2), DateTimes.parseTimestamp(2, "2022-12-31 23:59:59.99"))));
        testUnwrap("timestamp(3)", "year(a) = 2022", between(new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-01-01 00:00:00.000")), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-12-31 23:59:59.999"))));
        testUnwrap("timestamp(4)", "year(a) = 2022", between(new Reference(createTimestampType(4), "a"), new Constant(createTimestampType(4), DateTimes.parseTimestamp(4, "2022-01-01 00:00:00.0000")), new Constant(createTimestampType(4), DateTimes.parseTimestamp(4, "2022-12-31 23:59:59.9999"))));
        testUnwrap("timestamp(5)", "year(a) = 2022", between(new Reference(createTimestampType(5), "a"), new Constant(createTimestampType(5), DateTimes.parseTimestamp(5, "2022-01-01 00:00:00.00000")), new Constant(createTimestampType(5), DateTimes.parseTimestamp(5, "2022-12-31 23:59:59.99999"))));
        testUnwrap("timestamp(6)", "year(a) = 2022", between(new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2022-01-01 00:00:00.000000")), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2022-12-31 23:59:59.999999"))));
        testUnwrap("timestamp(7)", "year(a) = 2022", between(new Reference(createTimestampType(7), "a"), new Constant(createTimestampType(7), DateTimes.parseTimestamp(7, "2022-01-01 00:00:00.0000000")), new Constant(createTimestampType(7), DateTimes.parseTimestamp(7, "2022-12-31 23:59:59.9999999"))));
        testUnwrap("timestamp(8)", "year(a) = 2022", between(new Reference(createTimestampType(8), "a"), new Constant(createTimestampType(8), DateTimes.parseTimestamp(8, "2022-01-01 00:00:00.00000000")), new Constant(createTimestampType(8), DateTimes.parseTimestamp(8, "2022-12-31 23:59:59.99999999"))));
        testUnwrap("timestamp(9)", "year(a) = 2022", between(new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2022-01-01 00:00:00.000000000")), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2022-12-31 23:59:59.999999999"))));
        testUnwrap("timestamp(10)", "year(a) = 2022", between(new Reference(createTimestampType(10), "a"), new Constant(createTimestampType(10), DateTimes.parseTimestamp(10, "2022-01-01 00:00:00.0000000000")), new Constant(createTimestampType(10), DateTimes.parseTimestamp(10, "2022-12-31 23:59:59.9999999999"))));
        testUnwrap("timestamp(11)", "year(a) = 2022", between(new Reference(createTimestampType(11), "a"), new Constant(createTimestampType(11), DateTimes.parseTimestamp(11, "2022-01-01 00:00:00.00000000000")), new Constant(createTimestampType(11), DateTimes.parseTimestamp(11, "2022-12-31 23:59:59.99999999999"))));
        testUnwrap("timestamp(12)", "year(a) = 2022", between(new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "2022-01-01 00:00:00.000000000000")), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "2022-12-31 23:59:59.999999999999"))));
    }

    @Test
    public void testInPredicate()
    {
        testUnwrap("date", "year(a) IN (1000, 1400, 1800)", new Logical(OR, ImmutableList.of(between(new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("1000-01-01"))), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("1000-12-31")))), between(new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("1400-01-01"))), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("1400-12-31")))), between(new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("1800-01-01"))), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("1800-12-31")))))));
        testUnwrap("timestamp", "year(a) IN (1000, 1400, 1800)", new Logical(OR, ImmutableList.of(between(new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1000-01-01 00:00:00.000")), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1000-12-31 23:59:59.999"))), between(new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1400-01-01 00:00:00.000")), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1400-12-31 23:59:59.999"))), between(new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1800-01-01 00:00:00.000")), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1800-12-31 23:59:59.999"))))));
    }

    @Test
    public void testNotEquals()
    {
        testUnwrap("date", "year(a) <> -0001", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("-0001-01-01")))), comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("-0001-12-31")))))));
        testUnwrap("date", "year(a) <> 1960", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("1960-01-01")))), comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("1960-12-31")))))));
        testUnwrap("date", "year(a) <> 2022", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("2022-01-01")))), comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("2022-12-31")))))));
        testUnwrap("date", "year(a) <> 9999", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("9999-01-01")))), comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("9999-12-31")))))));

        testUnwrap("timestamp", "year(a) <> -0001", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "-0001-01-01 00:00:00.000"))), comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "-0001-12-31 23:59:59.999"))))));
        testUnwrap("timestamp", "year(a) <> 1960", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1960-01-01 00:00:00.000"))), comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1960-12-31 23:59:59.999"))))));
        testUnwrap("timestamp", "year(a) <> 2022", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-01-01 00:00:00.000"))), comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-12-31 23:59:59.999"))))));
        testUnwrap("timestamp", "year(a) <> 9999", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "9999-01-01 00:00:00.000"))), comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "9999-12-31 23:59:59.999"))))));

        testUnwrap("timestamp(0)", "year(a) <> 2022", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2022-01-01 00:00:00"))), comparison(GREATER_THAN, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2022-12-31 23:59:59"))))));
        testUnwrap("timestamp(1)", "year(a) <> 2022", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(createTimestampType(1), "a"), new Constant(createTimestampType(1), DateTimes.parseTimestamp(1, "2022-01-01 00:00:00.0"))), comparison(GREATER_THAN, new Reference(createTimestampType(1), "a"), new Constant(createTimestampType(1), DateTimes.parseTimestamp(1, "2022-12-31 23:59:59.9"))))));
        testUnwrap("timestamp(2)", "year(a) <> 2022", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(createTimestampType(2), "a"), new Constant(createTimestampType(2), DateTimes.parseTimestamp(2, "2022-01-01 00:00:00.00"))), comparison(GREATER_THAN, new Reference(createTimestampType(2), "a"), new Constant(createTimestampType(2), DateTimes.parseTimestamp(2, "2022-12-31 23:59:59.99"))))));
        testUnwrap("timestamp(3)", "year(a) <> 2022", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-01-01 00:00:00.000"))), comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-12-31 23:59:59.999"))))));
        testUnwrap("timestamp(4)", "year(a) <> 2022", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(createTimestampType(4), "a"), new Constant(createTimestampType(4), DateTimes.parseTimestamp(4, "2022-01-01 00:00:00.0000"))), comparison(GREATER_THAN, new Reference(createTimestampType(4), "a"), new Constant(createTimestampType(4), DateTimes.parseTimestamp(4, "2022-12-31 23:59:59.9999"))))));
        testUnwrap("timestamp(5)", "year(a) <> 2022", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(createTimestampType(5), "a"), new Constant(createTimestampType(5), DateTimes.parseTimestamp(5, "2022-01-01 00:00:00.00000"))), comparison(GREATER_THAN, new Reference(createTimestampType(5), "a"), new Constant(createTimestampType(5), DateTimes.parseTimestamp(5, "2022-12-31 23:59:59.99999"))))));
        testUnwrap("timestamp(6)", "year(a) <> 2022", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2022-01-01 00:00:00.000000"))), comparison(GREATER_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2022-12-31 23:59:59.999999"))))));
        testUnwrap("timestamp(7)", "year(a) <> 2022", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(createTimestampType(7), "a"), new Constant(createTimestampType(7), DateTimes.parseTimestamp(7, "2022-01-01 00:00:00.0000000"))), comparison(GREATER_THAN, new Reference(createTimestampType(7), "a"), new Constant(createTimestampType(7), DateTimes.parseTimestamp(7, "2022-12-31 23:59:59.9999999"))))));
        testUnwrap("timestamp(8)", "year(a) <> 2022", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(createTimestampType(8), "a"), new Constant(createTimestampType(8), DateTimes.parseTimestamp(8, "2022-01-01 00:00:00.00000000"))), comparison(GREATER_THAN, new Reference(createTimestampType(8), "a"), new Constant(createTimestampType(8), DateTimes.parseTimestamp(8, "2022-12-31 23:59:59.99999999"))))));
        testUnwrap("timestamp(9)", "year(a) <> 2022", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2022-01-01 00:00:00.000000000"))), comparison(GREATER_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2022-12-31 23:59:59.999999999"))))));
        testUnwrap("timestamp(10)", "year(a) <> 2022", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(createTimestampType(10), "a"), new Constant(createTimestampType(10), DateTimes.parseTimestamp(10, "2022-01-01 00:00:00.0000000000"))), comparison(GREATER_THAN, new Reference(createTimestampType(10), "a"), new Constant(createTimestampType(10), DateTimes.parseTimestamp(10, "2022-12-31 23:59:59.9999999999"))))));
        testUnwrap("timestamp(11)", "year(a) <> 2022", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(createTimestampType(11), "a"), new Constant(createTimestampType(11), DateTimes.parseTimestamp(11, "2022-01-01 00:00:00.00000000000"))), comparison(GREATER_THAN, new Reference(createTimestampType(11), "a"), new Constant(createTimestampType(11), DateTimes.parseTimestamp(11, "2022-12-31 23:59:59.99999999999"))))));
        testUnwrap("timestamp(12)", "year(a) <> 2022", new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "2022-01-01 00:00:00.000000000000"))), comparison(GREATER_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "2022-12-31 23:59:59.999999999999"))))));
    }

    @Test
    public void testLessThan()
    {
        testUnwrap("date", "year(a) < -0001", comparison(LESS_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("-0001-01-01")))));
        testUnwrap("date", "year(a) < 1960", comparison(LESS_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("1960-01-01")))));
        testUnwrap("date", "year(a) < 2022", comparison(LESS_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("2022-01-01")))));
        testUnwrap("date", "year(a) < 9999", comparison(LESS_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("9999-01-01")))));

        testUnwrap("timestamp", "year(a) < -0001", comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "-0001-01-01 00:00:00.000"))));
        testUnwrap("timestamp", "year(a) < 1960", comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1960-01-01 00:00:00.000"))));
        testUnwrap("timestamp", "year(a) < 2022", comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-01-01 00:00:00.000"))));
        testUnwrap("timestamp", "year(a) < 9999", comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "9999-01-01 00:00:00.000"))));

        testUnwrap("timestamp(0)", "year(a) < 2022", comparison(LESS_THAN, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2022-01-01 00:00:00"))));
        testUnwrap("timestamp(1)", "year(a) < 2022", comparison(LESS_THAN, new Reference(createTimestampType(1), "a"), new Constant(createTimestampType(1), DateTimes.parseTimestamp(1, "2022-01-01 00:00:00.0"))));
        testUnwrap("timestamp(2)", "year(a) < 2022", comparison(LESS_THAN, new Reference(createTimestampType(2), "a"), new Constant(createTimestampType(2), DateTimes.parseTimestamp(2, "2022-01-01 00:00:00.00"))));
        testUnwrap("timestamp(3)", "year(a) < 2022", comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-01-01 00:00:00.000"))));
        testUnwrap("timestamp(4)", "year(a) < 2022", comparison(LESS_THAN, new Reference(createTimestampType(4), "a"), new Constant(createTimestampType(4), DateTimes.parseTimestamp(4, "2022-01-01 00:00:00.0000"))));
        testUnwrap("timestamp(5)", "year(a) < 2022", comparison(LESS_THAN, new Reference(createTimestampType(5), "a"), new Constant(createTimestampType(5), DateTimes.parseTimestamp(5, "2022-01-01 00:00:00.00000"))));
        testUnwrap("timestamp(6)", "year(a) < 2022", comparison(LESS_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2022-01-01 00:00:00.000000"))));
        testUnwrap("timestamp(7)", "year(a) < 2022", comparison(LESS_THAN, new Reference(createTimestampType(7), "a"), new Constant(createTimestampType(7), DateTimes.parseTimestamp(7, "2022-01-01 00:00:00.0000000"))));
        testUnwrap("timestamp(8)", "year(a) < 2022", comparison(LESS_THAN, new Reference(createTimestampType(8), "a"), new Constant(createTimestampType(8), DateTimes.parseTimestamp(8, "2022-01-01 00:00:00.00000000"))));
        testUnwrap("timestamp(9)", "year(a) < 2022", comparison(LESS_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2022-01-01 00:00:00.000000000"))));
        testUnwrap("timestamp(10)", "year(a) < 2022", comparison(LESS_THAN, new Reference(createTimestampType(10), "a"), new Constant(createTimestampType(10), DateTimes.parseTimestamp(10, "2022-01-01 00:00:00.0000000000"))));
        testUnwrap("timestamp(11)", "year(a) < 2022", comparison(LESS_THAN, new Reference(createTimestampType(11), "a"), new Constant(createTimestampType(11), DateTimes.parseTimestamp(11, "2022-01-01 00:00:00.00000000000"))));
        testUnwrap("timestamp(12)", "year(a) < 2022", comparison(LESS_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "2022-01-01 00:00:00.000000000000"))));
    }

    @Test
    public void testLessThanOrEqual()
    {
        testUnwrap("date", "year(a) <= -0001", comparison(LESS_THAN_OR_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("-0001-12-31")))));
        testUnwrap("date", "year(a) <= 1960", comparison(LESS_THAN_OR_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("1960-12-31")))));
        testUnwrap("date", "year(a) <= 2022", comparison(LESS_THAN_OR_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("2022-12-31")))));
        testUnwrap("date", "year(a) <= 9999", comparison(LESS_THAN_OR_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("9999-12-31")))));

        testUnwrap("timestamp", "year(a) <= -0001", comparison(LESS_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "-0001-12-31 23:59:59.999"))));
        testUnwrap("timestamp", "year(a) <= 1960", comparison(LESS_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1960-12-31 23:59:59.999"))));
        testUnwrap("timestamp", "year(a) <= 2022", comparison(LESS_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-12-31 23:59:59.999"))));
        testUnwrap("timestamp", "year(a) <= 9999", comparison(LESS_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "9999-12-31 23:59:59.999"))));

        testUnwrap("timestamp(0)", "year(a) <= 2022", comparison(LESS_THAN_OR_EQUAL, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2022-12-31 23:59:59"))));
        testUnwrap("timestamp(1)", "year(a) <= 2022", comparison(LESS_THAN_OR_EQUAL, new Reference(createTimestampType(1), "a"), new Constant(createTimestampType(1), DateTimes.parseTimestamp(1, "2022-12-31 23:59:59.9"))));
        testUnwrap("timestamp(2)", "year(a) <= 2022", comparison(LESS_THAN_OR_EQUAL, new Reference(createTimestampType(2), "a"), new Constant(createTimestampType(2), DateTimes.parseTimestamp(2, "2022-12-31 23:59:59.99"))));
        testUnwrap("timestamp(3)", "year(a) <= 2022", comparison(LESS_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-12-31 23:59:59.999"))));
        testUnwrap("timestamp(4)", "year(a) <= 2022", comparison(LESS_THAN_OR_EQUAL, new Reference(createTimestampType(4), "a"), new Constant(createTimestampType(4), DateTimes.parseTimestamp(4, "2022-12-31 23:59:59.9999"))));
        testUnwrap("timestamp(5)", "year(a) <= 2022", comparison(LESS_THAN_OR_EQUAL, new Reference(createTimestampType(5), "a"), new Constant(createTimestampType(5), DateTimes.parseTimestamp(5, "2022-12-31 23:59:59.99999"))));
        testUnwrap("timestamp(6)", "year(a) <= 2022", comparison(LESS_THAN_OR_EQUAL, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2022-12-31 23:59:59.999999"))));
        testUnwrap("timestamp(7)", "year(a) <= 2022", comparison(LESS_THAN_OR_EQUAL, new Reference(createTimestampType(7), "a"), new Constant(createTimestampType(7), DateTimes.parseTimestamp(7, "2022-12-31 23:59:59.9999999"))));
        testUnwrap("timestamp(8)", "year(a) <= 2022", comparison(LESS_THAN_OR_EQUAL, new Reference(createTimestampType(8), "a"), new Constant(createTimestampType(8), DateTimes.parseTimestamp(8, "2022-12-31 23:59:59.99999999"))));
        testUnwrap("timestamp(9)", "year(a) <= 2022", comparison(LESS_THAN_OR_EQUAL, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2022-12-31 23:59:59.999999999"))));
        testUnwrap("timestamp(10)", "year(a) <= 2022", comparison(LESS_THAN_OR_EQUAL, new Reference(createTimestampType(10), "a"), new Constant(createTimestampType(10), DateTimes.parseTimestamp(10, "2022-12-31 23:59:59.9999999999"))));
        testUnwrap("timestamp(11)", "year(a) <= 2022", comparison(LESS_THAN_OR_EQUAL, new Reference(createTimestampType(11), "a"), new Constant(createTimestampType(11), DateTimes.parseTimestamp(11, "2022-12-31 23:59:59.99999999999"))));
        testUnwrap("timestamp(12)", "year(a) <= 2022", comparison(LESS_THAN_OR_EQUAL, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "2022-12-31 23:59:59.999999999999"))));
    }

    @Test
    public void testGreaterThan()
    {
        testUnwrap("date", "year(a) > -0001", comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("-0001-12-31")))));
        testUnwrap("date", "year(a) > 1960", comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("1960-12-31")))));
        testUnwrap("date", "year(a) > 2022", comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("2022-12-31")))));
        testUnwrap("date", "year(a) > 9999", comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("9999-12-31")))));

        testUnwrap("timestamp", "year(a) > -0001", comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "-0001-12-31 23:59:59.999"))));
        testUnwrap("timestamp", "year(a) > 1960", comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1960-12-31 23:59:59.999"))));
        testUnwrap("timestamp", "year(a) > 2022", comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-12-31 23:59:59.999"))));
        testUnwrap("timestamp", "year(a) > 9999", comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "9999-12-31 23:59:59.999"))));

        testUnwrap("timestamp(0)", "year(a) > 2022", comparison(GREATER_THAN, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2022-12-31 23:59:59"))));
        testUnwrap("timestamp(1)", "year(a) > 2022", comparison(GREATER_THAN, new Reference(createTimestampType(1), "a"), new Constant(createTimestampType(1), DateTimes.parseTimestamp(1, "2022-12-31 23:59:59.9"))));
        testUnwrap("timestamp(2)", "year(a) > 2022", comparison(GREATER_THAN, new Reference(createTimestampType(2), "a"), new Constant(createTimestampType(2), DateTimes.parseTimestamp(2, "2022-12-31 23:59:59.99"))));
        testUnwrap("timestamp(3)", "year(a) > 2022", comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-12-31 23:59:59.999"))));
        testUnwrap("timestamp(4)", "year(a) > 2022", comparison(GREATER_THAN, new Reference(createTimestampType(4), "a"), new Constant(createTimestampType(4), DateTimes.parseTimestamp(4, "2022-12-31 23:59:59.9999"))));
        testUnwrap("timestamp(5)", "year(a) > 2022", comparison(GREATER_THAN, new Reference(createTimestampType(5), "a"), new Constant(createTimestampType(5), DateTimes.parseTimestamp(5, "2022-12-31 23:59:59.99999"))));
        testUnwrap("timestamp(6)", "year(a) > 2022", comparison(GREATER_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2022-12-31 23:59:59.999999"))));
        testUnwrap("timestamp(7)", "year(a) > 2022", comparison(GREATER_THAN, new Reference(createTimestampType(7), "a"), new Constant(createTimestampType(7), DateTimes.parseTimestamp(7, "2022-12-31 23:59:59.9999999"))));
        testUnwrap("timestamp(8)", "year(a) > 2022", comparison(GREATER_THAN, new Reference(createTimestampType(8), "a"), new Constant(createTimestampType(8), DateTimes.parseTimestamp(8, "2022-12-31 23:59:59.99999999"))));
        testUnwrap("timestamp(9)", "year(a) > 2022", comparison(GREATER_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2022-12-31 23:59:59.999999999"))));
        testUnwrap("timestamp(10)", "year(a) > 2022", comparison(GREATER_THAN, new Reference(createTimestampType(10), "a"), new Constant(createTimestampType(10), DateTimes.parseTimestamp(10, "2022-12-31 23:59:59.9999999999"))));
        testUnwrap("timestamp(11)", "year(a) > 2022", comparison(GREATER_THAN, new Reference(createTimestampType(11), "a"), new Constant(createTimestampType(11), DateTimes.parseTimestamp(11, "2022-12-31 23:59:59.99999999999"))));
        testUnwrap("timestamp(12)", "year(a) > 2022", comparison(GREATER_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "2022-12-31 23:59:59.999999999999"))));
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        testUnwrap("date", "year(a) >= -0001", comparison(GREATER_THAN_OR_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("-0001-01-01")))));
        testUnwrap("date", "year(a) >= 1960", comparison(GREATER_THAN_OR_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("1960-01-01")))));
        testUnwrap("date", "year(a) >= 2022", comparison(GREATER_THAN_OR_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("2022-01-01")))));
        testUnwrap("date", "year(a) >= 9999", comparison(GREATER_THAN_OR_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("9999-01-01")))));

        testUnwrap("timestamp", "year(a) >= -0001", comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "-0001-01-01 00:00:00.000"))));
        testUnwrap("timestamp", "year(a) >= 1960", comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1960-01-01 00:00:00.000"))));
        testUnwrap("timestamp", "year(a) >= 2022", comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-01-01 00:00:00.000"))));
        testUnwrap("timestamp", "year(a) >= 9999", comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "9999-01-01 00:00:00.000"))));

        testUnwrap("timestamp(0)", "year(a) >= 2022", comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2022-01-01 00:00:00"))));
        testUnwrap("timestamp(1)", "year(a) >= 2022", comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(1), "a"), new Constant(createTimestampType(1), DateTimes.parseTimestamp(1, "2022-01-01 00:00:00.0"))));
        testUnwrap("timestamp(2)", "year(a) >= 2022", comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(2), "a"), new Constant(createTimestampType(2), DateTimes.parseTimestamp(2, "2022-01-01 00:00:00.00"))));
        testUnwrap("timestamp(3)", "year(a) >= 2022", comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-01-01 00:00:00.000"))));
        testUnwrap("timestamp(4)", "year(a) >= 2022", comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(4), "a"), new Constant(createTimestampType(4), DateTimes.parseTimestamp(4, "2022-01-01 00:00:00.0000"))));
        testUnwrap("timestamp(5)", "year(a) >= 2022", comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(5), "a"), new Constant(createTimestampType(5), DateTimes.parseTimestamp(5, "2022-01-01 00:00:00.00000"))));
        testUnwrap("timestamp(6)", "year(a) >= 2022", comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2022-01-01 00:00:00.000000"))));
        testUnwrap("timestamp(7)", "year(a) >= 2022", comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(7), "a"), new Constant(createTimestampType(7), DateTimes.parseTimestamp(7, "2022-01-01 00:00:00.0000000"))));
        testUnwrap("timestamp(8)", "year(a) >= 2022", comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(8), "a"), new Constant(createTimestampType(8), DateTimes.parseTimestamp(8, "2022-01-01 00:00:00.00000000"))));
        testUnwrap("timestamp(9)", "year(a) >= 2022", comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2022-01-01 00:00:00.000000000"))));
        testUnwrap("timestamp(10)", "year(a) >= 2022", comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(10), "a"), new Constant(createTimestampType(10), DateTimes.parseTimestamp(10, "2022-01-01 00:00:00.0000000000"))));
        testUnwrap("timestamp(11)", "year(a) >= 2022", comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(11), "a"), new Constant(createTimestampType(11), DateTimes.parseTimestamp(11, "2022-01-01 00:00:00.00000000000"))));
        testUnwrap("timestamp(12)", "year(a) >= 2022", comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "2022-01-01 00:00:00.000000000000"))));
    }

    @Test
    public void testDistinctFrom()
    {
        testUnwrap("date", "year(a) IS DISTINCT FROM -0001", new Logical(OR, ImmutableList.of(new IsNull(new Reference(DATE, "a")), comparison(LESS_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("-0001-01-01")))), comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("-0001-12-31")))))));
        testUnwrap("date", "year(a) IS DISTINCT FROM 1960", new Logical(OR, ImmutableList.of(new IsNull(new Reference(DATE, "a")), comparison(LESS_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("1960-01-01")))), comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("1960-12-31")))))));
        testUnwrap("date", "year(a) IS DISTINCT FROM 2022", new Logical(OR, ImmutableList.of(new IsNull(new Reference(DATE, "a")), comparison(LESS_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("2022-01-01")))), comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("2022-12-31")))))));
        testUnwrap("date", "year(a) IS DISTINCT FROM 9999", new Logical(OR, ImmutableList.of(new IsNull(new Reference(DATE, "a")), comparison(LESS_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("9999-01-01")))), comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("9999-12-31")))))));

        testUnwrap("timestamp", "year(a) IS DISTINCT FROM -0001", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(3), "a")), comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "-0001-01-01 00:00:00.000"))), comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "-0001-12-31 23:59:59.999"))))));
        testUnwrap("timestamp", "year(a) IS DISTINCT FROM 1960", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(3), "a")), comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1960-01-01 00:00:00.000"))), comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1960-12-31 23:59:59.999"))))));
        testUnwrap("timestamp", "year(a) IS DISTINCT FROM 2022", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(3), "a")), comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-01-01 00:00:00.000"))), comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-12-31 23:59:59.999"))))));
        testUnwrap("timestamp", "year(a) IS DISTINCT FROM 9999", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(3), "a")), comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "9999-01-01 00:00:00.000"))), comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "9999-12-31 23:59:59.999"))))));

        testUnwrap("timestamp(0)", "year(a) IS DISTINCT FROM 2022", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(0), "a")), comparison(LESS_THAN, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2022-01-01 00:00:00"))), comparison(GREATER_THAN, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2022-12-31 23:59:59"))))));
        testUnwrap("timestamp(1)", "year(a) IS DISTINCT FROM 2022", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(1), "a")), comparison(LESS_THAN, new Reference(createTimestampType(1), "a"), new Constant(createTimestampType(1), DateTimes.parseTimestamp(1, "2022-01-01 00:00:00.0"))), comparison(GREATER_THAN, new Reference(createTimestampType(1), "a"), new Constant(createTimestampType(1), DateTimes.parseTimestamp(1, "2022-12-31 23:59:59.9"))))));
        testUnwrap("timestamp(2)", "year(a) IS DISTINCT FROM 2022", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(2), "a")), comparison(LESS_THAN, new Reference(createTimestampType(2), "a"), new Constant(createTimestampType(2), DateTimes.parseTimestamp(2, "2022-01-01 00:00:00.00"))), comparison(GREATER_THAN, new Reference(createTimestampType(2), "a"), new Constant(createTimestampType(2), DateTimes.parseTimestamp(2, "2022-12-31 23:59:59.99"))))));
        testUnwrap("timestamp(3)", "year(a) IS DISTINCT FROM 2022", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(3), "a")), comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-01-01 00:00:00.000"))), comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2022-12-31 23:59:59.999"))))));
        testUnwrap("timestamp(4)", "year(a) IS DISTINCT FROM 2022", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(4), "a")), comparison(LESS_THAN, new Reference(createTimestampType(4), "a"), new Constant(createTimestampType(4), DateTimes.parseTimestamp(4, "2022-01-01 00:00:00.0000"))), comparison(GREATER_THAN, new Reference(createTimestampType(4), "a"), new Constant(createTimestampType(4), DateTimes.parseTimestamp(4, "2022-12-31 23:59:59.9999"))))));
        testUnwrap("timestamp(5)", "year(a) IS DISTINCT FROM 2022", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(5), "a")), comparison(LESS_THAN, new Reference(createTimestampType(5), "a"), new Constant(createTimestampType(5), DateTimes.parseTimestamp(5, "2022-01-01 00:00:00.00000"))), comparison(GREATER_THAN, new Reference(createTimestampType(5), "a"), new Constant(createTimestampType(5), DateTimes.parseTimestamp(5, "2022-12-31 23:59:59.99999"))))));
        testUnwrap("timestamp(6)", "year(a) IS DISTINCT FROM 2022", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(6), "a")), comparison(LESS_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2022-01-01 00:00:00.000000"))), comparison(GREATER_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2022-12-31 23:59:59.999999"))))));
        testUnwrap("timestamp(7)", "year(a) IS DISTINCT FROM 2022", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(7), "a")), comparison(LESS_THAN, new Reference(createTimestampType(7), "a"), new Constant(createTimestampType(7), DateTimes.parseTimestamp(7, "2022-01-01 00:00:00.0000000"))), comparison(GREATER_THAN, new Reference(createTimestampType(7), "a"), new Constant(createTimestampType(7), DateTimes.parseTimestamp(7, "2022-12-31 23:59:59.9999999"))))));
        testUnwrap("timestamp(8)", "year(a) IS DISTINCT FROM 2022", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(8), "a")), comparison(LESS_THAN, new Reference(createTimestampType(8), "a"), new Constant(createTimestampType(8), DateTimes.parseTimestamp(8, "2022-01-01 00:00:00.00000000"))), comparison(GREATER_THAN, new Reference(createTimestampType(8), "a"), new Constant(createTimestampType(8), DateTimes.parseTimestamp(8, "2022-12-31 23:59:59.99999999"))))));
        testUnwrap("timestamp(9)", "year(a) IS DISTINCT FROM 2022", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(9), "a")), comparison(LESS_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2022-01-01 00:00:00.000000000"))), comparison(GREATER_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2022-12-31 23:59:59.999999999"))))));
        testUnwrap("timestamp(10)", "year(a) IS DISTINCT FROM 2022", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(10), "a")), comparison(LESS_THAN, new Reference(createTimestampType(10), "a"), new Constant(createTimestampType(10), DateTimes.parseTimestamp(10, "2022-01-01 00:00:00.0000000000"))), comparison(GREATER_THAN, new Reference(createTimestampType(10), "a"), new Constant(createTimestampType(10), DateTimes.parseTimestamp(10, "2022-12-31 23:59:59.9999999999"))))));
        testUnwrap("timestamp(11)", "year(a) IS DISTINCT FROM 2022", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(11), "a")), comparison(LESS_THAN, new Reference(createTimestampType(11), "a"), new Constant(createTimestampType(11), DateTimes.parseTimestamp(11, "2022-01-01 00:00:00.00000000000"))), comparison(GREATER_THAN, new Reference(createTimestampType(11), "a"), new Constant(createTimestampType(11), DateTimes.parseTimestamp(11, "2022-12-31 23:59:59.99999999999"))))));
        testUnwrap("timestamp(12)", "year(a) IS DISTINCT FROM 2022", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(12), "a")), comparison(LESS_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "2022-01-01 00:00:00.000000000000"))), comparison(GREATER_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "2022-12-31 23:59:59.999999999999"))))));
    }

    @Test
    public void testNull()
    {
        testUnwrap("date", "year(a) = CAST(NULL AS BIGINT)", new Constant(BOOLEAN, null));
        testUnwrap("timestamp", "year(a) = CAST(NULL AS BIGINT)", new Constant(BOOLEAN, null));
    }

    @Test
    public void testNaN()
    {
        testUnwrap("date", "year(a) = nan()", new Logical(AND, ImmutableList.of(new IsNull(new Call(YEAR_DATE, ImmutableList.of(new Reference(DATE, "a")))), new Constant(BOOLEAN, null))));
        testUnwrap("timestamp", "year(a) = nan()", new Logical(AND, ImmutableList.of(new IsNull(new Call(YEAR_TIMESTAMP_3, ImmutableList.of(new Reference(createTimestampType(3), "a")))), new Constant(BOOLEAN, null))));
    }

    @Test
    public void smokeTests()
    {
        // smoke tests for various type combinations
        for (String type : asList("SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE")) {
            testUnwrap("date", format("year(a) = %s '2022'", type), between(new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("2022-01-01"))), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("2022-12-31")))));
        }
    }

    @Test
    public void testTermOrder()
    {
        // ensure the optimization works when the terms of the comparison are reversed
        // vs the canonical <expr> <op> <literal> form
        assertPlan(
                "SELECT * FROM (VALUES DATE '2022-01-01') t(a) WHERE 2022 = year(a)",
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(PUSH_FILTER_INTO_VALUES_MAX_ROW_COUNT, "0")
                        .build(),
                output(
                        filter(
                                between(new Reference(DATE, "A"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("2022-01-01"))), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("2022-12-31")))),
                                values("A"))));
    }

    @Test
    public void testLeapYear()
    {
        testUnwrap("date", "year(a) = 2024", between(new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("2024-01-01"))), new Constant(DATE, (long) DateTimeUtils.parseDate(utf8Slice("2024-12-31")))));
        testUnwrap("timestamp", "year(a) = 2024", between(new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2024-01-01 00:00:00.000")), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2024-12-31 23:59:59.999"))));
    }

    @Test
    public void testCalculateRangeEndInclusive()
    {
        assertThat(calculateRangeEndInclusive(1960, DATE)).isEqualTo(LocalDate.of(1960, 12, 31).toEpochDay());
        assertThat(calculateRangeEndInclusive(2024, DATE)).isEqualTo(LocalDate.of(2024, 12, 31).toEpochDay());

        assertThat(calculateRangeEndInclusive(1960, TIMESTAMP_SECONDS)).isEqualTo(toEpochMicros(LocalDateTime.of(1960, 12, 31, 23, 59, 59)));
        assertThat(calculateRangeEndInclusive(1960, TIMESTAMP_MILLIS)).isEqualTo(toEpochMicros(LocalDateTime.of(1960, 12, 31, 23, 59, 59, 999_000_000)));
        assertThat(calculateRangeEndInclusive(1960, TIMESTAMP_MICROS)).isEqualTo(toEpochMicros(LocalDateTime.of(1960, 12, 31, 23, 59, 59, 999_999_000)));
        assertThat(calculateRangeEndInclusive(1960, TIMESTAMP_NANOS)).isEqualTo(new LongTimestamp(toEpochMicros(LocalDateTime.of(1960, 12, 31, 23, 59, 59, 999_999_000)), 999_000));
        assertThat(calculateRangeEndInclusive(1960, TIMESTAMP_PICOS)).isEqualTo(new LongTimestamp(toEpochMicros(LocalDateTime.of(1960, 12, 31, 23, 59, 59, 999_999_000)), 999_999));
        assertThat(calculateRangeEndInclusive(2024, TIMESTAMP_SECONDS)).isEqualTo(toEpochMicros(LocalDateTime.of(2024, 12, 31, 23, 59, 59)));
        assertThat(calculateRangeEndInclusive(2024, TIMESTAMP_MILLIS)).isEqualTo(toEpochMicros(LocalDateTime.of(2024, 12, 31, 23, 59, 59, 999_000_000)));
        assertThat(calculateRangeEndInclusive(2024, TIMESTAMP_MICROS)).isEqualTo(toEpochMicros(LocalDateTime.of(2024, 12, 31, 23, 59, 59, 999_999_000)));
        assertThat(calculateRangeEndInclusive(2024, TIMESTAMP_NANOS)).isEqualTo(new LongTimestamp(toEpochMicros(LocalDateTime.of(2024, 12, 31, 23, 59, 59, 999_999_000)), 999_000));
        assertThat(calculateRangeEndInclusive(2024, TIMESTAMP_PICOS)).isEqualTo(new LongTimestamp(toEpochMicros(LocalDateTime.of(2024, 12, 31, 23, 59, 59, 999_999_000)), 999_999));
    }

    @Test
    public void testUnwrapYearBindsNonDeterministicOperandOnce()
    {
        Reference operand = new Reference(TIMESTAMP_MILLIS, "operand");

        assertThat(unwrap(comparison(IDENTICAL, yearTimestamp(randomTimestamp()), new Constant(BIGINT, 2021L))))
                .isEqualTo(new Let(
                        new Symbol(TIMESTAMP_MILLIS, "operand"),
                        randomTimestamp(),
                        new Logical(AND, ImmutableList.of(
                                not(new IsNull(operand)),
                                between(operand, timestampConstant("2021-01-01 00:00:00.000"), timestampConstant("2021-12-31 23:59:59.999"))))));
    }

    @Test
    public void testUnwrapYearInBindsNonDeterministicOperandOnce()
    {
        Reference operand = new Reference(TIMESTAMP_MILLIS, "operand");

        assertThat(unwrap(new In(yearTimestamp(randomTimestamp()), ImmutableList.of(new Constant(BIGINT, 2019L), new Constant(BIGINT, 2021L)))))
                .isEqualTo(new Let(
                        new Symbol(TIMESTAMP_MILLIS, "operand"),
                        randomTimestamp(),
                        new Logical(OR, ImmutableList.of(
                                letBetween("between", operand, "2019-01-01 00:00:00.000", "2019-12-31 23:59:59.999"),
                                letBetween("between_0", operand, "2021-01-01 00:00:00.000", "2021-12-31 23:59:59.999")))));
    }

    @Test
    public void testUnwrapYearInKeepsSingleValueUnbound()
    {
        assertThat(unwrap(new In(yearTimestamp(randomTimestamp()), ImmutableList.of(new Constant(BIGINT, 2021L)))))
                .isEqualTo(letBetween("between", randomTimestamp(), "2021-01-01 00:00:00.000", "2021-12-31 23:59:59.999"));
    }

    @Test
    public void testUnwrapYearInKeepsUnreferencedOperandUnbound()
    {
        assertThat(unwrap(new In(yearTimestamp(randomTimestamp()), ImmutableList.of(new Constant(BIGINT, null)))))
                .isEqualTo(new Constant(BOOLEAN, null));
    }

    @Test
    public void testUnwrapYearInKeepsSingleOccurrenceUnbound()
    {
        assertThat(unwrap(new In(yearTimestamp(randomTimestamp()), ImmutableList.of(new Constant(BIGINT, 2019L), new Constant(BIGINT, null)))))
                .isEqualTo(new Logical(OR, ImmutableList.of(
                        letBetween("between", randomTimestamp(), "2019-01-01 00:00:00.000", "2019-12-31 23:59:59.999"),
                        new Constant(BOOLEAN, null))));
    }

    @Test
    public void testUnwrapYearKeepsTrivialOperandInline()
    {
        Reference operand = new Reference(TIMESTAMP_MILLIS, "a");

        assertThat(unwrap(comparison(IDENTICAL, yearTimestamp(operand), new Constant(BIGINT, 2021L))))
                .isEqualTo(new Logical(AND, ImmutableList.of(
                        not(new IsNull(operand)),
                        between(operand, timestampConstant("2021-01-01 00:00:00.000"), timestampConstant("2021-12-31 23:59:59.999")))));
    }

    @Test
    public void testUnwrapYearBindsCastOperandOnce()
    {
        Expression cast = new Cast(new Reference(DATE, "a"), TIMESTAMP_MILLIS);
        Reference operand = new Reference(TIMESTAMP_MILLIS, "operand");

        assertThat(unwrap(comparison(IDENTICAL, yearTimestamp(cast), new Constant(BIGINT, 2021L))))
                .isEqualTo(new Let(
                        new Symbol(TIMESTAMP_MILLIS, "operand"),
                        cast,
                        new Logical(AND, ImmutableList.of(
                                not(new IsNull(operand)),
                                between(operand, timestampConstant("2021-01-01 00:00:00.000"), timestampConstant("2021-12-31 23:59:59.999"))))));
    }

    @Test
    public void testUnwrapYearInBindsCastOperandOnce()
    {
        Expression cast = new Cast(new Reference(DATE, "a"), TIMESTAMP_MILLIS);
        Reference operand = new Reference(TIMESTAMP_MILLIS, "operand");

        assertThat(unwrap(new In(yearTimestamp(cast), ImmutableList.of(new Constant(BIGINT, 2019L), new Constant(BIGINT, 2021L)))))
                .isEqualTo(new Let(
                        new Symbol(TIMESTAMP_MILLIS, "operand"),
                        cast,
                        new Logical(OR, ImmutableList.of(
                                letBetween("between", operand, "2019-01-01 00:00:00.000", "2019-12-31 23:59:59.999"),
                                letBetween("between_0", operand, "2021-01-01 00:00:00.000", "2021-12-31 23:59:59.999")))));
    }

    private static long toEpochMicros(LocalDateTime localDateTime)
    {
        return multiplyExact(localDateTime.toEpochSecond(UTC), MICROSECONDS_PER_SECOND)
                + localDateTime.getNano() / NANOSECONDS_PER_MICROSECOND;
    }

    private void testUnwrap(String inputType, String inputPredicate, Expression expected)
    {
        Expression antiOptimization = comparison(EQUAL, new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 42.0));
        if (expected instanceof Logical logical && logical.operator() == OR) {
            expected = new Logical(OR, ImmutableList.<Expression>builder()
                    .addAll(logical.terms())
                    .add(antiOptimization)
                    .build());
        }
        else {
            expected = new Logical(OR, ImmutableList.of(expected, antiOptimization));
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

    private static Expression unwrap(Expression expression)
    {
        return unwrapYear(TEST_SESSION, FUNCTIONS.getPlannerContext(), emptySymbolAllocator(), expression);
    }

    private static Expression letBetween(String name, Expression value, String low, String high)
    {
        Reference bound = new Reference(value.type(), name);
        return new Let(new Symbol(value.type(), name), value, between(bound, timestampConstant(low), timestampConstant(high)));
    }

    private static Expression yearTimestamp(Expression operand)
    {
        return new Call(YEAR_TIMESTAMP_3, ImmutableList.of(operand));
    }

    private static Expression randomTimestamp()
    {
        return new Cast(new Call(FROM_UNIXTIME, ImmutableList.of(new Call(RANDOM, ImmutableList.of()))), TIMESTAMP_MILLIS);
    }

    private static Constant timestampConstant(String timestamp)
    {
        return new Constant(TIMESTAMP_MILLIS, DateTimes.parseTimestamp(3, timestamp));
    }

    private static Expression not(Expression value)
    {
        return IrExpressions.not(FUNCTIONS.getMetadata(), getCharVarcharCoercion(TEST_SESSION), value);
    }
}
