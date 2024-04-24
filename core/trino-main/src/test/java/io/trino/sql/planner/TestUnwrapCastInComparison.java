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
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.type.DateTimes;
import io.trino.util.DateTimeUtils;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.NOT_EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.type.Reals.toReal;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class TestUnwrapCastInComparison
        extends BasePlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", fromTypes());

    @Test
    public void testEquals()
    {
        // representable
        testUnwrap("smallint", "a = DOUBLE '1'", new Comparison(EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 1L)));

        testUnwrap("bigint", "a = DOUBLE '1'", new Comparison(EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)));

        // non-representable
        testUnwrap("smallint", "a = DOUBLE '1.1'", new Logical(AND, ImmutableList.of(new IsNull(new Reference(SMALLINT, "a")), new Constant(BOOLEAN, null))));
        testUnwrap("smallint", "a = DOUBLE '1.9'", new Logical(AND, ImmutableList.of(new IsNull(new Reference(SMALLINT, "a")), new Constant(BOOLEAN, null))));

        testUnwrap("bigint", "a = DOUBLE '1.1'", new Logical(AND, ImmutableList.of(new IsNull(new Reference(BIGINT, "a")), new Constant(BOOLEAN, null))));

        // below top of range
        testUnwrap("smallint", "a = DOUBLE '32766'", new Comparison(EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 32766L)));

        // round to top of range
        testUnwrap("smallint", "a = DOUBLE '32766.9'", new Logical(AND, ImmutableList.of(new IsNull(new Reference(SMALLINT, "a")), new Constant(BOOLEAN, null))));

        // top of range
        testUnwrap("smallint", "a = DOUBLE '32767'", new Comparison(EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 32767L)));

        // above range
        testUnwrap("smallint", "a = DOUBLE '32768.1'", new Logical(AND, ImmutableList.of(new IsNull(new Reference(SMALLINT, "a")), new Constant(BOOLEAN, null))));

        // above bottom of range
        testUnwrap("smallint", "a = DOUBLE '-32767'", new Comparison(EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, -32767L)));

        // round to bottom of range
        testUnwrap("smallint", "a = DOUBLE '-32767.9'", new Logical(AND, ImmutableList.of(new IsNull(new Reference(SMALLINT, "a")), new Constant(BOOLEAN, null))));

        // bottom of range
        testUnwrap("smallint", "a = DOUBLE '-32768'", new Comparison(EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, -32768L)));

        // below range
        testUnwrap("smallint", "a = DOUBLE '-32768.1'", new Logical(AND, ImmutableList.of(new IsNull(new Reference(SMALLINT, "a")), new Constant(BOOLEAN, null))));

        // -2^64 constant
        testUnwrap("bigint", "a = DOUBLE '-18446744073709551616'", new Logical(AND, ImmutableList.of(new IsNull(new Reference(BIGINT, "a")), new Constant(BOOLEAN, null))));

        // varchar and char, same length
        testUnwrap("varchar(3)", "a = CAST('abc' AS char(3))", new Comparison(EQUAL, new Reference(createVarcharType(3), "a"), new Constant(createVarcharType(3), Slices.utf8Slice("abc"))));
        // longer varchar and char
        // actually unwrapping didn't happen
        testUnwrap("varchar(10)", "a = CAST('abc' AS char(3))", new Comparison(EQUAL, new Cast(new Reference(createVarcharType(10), "a"), createCharType(10)), new Constant(createCharType(10), Slices.utf8Slice("abc"))));
        // unbounded varchar and char
        // actually unwrapping didn't happen
        testUnwrap("varchar", "a = CAST('abc' AS char(3))", new Comparison(EQUAL, new Cast(new Reference(VARCHAR, "a"), createCharType(65536)), new Constant(createCharType(65536), Slices.utf8Slice("abc"))));
    }

    @Test
    public void testNotEquals()
    {
        // representable
        testUnwrap("smallint", "a <> DOUBLE '1'", new Comparison(NOT_EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 1L)));

        testUnwrap("bigint", "a <> DOUBLE '1'", new Comparison(NOT_EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)));

        // non-representable
        testUnwrap("smallint", "a <> DOUBLE '1.1'", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(SMALLINT, "a"))), new Constant(BOOLEAN, null))));
        testUnwrap("smallint", "a <> DOUBLE '1.9'", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(SMALLINT, "a"))), new Constant(BOOLEAN, null))));

        testUnwrap("smallint", "a <> DOUBLE '1.9'", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(SMALLINT, "a"))), new Constant(BOOLEAN, null))));

        testUnwrap("bigint", "a <> DOUBLE '1.1'", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(BIGINT, "a"))), new Constant(BOOLEAN, null))));

        // below top of range
        testUnwrap("smallint", "a <> DOUBLE '32766'", new Comparison(NOT_EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 32766L)));

        // round to top of range
        testUnwrap("smallint", "a <> DOUBLE '32766.9'", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(SMALLINT, "a"))), new Constant(BOOLEAN, null))));

        // top of range
        testUnwrap("smallint", "a <> DOUBLE '32767'", new Comparison(NOT_EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 32767L)));

        // above range
        testUnwrap("smallint", "a <> DOUBLE '32768.1'", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(SMALLINT, "a"))), new Constant(BOOLEAN, null))));

        // 2^64 constant
        testUnwrap("bigint", "a <> DOUBLE '18446744073709551616'", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(BIGINT, "a"))), new Constant(BOOLEAN, null))));

        // above bottom of range
        testUnwrap("smallint", "a <> DOUBLE '-32767'", new Comparison(NOT_EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, -32767L)));

        // round to bottom of range
        testUnwrap("smallint", "a <> DOUBLE '-32767.9'", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(SMALLINT, "a"))), new Constant(BOOLEAN, null))));

        // bottom of range
        testUnwrap("smallint", "a <> DOUBLE '-32768'", new Comparison(NOT_EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, -32768L)));

        // below range
        testUnwrap("smallint", "a <> DOUBLE '-32768.1'", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(SMALLINT, "a"))), new Constant(BOOLEAN, null))));
    }

    @Test
    public void testLessThan()
    {
        // representable
        testUnwrap("smallint", "a < DOUBLE '1'", new Comparison(LESS_THAN, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 1L)));

        testUnwrap("bigint", "a < DOUBLE '1'", new Comparison(LESS_THAN, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)));

        // non-representable
        testUnwrap("smallint", "a < DOUBLE '1.1'", new Comparison(LESS_THAN_OR_EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 1L)));

        testUnwrap("bigint", "a < DOUBLE '1.1'", new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)));

        testUnwrap("smallint", "a < DOUBLE '1.9'", new Comparison(LESS_THAN, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 2L)));

        // below top of range
        testUnwrap("smallint", "a < DOUBLE '32766'", new Comparison(LESS_THAN, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 32766L)));

        // round to top of range
        testUnwrap("smallint", "a < DOUBLE '32766.9'", new Comparison(LESS_THAN, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 32767L)));

        // top of range
        testUnwrap("smallint", "a < DOUBLE '32767'", new Comparison(NOT_EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 32767L)));

        // above range
        testUnwrap("smallint", "a < DOUBLE '32768.1'", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(SMALLINT, "a"))), new Constant(BOOLEAN, null))));

        // above bottom of range
        testUnwrap("smallint", "a < DOUBLE '-32767'", new Comparison(LESS_THAN, new Reference(SMALLINT, "a"), new Constant(SMALLINT, -32767L)));

        // round to bottom of range
        testUnwrap("smallint", "a < DOUBLE '-32767.9'", new Comparison(EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, -32768L)));

        // bottom of range
        testUnwrap("smallint", "a < DOUBLE '-32768'", new Logical(AND, ImmutableList.of(new IsNull(new Reference(SMALLINT, "a")), new Constant(BOOLEAN, null))));

        // below range
        testUnwrap("smallint", "a < DOUBLE '-32768.1'", new Logical(AND, ImmutableList.of(new IsNull(new Reference(SMALLINT, "a")), new Constant(BOOLEAN, null))));

        // -2^64 constant
        testUnwrap("bigint", "a < DOUBLE '-18446744073709551616'", new Logical(AND, ImmutableList.of(new IsNull(new Reference(BIGINT, "a")), new Constant(BOOLEAN, null))));
    }

    @Test
    public void testLessThanOrEqual()
    {
        // representable
        testUnwrap("smallint", "a <= DOUBLE '1'", new Comparison(LESS_THAN_OR_EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 1L)));

        testUnwrap("bigint", "a <= DOUBLE '1'", new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)));

        // non-representable
        testUnwrap("smallint", "a <= DOUBLE '1.1'", new Comparison(LESS_THAN_OR_EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 1L)));

        testUnwrap("bigint", "a <= DOUBLE '1.1'", new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)));

        testUnwrap("smallint", "a <= DOUBLE '1.9'", new Comparison(LESS_THAN, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 2L)));

        // below top of range
        testUnwrap("smallint", "a <= DOUBLE '32766'", new Comparison(LESS_THAN_OR_EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 32766L)));

        // round to top of range
        testUnwrap("smallint", "a <= DOUBLE '32766.9'", new Comparison(LESS_THAN, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 32767L)));

        // top of range
        testUnwrap("smallint", "a <= DOUBLE '32767'", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(SMALLINT, "a"))), new Constant(BOOLEAN, null))));

        // above range
        testUnwrap("smallint", "a <= DOUBLE '32768.1'", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(SMALLINT, "a"))), new Constant(BOOLEAN, null))));

        // 2^64 constant
        testUnwrap("bigint", "a <= DOUBLE '18446744073709551616'", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(BIGINT, "a"))), new Constant(BOOLEAN, null))));

        // above bottom of range
        testUnwrap("smallint", "a <= DOUBLE '-32767'", new Comparison(LESS_THAN_OR_EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, -32767L)));

        // round to bottom of range
        testUnwrap("smallint", "a <= DOUBLE '-32767.9'", new Comparison(EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, -32768L)));

        // bottom of range
        testUnwrap("smallint", "a <= DOUBLE '-32768'", new Comparison(EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, -32768L)));

        // below range
        testUnwrap("smallint", "a <= DOUBLE '-32768.1'", new Logical(AND, ImmutableList.of(new IsNull(new Reference(SMALLINT, "a")), new Constant(BOOLEAN, null))));
    }

    @Test
    public void testGreaterThan()
    {
        // representable
        testUnwrap("smallint", "a > DOUBLE '1'", new Comparison(GREATER_THAN, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 1L)));

        testUnwrap("bigint", "a > DOUBLE '1'", new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)));

        // non-representable
        testUnwrap("smallint", "a > DOUBLE '1.1'", new Comparison(GREATER_THAN, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 1L)));

        testUnwrap("smallint", "a > DOUBLE '1.9'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 2L)));

        testUnwrap("bigint", "a > DOUBLE '1.9'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 2L)));

        // below top of range
        testUnwrap("smallint", "a > DOUBLE '32766'", new Comparison(GREATER_THAN, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 32766L)));

        // round to top of range
        testUnwrap("smallint", "a > DOUBLE '32766.9'", new Comparison(EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 32767L)));

        // top of range
        testUnwrap("smallint", "a > DOUBLE '32767'", new Logical(AND, ImmutableList.of(new IsNull(new Reference(SMALLINT, "a")), new Constant(BOOLEAN, null))));

        // above range
        testUnwrap("smallint", "a > DOUBLE '32768.1'", new Logical(AND, ImmutableList.of(new IsNull(new Reference(SMALLINT, "a")), new Constant(BOOLEAN, null))));

        // 2^64 constant
        testUnwrap("bigint", "a > DOUBLE '18446744073709551616'", new Logical(AND, ImmutableList.of(new IsNull(new Reference(BIGINT, "a")), new Constant(BOOLEAN, null))));

        // above bottom of range
        testUnwrap("smallint", "a > DOUBLE '-32767'", new Comparison(GREATER_THAN, new Reference(SMALLINT, "a"), new Constant(SMALLINT, -32767L)));

        // round to bottom of range
        testUnwrap("smallint", "a > DOUBLE '-32767.9'", new Comparison(GREATER_THAN, new Reference(SMALLINT, "a"), new Constant(SMALLINT, -32768L)));

        // bottom of range
        testUnwrap("smallint", "a > DOUBLE '-32768'", new Comparison(NOT_EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, -32768L)));

        // below range
        testUnwrap("smallint", "a > DOUBLE '-32768.1'", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(SMALLINT, "a"))), new Constant(BOOLEAN, null))));
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        // representable
        testUnwrap("smallint", "a >= DOUBLE '1'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 1L)));

        testUnwrap("bigint", "a >= DOUBLE '1'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)));

        // non-representable
        testUnwrap("smallint", "a >= DOUBLE '1.1'", new Comparison(GREATER_THAN, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 1L)));

        testUnwrap("bigint", "a >= DOUBLE '1.1'", new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)));

        testUnwrap("smallint", "a >= DOUBLE '1.9'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 2L)));

        // below top of range
        testUnwrap("smallint", "a >= DOUBLE '32766'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 32766L)));

        // round to top of range
        testUnwrap("smallint", "a >= DOUBLE '32766.9'", new Comparison(EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 32767L)));

        // top of range
        testUnwrap("smallint", "a >= DOUBLE '32767'", new Comparison(EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 32767L)));

        // above range
        testUnwrap("smallint", "a >= DOUBLE '32768.1'", new Logical(AND, ImmutableList.of(new IsNull(new Reference(SMALLINT, "a")), new Constant(BOOLEAN, null))));

        // above bottom of range
        testUnwrap("smallint", "a >= DOUBLE '-32767'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, -32767L)));

        // round to bottom of range
        testUnwrap("smallint", "a >= DOUBLE '-32767.9'", new Comparison(GREATER_THAN, new Reference(SMALLINT, "a"), new Constant(SMALLINT, -32768L)));

        // bottom of range
        testUnwrap("smallint", "a >= DOUBLE '-32768'", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(SMALLINT, "a"))), new Constant(BOOLEAN, null))));

        // below range
        testUnwrap("smallint", "a >= DOUBLE '-32768.1'", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(SMALLINT, "a"))), new Constant(BOOLEAN, null))));

        // -2^64 constant
        testUnwrap("bigint", "a >= DOUBLE '-18446744073709551616'", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(BIGINT, "a"))), new Constant(BOOLEAN, null))));
    }

    @Test
    public void testDistinctFrom()
    {
        // representable
        testUnwrap("smallint", "a IS DISTINCT FROM DOUBLE '1'", new Comparison(IS_DISTINCT_FROM, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 1L)));

        testUnwrap("bigint", "a IS DISTINCT FROM DOUBLE '1'", new Comparison(IS_DISTINCT_FROM, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)));

        // non-representable
        testRemoveFilter("smallint", "a IS DISTINCT FROM DOUBLE '1.1'");

        testRemoveFilter("smallint", "a IS DISTINCT FROM DOUBLE '1.9'");

        testRemoveFilter("bigint", "a IS DISTINCT FROM DOUBLE '1.9'");

        // below top of range
        testUnwrap("smallint", "a IS DISTINCT FROM DOUBLE '32766'", new Comparison(IS_DISTINCT_FROM, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 32766L)));

        // round to top of range
        testRemoveFilter("smallint", "a IS DISTINCT FROM DOUBLE '32766.9'");

        // top of range
        testUnwrap("smallint", "a IS DISTINCT FROM DOUBLE '32767'", new Comparison(IS_DISTINCT_FROM, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 32767L)));

        // above range
        testRemoveFilter("smallint", "a IS DISTINCT FROM DOUBLE '32768.1'");

        // 2^64 constant
        testRemoveFilter("bigint", "a IS DISTINCT FROM DOUBLE '18446744073709551616'");

        // above bottom of range
        testUnwrap("smallint", "a IS DISTINCT FROM DOUBLE '-32767'", new Comparison(IS_DISTINCT_FROM, new Reference(SMALLINT, "a"), new Constant(SMALLINT, -32767L)));

        // round to bottom of range
        testRemoveFilter("smallint", "a IS DISTINCT FROM DOUBLE '-32767.9'");

        // bottom of range
        testUnwrap("smallint", "a IS DISTINCT FROM DOUBLE '-32768'", new Comparison(IS_DISTINCT_FROM, new Reference(SMALLINT, "a"), new Constant(SMALLINT, -32768L)));

        // below range
        testRemoveFilter("smallint", "a IS DISTINCT FROM DOUBLE '-32768.1'");
    }

    @Test
    public void testNull()
    {
        testUnwrap("smallint", "a = CAST(NULL AS DOUBLE)", new Constant(BOOLEAN, null));

        testUnwrap("bigint", "a = CAST(NULL AS DOUBLE)", new Constant(BOOLEAN, null));

        testUnwrap("smallint", "a <> CAST(NULL AS DOUBLE)", new Constant(BOOLEAN, null));

        testUnwrap("smallint", "a > CAST(NULL AS DOUBLE)", new Constant(BOOLEAN, null));

        testUnwrap("smallint", "a < CAST(NULL AS DOUBLE)", new Constant(BOOLEAN, null));

        testUnwrap("smallint", "a >= CAST(NULL AS DOUBLE)", new Constant(BOOLEAN, null));

        testUnwrap("smallint", "a <= CAST(NULL AS DOUBLE)", new Constant(BOOLEAN, null));

        testUnwrap("smallint", "a IS DISTINCT FROM CAST(NULL AS DOUBLE)", new Not(new IsNull(new Cast(new Reference(SMALLINT, "a"), DOUBLE))));

        testUnwrap("bigint", "a IS DISTINCT FROM CAST(NULL AS DOUBLE)", new Not(new IsNull(new Cast(new Reference(BIGINT, "a"), DOUBLE))));
    }

    @Test
    public void testNaN()
    {
        testUnwrap("smallint", "a = nan()", new Logical(AND, ImmutableList.of(new IsNull(new Reference(SMALLINT, "a")), new Constant(BOOLEAN, null))));

        testUnwrap("bigint", "a = nan()", new Logical(AND, ImmutableList.of(new IsNull(new Reference(BIGINT, "a")), new Constant(BOOLEAN, null))));

        testUnwrap("smallint", "a < nan()", new Logical(AND, ImmutableList.of(new IsNull(new Reference(SMALLINT, "a")), new Constant(BOOLEAN, null))));

        testUnwrap("smallint", "a <> nan()", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(SMALLINT, "a"))), new Constant(BOOLEAN, null))));

        testRemoveFilter("smallint", "a IS DISTINCT FROM nan()");

        testRemoveFilter("bigint", "a IS DISTINCT FROM nan()");

        testUnwrap("real", "a = nan()", new Logical(AND, ImmutableList.of(new IsNull(new Reference(REAL, "a")), new Constant(BOOLEAN, null))));

        testUnwrap("real", "a < nan()", new Logical(AND, ImmutableList.of(new IsNull(new Reference(REAL, "a")), new Constant(BOOLEAN, null))));

        testUnwrap("real", "a <> nan()", new Logical(OR, ImmutableList.of(new Not(new IsNull(new Reference(REAL, "a"))), new Constant(BOOLEAN, null))));

        testUnwrap("real", "a IS DISTINCT FROM nan()", new Comparison(IS_DISTINCT_FROM, new Reference(REAL, "a"), new Constant(REAL, toReal(Float.NaN))));
    }

    @Test
    public void smokeTests()
    {
        // smoke tests for various type combinations
        for (String type : asList("SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE")) {
            testUnwrap("tinyint", format("a = %s '1'", type), new Comparison(EQUAL, new Reference(TINYINT, "a"), new Constant(TINYINT, 1L)));
        }

        for (String type : asList("INTEGER", "BIGINT", "REAL", "DOUBLE")) {
            testUnwrap("smallint", format("a = %s '1'", type), new Comparison(EQUAL, new Reference(SMALLINT, "a"), new Constant(SMALLINT, 1L)));
        }

        for (String type : asList("BIGINT", "DOUBLE")) {
            testUnwrap("integer", format("a = %s '1'", type), new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)));
        }

        testUnwrap("real", "a = DOUBLE '1'", new Comparison(EQUAL, new Reference(REAL, "a"), new Constant(REAL, toReal(1.0f))));
    }

    @Test
    public void testTermOrder()
    {
        // ensure the optimization works when the terms of the comparison are reversed
        // vs the canonical <expr> <op> <literal> form
        assertPlan("SELECT * FROM (VALUES REAL '1') t(a) WHERE DOUBLE '1' = a",
                output(
                        filter(
                                new Comparison(EQUAL, new Reference(REAL, "A"), new Constant(REAL, toReal(1.0f))),
                                values("A"))));
    }

    @Test
    public void testCastDateToTimestampWithTimeZone()
    {
        Session session = getPlanTester().getDefaultSession();

        Session utcSession = withZone(session, TimeZoneKey.UTC_KEY);
        // east of Greenwich
        Session warsawSession = withZone(session, TimeZoneKey.getTimeZoneKey("Europe/Warsaw"));
        // west of Greenwich
        Session losAngelesSession = withZone(session, TimeZoneKey.getTimeZoneKey("America/Los_Angeles"));

        // same zone
        testUnwrap(utcSession, "date", "a > TIMESTAMP '2020-10-26 11:02:18 UTC'", new Comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("2020-10-26"))));
        testUnwrap(warsawSession, "date", "a > TIMESTAMP '2020-10-26 11:02:18 Europe/Warsaw'", new Comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("2020-10-26"))));
        testUnwrap(losAngelesSession, "date", "a > TIMESTAMP '2020-10-26 11:02:18 America/Los_Angeles'", new Comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("2020-10-26"))));

        // different zone
        testUnwrap(warsawSession, "date", "a > TIMESTAMP '2020-10-26 11:02:18 UTC'", new Comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("2020-10-26"))));
        testUnwrap(losAngelesSession, "date", "a > TIMESTAMP '2020-10-26 11:02:18 UTC'", new Comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("2020-10-26"))));

        // maximum precision
        testUnwrap(warsawSession, "date", "a > TIMESTAMP '2020-10-26 11:02:18.123456789321 UTC'", new Comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("2020-10-26"))));
        testUnwrap(losAngelesSession, "date", "a > TIMESTAMP '2020-10-26 11:02:18.123456789321 UTC'", new Comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("2020-10-26"))));

        // DST forward -- Warsaw changed clock 1h forward on 2020-03-29T01:00 UTC (2020-03-29T02:00 local time)
        // Note that in given session input TIMESTAMP values  2020-03-29 02:31 and 2020-03-29 03:31 produce the same value 2020-03-29 01:31 UTC (conversion is not monotonic)
        // last before
        testUnwrap(warsawSession, "date", "a > TIMESTAMP '2020-03-29 00:59:59 UTC'", new Comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("2020-03-29"))));
        testUnwrap(warsawSession, "date", "a > TIMESTAMP '2020-03-29 00:59:59.999 UTC'", new Comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("2020-03-29"))));
        testUnwrap(warsawSession, "date", "a > TIMESTAMP '2020-03-29 00:59:59.13 UTC'", new Comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("2020-03-29"))));
        testUnwrap(warsawSession, "date", "a > TIMESTAMP '2020-03-29 00:59:59.999999 UTC'", new Comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("2020-03-29"))));
        testUnwrap(warsawSession, "date", "a > TIMESTAMP '2020-03-29 00:59:59.999999999 UTC'", new Comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("2020-03-29"))));
        testUnwrap(warsawSession, "date", "a > TIMESTAMP '2020-03-29 00:59:59.999999999999 UTC'", new Comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("2020-03-29"))));

        // equal
        testUnwrap(utcSession, "date", "a = TIMESTAMP '1981-06-22 00:00:00 UTC'", new Comparison(EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a = TIMESTAMP '1981-06-22 00:00:00.000000 UTC'", new Comparison(EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a = TIMESTAMP '1981-06-22 00:00:00.000000000 UTC'", new Comparison(EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a = TIMESTAMP '1981-06-22 00:00:00.000000000000 UTC'", new Comparison(EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));

        // not equal
        testUnwrap(utcSession, "date", "a <> TIMESTAMP '1981-06-22 00:00:00 UTC'", new Comparison(NOT_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a <> TIMESTAMP '1981-06-22 00:00:00.000000 UTC'", new Comparison(NOT_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a <> TIMESTAMP '1981-06-22 00:00:00.000000000 UTC'", new Comparison(NOT_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a <> TIMESTAMP '1981-06-22 00:00:00.000000000000 UTC'", new Comparison(NOT_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));

        // less than
        testUnwrap(utcSession, "date", "a < TIMESTAMP '1981-06-22 00:00:00 UTC'", new Comparison(LESS_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a < TIMESTAMP '1981-06-22 00:00:00.000000 UTC'", new Comparison(LESS_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a < TIMESTAMP '1981-06-22 00:00:00.000000000 UTC'", new Comparison(LESS_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a < TIMESTAMP '1981-06-22 00:00:00.000000000000 UTC'", new Comparison(LESS_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));

        // less than or equal
        testUnwrap(utcSession, "date", "a <= TIMESTAMP '1981-06-22 00:00:00 UTC'", new Comparison(LESS_THAN_OR_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a <= TIMESTAMP '1981-06-22 00:00:00.000000 UTC'", new Comparison(LESS_THAN_OR_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a <= TIMESTAMP '1981-06-22 00:00:00.000000000 UTC'", new Comparison(LESS_THAN_OR_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a <= TIMESTAMP '1981-06-22 00:00:00.000000000000 UTC'", new Comparison(LESS_THAN_OR_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));

        // greater than
        testUnwrap(utcSession, "date", "a > TIMESTAMP '1981-06-22 00:00:00 UTC'", new Comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a > TIMESTAMP '1981-06-22 00:00:00.000000 UTC'", new Comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a > TIMESTAMP '1981-06-22 00:00:00.000000000 UTC'", new Comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a > TIMESTAMP '1981-06-22 00:00:00.000000000000 UTC'", new Comparison(GREATER_THAN, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));

        // greater than or equal
        testUnwrap(utcSession, "date", "a >= TIMESTAMP '1981-06-22 00:00:00 UTC'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a >= TIMESTAMP '1981-06-22 00:00:00.000000 UTC'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a >= TIMESTAMP '1981-06-22 00:00:00.000000000 UTC'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a >= TIMESTAMP '1981-06-22 00:00:00.000000000000 UTC'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));

        // is distinct
        testUnwrap(utcSession, "date", "a IS DISTINCT FROM TIMESTAMP '1981-06-22 00:00:00 UTC'", new Comparison(IS_DISTINCT_FROM, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a IS DISTINCT FROM TIMESTAMP '1981-06-22 00:00:00.000000 UTC'", new Comparison(IS_DISTINCT_FROM, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a IS DISTINCT FROM TIMESTAMP '1981-06-22 00:00:00.000000000 UTC'", new Comparison(IS_DISTINCT_FROM, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
        testUnwrap(utcSession, "date", "a IS DISTINCT FROM TIMESTAMP '1981-06-22 00:00:00.000000000000 UTC'", new Comparison(IS_DISTINCT_FROM, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));

        // is not distinct
        testUnwrap(utcSession, "date", "a IS NOT DISTINCT FROM TIMESTAMP '1981-06-22 00:00:00 UTC'", new Not(new Comparison(IS_DISTINCT_FROM, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22")))));
        testUnwrap(utcSession, "date", "a IS NOT DISTINCT FROM TIMESTAMP '1981-06-22 00:00:00.000000 UTC'", new Not(new Comparison(IS_DISTINCT_FROM, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22")))));
        testUnwrap(utcSession, "date", "a IS NOT DISTINCT FROM TIMESTAMP '1981-06-22 00:00:00.000000000 UTC'", new Not(new Comparison(IS_DISTINCT_FROM, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22")))));
        testUnwrap(utcSession, "date", "a IS NOT DISTINCT FROM TIMESTAMP '1981-06-22 00:00:00.000000000000 UTC'", new Not(new Comparison(IS_DISTINCT_FROM, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22")))));

        // null date literal
        testUnwrap("date", "CAST(a AS TIMESTAMP WITH TIME ZONE) = NULL", new Constant(BOOLEAN, null));
        testUnwrap("date", "CAST(a AS TIMESTAMP WITH TIME ZONE) < NULL", new Constant(BOOLEAN, null));
        testUnwrap("date", "CAST(a AS TIMESTAMP WITH TIME ZONE) <= NULL", new Constant(BOOLEAN, null));
        testUnwrap("date", "CAST(a AS TIMESTAMP WITH TIME ZONE) > NULL", new Constant(BOOLEAN, null));
        testUnwrap("date", "CAST(a AS TIMESTAMP WITH TIME ZONE) >= NULL", new Constant(BOOLEAN, null));
        testUnwrap("date", "CAST(a AS TIMESTAMP WITH TIME ZONE) IS DISTINCT FROM NULL", new Not(new IsNull(new Cast(new Reference(DATE, "a"), TIMESTAMP_TZ_MILLIS))));

        // timestamp with time zone value on the left
        testUnwrap(utcSession, "date", "TIMESTAMP '1981-06-22 00:00:00 UTC' = a", new Comparison(EQUAL, new Reference(DATE, "a"), new Constant(DATE, (long) DateTimeUtils.parseDate("1981-06-22"))));
    }

    @Test
    public void testCastTimestampToTimestampWithTimeZone()
    {
        Session session = getPlanTester().getDefaultSession();

        Session utcSession = withZone(session, TimeZoneKey.UTC_KEY);
        // east of Greenwich
        Session warsawSession = withZone(session, TimeZoneKey.getTimeZoneKey("Europe/Warsaw"));
        // west of Greenwich
        Session losAngelesSession = withZone(session, TimeZoneKey.getTimeZoneKey("America/Los_Angeles"));

        // same zone
        testUnwrap(utcSession, "timestamp(0)", "a > TIMESTAMP '2020-10-26 11:02:18 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2020-10-26 11:02:18"))));
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-10-26 11:02:18 Europe/Warsaw'", new Comparison(GREATER_THAN, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2020-10-26 11:02:18"))));
        testUnwrap(losAngelesSession, "timestamp(0)", "a > TIMESTAMP '2020-10-26 11:02:18 America/Los_Angeles'", new Comparison(GREATER_THAN, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2020-10-26 11:02:18"))));

        // different zone
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-10-26 11:02:18 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2020-10-26 12:02:18"))));
        testUnwrap(losAngelesSession, "timestamp(0)", "a > TIMESTAMP '2020-10-26 11:02:18 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2020-10-26 04:02:18"))));

        // short timestamp, short timestamp with time zone being coerced to long timestamp with time zone
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-10-26 11:02:18.12 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2020-10-26 12:02:18.120000"))));
        testUnwrap(losAngelesSession, "timestamp(6)", "a > TIMESTAMP '2020-10-26 11:02:18.12 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2020-10-26 04:02:18.120000"))));

        // long timestamp, short timestamp with time zone being coerced to long timestamp with time zone
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-10-26 11:02:18.12 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2020-10-26 12:02:18.120000000"))));
        testUnwrap(losAngelesSession, "timestamp(9)", "a > TIMESTAMP '2020-10-26 11:02:18.12 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2020-10-26 04:02:18.120000000"))));

        // long timestamp, long timestamp with time zone
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-10-26 11:02:18.123456 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2020-10-26 12:02:18.123456000"))));
        testUnwrap(losAngelesSession, "timestamp(9)", "a > TIMESTAMP '2020-10-26 11:02:18.123456 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2020-10-26 04:02:18.123456000"))));

        // maximum precision
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-10-26 11:02:18.123456789321 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "2020-10-26 12:02:18.123456789321"))));
        testUnwrap(losAngelesSession, "timestamp(12)", "a > TIMESTAMP '2020-10-26 11:02:18.123456789321 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "2020-10-26 04:02:18.123456789321"))));

        // DST forward -- Warsaw changed clock 1h forward on 2020-03-29T01:00 UTC (2020-03-29T02:00 local time)
        // Note that in given session input TIMESTAMP values  2020-03-29 02:31 and 2020-03-29 03:31 produce the same value 2020-03-29 01:31 UTC (conversion is not monotonic)
        // last before
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-03-29 00:59:59 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2020-03-29 01:59:59"))));
        testUnwrap(warsawSession, "timestamp(3)", "a > TIMESTAMP '2020-03-29 00:59:59.999 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2020-03-29 01:59:59.999"))));
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-03-29 00:59:59.13 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2020-03-29 01:59:59.130000"))));
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-03-29 00:59:59.999999 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2020-03-29 01:59:59.999999"))));
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-03-29 00:59:59.999999999 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2020-03-29 01:59:59.999999999"))));
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-03-29 00:59:59.999999999999 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "2020-03-29 01:59:59.999999999999"))));
        // first after
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-03-29 02:00:00 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2020-03-29 04:00:00"))));
        testUnwrap(warsawSession, "timestamp(3)", "a > TIMESTAMP '2020-03-29 02:00:00.000 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2020-03-29 04:00:00.000"))));
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-03-29 02:00:00.000000 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2020-03-29 04:00:00.000000"))));
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-03-29 02:00:00.000000000 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2020-03-29 04:00:00.000000000"))));
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-03-29 02:00:00.000000000000 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "2020-03-29 04:00:00.000000000000"))));

        // DST backward -- Warsaw changed clock 1h backward on 2020-10-25T01:00 UTC (2020-03-29T03:00 local time)
        // Note that in given session no input TIMESTAMP value can produce TIMESTAMP WITH TIME ZONE within [2020-10-25 00:00:00 UTC, 2020-10-25 01:00:00 UTC], so '>=' is OK
        // last before
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-10-25 00:59:59 UTC'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2020-10-25 02:59:59"))));
        testUnwrap(warsawSession, "timestamp(3)", "a > TIMESTAMP '2020-10-25 00:59:59.999 UTC'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2020-10-25 02:59:59.999"))));
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-10-25 00:59:59.999999 UTC'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2020-10-25 02:59:59.999999"))));
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-10-25 00:59:59.999999999 UTC'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2020-10-25 02:59:59.999999999"))));
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-10-25 00:59:59.999999999999 UTC'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "2020-10-25 02:59:59.999999999999"))));
        // first within
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-10-25 01:00:00 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2020-10-25 02:00:00"))));
        testUnwrap(warsawSession, "timestamp(3)", "a > TIMESTAMP '2020-10-25 01:00:00.000 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2020-10-25 02:00:00.000"))));
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-10-25 01:00:00.000000 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2020-10-25 02:00:00.000000"))));
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-10-25 01:00:00.000000000 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2020-10-25 02:00:00.000000000"))));
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-10-25 01:00:00.000000000000 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "2020-10-25 02:00:00.000000000000"))));
        // last within
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-10-25 01:59:59 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2020-10-25 02:59:59"))));
        testUnwrap(warsawSession, "timestamp(3)", "a > TIMESTAMP '2020-10-25 01:59:59.999 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2020-10-25 02:59:59.999"))));
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-10-25 01:59:59.999999 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2020-10-25 02:59:59.999999"))));
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-10-25 01:59:59.999999999 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2020-10-25 02:59:59.999999999"))));
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-10-25 01:59:59.999999999999 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "2020-10-25 02:59:59.999999999999"))));
        // first after
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-10-25 02:00:00 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(0), "a"), new Constant(createTimestampType(0), DateTimes.parseTimestamp(0, "2020-10-25 03:00:00"))));
        testUnwrap(warsawSession, "timestamp(3)", "a > TIMESTAMP '2020-10-25 02:00:00.000 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "2020-10-25 03:00:00.000"))));
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-10-25 02:00:00.000000 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "2020-10-25 03:00:00.000000"))));
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-10-25 02:00:00.000000000 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2020-10-25 03:00:00.000000000"))));
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-10-25 02:00:00.000000000000 UTC'", new Comparison(GREATER_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "2020-10-25 03:00:00.000000000000"))));
    }

    @Test
    public void testNoEffect()
    {
        // BIGINT->DOUBLE implicit cast is not injective if the double constant is >= 2^53 and <= double(2^63 - 1)
        testUnwrap("bigint", "a = DOUBLE '9007199254740992'", new Comparison(EQUAL, new Cast(new Reference(BIGINT, "a"), DOUBLE), new Constant(DOUBLE, 9.007199254740992E15)));

        testUnwrap("bigint", "a = DOUBLE '9223372036854775807'", new Comparison(EQUAL, new Cast(new Reference(BIGINT, "a"), DOUBLE), new Constant(DOUBLE, 9.223372036854776E18)));

        // BIGINT->DOUBLE implicit cast is not injective if the double constant is <= -2^53 and >= double(-2^63 + 1)
        testUnwrap("bigint", "a = DOUBLE '-9007199254740992'", new Comparison(EQUAL, new Cast(new Reference(BIGINT, "a"), DOUBLE), new Constant(DOUBLE, -9.007199254740992E15)));

        testUnwrap("bigint", "a = DOUBLE '-9223372036854775807'", new Comparison(EQUAL, new Cast(new Reference(BIGINT, "a"), DOUBLE), new Constant(DOUBLE, -9.223372036854776E18)));

        // BIGINT->REAL implicit cast is not injective if the real constant is >= 2^23 and <= real(2^63 - 1)
        testUnwrap("bigint", "a = REAL '8388608'", new Comparison(EQUAL, new Cast(new Reference(BIGINT, "a"), REAL), new Constant(REAL, toReal(8388608.0f))));

        testUnwrap("bigint", "a = REAL '9223372036854775807'", new Comparison(EQUAL, new Cast(new Reference(BIGINT, "a"), REAL), new Constant(REAL, toReal(9.223372E18f))));

        // BIGINT->REAL implicit cast is not injective if the real constant is <= -2^23 and >= real(-2^63 + 1)
        testUnwrap("bigint", "a = REAL '-8388608'", new Comparison(EQUAL, new Cast(new Reference(BIGINT, "a"), REAL), new Constant(REAL, toReal(-8388608.0f))));

        testUnwrap("bigint", "a = REAL '-9223372036854775807'", new Comparison(EQUAL, new Cast(new Reference(BIGINT, "a"), REAL), new Constant(REAL, toReal(-9.223372E18f))));

        // INTEGER->REAL implicit cast is not injective if the real constant is >= 2^23 and <= 2^31 - 1
        testUnwrap("integer", "a = REAL '8388608'", new Comparison(EQUAL, new Cast(new Reference(INTEGER, "a"), REAL), new Constant(REAL, toReal(8388608.0f))));

        testUnwrap("integer", "a = REAL '2147483647'", new Comparison(EQUAL, new Cast(new Reference(INTEGER, "a"), REAL), new Constant(REAL, toReal(2.1474836E9f))));

        // INTEGER->REAL implicit cast is not injective if the real constant is <= -2^23 and >= -2^31 + 1
        testUnwrap("integer", "a = REAL '-8388608'", new Comparison(EQUAL, new Cast(new Reference(INTEGER, "a"), REAL), new Constant(REAL, toReal(-8388608.0f))));

        testUnwrap("integer", "a = REAL '-2147483647'", new Comparison(EQUAL, new Cast(new Reference(INTEGER, "a"), REAL), new Constant(REAL, toReal(-2.1474836E9f))));

        // DECIMAL(p)->DOUBLE not injective for p > 15
        testUnwrap("decimal(16)", "a = DOUBLE '1'", new Comparison(EQUAL, new Cast(new Reference(createDecimalType(16), "a"), DOUBLE), new Constant(DOUBLE, 1.0)));

        // DECIMAL(p)->REAL not injective for p > 7
        testUnwrap("decimal(8)", "a = REAL '1'", new Comparison(EQUAL, new Cast(new Reference(createDecimalType(8), "a"), REAL), new Constant(REAL, toReal(1.0f))));

        // no implicit cast between VARCHAR->INTEGER
        testUnwrap("varchar", "CAST(a AS INTEGER) = INTEGER '1'", new Comparison(EQUAL, new Cast(new Reference(VARCHAR, "a"), INTEGER), new Constant(INTEGER, 1L)));

        // no implicit cast between DOUBLE->INTEGER
        testUnwrap("double", "CAST(a AS INTEGER) = INTEGER '1'", new Comparison(EQUAL, new Cast(new Reference(DOUBLE, "a"), INTEGER), new Constant(INTEGER, 1L)));
    }

    @Test
    public void testUnwrapCastTimestampAsDate()
    {
        // equal
        testUnwrap("timestamp(3)", "CAST(a AS DATE) = DATE '1981-06-22'", new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-22 00:00:00.000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-23 00:00:00.000"))))));
        testUnwrap("timestamp(6)", "CAST(a AS DATE) = DATE '1981-06-22'", new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-22 00:00:00.000000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-23 00:00:00.000000"))))));
        testUnwrap("timestamp(9)", "CAST(a AS DATE) = DATE '1981-06-22'", new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-22 00:00:00.000000000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-23 00:00:00.000000000"))))));
        testUnwrap("timestamp(12)", "CAST(a AS DATE) = DATE '1981-06-22'", new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-22 00:00:00.000000000000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-23 00:00:00.000000000000"))))));

        // not equal
        testUnwrap("timestamp(3)", "CAST(a AS DATE) <> DATE '1981-06-22'", new Logical(OR, ImmutableList.of(new Comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-22 00:00:00.000"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-23 00:00:00.000"))))));
        testUnwrap("timestamp(6)", "CAST(a AS DATE) <> DATE '1981-06-22'", new Logical(OR, ImmutableList.of(new Comparison(LESS_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-22 00:00:00.000000"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-23 00:00:00.000000"))))));
        testUnwrap("timestamp(9)", "CAST(a AS DATE) <> DATE '1981-06-22'", new Logical(OR, ImmutableList.of(new Comparison(LESS_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-22 00:00:00.000000000"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-23 00:00:00.000000000"))))));
        testUnwrap("timestamp(12)", "CAST(a AS DATE) <> DATE '1981-06-22'", new Logical(OR, ImmutableList.of(new Comparison(LESS_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-22 00:00:00.000000000000"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-23 00:00:00.000000000000"))))));

        // less than
        testUnwrap("timestamp(3)", "CAST(a AS DATE) < DATE '1981-06-22'", new Comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-22 00:00:00.000"))));
        testUnwrap("timestamp(6)", "CAST(a AS DATE) < DATE '1981-06-22'", new Comparison(LESS_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-22 00:00:00.000000"))));
        testUnwrap("timestamp(9)", "CAST(a AS DATE) < DATE '1981-06-22'", new Comparison(LESS_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-22 00:00:00.000000000"))));
        testUnwrap("timestamp(12)", "CAST(a AS DATE) < DATE '1981-06-22'", new Comparison(LESS_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-22 00:00:00.000000000000"))));

        // less than or equal
        testUnwrap("timestamp(3)", "CAST(a AS DATE) <= DATE '1981-06-22'", new Comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-23 00:00:00.000"))));
        testUnwrap("timestamp(6)", "CAST(a AS DATE) <= DATE '1981-06-22'", new Comparison(LESS_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-23 00:00:00.000000"))));
        testUnwrap("timestamp(9)", "CAST(a AS DATE) <= DATE '1981-06-22'", new Comparison(LESS_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-23 00:00:00.000000000"))));
        testUnwrap("timestamp(12)", "CAST(a AS DATE) <= DATE '1981-06-22'", new Comparison(LESS_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-23 00:00:00.000000000000"))));

        // greater than
        testUnwrap("timestamp(3)", "CAST(a AS DATE) > DATE '1981-06-22'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-23 00:00:00.000"))));
        testUnwrap("timestamp(6)", "CAST(a AS DATE) > DATE '1981-06-22'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-23 00:00:00.000000"))));
        testUnwrap("timestamp(9)", "CAST(a AS DATE) > DATE '1981-06-22'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-23 00:00:00.000000000"))));
        testUnwrap("timestamp(12)", "CAST(a AS DATE) > DATE '1981-06-22'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-23 00:00:00.000000000000"))));

        // greater than or equal
        testUnwrap("timestamp(3)", "CAST(a AS DATE) >= DATE '1981-06-22'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-22 00:00:00.000"))));
        testUnwrap("timestamp(6)", "CAST(a AS DATE) >= DATE '1981-06-22'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-22 00:00:00.000000"))));
        testUnwrap("timestamp(9)", "CAST(a AS DATE) >= DATE '1981-06-22'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-22 00:00:00.000000000"))));
        testUnwrap("timestamp(12)", "CAST(a AS DATE) >= DATE '1981-06-22'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-22 00:00:00.000000000000"))));

        // is distinct
        testUnwrap("timestamp(3)", "CAST(a AS DATE) IS DISTINCT FROM DATE '1981-06-22'", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(3), "a")), new Comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-22 00:00:00.000"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-23 00:00:00.000"))))));
        testUnwrap("timestamp(6)", "CAST(a AS DATE) IS DISTINCT FROM DATE '1981-06-22'", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(6), "a")), new Comparison(LESS_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-22 00:00:00.000000"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-23 00:00:00.000000"))))));
        testUnwrap("timestamp(9)", "CAST(a AS DATE) IS DISTINCT FROM DATE '1981-06-22'", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(9), "a")), new Comparison(LESS_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-22 00:00:00.000000000"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-23 00:00:00.000000000"))))));
        testUnwrap("timestamp(12)", "CAST(a AS DATE) IS DISTINCT FROM DATE '1981-06-22'", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(12), "a")), new Comparison(LESS_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-22 00:00:00.000000000000"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-23 00:00:00.000000000000"))))));

        // is not distinct
        testUnwrap("timestamp(3)", "CAST(a AS DATE) IS NOT DISTINCT FROM DATE '1981-06-22'", new Logical(AND, ImmutableList.of(new Not(new IsNull(new Reference(createTimestampType(3), "a"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-22 00:00:00.000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-23 00:00:00.000"))))));
        testUnwrap("timestamp(6)", "CAST(a AS DATE) IS NOT DISTINCT FROM DATE '1981-06-22'", new Logical(AND, ImmutableList.of(new Not(new IsNull(new Reference(createTimestampType(6), "a"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-22 00:00:00.000000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-23 00:00:00.000000"))))));
        testUnwrap("timestamp(9)", "CAST(a AS DATE) IS NOT DISTINCT FROM DATE '1981-06-22'", new Logical(AND, ImmutableList.of(new Not(new IsNull(new Reference(createTimestampType(9), "a"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-22 00:00:00.000000000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-23 00:00:00.000000000"))))));
        testUnwrap("timestamp(12)", "CAST(a AS DATE) IS NOT DISTINCT FROM DATE '1981-06-22'", new Logical(AND, ImmutableList.of(new Not(new IsNull(new Reference(createTimestampType(12), "a"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-22 00:00:00.000000000000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-23 00:00:00.000000000000"))))));

        // null date literal
        testUnwrap("timestamp(3)", "CAST(a AS DATE) = NULL", new Constant(BOOLEAN, null));
        testUnwrap("timestamp(3)", "CAST(a AS DATE) < NULL", new Constant(BOOLEAN, null));
        testUnwrap("timestamp(3)", "CAST(a AS DATE) <= NULL", new Constant(BOOLEAN, null));
        testUnwrap("timestamp(3)", "CAST(a AS DATE) > NULL", new Constant(BOOLEAN, null));
        testUnwrap("timestamp(3)", "CAST(a AS DATE) >= NULL", new Constant(BOOLEAN, null));
        testUnwrap("timestamp(3)", "CAST(a AS DATE) IS DISTINCT FROM NULL", new Not(new IsNull(new Cast(new Reference(createTimestampType(3), "a"), DATE))));

        // non-optimized expression on the right
        testUnwrap("timestamp(3)", "CAST(a AS DATE) = DATE '1981-06-22' + INTERVAL '2' DAY", new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-24 00:00:00.000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-25 00:00:00.000"))))));

        // cast on the right
        testUnwrap("timestamp(3)", "DATE '1981-06-22' = CAST(a AS DATE)", new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-22 00:00:00.000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-23 00:00:00.000"))))));
    }

    @Test
    public void testUnwrapConvertTimestampToDate()
    {
        // equal
        testUnwrap("timestamp(3)", "date(a) = DATE '1981-06-22'", new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-22 00:00:00.000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-23 00:00:00.000"))))));
        testUnwrap("timestamp(6)", "date(a) = DATE '1981-06-22'", new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-22 00:00:00.000000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-23 00:00:00.000000"))))));
        testUnwrap("timestamp(9)", "date(a) = DATE '1981-06-22'", new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-22 00:00:00.000000000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-23 00:00:00.000000000"))))));
        testUnwrap("timestamp(12)", "date(a) = DATE '1981-06-22'", new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-22 00:00:00.000000000000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-23 00:00:00.000000000000"))))));

        // not equal
        testUnwrap("timestamp(3)", "date(a) <> DATE '1981-06-22'", new Logical(OR, ImmutableList.of(new Comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-22 00:00:00.000"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-23 00:00:00.000"))))));
        testUnwrap("timestamp(6)", "date(a) <> DATE '1981-06-22'", new Logical(OR, ImmutableList.of(new Comparison(LESS_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-22 00:00:00.000000"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-23 00:00:00.000000"))))));
        testUnwrap("timestamp(9)", "date(a) <> DATE '1981-06-22'", new Logical(OR, ImmutableList.of(new Comparison(LESS_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-22 00:00:00.000000000"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-23 00:00:00.000000000"))))));
        testUnwrap("timestamp(12)", "date(a) <> DATE '1981-06-22'", new Logical(OR, ImmutableList.of(new Comparison(LESS_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-22 00:00:00.000000000000"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-23 00:00:00.000000000000"))))));

        // less than
        testUnwrap("timestamp(3)", "date(a) < DATE '1981-06-22'", new Comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-22 00:00:00.000"))));
        testUnwrap("timestamp(6)", "date(a) < DATE '1981-06-22'", new Comparison(LESS_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-22 00:00:00.000000"))));
        testUnwrap("timestamp(9)", "date(a) < DATE '1981-06-22'", new Comparison(LESS_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-22 00:00:00.000000000"))));
        testUnwrap("timestamp(12)", "date(a) < DATE '1981-06-22'", new Comparison(LESS_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-22 00:00:00.000000000000"))));

        // less than or equal
        testUnwrap("timestamp(3)", "date(a) <= DATE '1981-06-22'", new Comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-23 00:00:00.000"))));
        testUnwrap("timestamp(6)", "date(a) <= DATE '1981-06-22'", new Comparison(LESS_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-23 00:00:00.000000"))));
        testUnwrap("timestamp(9)", "date(a) <= DATE '1981-06-22'", new Comparison(LESS_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-23 00:00:00.000000000"))));
        testUnwrap("timestamp(12)", "date(a) <= DATE '1981-06-22'", new Comparison(LESS_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-23 00:00:00.000000000000"))));

        // greater than
        testUnwrap("timestamp(3)", "date(a) > DATE '1981-06-22'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-23 00:00:00.000"))));
        testUnwrap("timestamp(6)", "date(a) > DATE '1981-06-22'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-23 00:00:00.000000"))));
        testUnwrap("timestamp(9)", "date(a) > DATE '1981-06-22'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-23 00:00:00.000000000"))));
        testUnwrap("timestamp(12)", "date(a) > DATE '1981-06-22'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-23 00:00:00.000000000000"))));

        // greater than or equal
        testUnwrap("timestamp(3)", "date(a) >= DATE '1981-06-22'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-22 00:00:00.000"))));
        testUnwrap("timestamp(6)", "date(a) >= DATE '1981-06-22'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-22 00:00:00.000000"))));
        testUnwrap("timestamp(9)", "date(a) >= DATE '1981-06-22'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-22 00:00:00.000000000"))));
        testUnwrap("timestamp(12)", "date(a) >= DATE '1981-06-22'", new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-22 00:00:00.000000000000"))));

        // is distinct
        testUnwrap("timestamp(3)", "date(a) IS DISTINCT FROM DATE '1981-06-22'", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(3), "a")), new Comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-22 00:00:00.000"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-23 00:00:00.000"))))));
        testUnwrap("timestamp(6)", "date(a) IS DISTINCT FROM DATE '1981-06-22'", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(6), "a")), new Comparison(LESS_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-22 00:00:00.000000"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-23 00:00:00.000000"))))));
        testUnwrap("timestamp(9)", "date(a) IS DISTINCT FROM DATE '1981-06-22'", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(9), "a")), new Comparison(LESS_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-22 00:00:00.000000000"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-23 00:00:00.000000000"))))));
        testUnwrap("timestamp(12)", "date(a) IS DISTINCT FROM DATE '1981-06-22'", new Logical(OR, ImmutableList.of(new IsNull(new Reference(createTimestampType(12), "a")), new Comparison(LESS_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-22 00:00:00.000000000000"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-23 00:00:00.000000000000"))))));

        // is not distinct
        testUnwrap("timestamp(3)", "date(a) IS NOT DISTINCT FROM DATE '1981-06-22'", new Logical(AND, ImmutableList.of(new Not(new IsNull(new Reference(createTimestampType(3), "a"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-22 00:00:00.000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-23 00:00:00.000"))))));
        testUnwrap("timestamp(6)", "date(a) IS NOT DISTINCT FROM DATE '1981-06-22'", new Logical(AND, ImmutableList.of(new Not(new IsNull(new Reference(createTimestampType(6), "a"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-22 00:00:00.000000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(6), "a"), new Constant(createTimestampType(6), DateTimes.parseTimestamp(6, "1981-06-23 00:00:00.000000"))))));
        testUnwrap("timestamp(9)", "date(a) IS NOT DISTINCT FROM DATE '1981-06-22'", new Logical(AND, ImmutableList.of(new Not(new IsNull(new Reference(createTimestampType(9), "a"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-22 00:00:00.000000000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(9), "a"), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "1981-06-23 00:00:00.000000000"))))));
        testUnwrap("timestamp(12)", "date(a) IS NOT DISTINCT FROM DATE '1981-06-22'", new Logical(AND, ImmutableList.of(new Not(new IsNull(new Reference(createTimestampType(12), "a"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-22 00:00:00.000000000000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(12), "a"), new Constant(createTimestampType(12), DateTimes.parseTimestamp(12, "1981-06-23 00:00:00.000000000000"))))));

        // null date literal
        testUnwrap("timestamp(3)", "date(a) = NULL", new Constant(BOOLEAN, null));
        testUnwrap("timestamp(3)", "date(a) < NULL", new Constant(BOOLEAN, null));
        testUnwrap("timestamp(3)", "date(a) <= NULL", new Constant(BOOLEAN, null));
        testUnwrap("timestamp(3)", "date(a) > NULL", new Constant(BOOLEAN, null));
        testUnwrap("timestamp(3)", "date(a) >= NULL", new Constant(BOOLEAN, null));
        testUnwrap("timestamp(3)", "date(a) IS DISTINCT FROM NULL", new Not(new IsNull(new Cast(new Reference(createTimestampType(3), "a"), DATE))));

        // non-optimized expression on the right
        testUnwrap("timestamp(3)", "date(a) = DATE '1981-06-22' + INTERVAL '2' DAY", new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-24 00:00:00.000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-25 00:00:00.000"))))));

        // cast on the right
        testUnwrap("timestamp(3)", "DATE '1981-06-22' = date(a)", new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-22 00:00:00.000"))), new Comparison(LESS_THAN, new Reference(createTimestampType(3), "a"), new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, "1981-06-23 00:00:00.000"))))));
    }

    private void testRemoveFilter(String inputType, String inputPredicate)
    {
        assertPlan(format("SELECT * FROM (VALUES CAST(NULL AS %s)) t(a) WHERE %s AND rand() = 42", inputType, inputPredicate),
                output(
                        filter(new Comparison(EQUAL, new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 42.0)),
                                values("a"))));
    }

    private void testUnwrap(String inputType, String inputPredicate, Expression expectedPredicate)
    {
        testUnwrap(getPlanTester().getDefaultSession(), inputType, inputPredicate, expectedPredicate);
    }

    private void testUnwrap(Session session, String inputType, String inputPredicate, Expression expected)
    {
        Expression antiOptimization = new Comparison(EQUAL, new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 42.0));
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
                session,
                output(
                        filter(
                                expected,
                                values("a"))));
    }

    private static Session withZone(Session session, TimeZoneKey timeZoneKey)
    {
        return Session.builder(requireNonNull(session, "session is null"))
                .setTimeZoneKey(requireNonNull(timeZoneKey, "timeZoneKey is null"))
                .build();
    }
}
