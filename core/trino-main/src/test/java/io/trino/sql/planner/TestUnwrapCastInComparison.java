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
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;
import static io.trino.sql.tree.LogicalExpression.Operator.OR;
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
        testUnwrap("smallint", "a = DOUBLE '1'", new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "1")));

        testUnwrap("bigint", "a = DOUBLE '1'", new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("BIGINT", "1")));

        // non-representable
        testUnwrap("smallint", "a = DOUBLE '1.1'", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));
        testUnwrap("smallint", "a = DOUBLE '1.9'", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));

        testUnwrap("bigint", "a = DOUBLE '1.1'", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));

        // below top of range
        testUnwrap("smallint", "a = DOUBLE '32766'", new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "32766")));

        // round to top of range
        testUnwrap("smallint", "a = DOUBLE '32766.9'", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));

        // top of range
        testUnwrap("smallint", "a = DOUBLE '32767'", new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "32767")));

        // above range
        testUnwrap("smallint", "a = DOUBLE '32768.1'", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));

        // above bottom of range
        testUnwrap("smallint", "a = DOUBLE '-32767'", new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "-32767")));

        // round to bottom of range
        testUnwrap("smallint", "a = DOUBLE '-32767.9'", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));

        // bottom of range
        testUnwrap("smallint", "a = DOUBLE '-32768'", new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "-32768")));

        // below range
        testUnwrap("smallint", "a = DOUBLE '-32768.1'", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));

        // -2^64 constant
        testUnwrap("bigint", "a = DOUBLE '-18446744073709551616'", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));

        // shorter varchar and char
        testNoUnwrap("varchar(1)", EQUAL, new Cast(new StringLiteral("abc"), dataType("char(3)")), "char(3)");
        // varchar and char, same length
        testUnwrap("varchar(3)", "a = CAST('abc' AS char(3))", new ComparisonExpression(EQUAL, new SymbolReference("a"), new StringLiteral("abc")));
        testNoUnwrap("varchar(3)", EQUAL, new Cast(new StringLiteral("ab"), dataType("char(3)")), "char(3)");
        // longer varchar and char
        // actually unwrapping didn't happen
        testUnwrap("varchar(10)", "a = CAST('abc' AS char(3))", new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), dataType("char(10)")), new Cast(new StringLiteral("abc"), dataType("char(10)"))));
        // unbounded varchar and char
        // actually unwrapping didn't happen
        testUnwrap("varchar", "a = CAST('abc' AS char(3))", new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), dataType("char(65536)")), new Cast(new StringLiteral("abc"), dataType("char(65536)"))));
        // unbounded varchar and char of maximum length (could be unwrapped, but currently it is not)
        testNoUnwrap("varchar", EQUAL, new Cast(new StringLiteral("abc"), dataType("char(65536)")), "char(65536)");
    }

    @Test
    public void testNotEquals()
    {
        // representable
        testUnwrap("smallint", "a <> DOUBLE '1'", new ComparisonExpression(NOT_EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "1")));

        testUnwrap("bigint", "a <> DOUBLE '1'", new ComparisonExpression(NOT_EQUAL, new SymbolReference("a"), new GenericLiteral("BIGINT", "1")));

        // non-representable
        testUnwrap("smallint", "a <> DOUBLE '1.1'", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("boolean")))));
        testUnwrap("smallint", "a <> DOUBLE '1.9'", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("boolean")))));

        testUnwrap("smallint", "a <> DOUBLE '1.9'", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("boolean")))));

        testUnwrap("bigint", "a <> DOUBLE '1.1'", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("boolean")))));

        // below top of range
        testUnwrap("smallint", "a <> DOUBLE '32766'", new ComparisonExpression(NOT_EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "32766")));

        // round to top of range
        testUnwrap("smallint", "a <> DOUBLE '32766.9'", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("boolean")))));

        // top of range
        testUnwrap("smallint", "a <> DOUBLE '32767'", new ComparisonExpression(NOT_EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "32767")));

        // above range
        testUnwrap("smallint", "a <> DOUBLE '32768.1'", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("boolean")))));

        // 2^64 constant
        testUnwrap("bigint", "a <> DOUBLE '18446744073709551616'", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("boolean")))));

        // above bottom of range
        testUnwrap("smallint", "a <> DOUBLE '-32767'", new ComparisonExpression(NOT_EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "-32767")));

        // round to bottom of range
        testUnwrap("smallint", "a <> DOUBLE '-32767.9'", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("boolean")))));

        // bottom of range
        testUnwrap("smallint", "a <> DOUBLE '-32768'", new ComparisonExpression(NOT_EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "-32768")));

        // below range
        testUnwrap("smallint", "a <> DOUBLE '-32768.1'", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("boolean")))));
    }

    @Test
    public void testLessThan()
    {
        // representable
        testUnwrap("smallint", "a < DOUBLE '1'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("SMALLINT", "1")));

        testUnwrap("bigint", "a < DOUBLE '1'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("BIGINT", "1")));

        // non-representable
        testUnwrap("smallint", "a < DOUBLE '1.1'", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "1")));

        testUnwrap("bigint", "a < DOUBLE '1.1'", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("BIGINT", "1")));

        testUnwrap("smallint", "a < DOUBLE '1.9'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("SMALLINT", "2")));

        // below top of range
        testUnwrap("smallint", "a < DOUBLE '32766'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("SMALLINT", "32766")));

        // round to top of range
        testUnwrap("smallint", "a < DOUBLE '32766.9'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("SMALLINT", "32767")));

        // top of range
        testUnwrap("smallint", "a < DOUBLE '32767'", new ComparisonExpression(NOT_EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "32767")));

        // above range
        testUnwrap("smallint", "a < DOUBLE '32768.1'", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("boolean")))));

        // above bottom of range
        testUnwrap("smallint", "a < DOUBLE '-32767'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("SMALLINT", "-32767")));

        // round to bottom of range
        testUnwrap("smallint", "a < DOUBLE '-32767.9'", new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "-32768")));

        // bottom of range
        testUnwrap("smallint", "a < DOUBLE '-32768'", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));

        // below range
        testUnwrap("smallint", "a < DOUBLE '-32768.1'", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));

        // -2^64 constant
        testUnwrap("bigint", "a < DOUBLE '-18446744073709551616'", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));
    }

    @Test
    public void testLessThanOrEqual()
    {
        // representable
        testUnwrap("smallint", "a <= DOUBLE '1'", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "1")));

        testUnwrap("bigint", "a <= DOUBLE '1'", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("BIGINT", "1")));

        // non-representable
        testUnwrap("smallint", "a <= DOUBLE '1.1'", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "1")));

        testUnwrap("bigint", "a <= DOUBLE '1.1'", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("BIGINT", "1")));

        testUnwrap("smallint", "a <= DOUBLE '1.9'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("SMALLINT", "2")));

        // below top of range
        testUnwrap("smallint", "a <= DOUBLE '32766'", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "32766")));

        // round to top of range
        testUnwrap("smallint", "a <= DOUBLE '32766.9'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("SMALLINT", "32767")));

        // top of range
        testUnwrap("smallint", "a <= DOUBLE '32767'", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("BOOLEAN")))));

        // above range
        testUnwrap("smallint", "a <= DOUBLE '32768.1'", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("boolean")))));

        // 2^64 constant
        testUnwrap("bigint", "a <= DOUBLE '18446744073709551616'", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("boolean")))));

        // above bottom of range
        testUnwrap("smallint", "a <= DOUBLE '-32767'", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "-32767")));

        // round to bottom of range
        testUnwrap("smallint", "a <= DOUBLE '-32767.9'", new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "-32768")));

        // bottom of range
        testUnwrap("smallint", "a <= DOUBLE '-32768'", new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "-32768")));

        // below range
        testUnwrap("smallint", "a <= DOUBLE '-32768.1'", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));
    }

    @Test
    public void testGreaterThan()
    {
        // representable
        testUnwrap("smallint", "a > DOUBLE '1'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("SMALLINT", "1")));

        testUnwrap("bigint", "a > DOUBLE '1'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("BIGINT", "1")));

        // non-representable
        testUnwrap("smallint", "a > DOUBLE '1.1'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("SMALLINT", "1")));

        testUnwrap("smallint", "a > DOUBLE '1.9'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "2")));

        testUnwrap("bigint", "a > DOUBLE '1.9'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("BIGINT", "2")));

        // below top of range
        testUnwrap("smallint", "a > DOUBLE '32766'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("SMALLINT", "32766")));

        // round to top of range
        testUnwrap("smallint", "a > DOUBLE '32766.9'", new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "32767")));

        // top of range
        testUnwrap("smallint", "a > DOUBLE '32767'", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));

        // above range
        testUnwrap("smallint", "a > DOUBLE '32768.1'", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));

        // 2^64 constant
        testUnwrap("bigint", "a > DOUBLE '18446744073709551616'", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));

        // above bottom of range
        testUnwrap("smallint", "a > DOUBLE '-32767'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("SMALLINT", "-32767")));

        // round to bottom of range
        testUnwrap("smallint", "a > DOUBLE '-32767.9'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("SMALLINT", "-32768")));

        // bottom of range
        testUnwrap("smallint", "a > DOUBLE '-32768'", new ComparisonExpression(NOT_EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "-32768")));

        // below range
        testUnwrap("smallint", "a > DOUBLE '-32768.1'", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("boolean")))));
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        // representable
        testUnwrap("smallint", "a >= DOUBLE '1'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "1")));

        testUnwrap("bigint", "a >= DOUBLE '1'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("BIGINT", "1")));

        // non-representable
        testUnwrap("smallint", "a >= DOUBLE '1.1'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("SMALLINT", "1")));

        testUnwrap("bigint", "a >= DOUBLE '1.1'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("BIGINT", "1")));

        testUnwrap("smallint", "a >= DOUBLE '1.9'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "2")));

        // below top of range
        testUnwrap("smallint", "a >= DOUBLE '32766'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "32766")));

        // round to top of range
        testUnwrap("smallint", "a >= DOUBLE '32766.9'", new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "32767")));

        // top of range
        testUnwrap("smallint", "a >= DOUBLE '32767'", new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "32767")));

        // above range
        testUnwrap("smallint", "a >= DOUBLE '32768.1'", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));

        // above bottom of range
        testUnwrap("smallint", "a >= DOUBLE '-32767'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "-32767")));

        // round to bottom of range
        testUnwrap("smallint", "a >= DOUBLE '-32767.9'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("SMALLINT", "-32768")));

        // bottom of range
        testUnwrap("smallint", "a >= DOUBLE '-32768'", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("boolean")))));

        // below range
        testUnwrap("smallint", "a >= DOUBLE '-32768.1'", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("boolean")))));

        // -2^64 constant
        testUnwrap("bigint", "a >= DOUBLE '-18446744073709551616'", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("boolean")))));
    }

    @Test
    public void testDistinctFrom()
    {
        // representable
        testUnwrap("smallint", "a IS DISTINCT FROM DOUBLE '1'", new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("a"), new GenericLiteral("SMALLINT", "1")));

        testUnwrap("bigint", "a IS DISTINCT FROM DOUBLE '1'", new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("a"), new GenericLiteral("BIGINT", "1")));

        // non-representable
        testRemoveFilter("smallint", "a IS DISTINCT FROM DOUBLE '1.1'");

        testRemoveFilter("smallint", "a IS DISTINCT FROM DOUBLE '1.9'");

        testRemoveFilter("bigint", "a IS DISTINCT FROM DOUBLE '1.9'");

        // below top of range
        testUnwrap("smallint", "a IS DISTINCT FROM DOUBLE '32766'", new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("a"), new GenericLiteral("SMALLINT", "32766")));

        // round to top of range
        testRemoveFilter("smallint", "a IS DISTINCT FROM DOUBLE '32766.9'");

        // top of range
        testUnwrap("smallint", "a IS DISTINCT FROM DOUBLE '32767'", new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("a"), new GenericLiteral("SMALLINT", "32767")));

        // above range
        testRemoveFilter("smallint", "a IS DISTINCT FROM DOUBLE '32768.1'");

        // 2^64 constant
        testRemoveFilter("bigint", "a IS DISTINCT FROM DOUBLE '18446744073709551616'");

        // above bottom of range
        testUnwrap("smallint", "a IS DISTINCT FROM DOUBLE '-32767'", new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("a"), new GenericLiteral("SMALLINT", "-32767")));

        // round to bottom of range
        testRemoveFilter("smallint", "a IS DISTINCT FROM DOUBLE '-32767.9'");

        // bottom of range
        testUnwrap("smallint", "a IS DISTINCT FROM DOUBLE '-32768'", new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("a"), new GenericLiteral("SMALLINT", "-32768")));

        // below range
        testRemoveFilter("smallint", "a IS DISTINCT FROM DOUBLE '-32768.1'");
    }

    @Test
    public void testNull()
    {
        testUnwrap("smallint", "a = CAST(NULL AS DOUBLE)", new Cast(new NullLiteral(), dataType("boolean")));

        testUnwrap("bigint", "a = CAST(NULL AS DOUBLE)", new Cast(new NullLiteral(), dataType("boolean")));

        testUnwrap("smallint", "a <> CAST(NULL AS DOUBLE)", new Cast(new NullLiteral(), dataType("boolean")));

        testUnwrap("smallint", "a > CAST(NULL AS DOUBLE)", new Cast(new NullLiteral(), dataType("boolean")));

        testUnwrap("smallint", "a < CAST(NULL AS DOUBLE)", new Cast(new NullLiteral(), dataType("boolean")));

        testUnwrap("smallint", "a >= CAST(NULL AS DOUBLE)", new Cast(new NullLiteral(), dataType("boolean")));

        testUnwrap("smallint", "a <= CAST(NULL AS DOUBLE)", new Cast(new NullLiteral(), dataType("boolean")));

        testUnwrap("smallint", "a IS DISTINCT FROM CAST(NULL AS DOUBLE)", new NotExpression(new IsNullPredicate(new Cast(new SymbolReference("a"), dataType("double")))));

        testUnwrap("bigint", "a IS DISTINCT FROM CAST(NULL AS DOUBLE)", new NotExpression(new IsNullPredicate(new Cast(new SymbolReference("a"), dataType("double")))));
    }

    @Test
    public void testNaN()
    {
        testUnwrap("smallint", "a = nan()", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));

        testUnwrap("bigint", "a = nan()", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));

        testUnwrap("smallint", "a < nan()", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));

        testUnwrap("smallint", "a <> nan()", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("boolean")))));

        testRemoveFilter("smallint", "a IS DISTINCT FROM nan()");

        testRemoveFilter("bigint", "a IS DISTINCT FROM nan()");

        testUnwrap("real", "a = nan()", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));

        testUnwrap("real", "a < nan()", new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new Cast(new NullLiteral(), dataType("boolean")))));

        testUnwrap("real", "a <> nan()", new LogicalExpression(OR, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new Cast(new NullLiteral(), dataType("boolean")))));

        testUnwrap("real", "a IS DISTINCT FROM nan()", new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("a"), new Cast(new FunctionCall(QualifiedName.of("nan"), ImmutableList.of()), dataType("real"))));
    }

    @Test
    public void smokeTests()
    {
        // smoke tests for various type combinations
        for (String type : asList("SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE")) {
            testUnwrap("tinyint", format("a = %s '1'", type), new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("TINYINT", "1")));
        }

        for (String type : asList("INTEGER", "BIGINT", "REAL", "DOUBLE")) {
            testUnwrap("smallint", format("a = %s '1'", type), new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("SMALLINT", "1")));
        }

        for (String type : asList("BIGINT", "DOUBLE")) {
            testUnwrap("integer", format("a = %s '1'", type), new ComparisonExpression(EQUAL, new SymbolReference("a"), new LongLiteral("1")));
        }

        testUnwrap("real", "a = DOUBLE '1'", new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("REAL", "1.0")));
    }

    @Test
    public void testTermOrder()
    {
        // ensure the optimization works when the terms of the comparison are reversed
        // vs the canonical <expr> <op> <literal> form
        assertPlan("SELECT * FROM (VALUES REAL '1') t(a) WHERE DOUBLE '1' = a",
                output(
                        filter(
                                new ComparisonExpression(EQUAL, new SymbolReference("A"), new GenericLiteral("REAL", "1.0")),
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
        testUnwrap(utcSession, "date", "a > TIMESTAMP '2020-10-26 11:02:18 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "2020-10-26")));
        testUnwrap(warsawSession, "date", "a > TIMESTAMP '2020-10-26 11:02:18 Europe/Warsaw'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "2020-10-26")));
        testUnwrap(losAngelesSession, "date", "a > TIMESTAMP '2020-10-26 11:02:18 America/Los_Angeles'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "2020-10-26")));

        // different zone
        testUnwrap(warsawSession, "date", "a > TIMESTAMP '2020-10-26 11:02:18 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "2020-10-26")));
        testUnwrap(losAngelesSession, "date", "a > TIMESTAMP '2020-10-26 11:02:18 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "2020-10-26")));

        // maximum precision
        testUnwrap(warsawSession, "date", "a > TIMESTAMP '2020-10-26 11:02:18.123456789321 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "2020-10-26")));
        testUnwrap(losAngelesSession, "date", "a > TIMESTAMP '2020-10-26 11:02:18.123456789321 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "2020-10-26")));

        // DST forward -- Warsaw changed clock 1h forward on 2020-03-29T01:00 UTC (2020-03-29T02:00 local time)
        // Note that in given session input TIMESTAMP values  2020-03-29 02:31 and 2020-03-29 03:31 produce the same value 2020-03-29 01:31 UTC (conversion is not monotonic)
        // last before
        testUnwrap(warsawSession, "date", "a > TIMESTAMP '2020-03-29 00:59:59 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "2020-03-29")));
        testUnwrap(warsawSession, "date", "a > TIMESTAMP '2020-03-29 00:59:59.999 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "2020-03-29")));
        testUnwrap(warsawSession, "date", "a > TIMESTAMP '2020-03-29 00:59:59.13 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "2020-03-29")));
        testUnwrap(warsawSession, "date", "a > TIMESTAMP '2020-03-29 00:59:59.999999 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "2020-03-29")));
        testUnwrap(warsawSession, "date", "a > TIMESTAMP '2020-03-29 00:59:59.999999999 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "2020-03-29")));
        testUnwrap(warsawSession, "date", "a > TIMESTAMP '2020-03-29 00:59:59.999999999999 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "2020-03-29")));

        // equal
        testUnwrap(utcSession, "date", "a = TIMESTAMP '1981-06-22 00:00:00 UTC'", new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a = TIMESTAMP '1981-06-22 00:00:00.000000 UTC'", new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a = TIMESTAMP '1981-06-22 00:00:00.000000000 UTC'", new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a = TIMESTAMP '1981-06-22 00:00:00.000000000000 UTC'", new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));

        // not equal
        testUnwrap(utcSession, "date", "a <> TIMESTAMP '1981-06-22 00:00:00 UTC'", new ComparisonExpression(NOT_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a <> TIMESTAMP '1981-06-22 00:00:00.000000 UTC'", new ComparisonExpression(NOT_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a <> TIMESTAMP '1981-06-22 00:00:00.000000000 UTC'", new ComparisonExpression(NOT_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a <> TIMESTAMP '1981-06-22 00:00:00.000000000000 UTC'", new ComparisonExpression(NOT_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));

        // less than
        testUnwrap(utcSession, "date", "a < TIMESTAMP '1981-06-22 00:00:00 UTC'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a < TIMESTAMP '1981-06-22 00:00:00.000000 UTC'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a < TIMESTAMP '1981-06-22 00:00:00.000000000 UTC'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a < TIMESTAMP '1981-06-22 00:00:00.000000000000 UTC'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));

        // less than or equal
        testUnwrap(utcSession, "date", "a <= TIMESTAMP '1981-06-22 00:00:00 UTC'", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a <= TIMESTAMP '1981-06-22 00:00:00.000000 UTC'", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a <= TIMESTAMP '1981-06-22 00:00:00.000000000 UTC'", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a <= TIMESTAMP '1981-06-22 00:00:00.000000000000 UTC'", new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));

        // greater than
        testUnwrap(utcSession, "date", "a > TIMESTAMP '1981-06-22 00:00:00 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a > TIMESTAMP '1981-06-22 00:00:00.000000 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a > TIMESTAMP '1981-06-22 00:00:00.000000000 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a > TIMESTAMP '1981-06-22 00:00:00.000000000000 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));

        // greater than or equal
        testUnwrap(utcSession, "date", "a >= TIMESTAMP '1981-06-22 00:00:00 UTC'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a >= TIMESTAMP '1981-06-22 00:00:00.000000 UTC'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a >= TIMESTAMP '1981-06-22 00:00:00.000000000 UTC'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a >= TIMESTAMP '1981-06-22 00:00:00.000000000000 UTC'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));

        // is distinct
        testUnwrap(utcSession, "date", "a IS DISTINCT FROM TIMESTAMP '1981-06-22 00:00:00 UTC'", new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a IS DISTINCT FROM TIMESTAMP '1981-06-22 00:00:00.000000 UTC'", new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a IS DISTINCT FROM TIMESTAMP '1981-06-22 00:00:00.000000000 UTC'", new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
        testUnwrap(utcSession, "date", "a IS DISTINCT FROM TIMESTAMP '1981-06-22 00:00:00.000000000000 UTC'", new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));

        // is not distinct
        testUnwrap(utcSession, "date", "a IS NOT DISTINCT FROM TIMESTAMP '1981-06-22 00:00:00 UTC'", new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22"))));
        testUnwrap(utcSession, "date", "a IS NOT DISTINCT FROM TIMESTAMP '1981-06-22 00:00:00.000000 UTC'", new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22"))));
        testUnwrap(utcSession, "date", "a IS NOT DISTINCT FROM TIMESTAMP '1981-06-22 00:00:00.000000000 UTC'", new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22"))));
        testUnwrap(utcSession, "date", "a IS NOT DISTINCT FROM TIMESTAMP '1981-06-22 00:00:00.000000000000 UTC'", new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22"))));

        // null date literal
        testUnwrap("date", "CAST(a AS TIMESTAMP WITH TIME ZONE) = NULL", new Cast(new NullLiteral(), dataType("boolean")));
        testUnwrap("date", "CAST(a AS TIMESTAMP WITH TIME ZONE) < NULL", new Cast(new NullLiteral(), dataType("boolean")));
        testUnwrap("date", "CAST(a AS TIMESTAMP WITH TIME ZONE) <= NULL", new Cast(new NullLiteral(), dataType("boolean")));
        testUnwrap("date", "CAST(a AS TIMESTAMP WITH TIME ZONE) > NULL", new Cast(new NullLiteral(), dataType("boolean")));
        testUnwrap("date", "CAST(a AS TIMESTAMP WITH TIME ZONE) >= NULL", new Cast(new NullLiteral(), dataType("boolean")));
        testUnwrap("date", "CAST(a AS TIMESTAMP WITH TIME ZONE) IS DISTINCT FROM NULL", new NotExpression(new IsNullPredicate(new Cast(new SymbolReference("a"), dataType("timestamp with time zone")))));

        // timestamp with time zone value on the left
        testUnwrap(utcSession, "date", "TIMESTAMP '1981-06-22 00:00:00 UTC' = a", new ComparisonExpression(EQUAL, new SymbolReference("a"), new GenericLiteral("DATE", "1981-06-22")));
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
        testUnwrap(utcSession, "timestamp(0)", "a > TIMESTAMP '2020-10-26 11:02:18 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-26 11:02:18")));
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-10-26 11:02:18 Europe/Warsaw'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-26 11:02:18")));
        testUnwrap(losAngelesSession, "timestamp(0)", "a > TIMESTAMP '2020-10-26 11:02:18 America/Los_Angeles'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-26 11:02:18")));

        // different zone
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-10-26 11:02:18 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-26 12:02:18")));
        testUnwrap(losAngelesSession, "timestamp(0)", "a > TIMESTAMP '2020-10-26 11:02:18 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-26 04:02:18")));

        // short timestamp, short timestamp with time zone being coerced to long timestamp with time zone
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-10-26 11:02:18.12 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-26 12:02:18.120000")));
        testUnwrap(losAngelesSession, "timestamp(6)", "a > TIMESTAMP '2020-10-26 11:02:18.12 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-26 04:02:18.120000")));

        // long timestamp, short timestamp with time zone being coerced to long timestamp with time zone
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-10-26 11:02:18.12 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-26 12:02:18.120000000")));
        testUnwrap(losAngelesSession, "timestamp(9)", "a > TIMESTAMP '2020-10-26 11:02:18.12 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-26 04:02:18.120000000")));

        // long timestamp, long timestamp with time zone
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-10-26 11:02:18.123456 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-26 12:02:18.123456000")));
        testUnwrap(losAngelesSession, "timestamp(9)", "a > TIMESTAMP '2020-10-26 11:02:18.123456 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-26 04:02:18.123456000")));

        // maximum precision
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-10-26 11:02:18.123456789321 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-26 12:02:18.123456789321")));
        testUnwrap(losAngelesSession, "timestamp(12)", "a > TIMESTAMP '2020-10-26 11:02:18.123456789321 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-26 04:02:18.123456789321")));

        // DST forward -- Warsaw changed clock 1h forward on 2020-03-29T01:00 UTC (2020-03-29T02:00 local time)
        // Note that in given session input TIMESTAMP values  2020-03-29 02:31 and 2020-03-29 03:31 produce the same value 2020-03-29 01:31 UTC (conversion is not monotonic)
        // last before
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-03-29 00:59:59 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-03-29 01:59:59")));
        testUnwrap(warsawSession, "timestamp(3)", "a > TIMESTAMP '2020-03-29 00:59:59.999 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-03-29 01:59:59.999")));
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-03-29 00:59:59.13 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-03-29 01:59:59.130000")));
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-03-29 00:59:59.999999 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-03-29 01:59:59.999999")));
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-03-29 00:59:59.999999999 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-03-29 01:59:59.999999999")));
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-03-29 00:59:59.999999999999 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-03-29 01:59:59.999999999999")));
        // first within
        testNoUnwrap(warsawSession, "timestamp(0)", GREATER_THAN, new GenericLiteral("TIMESTAMP", "2020-03-29 01:00:00 UTC"), "timestamp(0) with time zone");
        testNoUnwrap(warsawSession, "timestamp(3)", GREATER_THAN, new GenericLiteral("TIMESTAMP", "2020-03-29 01:00:00.000 UTC"), "timestamp(3) with time zone");
        testNoUnwrap(warsawSession, "timestamp(6)", GREATER_THAN, new GenericLiteral("TIMESTAMP", "2020-03-29 01:00:00.000000 UTC"), "timestamp(6) with time zone");
        testNoUnwrap(warsawSession, "timestamp(9)", GREATER_THAN, new GenericLiteral("TIMESTAMP", "2020-03-29 01:00:00.000000000 UTC"), "timestamp(9) with time zone");
        testNoUnwrap(warsawSession, "timestamp(12)", GREATER_THAN, new GenericLiteral("TIMESTAMP", "2020-03-29 01:00:00.000000000000 UTC"), "timestamp(12) with time zone");
        // last within
        testNoUnwrap(warsawSession, "timestamp(0)", GREATER_THAN, new GenericLiteral("TIMESTAMP", "2020-03-29 01:59:59 UTC"), "timestamp(0) with time zone");
        testNoUnwrap(warsawSession, "timestamp(3)", GREATER_THAN, new GenericLiteral("TIMESTAMP", "2020-03-29 01:59:59.999 UTC"), "timestamp(3) with time zone");
        testNoUnwrap(warsawSession, "timestamp(6)", GREATER_THAN, new GenericLiteral("TIMESTAMP", "2020-03-29 01:59:59.999999 UTC"), "timestamp(6) with time zone");
        testNoUnwrap(warsawSession, "timestamp(9)", GREATER_THAN, new GenericLiteral("TIMESTAMP", "2020-03-29 01:59:59.999999999 UTC"), "timestamp(9) with time zone");
        testNoUnwrap(warsawSession, "timestamp(12)", GREATER_THAN, new GenericLiteral("TIMESTAMP", "2020-03-29 01:59:59.999999999999 UTC"), "timestamp(12) with time zone");
        // first after
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-03-29 02:00:00 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-03-29 04:00:00")));
        testUnwrap(warsawSession, "timestamp(3)", "a > TIMESTAMP '2020-03-29 02:00:00.000 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-03-29 04:00:00.000")));
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-03-29 02:00:00.000000 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-03-29 04:00:00.000000")));
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-03-29 02:00:00.000000000 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-03-29 04:00:00.000000000")));
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-03-29 02:00:00.000000000000 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-03-29 04:00:00.000000000000")));

        // DST backward -- Warsaw changed clock 1h backward on 2020-10-25T01:00 UTC (2020-03-29T03:00 local time)
        // Note that in given session no input TIMESTAMP value can produce TIMESTAMP WITH TIME ZONE within [2020-10-25 00:00:00 UTC, 2020-10-25 01:00:00 UTC], so '>=' is OK
        // last before
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-10-25 00:59:59 UTC'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 02:59:59")));
        testUnwrap(warsawSession, "timestamp(3)", "a > TIMESTAMP '2020-10-25 00:59:59.999 UTC'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 02:59:59.999")));
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-10-25 00:59:59.999999 UTC'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 02:59:59.999999")));
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-10-25 00:59:59.999999999 UTC'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 02:59:59.999999999")));
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-10-25 00:59:59.999999999999 UTC'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 02:59:59.999999999999")));
        // first within
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-10-25 01:00:00 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 02:00:00")));
        testUnwrap(warsawSession, "timestamp(3)", "a > TIMESTAMP '2020-10-25 01:00:00.000 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 02:00:00.000")));
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-10-25 01:00:00.000000 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 02:00:00.000000")));
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-10-25 01:00:00.000000000 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 02:00:00.000000000")));
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-10-25 01:00:00.000000000000 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 02:00:00.000000000000")));
        // last within
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-10-25 01:59:59 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 02:59:59")));
        testUnwrap(warsawSession, "timestamp(3)", "a > TIMESTAMP '2020-10-25 01:59:59.999 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 02:59:59.999")));
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-10-25 01:59:59.999999 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 02:59:59.999999")));
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-10-25 01:59:59.999999999 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 02:59:59.999999999")));
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-10-25 01:59:59.999999999999 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 02:59:59.999999999999")));
        // first after
        testUnwrap(warsawSession, "timestamp(0)", "a > TIMESTAMP '2020-10-25 02:00:00 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 03:00:00")));
        testUnwrap(warsawSession, "timestamp(3)", "a > TIMESTAMP '2020-10-25 02:00:00.000 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 03:00:00.000")));
        testUnwrap(warsawSession, "timestamp(6)", "a > TIMESTAMP '2020-10-25 02:00:00.000000 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 03:00:00.000000")));
        testUnwrap(warsawSession, "timestamp(9)", "a > TIMESTAMP '2020-10-25 02:00:00.000000000 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 03:00:00.000000000")));
        testUnwrap(warsawSession, "timestamp(12)", "a > TIMESTAMP '2020-10-25 02:00:00.000000000000 UTC'", new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "2020-10-25 03:00:00.000000000000")));
    }

    @Test
    public void testNoEffect()
    {
        // BIGINT->DOUBLE implicit cast is not injective if the double constant is >= 2^53 and <= double(2^63 - 1)
        testUnwrap("bigint", "a = DOUBLE '9007199254740992'", new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), dataType("double")), new DoubleLiteral("9.007199254740992E15")));

        testUnwrap("bigint", "a = DOUBLE '9223372036854775807'", new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), dataType("double")), new DoubleLiteral("9.223372036854776E18")));

        // BIGINT->DOUBLE implicit cast is not injective if the double constant is <= -2^53 and >= double(-2^63 + 1)
        testUnwrap("bigint", "a = DOUBLE '-9007199254740992'", new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), dataType("double")), new DoubleLiteral("-9.007199254740992E15")));

        testUnwrap("bigint", "a = DOUBLE '-9223372036854775807'", new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), dataType("double")), new DoubleLiteral("-9.223372036854776E18")));

        // BIGINT->REAL implicit cast is not injective if the real constant is >= 2^23 and <= real(2^63 - 1)
        testUnwrap("bigint", "a = REAL '8388608'", new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), dataType("real")), new GenericLiteral("REAL", "8388608.0")));

        testUnwrap("bigint", "a = REAL '9223372036854775807'", new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), dataType("real")), new GenericLiteral("REAL", "9.223372E18")));

        // BIGINT->REAL implicit cast is not injective if the real constant is <= -2^23 and >= real(-2^63 + 1)
        testUnwrap("bigint", "a = REAL '-8388608'", new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), dataType("real")), new GenericLiteral("REAL", "-8388608.0")));

        testUnwrap("bigint", "a = REAL '-9223372036854775807'", new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), dataType("real")), new GenericLiteral("REAL", "-9.223372E18")));

        // INTEGER->REAL implicit cast is not injective if the real constant is >= 2^23 and <= 2^31 - 1
        testUnwrap("integer", "a = REAL '8388608'", new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), dataType("real")), new GenericLiteral("REAL", "8388608.0")));

        testUnwrap("integer", "a = REAL '2147483647'", new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), dataType("real")), new GenericLiteral("REAL", "2.1474836E9")));

        // INTEGER->REAL implicit cast is not injective if the real constant is <= -2^23 and >= -2^31 + 1
        testUnwrap("integer", "a = REAL '-8388608'", new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), dataType("real")), new GenericLiteral("REAL", "-8388608.0")));

        testUnwrap("integer", "a = REAL '-2147483647'", new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), dataType("real")), new GenericLiteral("REAL", "-2.1474836E9")));

        // DECIMAL(p)->DOUBLE not injective for p > 15
        testUnwrap("decimal(16)", "a = DOUBLE '1'", new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), dataType("double")), new DoubleLiteral("1.0")));

        // DECIMAL(p)->REAL not injective for p > 7
        testUnwrap("decimal(8)", "a = REAL '1'", new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), dataType("real")), new GenericLiteral("REAL", "1.0")));

        // no implicit cast between VARCHAR->INTEGER
        testUnwrap("varchar", "CAST(a AS INTEGER) = INTEGER '1'", new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), dataType("integer")), new LongLiteral("1")));

        // no implicit cast between DOUBLE->INTEGER
        testUnwrap("double", "CAST(a AS INTEGER) = INTEGER '1'", new ComparisonExpression(EQUAL, new Cast(new SymbolReference("a"), dataType("integer")), new LongLiteral("1")));
    }

    @Test
    public void testUnwrapCastTimestampAsDate()
    {
        // equal
        testUnwrap("timestamp(3)", "CAST(a AS DATE) = DATE '1981-06-22'", new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000")))));
        testUnwrap("timestamp(6)", "CAST(a AS DATE) = DATE '1981-06-22'", new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000")))));
        testUnwrap("timestamp(9)", "CAST(a AS DATE) = DATE '1981-06-22'", new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000")))));
        testUnwrap("timestamp(12)", "CAST(a AS DATE) = DATE '1981-06-22'", new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000000")))));

        // not equal
        testUnwrap("timestamp(3)", "CAST(a AS DATE) <> DATE '1981-06-22'", new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000")))));
        testUnwrap("timestamp(6)", "CAST(a AS DATE) <> DATE '1981-06-22'", new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000")))));
        testUnwrap("timestamp(9)", "CAST(a AS DATE) <> DATE '1981-06-22'", new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000")))));
        testUnwrap("timestamp(12)", "CAST(a AS DATE) <> DATE '1981-06-22'", new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000000")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000000")))));

        // less than
        testUnwrap("timestamp(3)", "CAST(a AS DATE) < DATE '1981-06-22'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000")));
        testUnwrap("timestamp(6)", "CAST(a AS DATE) < DATE '1981-06-22'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000")));
        testUnwrap("timestamp(9)", "CAST(a AS DATE) < DATE '1981-06-22'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000")));
        testUnwrap("timestamp(12)", "CAST(a AS DATE) < DATE '1981-06-22'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000000")));

        // less than or equal
        testUnwrap("timestamp(3)", "CAST(a AS DATE) <= DATE '1981-06-22'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000")));
        testUnwrap("timestamp(6)", "CAST(a AS DATE) <= DATE '1981-06-22'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000")));
        testUnwrap("timestamp(9)", "CAST(a AS DATE) <= DATE '1981-06-22'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000")));
        testUnwrap("timestamp(12)", "CAST(a AS DATE) <= DATE '1981-06-22'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000000")));

        // greater than
        testUnwrap("timestamp(3)", "CAST(a AS DATE) > DATE '1981-06-22'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000")));
        testUnwrap("timestamp(6)", "CAST(a AS DATE) > DATE '1981-06-22'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000")));
        testUnwrap("timestamp(9)", "CAST(a AS DATE) > DATE '1981-06-22'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000")));
        testUnwrap("timestamp(12)", "CAST(a AS DATE) > DATE '1981-06-22'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000000")));

        // greater than or equal
        testUnwrap("timestamp(3)", "CAST(a AS DATE) >= DATE '1981-06-22'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000")));
        testUnwrap("timestamp(6)", "CAST(a AS DATE) >= DATE '1981-06-22'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000")));
        testUnwrap("timestamp(9)", "CAST(a AS DATE) >= DATE '1981-06-22'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000")));
        testUnwrap("timestamp(12)", "CAST(a AS DATE) >= DATE '1981-06-22'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000000")));

        // is distinct
        testUnwrap("timestamp(3)", "CAST(a AS DATE) IS DISTINCT FROM DATE '1981-06-22'", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000")))));
        testUnwrap("timestamp(6)", "CAST(a AS DATE) IS DISTINCT FROM DATE '1981-06-22'", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000")))));
        testUnwrap("timestamp(9)", "CAST(a AS DATE) IS DISTINCT FROM DATE '1981-06-22'", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000")))));
        testUnwrap("timestamp(12)", "CAST(a AS DATE) IS DISTINCT FROM DATE '1981-06-22'", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000000")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000000")))));

        // is not distinct
        testUnwrap("timestamp(3)", "CAST(a AS DATE) IS NOT DISTINCT FROM DATE '1981-06-22'", new LogicalExpression(AND, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000")))));
        testUnwrap("timestamp(6)", "CAST(a AS DATE) IS NOT DISTINCT FROM DATE '1981-06-22'", new LogicalExpression(AND, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000")))));
        testUnwrap("timestamp(9)", "CAST(a AS DATE) IS NOT DISTINCT FROM DATE '1981-06-22'", new LogicalExpression(AND, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000")))));
        testUnwrap("timestamp(12)", "CAST(a AS DATE) IS NOT DISTINCT FROM DATE '1981-06-22'", new LogicalExpression(AND, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000000")))));

        // null date literal
        testUnwrap("timestamp(3)", "CAST(a AS DATE) = NULL", new Cast(new NullLiteral(), dataType("boolean")));
        testUnwrap("timestamp(3)", "CAST(a AS DATE) < NULL", new Cast(new NullLiteral(), dataType("boolean")));
        testUnwrap("timestamp(3)", "CAST(a AS DATE) <= NULL", new Cast(new NullLiteral(), dataType("boolean")));
        testUnwrap("timestamp(3)", "CAST(a AS DATE) > NULL", new Cast(new NullLiteral(), dataType("boolean")));
        testUnwrap("timestamp(3)", "CAST(a AS DATE) >= NULL", new Cast(new NullLiteral(), dataType("boolean")));
        testUnwrap("timestamp(3)", "CAST(a AS DATE) IS DISTINCT FROM NULL", new NotExpression(new IsNullPredicate(new Cast(new SymbolReference("a"), dataType("date")))));

        // non-optimized expression on the right
        testUnwrap("timestamp(3)", "CAST(a AS DATE) = DATE '1981-06-22' + INTERVAL '2' DAY", new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-24 00:00:00.000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-25 00:00:00.000")))));

        // cast on the right
        testUnwrap("timestamp(3)", "DATE '1981-06-22' = CAST(a AS DATE)", new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000")))));
    }

    @Test
    public void testUnwrapConvertTimestatmpToDate()
    {
        // equal
        testUnwrap("timestamp(3)", "date(a) = DATE '1981-06-22'", new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000")))));
        testUnwrap("timestamp(6)", "date(a) = DATE '1981-06-22'", new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000")))));
        testUnwrap("timestamp(9)", "date(a) = DATE '1981-06-22'", new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000")))));
        testUnwrap("timestamp(12)", "date(a) = DATE '1981-06-22'", new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000000")))));

        // not equal
        testUnwrap("timestamp(3)", "date(a) <> DATE '1981-06-22'", new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000")))));
        testUnwrap("timestamp(6)", "date(a) <> DATE '1981-06-22'", new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000")))));
        testUnwrap("timestamp(9)", "date(a) <> DATE '1981-06-22'", new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000")))));
        testUnwrap("timestamp(12)", "date(a) <> DATE '1981-06-22'", new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000000")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000000")))));

        // less than
        testUnwrap("timestamp(3)", "date(a) < DATE '1981-06-22'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000")));
        testUnwrap("timestamp(6)", "date(a) < DATE '1981-06-22'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000")));
        testUnwrap("timestamp(9)", "date(a) < DATE '1981-06-22'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000")));
        testUnwrap("timestamp(12)", "date(a) < DATE '1981-06-22'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000000")));

        // less than or equal
        testUnwrap("timestamp(3)", "date(a) <= DATE '1981-06-22'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000")));
        testUnwrap("timestamp(6)", "date(a) <= DATE '1981-06-22'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000")));
        testUnwrap("timestamp(9)", "date(a) <= DATE '1981-06-22'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000")));
        testUnwrap("timestamp(12)", "date(a) <= DATE '1981-06-22'", new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000000")));

        // greater than
        testUnwrap("timestamp(3)", "date(a) > DATE '1981-06-22'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000")));
        testUnwrap("timestamp(6)", "date(a) > DATE '1981-06-22'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000")));
        testUnwrap("timestamp(9)", "date(a) > DATE '1981-06-22'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000")));
        testUnwrap("timestamp(12)", "date(a) > DATE '1981-06-22'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000000")));

        // greater than or equal
        testUnwrap("timestamp(3)", "date(a) >= DATE '1981-06-22'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000")));
        testUnwrap("timestamp(6)", "date(a) >= DATE '1981-06-22'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000")));
        testUnwrap("timestamp(9)", "date(a) >= DATE '1981-06-22'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000")));
        testUnwrap("timestamp(12)", "date(a) >= DATE '1981-06-22'", new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000000")));

        // is distinct
        testUnwrap("timestamp(3)", "date(a) IS DISTINCT FROM DATE '1981-06-22'", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000")))));
        testUnwrap("timestamp(6)", "date(a) IS DISTINCT FROM DATE '1981-06-22'", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000")))));
        testUnwrap("timestamp(9)", "date(a) IS DISTINCT FROM DATE '1981-06-22'", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000")))));
        testUnwrap("timestamp(12)", "date(a) IS DISTINCT FROM DATE '1981-06-22'", new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000000")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000000")))));

        // is not distinct
        testUnwrap("timestamp(3)", "date(a) IS NOT DISTINCT FROM DATE '1981-06-22'", new LogicalExpression(AND, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000")))));
        testUnwrap("timestamp(6)", "date(a) IS NOT DISTINCT FROM DATE '1981-06-22'", new LogicalExpression(AND, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000")))));
        testUnwrap("timestamp(9)", "date(a) IS NOT DISTINCT FROM DATE '1981-06-22'", new LogicalExpression(AND, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000")))));
        testUnwrap("timestamp(12)", "date(a) IS NOT DISTINCT FROM DATE '1981-06-22'", new LogicalExpression(AND, ImmutableList.of(new NotExpression(new IsNullPredicate(new SymbolReference("a"))), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000000000000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000000000000")))));

        // null date literal
        testUnwrap("timestamp(3)", "date(a) = NULL", new Cast(new NullLiteral(), dataType("boolean")));
        testUnwrap("timestamp(3)", "date(a) < NULL", new Cast(new NullLiteral(), dataType("boolean")));
        testUnwrap("timestamp(3)", "date(a) <= NULL", new Cast(new NullLiteral(), dataType("boolean")));
        testUnwrap("timestamp(3)", "date(a) > NULL", new Cast(new NullLiteral(), dataType("boolean")));
        testUnwrap("timestamp(3)", "date(a) >= NULL", new Cast(new NullLiteral(), dataType("boolean")));
        testUnwrap("timestamp(3)", "date(a) IS DISTINCT FROM NULL", new NotExpression(new IsNullPredicate(new Cast(new SymbolReference("a"), dataType("date")))));

        // non-optimized expression on the right
        testUnwrap("timestamp(3)", "date(a) = DATE '1981-06-22' + INTERVAL '2' DAY", new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-24 00:00:00.000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-25 00:00:00.000")))));

        // cast on the right
        testUnwrap("timestamp(3)", "DATE '1981-06-22' = date(a)", new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-22 00:00:00.000")), new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new GenericLiteral("TIMESTAMP", "1981-06-23 00:00:00.000")))));
    }

    private void testNoUnwrap(String inputType, ComparisonExpression.Operator operator, Expression inputPredicate, String expectedCastType)
    {
        testNoUnwrap(getPlanTester().getDefaultSession(), inputType, operator, inputPredicate, expectedCastType);
    }

    private void testNoUnwrap(Session session, String inputType, ComparisonExpression.Operator operator, Expression inputPredicate, String expectedCastType)
    {
        assertPlan(format("SELECT * FROM (VALUES CAST(NULL AS %s)) t(a) WHERE a %s %s", inputType, operator.getValue(), inputPredicate.toString()),
                session,
                output(
                        filter(
                                new ComparisonExpression(operator, new Cast(new SymbolReference("a"), dataType(expectedCastType)), inputPredicate),
                                values("a"))));
    }

    private void testRemoveFilter(String inputType, String inputPredicate)
    {
        assertPlan(format("SELECT * FROM (VALUES CAST(NULL AS %s)) t(a) WHERE %s AND rand() = 42", inputType, inputPredicate),
                output(
                        filter(new ComparisonExpression(EQUAL, new FunctionCall(QualifiedName.of("random"), ImmutableList.of()), new DoubleLiteral("42.0")),
                                values("a"))));
    }

    private void testUnwrap(String inputType, String inputPredicate, Expression expectedPredicate)
    {
        testUnwrap(getPlanTester().getDefaultSession(), inputType, inputPredicate, expectedPredicate);
    }

    private void testUnwrap(Session session, String inputType, String inputPredicate, Expression expected)
    {
        Expression antiOptimization = new ComparisonExpression(EQUAL, new FunctionCall(QualifiedName.of("random"), ImmutableList.of()), new DoubleLiteral("42.0"));
        if (expected instanceof LogicalExpression logical && logical.getOperator() == OR) {
            expected = new LogicalExpression(OR, ImmutableList.<Expression>builder()
                    .addAll(logical.getTerms())
                    .add(antiOptimization)
                    .build());
        }
        else {
            expected = new LogicalExpression(OR, ImmutableList.of(expected, antiOptimization));
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
