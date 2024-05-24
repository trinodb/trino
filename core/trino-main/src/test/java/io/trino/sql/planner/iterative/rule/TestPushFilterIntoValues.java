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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

class TestPushFilterIntoValues
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireWhenValuesHasNoOutputs()
    {
        tester().assertThat(new PushFilterIntoValues(tester().getPlannerContext()))
                .on(p -> p.filter(TRUE, p.values(5)))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenValuesHasNonRowExpression()
    {
        tester().assertThat(new PushFilterIntoValues(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    return p.filter(TRUE, p.valuesOfExpressions(
                            ImmutableList.of(a, b),
                            ImmutableList.of(
                                    new Row(ImmutableList.of(new Constant(BIGINT, 1L), new Constant(BIGINT, 2L))),
                                    new Cast(new Row(ImmutableList.of(new Constant(INTEGER, 3L), new Constant(INTEGER, 4L))), anonymousRow(BIGINT, BIGINT)))));
                }).doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenFilterPredicateNonDeterministic()
    {
        ResolvedFunction random = new TestingFunctionResolution().resolveFunction("random", fromTypes());

        tester().assertThat(new PushFilterIntoValues(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a", DOUBLE);
                    Symbol b = p.symbol("b", DOUBLE);
                    return p.filter(
                            new Comparison(GREATER_THAN, new Reference(DOUBLE, "a"), new Call(random, ImmutableList.of())),
                            p.values(5, a, b));
                }).doesNotFire();
    }

    @Test
    public void testEmptyValues()
    {
        tester().assertThat(new PushFilterIntoValues(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    return p.filter(
                            new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Constant(BIGINT, 0L)),
                            p.values(a, b));
                }).matches(
                        values(ImmutableList.of("a", "b")));
    }

    @Test
    public void testDoesNotFireWhenValuesNonDeterministic()
    {
        ResolvedFunction random = new TestingFunctionResolution().resolveFunction("random", fromTypes());

        tester().assertThat(new PushFilterIntoValues(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a", DOUBLE);
                    Symbol b = p.symbol("b", DOUBLE);
                    return p.filter(
                            TRUE,
                            p.values(
                                    ImmutableList.of(a, b),
                                    ImmutableList.of(ImmutableList.of(new Constant(DOUBLE, 1.0), new Call(random, ImmutableList.of())))));
                }).doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenValuesCorrelated()
    {
        tester().assertThat(new PushFilterIntoValues(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a", DOUBLE);
                    Symbol b = p.symbol("b", DOUBLE);
                    return p.filter(
                            TRUE,
                            p.values(
                                    ImmutableList.of(a, b),
                                    ImmutableList.of(ImmutableList.of(new Constant(DOUBLE, 1.0), new Reference(DOUBLE, "c")))));
                }).doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenFilterCorrelated()
    {
        tester().assertThat(new PushFilterIntoValues(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a", DOUBLE);
                    Symbol b = p.symbol("b", DOUBLE);
                    return p.filter(
                            new Comparison(GREATER_THAN, new Reference(DOUBLE, "c"), new Constant(DOUBLE, 0.0)),
                            p.values(5, a, b));
                }).doesNotFire();
    }

    @Test
    public void testRetainAllRowsAndRemoveFilter()
    {
        tester().assertThat(new PushFilterIntoValues(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    return p.filter(
                            new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")),
                            p.values(
                                    ImmutableList.of(a, b),
                                    ImmutableList.of(
                                            ImmutableList.of(new Constant(BIGINT, 5L), new Constant(BIGINT, 4L)),
                                            ImmutableList.of(new Constant(BIGINT, 500L), new Constant(BIGINT, 400L)))));
                }).matches(
                        values(ImmutableList.of("a", "b")));
    }

    @Test
    public void testRemoveSomeRowsAndRemoveFilter()
    {
        tester().assertThat(new PushFilterIntoValues(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    return p.filter(
                            new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")),
                            p.values(
                                    ImmutableList.of(a, b),
                                    ImmutableList.of(
                                            ImmutableList.of(new Constant(BIGINT, 5L), new Constant(BIGINT, 4L)),
                                            ImmutableList.of(new Constant(BIGINT, 1L), new Constant(BIGINT, 2L)))));
                }).matches(
                        values(
                                ImmutableList.of("a", "b"),
                                ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 5L), new Constant(BIGINT, 4L)))));
    }

    @Test
    public void testRemoveAllRowsAndRemoveFilter()
    {
        tester().assertThat(new PushFilterIntoValues(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    return p.filter(
                            new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")),
                            p.values(
                                    ImmutableList.of(a, b),
                                    ImmutableList.of(
                                            ImmutableList.of(new Constant(BIGINT, 4L), new Constant(BIGINT, 5L)),
                                            ImmutableList.of(new Constant(BIGINT, 1L), new Constant(BIGINT, 2L)))));
                }).matches(
                        values(ImmutableList.of("a", "b")));
    }
}
