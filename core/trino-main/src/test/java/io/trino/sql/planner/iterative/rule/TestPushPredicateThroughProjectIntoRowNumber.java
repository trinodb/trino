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
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.RowNumberSymbolMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushPredicateThroughProjectIntoRowNumber
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction MODULUS_INTEGER = FUNCTIONS.resolveOperator(OperatorType.MODULUS, ImmutableList.of(INTEGER, INTEGER));

    @Test
    public void testRowNumberSymbolPruned()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)),
                            p.project(
                                    Assignments.identity(a),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.empty(),
                                            rowNumber,
                                            p.values(a))));
                })
                .doesNotFire();
    }

    @Test
    public void testNoUpperBoundForRowNumberSymbol()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            new Comparison(EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)),
                            p.project(
                                    Assignments.identity(a, rowNumber),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.empty(),
                                            rowNumber,
                                            p.values(a))));
                })
                .doesNotFire();
    }

    @Test
    public void testNonPositiveUpperBoundForRowNumberSymbol()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            new Logical(AND, ImmutableList.of(new Comparison(EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)), new Comparison(LESS_THAN, new Reference(BIGINT, "row_number"), new Constant(BIGINT, -10L)))),
                            p.project(
                                    Assignments.identity(a, rowNumber),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.empty(),
                                            rowNumber,
                                            p.values(a))));
                })
                .matches(values("a", "row_number"));
    }

    @Test
    public void testPredicateNotSatisfied()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(BIGINT, "row_number"), new Constant(BIGINT, 2L)), new Comparison(LESS_THAN, new Reference(BIGINT, "row_number"), new Constant(BIGINT, 5L)))),
                            p.project(
                                    Assignments.identity(rowNumber),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.empty(),
                                            rowNumber,
                                            p.values(a))));
                })
                .matches(filter(
                        new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(BIGINT, "row_number"), new Constant(BIGINT, 2L)), new Comparison(LESS_THAN, new Reference(BIGINT, "row_number"), new Constant(BIGINT, 5L)))),
                        project(
                                ImmutableMap.of("row_number", io.trino.sql.planner.assertions.PlanMatchPattern.expression(new Reference(BIGINT, "row_number"))),
                                rowNumber(
                                        pattern -> pattern
                                                .maxRowCountPerPartition(Optional.of(4)),
                                        values(ImmutableList.of("a")))
                                        .withAlias("row_number", new RowNumberSymbolMatcher()))));
    }

    @Test
    public void testPredicateNotSatisfiedAndMaxRowCountNotUpdated()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(BIGINT, "row_number"), new Constant(BIGINT, 2L)), new Comparison(LESS_THAN, new Reference(BIGINT, "row_number"), new Constant(BIGINT, 5L)))),
                            p.project(
                                    Assignments.identity(rowNumber),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.of(3),
                                            rowNumber,
                                            p.values(a))));
                })
                .doesNotFire();
    }

    @Test
    public void testPredicateSatisfied()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            new Comparison(LESS_THAN, new Reference(BIGINT, "row_number"), new Constant(BIGINT, 5L)),
                            p.project(
                                    Assignments.identity(rowNumber),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.of(3),
                                            rowNumber,
                                            p.values(a))));
                })
                .matches(project(
                        ImmutableMap.of("row_number", io.trino.sql.planner.assertions.PlanMatchPattern.expression(new Reference(BIGINT, "row_number"))),
                        rowNumber(
                                pattern -> pattern
                                        .maxRowCountPerPartition(Optional.of(3)),
                                values(ImmutableList.of("a")))
                                .withAlias("row_number", new RowNumberSymbolMatcher())));

        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            new Comparison(LESS_THAN, new Reference(BIGINT, "row_number"), new Constant(BIGINT, 3L)),
                            p.project(
                                    Assignments.identity(rowNumber),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.of(5),
                                            rowNumber,
                                            p.values(a))));
                })
                .matches(project(
                        ImmutableMap.of("row_number", io.trino.sql.planner.assertions.PlanMatchPattern.expression(new Reference(BIGINT, "row_number"))),
                        rowNumber(
                                pattern -> pattern
                                        .maxRowCountPerPartition(Optional.of(2)),
                                values(ImmutableList.of("a")))
                                .withAlias("row_number", new RowNumberSymbolMatcher())));
    }

    @Test
    public void testPredicatePartiallySatisfied()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Reference(BIGINT, "row_number"), new Constant(BIGINT, 5L)), new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Constant(BIGINT, 0L)))),
                            p.project(
                                    Assignments.identity(rowNumber, a),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.of(3),
                                            rowNumber,
                                            p.values(a))));
                })
                .matches(filter(
                        new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Constant(BIGINT, 0L)),
                        project(
                                ImmutableMap.of("row_number", expression(new Reference(BIGINT, "row_number")), "a", expression(new Reference(BIGINT, "a"))),
                                rowNumber(
                                        pattern -> pattern
                                                .maxRowCountPerPartition(Optional.of(3)),
                                        values(ImmutableList.of("a")))
                                        .withAlias("row_number", new RowNumberSymbolMatcher()))));

        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Reference(BIGINT, "row_number"), new Constant(BIGINT, 5L)), new Comparison(EQUAL, new Call(MODULUS_INTEGER, ImmutableList.of(new Reference(INTEGER, "row_number"), new Constant(INTEGER, 2L))), new Constant(INTEGER, 0L)))),
                            p.project(
                                    Assignments.identity(rowNumber),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.of(3),
                                            rowNumber,
                                            p.values(a))));
                })
                .matches(filter(
                        new Comparison(EQUAL, new Call(MODULUS_INTEGER, ImmutableList.of(new Reference(INTEGER, "row_number"), new Constant(INTEGER, 2L))), new Constant(INTEGER, 0L)),
                        project(
                                ImmutableMap.of("row_number", io.trino.sql.planner.assertions.PlanMatchPattern.expression(new Reference(BIGINT, "row_number"))),
                                rowNumber(
                                        pattern -> pattern
                                                .maxRowCountPerPartition(Optional.of(3)),
                                        values(ImmutableList.of("a")))
                                        .withAlias("row_number", new RowNumberSymbolMatcher()))));
    }
}
