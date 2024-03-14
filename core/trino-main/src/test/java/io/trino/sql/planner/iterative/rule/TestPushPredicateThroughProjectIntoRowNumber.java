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
import io.trino.sql.ir.ArithmeticBinaryExpression;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.GenericLiteral;
import io.trino.sql.ir.LogicalExpression;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.RowNumberSymbolMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.MODULUS;
import static io.trino.sql.ir.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.ir.LogicalExpression.Operator.AND;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushPredicateThroughProjectIntoRowNumber
        extends BaseRuleTest
{
    @Test
    public void testRowNumberSymbolPruned()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoRowNumber(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.filter(
                            new ComparisonExpression(EQUAL, new SymbolReference("a"), GenericLiteral.constant(INTEGER, 1L)),
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
                            new ComparisonExpression(EQUAL, new SymbolReference("a"), GenericLiteral.constant(BIGINT, 1L)),
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
                            new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference("a"), GenericLiteral.constant(BIGINT, 1L)), new ComparisonExpression(LESS_THAN, new SymbolReference("row_number"), GenericLiteral.constant(BIGINT, -10L)))),
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
                            new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("row_number"), GenericLiteral.constant(BIGINT, 2L)), new ComparisonExpression(LESS_THAN, new SymbolReference("row_number"), GenericLiteral.constant(BIGINT, 5L)))),
                            p.project(
                                    Assignments.identity(rowNumber),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.empty(),
                                            rowNumber,
                                            p.values(a))));
                })
                .matches(filter(
                        new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("row_number"), GenericLiteral.constant(BIGINT, 2L)), new ComparisonExpression(LESS_THAN, new SymbolReference("row_number"), GenericLiteral.constant(BIGINT, 5L)))),
                        project(
                                ImmutableMap.of("row_number", io.trino.sql.planner.assertions.PlanMatchPattern.expression(new SymbolReference("row_number"))),
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
                            new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("row_number"), GenericLiteral.constant(BIGINT, 2L)), new ComparisonExpression(LESS_THAN, new SymbolReference("row_number"), GenericLiteral.constant(BIGINT, 5L)))),
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
                            new ComparisonExpression(LESS_THAN, new SymbolReference("row_number"), GenericLiteral.constant(BIGINT, 5L)),
                            p.project(
                                    Assignments.identity(rowNumber),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.of(3),
                                            rowNumber,
                                            p.values(a))));
                })
                .matches(project(
                        ImmutableMap.of("row_number", io.trino.sql.planner.assertions.PlanMatchPattern.expression(new SymbolReference("row_number"))),
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
                            new ComparisonExpression(LESS_THAN, new SymbolReference("row_number"), GenericLiteral.constant(BIGINT, 3L)),
                            p.project(
                                    Assignments.identity(rowNumber),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.of(5),
                                            rowNumber,
                                            p.values(a))));
                })
                .matches(project(
                        ImmutableMap.of("row_number", io.trino.sql.planner.assertions.PlanMatchPattern.expression(new SymbolReference("row_number"))),
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
                            new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("row_number"), GenericLiteral.constant(BIGINT, 5L)), new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), GenericLiteral.constant(BIGINT, 0L)))),
                            p.project(
                                    Assignments.identity(rowNumber, a),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.of(3),
                                            rowNumber,
                                            p.values(a))));
                })
                .matches(filter(
                        new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), GenericLiteral.constant(BIGINT, 0L)),
                        project(
                                ImmutableMap.of("row_number", io.trino.sql.planner.assertions.PlanMatchPattern.expression(new SymbolReference("row_number")), "a", expression(new SymbolReference("a"))),
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
                            new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("row_number"), GenericLiteral.constant(BIGINT, 5L)), new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(MODULUS, new SymbolReference("row_number"), GenericLiteral.constant(INTEGER, 2L)), GenericLiteral.constant(BIGINT, 0L)))),
                            p.project(
                                    Assignments.identity(rowNumber),
                                    p.rowNumber(
                                            ImmutableList.of(),
                                            Optional.of(3),
                                            rowNumber,
                                            p.values(a))));
                })
                .matches(filter(
                        new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(MODULUS, new SymbolReference("row_number"), GenericLiteral.constant(INTEGER, 2L)), GenericLiteral.constant(BIGINT, 0L)),
                        project(
                                ImmutableMap.of("row_number", io.trino.sql.planner.assertions.PlanMatchPattern.expression(new SymbolReference("row_number"))),
                                rowNumber(
                                        pattern -> pattern
                                                .maxRowCountPerPartition(Optional.of(3)),
                                        values(ImmutableList.of("a")))
                                        .withAlias("row_number", new RowNumberSymbolMatcher()))));
    }
}
