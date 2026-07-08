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
package io.trino.sql.ir.optimizer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.rule.MergeCaseClauses;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.ir.IrUtils.or;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.ir.optimizer.IrExpressionOptimizer.newOptimizer;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMergeCaseClauses
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", ImmutableList.of());
    private static final ResolvedFunction DIVIDE_BIGINT = FUNCTIONS.resolveOperator(DIVIDE, ImmutableList.of(BIGINT, BIGINT));

    private static final Expression A = new Reference(BOOLEAN, "a");
    private static final Expression B = new Reference(BOOLEAN, "b");
    private static final Expression C = new Reference(BOOLEAN, "c");
    private static final Expression R1 = new Reference(VARCHAR, "r1");
    private static final Expression R2 = new Reference(VARCHAR, "r2");
    private static final Expression DEFAULT = new Reference(VARCHAR, "d");

    @Test
    void testMergeAdjacentClauses()
    {
        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(A, R1),
                                new WhenClause(B, R1),
                                new WhenClause(C, R2)),
                        DEFAULT)))
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(
                                new WhenClause(or(A, B), R1),
                                new WhenClause(C, R2)),
                        DEFAULT)));

        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(A, R1),
                                new WhenClause(B, R1),
                                new WhenClause(C, R1)),
                        DEFAULT)))
                .describedAs("run of three clauses")
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(new WhenClause(or(A, B, C), R1)),
                        DEFAULT)));

        Expression randomResult = new Call(RANDOM, ImmutableList.of());
        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(A, new Call(RANDOM, ImmutableList.of())),
                                new WhenClause(B, new Call(RANDOM, ImmutableList.of()))),
                        new Constant(DOUBLE, 0.0))))
                .describedAs("non-deterministic results: only one copy is evaluated per row, before and after")
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(new WhenClause(or(A, B), randomResult)),
                        new Constant(DOUBLE, 0.0))));
    }

    @Test
    void testDropTrailingClauses()
    {
        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(A, R1),
                                new WhenClause(B, DEFAULT)),
                        DEFAULT)))
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(new WhenClause(A, R1)),
                        DEFAULT)));

        assertThat(optimize(
                new Case(
                        ImmutableList.of(new WhenClause(A, DEFAULT)),
                        DEFAULT)))
                .describedAs("all clauses produce the default")
                .isEqualTo(Optional.of(DEFAULT));

        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(A, R1),
                                new WhenClause(B, R1),
                                new WhenClause(C, DEFAULT)),
                        DEFAULT)))
                .describedAs("drop trailing clause, then merge")
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(new WhenClause(or(A, B), R1)),
                        DEFAULT)));
    }

    @Test
    void testDoesNotFire()
    {
        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(A, R1),
                                new WhenClause(B, R2),
                                new WhenClause(C, R1)),
                        DEFAULT)))
                .describedAs("non-adjacent clauses with the same result")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(A, DEFAULT),
                                new WhenClause(B, R1)),
                        DEFAULT)))
                .describedAs("non-trailing clause producing the default")
                .isEqualTo(Optional.empty());

        Expression failingOperand = comparison(
                EQUAL,
                new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Constant(BIGINT, 0L))),
                new Constant(BIGINT, 1L));
        assertThat(optimize(
                new Case(
                        ImmutableList.of(new WhenClause(failingOperand, DEFAULT)),
                        DEFAULT)))
                .describedAs("trailing clause with failing operand")
                .isEqualTo(Optional.empty());
    }

    @Test
    void testComposesWithFlattenNestedCase()
    {
        assertThat(newOptimizer(PLANNER_CONTEXT).process(
                ifExpression(A, R1, ifExpression(B, R1, DEFAULT)),
                testSession(),
                new SymbolAllocator(),
                ImmutableMap.of()))
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(new WhenClause(or(A, B), R1)),
                        DEFAULT)));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new MergeCaseClauses(PLANNER_CONTEXT).apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }
}
