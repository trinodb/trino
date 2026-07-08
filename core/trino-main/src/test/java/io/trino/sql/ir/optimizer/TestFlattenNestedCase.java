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
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.rule.FlattenNestedCase;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.optimizer.IrExpressionOptimizer.newOptimizer;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFlattenNestedCase
{
    private static final Expression A = new Reference(BOOLEAN, "a");
    private static final Expression B = new Reference(BOOLEAN, "b");
    private static final Expression C = new Reference(BOOLEAN, "c");
    private static final Expression R1 = new Reference(VARCHAR, "r1");
    private static final Expression R2 = new Reference(VARCHAR, "r2");
    private static final Expression DEFAULT = new Reference(VARCHAR, "d");

    @Test
    void testCaseInDefault()
    {
        assertThat(optimize(
                new Case(
                        ImmutableList.of(new WhenClause(A, R1)),
                        new Case(
                                ImmutableList.of(new WhenClause(B, R2)),
                                DEFAULT))))
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(
                                new WhenClause(A, R1),
                                new WhenClause(B, R2)),
                        DEFAULT)));
    }

    @Test
    void testCaseInLastClause()
    {
        assertThat(optimize(
                new Case(
                        ImmutableList.of(new WhenClause(A, new Case(ImmutableList.of(new WhenClause(B, R1)), DEFAULT))),
                        DEFAULT)))
                .describedAs("nested IF with shared default")
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(new WhenClause(and(A, B), R1)),
                        DEFAULT)));

        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(A, R1),
                                new WhenClause(B, new Case(ImmutableList.of(new WhenClause(C, R2)), DEFAULT))),
                        DEFAULT)))
                .describedAs("nested case in last clause of multi-clause case")
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(
                                new WhenClause(A, R1),
                                new WhenClause(and(B, C), R2)),
                        DEFAULT)));
    }

    @Test
    void testBothPatterns()
    {
        Expression sharedDefault = new Case(ImmutableList.of(new WhenClause(C, R2)), DEFAULT);

        assertThat(optimize(
                new Case(
                        ImmutableList.of(new WhenClause(A, new Case(ImmutableList.of(new WhenClause(B, R1)), sharedDefault))),
                        sharedDefault)))
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(
                                new WhenClause(and(A, B), R1),
                                new WhenClause(C, R2)),
                        DEFAULT)));
    }

    @Test
    void testDoesNotFire()
    {
        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(A, new Case(ImmutableList.of(new WhenClause(B, R1)), DEFAULT)),
                                new WhenClause(C, R2)),
                        DEFAULT)))
                .describedAs("nested case in non-last clause")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Case(
                        ImmutableList.of(new WhenClause(A, new Case(ImmutableList.of(new WhenClause(B, R1)), R2))),
                        DEFAULT)))
                .describedAs("nested case with different default")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Case(
                        ImmutableList.of(new WhenClause(A, new Case(
                                ImmutableList.of(
                                        new WhenClause(B, R1),
                                        new WhenClause(C, R2)),
                                DEFAULT))),
                        DEFAULT)))
                .describedAs("nested case with multiple clauses")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Case(
                        ImmutableList.of(new WhenClause(A, R1)),
                        DEFAULT)))
                .describedAs("no nested case")
                .isEqualTo(Optional.empty());
    }

    @Test
    void testChainedIfFlattensFully()
    {
        assertThat(newOptimizer(PLANNER_CONTEXT).process(
                ifExpression(A, ifExpression(B, ifExpression(C, R1, DEFAULT), DEFAULT), DEFAULT),
                testSession(),
                new SymbolAllocator(),
                ImmutableMap.of()))
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(new WhenClause(and(A, B, C), R1)),
                        DEFAULT)));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new FlattenNestedCase().apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }
}
