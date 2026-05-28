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
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.InlineTrivialLet;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.ir.optimizer.IrExpressionOptimizer.newOptimizer;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestInlineTrivialLet
{
    @Test
    void testConstantValue()
    {
        assertThat(optimize(
                new Let(new Symbol(BIGINT, "x"),
                        new Constant(BIGINT, 3L),
                        new Logical(AND, ImmutableList.of(
                                comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, 0L)),
                                comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, 5L)))))))
                .isEqualTo(Optional.of(new Logical(AND, ImmutableList.of(
                        comparison(GREATER_THAN_OR_EQUAL, new Constant(BIGINT, 3L), new Constant(BIGINT, 0L)),
                        comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 3L), new Constant(BIGINT, 5L))))));
    }

    @Test
    void testReferenceValue()
    {
        assertThat(optimize(
                new Let(new Symbol(BIGINT, "x"),
                        new Reference(BIGINT, "a"),
                        comparison(EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L)))))
                .isEqualTo(Optional.of(comparison(EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L))));
    }

    @Test
    void testNonTrivialValueIsNotInlined()
    {
        assertThat(optimize(
                new Let(new Symbol(BOOLEAN, "x"),
                        comparison(EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)),
                        new Reference(BOOLEAN, "x"))))
                .isEmpty();
    }

    @Test
    void testShadowedBindingIsNotSubstituted()
    {
        // The inner Let re-binds x; its body must keep referring to the inner binding
        assertThat(optimize(
                new Let(new Symbol(BIGINT, "x"),
                        new Constant(BIGINT, 1L),
                        new Let(new Symbol(BIGINT, "x"),
                                new Reference(BIGINT, "y"),
                                new Reference(BIGINT, "x")))))
                .isEqualTo(Optional.of(new Let(
                        new Symbol(BIGINT, "x"),
                        new Reference(BIGINT, "y"),
                        new Reference(BIGINT, "x"))));
    }

    @Test
    void testComposesWithConstantFolding()
    {
        // Once the value is a constant, inlining lets the rest of the optimizer fold the whole predicate
        assertThat(newOptimizer(PLANNER_CONTEXT).process(
                new Let(new Symbol(BIGINT, "x"),
                        new Constant(BIGINT, 3L),
                        new Logical(AND, ImmutableList.of(
                                comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, 0L)),
                                comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, 5L))))),
                testSession(),
                new SymbolAllocator(),
                ImmutableMap.of()))
                .isEqualTo(Optional.of(TRUE));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new InlineTrivialLet().apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }
}
