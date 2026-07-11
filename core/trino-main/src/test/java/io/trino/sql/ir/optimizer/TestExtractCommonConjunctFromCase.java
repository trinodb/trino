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
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.rule.ExtractCommonConjunctFromCase;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

final class TestExtractCommonConjunctFromCase
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final PlannerContext PLANNER_CONTEXT = FUNCTIONS.getPlannerContext();
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", ImmutableList.of());
    private static final Expression SHARED = comparison(EQUAL, new Reference(BIGINT, "a"), new Reference(BIGINT, "b"));
    private static final Expression OTHER = comparison(EQUAL, new Reference(BIGINT, "c"), new Reference(BIGINT, "d"));
    private static final Expression THIRD = comparison(EQUAL, new Reference(BIGINT, "e"), new Reference(BIGINT, "f"));
    private static final Expression NON_DETERMINISTIC = comparison(EQUAL, new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 0.5));
    private static final Expression MAY_FAIL = comparison(EQUAL, new Cast(new Reference(VARCHAR, "s"), BIGINT), new Constant(BIGINT, 1L));
    private static final Reference CONDITION = new Reference(BOOLEAN, "condition");
    private static final Reference CONDITION2 = new Reference(BOOLEAN, "condition2");

    @Test
    void extractsConjunctSharedByEveryBranch()
    {
        assertThat(optimize(new Case(
                ImmutableList.of(new WhenClause(CONDITION, SHARED)),
                Logical.and(OTHER, SHARED))))
                .isEqualTo(Optional.of(new Logical(AND, ImmutableList.of(
                        SHARED,
                        new Case(
                                ImmutableList.of(new WhenClause(CONDITION, TRUE)),
                                OTHER)))));
    }

    @Test
    void extractsConjunctSharedAcrossMultipleWhenClauses()
    {
        assertThat(optimize(new Case(
                ImmutableList.of(
                        new WhenClause(CONDITION, Logical.and(SHARED, OTHER)),
                        new WhenClause(CONDITION2, Logical.and(SHARED, THIRD))),
                SHARED)))
                .isEqualTo(Optional.of(new Logical(AND, ImmutableList.of(
                        SHARED,
                        new Case(
                                ImmutableList.of(
                                        new WhenClause(CONDITION, OTHER),
                                        new WhenClause(CONDITION2, THIRD)),
                                TRUE)))));
    }

    @Test
    void doesNotFireWhenNoConjunctIsCommonToAllBranches()
    {
        assertThat(optimize(new Case(
                ImmutableList.of(new WhenClause(CONDITION, SHARED)),
                OTHER)))
                .isEqualTo(Optional.empty());
    }

    @Test
    void doesNotExtractNonDeterministicConjunct()
    {
        assertThat(optimize(new Case(
                ImmutableList.of(new WhenClause(CONDITION, Logical.and(NON_DETERMINISTIC, OTHER))),
                Logical.and(NON_DETERMINISTIC, SHARED))))
                .isEqualTo(Optional.empty());
    }

    @Test
    void doesNotExtractConjunctThatMayFail()
    {
        assertThat(optimize(new Case(
                ImmutableList.of(new WhenClause(CONDITION, Logical.and(OTHER, MAY_FAIL))),
                Logical.and(SHARED, MAY_FAIL))))
                .isEqualTo(Optional.empty());
    }

    @Test
    void doesNotExtractTrueConjunct()
    {
        assertThat(optimize(new Case(
                ImmutableList.of(new WhenClause(CONDITION, Logical.and(TRUE, OTHER))),
                Logical.and(TRUE, SHARED))))
                .isEqualTo(Optional.empty());
    }

    @Test
    void doesNotFireOnNonBooleanCase()
    {
        assertThat(optimize(new Case(
                ImmutableList.of(new WhenClause(CONDITION, new Reference(BIGINT, "x"))),
                new Reference(BIGINT, "y"))))
                .isEqualTo(Optional.empty());
    }

    @Test
    void doesNotFireOnCaseWithImplicitElse()
    {
        assertThat(optimize(new Case(
                ImmutableList.of(new WhenClause(CONDITION, Logical.and(SHARED, OTHER))),
                new Constant(BOOLEAN, null))))
                .isEqualTo(Optional.empty());
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new ExtractCommonConjunctFromCase(PLANNER_CONTEXT).apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }
}
