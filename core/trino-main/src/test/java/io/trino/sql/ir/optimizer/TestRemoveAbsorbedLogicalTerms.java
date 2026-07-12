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
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.RemoveAbsorbedLogicalTerms;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.IrUtils.or;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRemoveAbsorbedLogicalTerms
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", ImmutableList.of());
    private static final ResolvedFunction DIVIDE_BIGINT = FUNCTIONS.resolveOperator(DIVIDE, ImmutableList.of(BIGINT, BIGINT));

    private static final Expression A = new Reference(BOOLEAN, "a");
    private static final Expression B = new Reference(BOOLEAN, "b");
    private static final Expression C = new Reference(BOOLEAN, "c");
    private static final Expression D = new Reference(BOOLEAN, "d");

    @Test
    void testAbsorption()
    {
        assertThat(optimize(or(A, and(A, B))))
                .isEqualTo(Optional.of(A));

        assertThat(optimize(and(A, or(A, B))))
                .isEqualTo(Optional.of(A));

        assertThat(optimize(or(A, and(A, B), C)))
                .isEqualTo(Optional.of(or(A, C)));

        assertThat(optimize(and(A, or(A, B), or(C, D))))
                .isEqualTo(Optional.of(and(A, or(C, D))));

        assertThat(optimize(or(and(A, B), and(A, B, C))))
                .describedAs("sub-terms of one term are a subset of the other's")
                .isEqualTo(Optional.of(and(A, B)));

        assertThat(optimize(or(and(A, B), and(B, A))))
                .describedAs("terms with equal sub-term sets: keep the first")
                .isEqualTo(Optional.of(and(A, B)));

        assertThat(optimize(or(and(A, B, C), and(B, A), A)))
                .describedAs("chained absorption")
                .isEqualTo(Optional.of(A));
    }

    @Test
    void testDoesNotFire()
    {
        assertThat(optimize(or(and(A, B), and(A, C))))
                .describedAs("neither sub-term set contains the other")
                .isEqualTo(Optional.empty());

        assertThat(optimize(and(A, and(A, B))))
                .describedAs("nested term with the same operator")
                .isEqualTo(Optional.empty());

        Expression nonDeterministic = comparison(
                GREATER_THAN_OR_EQUAL,
                new Call(RANDOM, ImmutableList.of()),
                new Constant(DOUBLE, 0.5));
        assertThat(optimize(or(nonDeterministic, and(nonDeterministic, B))))
                .describedAs("non-deterministic shared term: the two occurrences may differ")
                .isEqualTo(Optional.empty());

        Expression failing = comparison(
                EQUAL,
                new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Constant(BIGINT, 0L))),
                new Constant(BIGINT, 1L));
        assertThat(optimize(or(A, and(A, failing))))
                .describedAs("absorbed extra term that may fail")
                .isEqualTo(Optional.empty());
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new RemoveAbsorbedLogicalTerms(PLANNER_CONTEXT).apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }
}
