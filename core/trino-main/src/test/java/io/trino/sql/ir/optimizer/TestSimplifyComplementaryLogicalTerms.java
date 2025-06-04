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
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.SimplifyComplementaryLogicalTerms;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static org.assertj.core.api.Assertions.assertThat;

class TestSimplifyComplementaryLogicalTerms
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution(createTestTransactionManager(), PLANNER_CONTEXT);
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", ImmutableList.of());

    @Test
    void testOr()
    {
        assertThat(optimize(
                new Logical(
                        OR,
                        ImmutableList.of(TRUE, not(FUNCTIONS.getMetadata(), TRUE)))))
                .isEqualTo(Optional.of(new Logical(OR, ImmutableList.of(TRUE, not(FUNCTIONS.getMetadata(), new IsNull(TRUE))))));

        assertThat(optimize(
                new Logical(
                        OR,
                        ImmutableList.of(TRUE, new Reference(BOOLEAN, "x")))))
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Logical(OR, ImmutableList.of(
                        new Reference(BOOLEAN, "a"),
                        new Reference(BOOLEAN, "b"),
                        not(FUNCTIONS.getMetadata(), new Reference(BOOLEAN, "a"))))))
                .isEqualTo(Optional.of(
                        new Logical(OR, ImmutableList.of(
                                new Logical(OR, ImmutableList.of(
                                        new Reference(BOOLEAN, "a"),
                                        not(FUNCTIONS.getMetadata(), new IsNull(new Reference(BOOLEAN, "a"))))),
                                new Reference(BOOLEAN, "b")))));

        assertThat(optimize(
                new Logical(OR, ImmutableList.of(
                        new IsNull(new Call(RANDOM, ImmutableList.of())),
                        not(FUNCTIONS.getMetadata(), new IsNull(new Call(RANDOM, ImmutableList.of())))))))
                .describedAs("Non-deterministic terms")
                .isEqualTo(Optional.empty());
    }

    @Test
    void testAnd()
    {
        assertThat(optimize(
                new Logical(
                        AND,
                        ImmutableList.of(TRUE, not(FUNCTIONS.getMetadata(), TRUE)))))
                .isEqualTo(Optional.of(new Logical(AND, ImmutableList.of(TRUE, new IsNull(TRUE)))));

        assertThat(optimize(
                new Logical(
                        AND,
                        ImmutableList.of(TRUE, new Reference(BOOLEAN, "x")))))
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Logical(AND, ImmutableList.of(
                        new Reference(BOOLEAN, "a"),
                        new Reference(BOOLEAN, "b"),
                        not(FUNCTIONS.getMetadata(), new Reference(BOOLEAN, "a"))))))
                .isEqualTo(Optional.of(
                        new Logical(AND, ImmutableList.of(
                                new Logical(AND, ImmutableList.of(
                                        new Reference(BOOLEAN, "a"),
                                        new IsNull(new Reference(BOOLEAN, "a")))),
                                new Reference(BOOLEAN, "b")))));

        assertThat(optimize(
                new Logical(AND, ImmutableList.of(
                        new IsNull(new Call(RANDOM, ImmutableList.of())),
                        not(FUNCTIONS.getMetadata(), new IsNull(new Call(RANDOM, ImmutableList.of())))))))
                .describedAs("Non-deterministic terms")
                .isEqualTo(Optional.empty());
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new SimplifyComplementaryLogicalTerms(PLANNER_CONTEXT).apply(expression, testSession(), ImmutableMap.of());
    }
}
