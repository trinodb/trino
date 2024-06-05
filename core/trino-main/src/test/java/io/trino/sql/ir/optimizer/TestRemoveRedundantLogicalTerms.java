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
import io.trino.sql.ir.optimizer.rule.RemoveRedundantLogicalTerms;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static org.assertj.core.api.Assertions.assertThat;

class TestRemoveRedundantLogicalTerms
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution(createTestTransactionManager(), PLANNER_CONTEXT);
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", ImmutableList.of());

    @Test
    void testOr()
    {
        assertThat(optimize(
                new Logical(
                        OR,
                        ImmutableList.of(TRUE, FALSE, new Reference(BOOLEAN, "a"), TRUE, new Reference(BOOLEAN, "a"), TRUE, FALSE))))
                .isEqualTo(Optional.of(new Logical(OR, ImmutableList.of(TRUE, new Reference(BOOLEAN, "a")))));

        assertThat(optimize(
                new Logical(OR, ImmutableList.of(
                        new Reference(BOOLEAN, "a"),
                        new Reference(BOOLEAN, "b"),
                        new Reference(BOOLEAN, "a"),
                        new Reference(BOOLEAN, "c")))))
                .isEqualTo(Optional.of(new Logical(OR, ImmutableList.of(
                        new Reference(BOOLEAN, "a"),
                        new Reference(BOOLEAN, "b"),
                        new Reference(BOOLEAN, "c")))));

        assertThat(optimize(
                new Logical(OR, ImmutableList.of(
                        new Reference(BOOLEAN, "a"),
                        new Reference(BOOLEAN, "a"),
                        new Reference(BOOLEAN, "a")))))
                .isEqualTo(Optional.of(new Reference(BOOLEAN, "a")));

        assertThat(optimize(
                new Logical(OR, ImmutableList.of(
                        new IsNull(new Call(RANDOM, ImmutableList.of())),
                        new IsNull(new Call(RANDOM, ImmutableList.of()))))))
                .describedAs("non-deterministic repeated terms")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Logical(OR, ImmutableList.of(
                        new Reference(BOOLEAN, "a"),
                        new Reference(BOOLEAN, "a"),
                        new IsNull(new Call(RANDOM, ImmutableList.of())),
                        new IsNull(new Call(RANDOM, ImmutableList.of()))))))
                .describedAs("mixed deterministic and non-deterministic")
                .isEqualTo(Optional.of(new Logical(OR, ImmutableList.of(
                        new Reference(BOOLEAN, "a"),
                        new IsNull(new Call(RANDOM, ImmutableList.of())),
                        new IsNull(new Call(RANDOM, ImmutableList.of()))))));
    }

    @Test
    void testAnd()
    {
        assertThat(optimize(
                new Logical(
                        AND,
                        ImmutableList.of(TRUE, FALSE, new Reference(BOOLEAN, "a"), TRUE, new Reference(BOOLEAN, "a"), TRUE, FALSE))))
                .isEqualTo(Optional.of(new Logical(AND, ImmutableList.of(FALSE, new Reference(BOOLEAN, "a")))));

        assertThat(optimize(
                new Logical(AND, ImmutableList.of(
                        new Reference(BOOLEAN, "a"),
                        new Reference(BOOLEAN, "b"),
                        new Reference(BOOLEAN, "a"),
                        new Reference(BOOLEAN, "c")))))
                .isEqualTo(Optional.of(new Logical(AND, ImmutableList.of(
                        new Reference(BOOLEAN, "a"),
                        new Reference(BOOLEAN, "b"),
                        new Reference(BOOLEAN, "c")))));

        assertThat(optimize(
                new Logical(AND, ImmutableList.of(
                        new Reference(BOOLEAN, "a"),
                        new Reference(BOOLEAN, "a"),
                        new Reference(BOOLEAN, "a")))))
                .isEqualTo(Optional.of(new Reference(BOOLEAN, "a")));

        assertThat(optimize(
                new Logical(AND, ImmutableList.of(
                        new IsNull(new Call(RANDOM, ImmutableList.of())),
                        new IsNull(new Call(RANDOM, ImmutableList.of()))))))
                .describedAs("non-deterministic repeated terms")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Logical(AND, ImmutableList.of(
                        new Reference(BOOLEAN, "a"),
                        new Reference(BOOLEAN, "a"),
                        new IsNull(new Call(RANDOM, ImmutableList.of())),
                        new IsNull(new Call(RANDOM, ImmutableList.of()))))))
                .describedAs("mixed deterministic and non-deterministic")
                .isEqualTo(Optional.of(new Logical(AND, ImmutableList.of(
                        new Reference(BOOLEAN, "a"),
                        new IsNull(new Call(RANDOM, ImmutableList.of())),
                        new IsNull(new Call(RANDOM, ImmutableList.of()))))));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new RemoveRedundantLogicalTerms().apply(expression, testSession(), ImmutableMap.of());
    }
}
