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
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.rule.RemoveRedundantCaseClauses;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRemoveRedundantCaseClauses
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution(createTestTransactionManager(), PLANNER_CONTEXT);
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", ImmutableList.of());

    @Test
    void test()
    {
        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(new Reference(BOOLEAN, "a"), new Reference(VARCHAR, "r1")),
                                new WhenClause(new Reference(BOOLEAN, "b"), new Reference(VARCHAR, "r2")),
                                new WhenClause(FALSE, new Reference(VARCHAR, "r3")),
                                new WhenClause(NULL_BOOLEAN, new Reference(VARCHAR, "r4")),
                                new WhenClause(new Reference(BOOLEAN, "a"), new Reference(VARCHAR, "r5"))),
                        new Reference(VARCHAR, "d"))))
                .describedAs("redundant terms")
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(
                                new WhenClause(new Reference(BOOLEAN, "a"), new Reference(VARCHAR, "r1")),
                                new WhenClause(new Reference(BOOLEAN, "b"), new Reference(VARCHAR, "r2"))),
                        new Reference(VARCHAR, "d"))));

        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(new Reference(BOOLEAN, "a"), new Reference(VARCHAR, "r1")),
                                new WhenClause(TRUE, new Reference(VARCHAR, "r2"))),
                        new Reference(VARCHAR, "d"))))
                .describedAs("short-circuit")
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(new WhenClause(new Reference(BOOLEAN, "a"), new Reference(VARCHAR, "r1"))),
                        new Reference(VARCHAR, "r2"))));

        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(TRUE, new Reference(VARCHAR, "r1")),
                                new WhenClause(new Reference(BOOLEAN, "a"), new Reference(VARCHAR, "r2"))),
                        new Reference(VARCHAR, "d"))))
                .describedAs("short-circuit on first term")
                .isEqualTo(Optional.of(new Reference(VARCHAR, "r1")));

        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(new Reference(BOOLEAN, "a"), new Reference(VARCHAR, "r1")),
                                new WhenClause(new Reference(BOOLEAN, "b"), new Reference(VARCHAR, "r2")),
                                new WhenClause(new Comparison(GREATER_THAN_OR_EQUAL, new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 0.0)), new Reference(VARCHAR, "r3")),
                                new WhenClause(new Comparison(GREATER_THAN_OR_EQUAL, new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 0.0)), new Reference(VARCHAR, "r4")),
                                new WhenClause(new Reference(BOOLEAN, "a"), new Reference(VARCHAR, "r5"))),
                        new Reference(VARCHAR, "d"))))
                .describedAs("non-deterministic terms")
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(
                                new WhenClause(new Reference(BOOLEAN, "a"), new Reference(VARCHAR, "r1")),
                                new WhenClause(new Reference(BOOLEAN, "b"), new Reference(VARCHAR, "r2")),
                                new WhenClause(new Comparison(GREATER_THAN_OR_EQUAL, new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 0.0)), new Reference(VARCHAR, "r3")),
                                new WhenClause(new Comparison(GREATER_THAN_OR_EQUAL, new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 0.0)), new Reference(VARCHAR, "r4"))),
                        new Reference(VARCHAR, "d"))));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new RemoveRedundantCaseClauses().apply(expression, testSession(), ImmutableMap.of());
    }
}
