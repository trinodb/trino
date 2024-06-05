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
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.RemoveRedundantInItems;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRemoveRedundantInItems
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", ImmutableList.of());
    private static final ResolvedFunction IS_INDETERMINATE = FUNCTIONS.resolveOperator(INDETERMINATE, ImmutableList.of(BIGINT));

    @Test
    void test()
    {
        assertThat(optimize(
                new In(new Reference(BIGINT, "x"), ImmutableList.of(new Reference(BIGINT, "y")))))
                .isEqualTo(Optional.of(new Comparison(EQUAL, new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))));

        assertThat(optimize(
                new In(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                new Reference(BIGINT, "x"),
                                new Reference(BIGINT, "y")))))
                .describedAs("exact match, no item can fail")
                .isEqualTo(Optional.of(ifExpression(new Call(IS_INDETERMINATE, ImmutableList.of(new Reference(BIGINT, "x"))), NULL_BOOLEAN, TRUE)));

        assertThat(optimize(
                new In(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                new Reference(BIGINT, "y"),
                                new Reference(BIGINT, "z"),
                                new Reference(BIGINT, "y")))))
                .describedAs("no exact match, no item can fail")
                .isEqualTo(Optional.of(new In(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                new Reference(BIGINT, "y"),
                                new Reference(BIGINT, "z")))));

        assertThat(optimize(
                new In(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                new Reference(BIGINT, "x"),
                                new Cast(new Reference(VARCHAR, "y"), BIGINT)))))
                .describedAs("exact match found, another item can fail")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new In(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                new Reference(BIGINT, "x"),
                                new Cast(new Reference(VARCHAR, "y"), BIGINT),
                                new Cast(new Reference(VARCHAR, "y"), BIGINT)))))
                .describedAs("exact match found, another item can fail, duplicate removed")
                .isEqualTo(Optional.of(
                        new In(
                                new Reference(BIGINT, "x"),
                                ImmutableList.of(
                                        new Reference(BIGINT, "x"),
                                        new Cast(new Reference(VARCHAR, "y"), BIGINT)))));

        assertThat(optimize(
                new In(
                        new Reference(DOUBLE, "x"),
                        ImmutableList.of(
                                new Call(RANDOM, ImmutableList.of()),
                                new Call(RANDOM, ImmutableList.of())))))
                .describedAs("non-deterministic items")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new In(
                        new Reference(DOUBLE, "x"),
                        ImmutableList.of(
                                new Reference(DOUBLE, "x"),
                                new Reference(DOUBLE, "x"),
                                new Call(RANDOM, ImmutableList.of()),
                                new Call(RANDOM, ImmutableList.of())))))
                .describedAs("non-deterministic items")
                .isEqualTo(Optional.of(new In(
                        new Reference(DOUBLE, "x"),
                        ImmutableList.of(
                                new Reference(DOUBLE, "x"),
                                new Call(RANDOM, ImmutableList.of()),
                                new Call(RANDOM, ImmutableList.of())))));

        assertThat(optimize(
                new In(
                        new Call(RANDOM, ImmutableList.of()),
                        ImmutableList.of(
                                new Reference(DOUBLE, "x"),
                                new Reference(DOUBLE, "x"),
                                new Call(RANDOM, ImmutableList.of()),
                                new Call(RANDOM, ImmutableList.of())))))
                .describedAs("non-deterministic value")
                .isEqualTo(Optional.of(new In(
                        new Call(RANDOM, ImmutableList.of()),
                        ImmutableList.of(
                                new Reference(DOUBLE, "x"),
                                new Call(RANDOM, ImmutableList.of()),
                                new Call(RANDOM, ImmutableList.of())))));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new RemoveRedundantInItems(PLANNER_CONTEXT).apply(expression, testSession(), ImmutableMap.of());
    }
}
