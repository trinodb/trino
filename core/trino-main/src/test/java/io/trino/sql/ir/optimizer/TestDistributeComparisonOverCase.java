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
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.rule.DistributeComparisonOverCase;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDistributeComparisonOverCase
{
    @Test
    void test()
    {
        assertThat(optimize(
                new Comparison(
                        LESS_THAN,
                        new Case(
                                ImmutableList.of(
                                        new WhenClause(new Reference(BOOLEAN, "a"), new Reference(BIGINT, "x")),
                                        new WhenClause(new Reference(BOOLEAN, "b"), new Reference(BIGINT, "y"))),
                                new Reference(BIGINT, "z")),
                        new Reference(BIGINT, "m"))))
                .describedAs("case(...) < reference")
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(
                                new WhenClause(new Reference(BOOLEAN, "a"),
                                        new Comparison(LESS_THAN, new Reference(BIGINT, "x"), new Reference(BIGINT, "m"))),
                                new WhenClause(new Reference(BOOLEAN, "b"),
                                        new Comparison(LESS_THAN, new Reference(BIGINT, "y"), new Reference(BIGINT, "m")))),
                        new Comparison(LESS_THAN, new Reference(BIGINT, "z"), new Reference(BIGINT, "m")))));

        assertThat(optimize(
                new Comparison(
                        LESS_THAN,
                        new Case(
                                ImmutableList.of(
                                        new WhenClause(new Reference(BOOLEAN, "a"), new Reference(BIGINT, "x")),
                                        new WhenClause(new Reference(BOOLEAN, "b"), new Reference(BIGINT, "y"))),
                                new Reference(BIGINT, "z")),
                        new Constant(BIGINT, 1L))))
                .describedAs("case(...) < constant")
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(
                                new WhenClause(new Reference(BOOLEAN, "a"),
                                        new Comparison(LESS_THAN, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L))),
                                new WhenClause(new Reference(BOOLEAN, "b"),
                                        new Comparison(LESS_THAN, new Reference(BIGINT, "y"), new Constant(BIGINT, 1L)))),
                        new Comparison(LESS_THAN, new Reference(BIGINT, "z"), new Constant(BIGINT, 1L)))));

        assertThat(optimize(
                new Comparison(
                        LESS_THAN,
                        new Reference(BIGINT, "m"),
                        new Case(
                                ImmutableList.of(
                                        new WhenClause(new Reference(BOOLEAN, "a"), new Reference(BIGINT, "x")),
                                        new WhenClause(new Reference(BOOLEAN, "b"), new Reference(BIGINT, "y"))),
                                new Reference(BIGINT, "z")))))
                .describedAs("reference < case(...)")
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(
                                new WhenClause(new Reference(BOOLEAN, "a"),
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "x"), new Reference(BIGINT, "m"))),
                                new WhenClause(new Reference(BOOLEAN, "b"),
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "y"), new Reference(BIGINT, "m")))),
                        new Comparison(GREATER_THAN, new Reference(BIGINT, "z"), new Reference(BIGINT, "m")))));

        assertThat(optimize(
                new Comparison(
                        LESS_THAN,
                        new Constant(BIGINT, 1L),
                        new Case(
                                ImmutableList.of(
                                        new WhenClause(new Reference(BOOLEAN, "a"), new Reference(BIGINT, "x")),
                                        new WhenClause(new Reference(BOOLEAN, "b"), new Reference(BIGINT, "y"))),
                                new Reference(BIGINT, "z")))))
                .describedAs("constant < case(...)")
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(
                                new WhenClause(new Reference(BOOLEAN, "a"),
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L))),
                                new WhenClause(new Reference(BOOLEAN, "b"),
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "y"), new Constant(BIGINT, 1L)))),
                        new Comparison(GREATER_THAN, new Reference(BIGINT, "z"), new Constant(BIGINT, 1L)))));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new DistributeComparisonOverCase().apply(expression, testSession(), ImmutableMap.of());
    }
}
