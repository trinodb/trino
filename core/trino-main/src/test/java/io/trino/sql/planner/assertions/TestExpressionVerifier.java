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
package io.trino.sql.planner.assertions;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.sql.ir.BetweenPredicate;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.LogicalExpression;
import io.trino.sql.ir.NotExpression;
import io.trino.sql.ir.SymbolReference;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.ir.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.ir.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.NOT_EQUAL;
import static io.trino.sql.ir.LogicalExpression.Operator.AND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestExpressionVerifier
{
    @Test
    public void test()
    {
        Expression actual = new NotExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference(INTEGER, "orderkey"), new Constant(INTEGER, 3L)), new ComparisonExpression(EQUAL, new SymbolReference(INTEGER, "custkey"), new Constant(INTEGER, 3L)), new ComparisonExpression(LESS_THAN, new SymbolReference(INTEGER, "orderkey"), new Constant(INTEGER, 10L)))));

        SymbolAliases symbolAliases = SymbolAliases.builder()
                .put("X", new SymbolReference(INTEGER, "orderkey"))
                .put("Y", new SymbolReference(INTEGER, "custkey"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);

        assertThat(verifier.process(actual, new NotExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference(INTEGER, "X"), new Constant(INTEGER, 3L)), new ComparisonExpression(EQUAL, new SymbolReference(INTEGER, "Y"), new Constant(INTEGER, 3L)), new ComparisonExpression(LESS_THAN, new SymbolReference(INTEGER, "X"), new Constant(INTEGER, 10L))))))).isTrue();
        assertThatThrownBy(() -> verifier.process(actual, new NotExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference(INTEGER, "X"), new Constant(INTEGER, 3L)), new ComparisonExpression(EQUAL, new SymbolReference(INTEGER, "Y"), new Constant(INTEGER, 3L)), new ComparisonExpression(LESS_THAN, new SymbolReference(INTEGER, "Z"), new Constant(INTEGER, 10L)))))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("missing expression for alias Z");
        assertThat(verifier.process(actual, new NotExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference(INTEGER, "X"), new Constant(INTEGER, 3L)), new ComparisonExpression(EQUAL, new SymbolReference(INTEGER, "X"), new Constant(INTEGER, 3L)), new ComparisonExpression(LESS_THAN, new SymbolReference(INTEGER, "X"), new Constant(INTEGER, 10L))))))).isFalse();
    }

    @Test
    public void testCast()
    {
        SymbolAliases aliases = SymbolAliases.builder()
                .put("X", new SymbolReference(BIGINT, "orderkey"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(aliases);
        assertThat(verifier.process(new Constant(VARCHAR, Slices.utf8Slice("2")), new Constant(VARCHAR, Slices.utf8Slice("2")))).isTrue();
        assertThat(verifier.process(new Constant(VARCHAR, Slices.utf8Slice("2")), new Cast(new Constant(VARCHAR, Slices.utf8Slice("2")), BIGINT))).isFalse();
        assertThat(verifier.process(new Cast(new SymbolReference(BIGINT, "orderkey"), VARCHAR), new Cast(new SymbolReference(BIGINT, "X"), VARCHAR))).isTrue();
    }

    @Test
    public void testBetween()
    {
        SymbolAliases symbolAliases = SymbolAliases.builder()
                .put("X", new SymbolReference(BIGINT, "orderkey"))
                .put("Y", new SymbolReference(BIGINT, "custkey"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);
        // Complete match
        assertThat(verifier.process(new BetweenPredicate(new SymbolReference(BIGINT, "orderkey"), new Constant(INTEGER, 1L), new Constant(INTEGER, 2L)), new BetweenPredicate(new SymbolReference(INTEGER, "X"), new Constant(INTEGER, 1L), new Constant(INTEGER, 2L)))).isTrue();
        // Different value
        assertThat(verifier.process(new BetweenPredicate(new SymbolReference(BIGINT, "orderkey"), new Constant(INTEGER, 1L), new Constant(INTEGER, 2L)), new BetweenPredicate(new SymbolReference(BIGINT, "Y"), new Constant(INTEGER, 1L), new Constant(INTEGER, 2L)))).isFalse();
        assertThat(verifier.process(new BetweenPredicate(new SymbolReference(BIGINT, "custkey"), new Constant(INTEGER, 1L), new Constant(INTEGER, 2L)), new BetweenPredicate(new SymbolReference(BIGINT, "X"), new Constant(INTEGER, 1L), new Constant(INTEGER, 2L)))).isFalse();
        // Different min or max
        assertThat(verifier.process(new BetweenPredicate(new SymbolReference(BIGINT, "orderkey"), new Constant(INTEGER, 2L), new Constant(INTEGER, 4L)), new BetweenPredicate(new SymbolReference(BIGINT, "X"), new Constant(INTEGER, 1L), new Constant(INTEGER, 2L)))).isFalse();
        assertThat(verifier.process(new BetweenPredicate(new SymbolReference(BIGINT, "orderkey"), new Constant(INTEGER, 1L), new Constant(INTEGER, 2L)), new BetweenPredicate(new SymbolReference(BIGINT, "X"), new Constant(VARCHAR, Slices.utf8Slice("1")), new Constant(VARCHAR, Slices.utf8Slice("2"))))).isFalse();
        assertThat(verifier.process(new BetweenPredicate(new SymbolReference(BIGINT, "orderkey"), new Constant(INTEGER, 1L), new Constant(INTEGER, 2L)), new BetweenPredicate(new SymbolReference(BIGINT, "X"), new Constant(INTEGER, 4L), new Constant(INTEGER, 7L)))).isFalse();
    }

    @Test
    public void testSymmetry()
    {
        SymbolAliases symbolAliases = SymbolAliases.builder()
                .put("a", new SymbolReference(BIGINT, "x"))
                .put("b", new SymbolReference(BIGINT, "y"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);

        assertThat(verifier.process(new ComparisonExpression(GREATER_THAN, new SymbolReference(BIGINT, "x"), new SymbolReference(BIGINT, "y")), new ComparisonExpression(GREATER_THAN, new SymbolReference(BIGINT, "a"), new SymbolReference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(GREATER_THAN, new SymbolReference(BIGINT, "x"), new SymbolReference(BIGINT, "y")), new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "b"), new SymbolReference(BIGINT, "a")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "y"), new SymbolReference(BIGINT, "x")), new ComparisonExpression(GREATER_THAN, new SymbolReference(BIGINT, "a"), new SymbolReference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "y"), new SymbolReference(BIGINT, "x")), new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "b"), new SymbolReference(BIGINT, "a")))).isTrue();

        assertThat(verifier.process(new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "x"), new SymbolReference(BIGINT, "y")), new ComparisonExpression(GREATER_THAN, new SymbolReference(BIGINT, "a"), new SymbolReference(BIGINT, "b")))).isFalse();
        assertThat(verifier.process(new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "x"), new SymbolReference(BIGINT, "y")), new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "b"), new SymbolReference(BIGINT, "a")))).isFalse();
        assertThat(verifier.process(new ComparisonExpression(GREATER_THAN, new SymbolReference(BIGINT, "y"), new SymbolReference(BIGINT, "x")), new ComparisonExpression(GREATER_THAN, new SymbolReference(BIGINT, "a"), new SymbolReference(BIGINT, "b")))).isFalse();
        assertThat(verifier.process(new ComparisonExpression(GREATER_THAN, new SymbolReference(BIGINT, "y"), new SymbolReference(BIGINT, "x")), new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "b"), new SymbolReference(BIGINT, "a")))).isFalse();

        assertThat(verifier.process(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference(BIGINT, "x"), new SymbolReference(BIGINT, "y")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference(BIGINT, "a"), new SymbolReference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference(BIGINT, "x"), new SymbolReference(BIGINT, "y")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(BIGINT, "b"), new SymbolReference(BIGINT, "a")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(BIGINT, "y"), new SymbolReference(BIGINT, "x")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference(BIGINT, "a"), new SymbolReference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(BIGINT, "y"), new SymbolReference(BIGINT, "x")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(BIGINT, "b"), new SymbolReference(BIGINT, "a")))).isTrue();

        assertThat(verifier.process(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(BIGINT, "x"), new SymbolReference(BIGINT, "y")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference(BIGINT, "a"), new SymbolReference(BIGINT, "b")))).isFalse();
        assertThat(verifier.process(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(BIGINT, "x"), new SymbolReference(BIGINT, "y")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(BIGINT, "b"), new SymbolReference(BIGINT, "a")))).isFalse();
        assertThat(verifier.process(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference(BIGINT, "y"), new SymbolReference(BIGINT, "x")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference(BIGINT, "a"), new SymbolReference(BIGINT, "b")))).isFalse();
        assertThat(verifier.process(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference(BIGINT, "y"), new SymbolReference(BIGINT, "x")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(BIGINT, "b"), new SymbolReference(BIGINT, "a")))).isFalse();

        assertThat(verifier.process(new ComparisonExpression(EQUAL, new SymbolReference(BIGINT, "x"), new SymbolReference(BIGINT, "y")), new ComparisonExpression(EQUAL, new SymbolReference(BIGINT, "a"), new SymbolReference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(EQUAL, new SymbolReference(BIGINT, "x"), new SymbolReference(BIGINT, "y")), new ComparisonExpression(EQUAL, new SymbolReference(BIGINT, "b"), new SymbolReference(BIGINT, "a")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(EQUAL, new SymbolReference(BIGINT, "y"), new SymbolReference(BIGINT, "x")), new ComparisonExpression(EQUAL, new SymbolReference(BIGINT, "a"), new SymbolReference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(EQUAL, new SymbolReference(BIGINT, "y"), new SymbolReference(BIGINT, "x")), new ComparisonExpression(EQUAL, new SymbolReference(BIGINT, "b"), new SymbolReference(BIGINT, "a")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(NOT_EQUAL, new SymbolReference(BIGINT, "x"), new SymbolReference(BIGINT, "y")), new ComparisonExpression(NOT_EQUAL, new SymbolReference(BIGINT, "a"), new SymbolReference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(NOT_EQUAL, new SymbolReference(BIGINT, "x"), new SymbolReference(BIGINT, "y")), new ComparisonExpression(NOT_EQUAL, new SymbolReference(BIGINT, "b"), new SymbolReference(BIGINT, "a")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(NOT_EQUAL, new SymbolReference(BIGINT, "y"), new SymbolReference(BIGINT, "x")), new ComparisonExpression(NOT_EQUAL, new SymbolReference(BIGINT, "a"), new SymbolReference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(NOT_EQUAL, new SymbolReference(BIGINT, "y"), new SymbolReference(BIGINT, "x")), new ComparisonExpression(NOT_EQUAL, new SymbolReference(BIGINT, "b"), new SymbolReference(BIGINT, "a")))).isTrue();

        assertThat(verifier.process(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(BIGINT, "x"), new SymbolReference(BIGINT, "y")), new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(BIGINT, "a"), new SymbolReference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(BIGINT, "x"), new SymbolReference(BIGINT, "y")), new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(BIGINT, "b"), new SymbolReference(BIGINT, "a")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(BIGINT, "y"), new SymbolReference(BIGINT, "x")), new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(BIGINT, "a"), new SymbolReference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(BIGINT, "y"), new SymbolReference(BIGINT, "x")), new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(BIGINT, "b"), new SymbolReference(BIGINT, "a")))).isTrue();
    }
}
