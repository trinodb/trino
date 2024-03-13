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
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestExpressionVerifier
{
    @Test
    public void test()
    {
        Expression actual = new NotExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference("orderkey"), new LongLiteral("3")), new ComparisonExpression(EQUAL, new SymbolReference("custkey"), new LongLiteral("3")), new ComparisonExpression(LESS_THAN, new SymbolReference("orderkey"), new LongLiteral("10")))));

        SymbolAliases symbolAliases = SymbolAliases.builder()
                .put("X", new SymbolReference("orderkey"))
                .put("Y", new SymbolReference("custkey"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);

        assertThat(verifier.process(actual, new NotExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference("X"), new LongLiteral("3")), new ComparisonExpression(EQUAL, new SymbolReference("Y"), new LongLiteral("3")), new ComparisonExpression(LESS_THAN, new SymbolReference("X"), new LongLiteral("10"))))))).isTrue();
        assertThatThrownBy(() -> verifier.process(actual, new NotExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference("X"), new LongLiteral("3")), new ComparisonExpression(EQUAL, new SymbolReference("Y"), new LongLiteral("3")), new ComparisonExpression(LESS_THAN, new SymbolReference("Z"), new LongLiteral("10")))))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("missing expression for alias Z");
        assertThat(verifier.process(actual, new NotExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference("X"), new LongLiteral("3")), new ComparisonExpression(EQUAL, new SymbolReference("X"), new LongLiteral("3")), new ComparisonExpression(LESS_THAN, new SymbolReference("X"), new LongLiteral("10"))))))).isFalse();
    }

    @Test
    public void testCast()
    {
        SymbolAliases aliases = SymbolAliases.builder()
                .put("X", new SymbolReference("orderkey"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(aliases);
        assertThat(verifier.process(new GenericLiteral("VARCHAR", "2"), new GenericLiteral("VARCHAR", "2"))).isTrue();
        assertThat(verifier.process(new GenericLiteral("VARCHAR", "2"), new Cast(new StringLiteral("2"), dataType("bigint")))).isFalse();
        assertThat(verifier.process(new Cast(new SymbolReference("orderkey"), dataType("varchar")), new Cast(new SymbolReference("X"), dataType("varchar")))).isTrue();
    }

    @Test
    public void testBetween()
    {
        SymbolAliases symbolAliases = SymbolAliases.builder()
                .put("X", new SymbolReference("orderkey"))
                .put("Y", new SymbolReference("custkey"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);
        // Complete match
        assertThat(verifier.process(new BetweenPredicate(new SymbolReference("orderkey"), new LongLiteral("1"), new LongLiteral("2")), new BetweenPredicate(new SymbolReference("X"), new LongLiteral("1"), new LongLiteral("2")))).isTrue();
        // Different value
        assertThat(verifier.process(new BetweenPredicate(new SymbolReference("orderkey"), new LongLiteral("1"), new LongLiteral("2")), new BetweenPredicate(new SymbolReference("Y"), new LongLiteral("1"), new LongLiteral("2")))).isFalse();
        assertThat(verifier.process(new BetweenPredicate(new SymbolReference("custkey"), new LongLiteral("1"), new LongLiteral("2")), new BetweenPredicate(new SymbolReference("X"), new LongLiteral("1"), new LongLiteral("2")))).isFalse();
        // Different min or max
        assertThat(verifier.process(new BetweenPredicate(new SymbolReference("orderkey"), new LongLiteral("2"), new LongLiteral("4")), new BetweenPredicate(new SymbolReference("X"), new LongLiteral("1"), new LongLiteral("2")))).isFalse();
        assertThat(verifier.process(new BetweenPredicate(new SymbolReference("orderkey"), new LongLiteral("1"), new LongLiteral("2")), new BetweenPredicate(new SymbolReference("X"), new StringLiteral("1"), new StringLiteral("2")))).isFalse();
        assertThat(verifier.process(new BetweenPredicate(new SymbolReference("orderkey"), new LongLiteral("1"), new LongLiteral("2")), new BetweenPredicate(new SymbolReference("X"), new LongLiteral("4"), new LongLiteral("7")))).isFalse();
    }

    @Test
    public void testSymmetry()
    {
        SymbolAliases symbolAliases = SymbolAliases.builder()
                .put("a", new SymbolReference("x"))
                .put("b", new SymbolReference("y"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);

        assertThat(verifier.process(new ComparisonExpression(GREATER_THAN, new SymbolReference("x"), new SymbolReference("y")), new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new SymbolReference("b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(GREATER_THAN, new SymbolReference("x"), new SymbolReference("y")), new ComparisonExpression(LESS_THAN, new SymbolReference("b"), new SymbolReference("a")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(LESS_THAN, new SymbolReference("y"), new SymbolReference("x")), new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new SymbolReference("b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(LESS_THAN, new SymbolReference("y"), new SymbolReference("x")), new ComparisonExpression(LESS_THAN, new SymbolReference("b"), new SymbolReference("a")))).isTrue();

        assertThat(verifier.process(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new SymbolReference("y")), new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new SymbolReference("b")))).isFalse();
        assertThat(verifier.process(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new SymbolReference("y")), new ComparisonExpression(LESS_THAN, new SymbolReference("b"), new SymbolReference("a")))).isFalse();
        assertThat(verifier.process(new ComparisonExpression(GREATER_THAN, new SymbolReference("y"), new SymbolReference("x")), new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new SymbolReference("b")))).isFalse();
        assertThat(verifier.process(new ComparisonExpression(GREATER_THAN, new SymbolReference("y"), new SymbolReference("x")), new ComparisonExpression(LESS_THAN, new SymbolReference("b"), new SymbolReference("a")))).isFalse();

        assertThat(verifier.process(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("x"), new SymbolReference("y")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new SymbolReference("b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("x"), new SymbolReference("y")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("b"), new SymbolReference("a")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("y"), new SymbolReference("x")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new SymbolReference("b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("y"), new SymbolReference("x")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("b"), new SymbolReference("a")))).isTrue();

        assertThat(verifier.process(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("x"), new SymbolReference("y")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new SymbolReference("b")))).isFalse();
        assertThat(verifier.process(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("x"), new SymbolReference("y")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("b"), new SymbolReference("a")))).isFalse();
        assertThat(verifier.process(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("y"), new SymbolReference("x")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new SymbolReference("b")))).isFalse();
        assertThat(verifier.process(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("y"), new SymbolReference("x")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("b"), new SymbolReference("a")))).isFalse();

        assertThat(verifier.process(new ComparisonExpression(EQUAL, new SymbolReference("x"), new SymbolReference("y")), new ComparisonExpression(EQUAL, new SymbolReference("a"), new SymbolReference("b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(EQUAL, new SymbolReference("x"), new SymbolReference("y")), new ComparisonExpression(EQUAL, new SymbolReference("b"), new SymbolReference("a")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(EQUAL, new SymbolReference("y"), new SymbolReference("x")), new ComparisonExpression(EQUAL, new SymbolReference("a"), new SymbolReference("b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(EQUAL, new SymbolReference("y"), new SymbolReference("x")), new ComparisonExpression(EQUAL, new SymbolReference("b"), new SymbolReference("a")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(NOT_EQUAL, new SymbolReference("x"), new SymbolReference("y")), new ComparisonExpression(NOT_EQUAL, new SymbolReference("a"), new SymbolReference("b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(NOT_EQUAL, new SymbolReference("x"), new SymbolReference("y")), new ComparisonExpression(NOT_EQUAL, new SymbolReference("b"), new SymbolReference("a")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(NOT_EQUAL, new SymbolReference("y"), new SymbolReference("x")), new ComparisonExpression(NOT_EQUAL, new SymbolReference("a"), new SymbolReference("b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(NOT_EQUAL, new SymbolReference("y"), new SymbolReference("x")), new ComparisonExpression(NOT_EQUAL, new SymbolReference("b"), new SymbolReference("a")))).isTrue();

        assertThat(verifier.process(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("x"), new SymbolReference("y")), new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("a"), new SymbolReference("b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("x"), new SymbolReference("y")), new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("b"), new SymbolReference("a")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("y"), new SymbolReference("x")), new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("a"), new SymbolReference("b")))).isTrue();
        assertThat(verifier.process(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("y"), new SymbolReference("x")), new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("b"), new SymbolReference("a")))).isTrue();
    }
}
