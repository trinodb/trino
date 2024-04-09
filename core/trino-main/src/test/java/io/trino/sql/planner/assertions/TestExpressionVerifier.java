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
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.Reference;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.NOT_EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestExpressionVerifier
{
    @Test
    public void test()
    {
        Expression actual = new Not(new Logical(AND, ImmutableList.of(new Comparison(EQUAL, new Reference(INTEGER, "orderkey"), new Constant(INTEGER, 3L)), new Comparison(EQUAL, new Reference(INTEGER, "custkey"), new Constant(INTEGER, 3L)), new Comparison(LESS_THAN, new Reference(INTEGER, "orderkey"), new Constant(INTEGER, 10L)))));

        SymbolAliases symbolAliases = SymbolAliases.builder()
                .put("X", new Reference(INTEGER, "orderkey"))
                .put("Y", new Reference(INTEGER, "custkey"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);

        assertThat(verifier.process(actual, new Not(new Logical(AND, ImmutableList.of(new Comparison(EQUAL, new Reference(INTEGER, "X"), new Constant(INTEGER, 3L)), new Comparison(EQUAL, new Reference(INTEGER, "Y"), new Constant(INTEGER, 3L)), new Comparison(LESS_THAN, new Reference(INTEGER, "X"), new Constant(INTEGER, 10L))))))).isTrue();
        assertThatThrownBy(() -> verifier.process(actual, new Not(new Logical(AND, ImmutableList.of(new Comparison(EQUAL, new Reference(INTEGER, "X"), new Constant(INTEGER, 3L)), new Comparison(EQUAL, new Reference(INTEGER, "Y"), new Constant(INTEGER, 3L)), new Comparison(LESS_THAN, new Reference(INTEGER, "Z"), new Constant(INTEGER, 10L)))))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("missing expression for alias Z");
        assertThat(verifier.process(actual, new Not(new Logical(AND, ImmutableList.of(new Comparison(EQUAL, new Reference(INTEGER, "X"), new Constant(INTEGER, 3L)), new Comparison(EQUAL, new Reference(INTEGER, "X"), new Constant(INTEGER, 3L)), new Comparison(LESS_THAN, new Reference(INTEGER, "X"), new Constant(INTEGER, 10L))))))).isFalse();
    }

    @Test
    public void testCast()
    {
        SymbolAliases aliases = SymbolAliases.builder()
                .put("X", new Reference(BIGINT, "orderkey"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(aliases);
        assertThat(verifier.process(new Constant(VARCHAR, Slices.utf8Slice("2")), new Constant(VARCHAR, Slices.utf8Slice("2")))).isTrue();
        assertThat(verifier.process(new Constant(VARCHAR, Slices.utf8Slice("2")), new Cast(new Constant(VARCHAR, Slices.utf8Slice("2")), BIGINT))).isFalse();
        assertThat(verifier.process(new Cast(new Reference(BIGINT, "orderkey"), VARCHAR), new Cast(new Reference(BIGINT, "X"), VARCHAR))).isTrue();
    }

    @Test
    public void testBetween()
    {
        SymbolAliases symbolAliases = SymbolAliases.builder()
                .put("X", new Reference(BIGINT, "orderkey"))
                .put("Y", new Reference(BIGINT, "custkey"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);
        // Complete match
        assertThat(verifier.process(new Between(new Reference(BIGINT, "orderkey"), new Constant(BIGINT, 1L), new Constant(BIGINT, 2L)), new Between(new Reference(BIGINT, "X"), new Constant(BIGINT, 1L), new Constant(BIGINT, 2L)))).isTrue();
        // Different value
        assertThat(verifier.process(new Between(new Reference(BIGINT, "orderkey"), new Constant(BIGINT, 1L), new Constant(BIGINT, 2L)), new Between(new Reference(BIGINT, "Y"), new Constant(BIGINT, 1L), new Constant(BIGINT, 2L)))).isFalse();
        assertThat(verifier.process(new Between(new Reference(BIGINT, "custkey"), new Constant(BIGINT, 1L), new Constant(BIGINT, 2L)), new Between(new Reference(BIGINT, "X"), new Constant(BIGINT, 1L), new Constant(BIGINT, 2L)))).isFalse();
    }

    @Test
    public void testSymmetry()
    {
        SymbolAliases symbolAliases = SymbolAliases.builder()
                .put("a", new Reference(BIGINT, "x"))
                .put("b", new Reference(BIGINT, "y"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);

        assertThat(verifier.process(new Comparison(GREATER_THAN, new Reference(BIGINT, "x"), new Reference(BIGINT, "y")), new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new Comparison(GREATER_THAN, new Reference(BIGINT, "x"), new Reference(BIGINT, "y")), new Comparison(LESS_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")))).isTrue();
        assertThat(verifier.process(new Comparison(LESS_THAN, new Reference(BIGINT, "y"), new Reference(BIGINT, "x")), new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new Comparison(LESS_THAN, new Reference(BIGINT, "y"), new Reference(BIGINT, "x")), new Comparison(LESS_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")))).isTrue();

        assertThat(verifier.process(new Comparison(LESS_THAN, new Reference(BIGINT, "x"), new Reference(BIGINT, "y")), new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))).isFalse();
        assertThat(verifier.process(new Comparison(LESS_THAN, new Reference(BIGINT, "x"), new Reference(BIGINT, "y")), new Comparison(LESS_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")))).isFalse();
        assertThat(verifier.process(new Comparison(GREATER_THAN, new Reference(BIGINT, "y"), new Reference(BIGINT, "x")), new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))).isFalse();
        assertThat(verifier.process(new Comparison(GREATER_THAN, new Reference(BIGINT, "y"), new Reference(BIGINT, "x")), new Comparison(LESS_THAN, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")))).isFalse();

        assertThat(verifier.process(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "x"), new Reference(BIGINT, "y")), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "x"), new Reference(BIGINT, "y")), new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")))).isTrue();
        assertThat(verifier.process(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "y"), new Reference(BIGINT, "x")), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "y"), new Reference(BIGINT, "x")), new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")))).isTrue();

        assertThat(verifier.process(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "x"), new Reference(BIGINT, "y")), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))).isFalse();
        assertThat(verifier.process(new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "x"), new Reference(BIGINT, "y")), new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")))).isFalse();
        assertThat(verifier.process(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "y"), new Reference(BIGINT, "x")), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))).isFalse();
        assertThat(verifier.process(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "y"), new Reference(BIGINT, "x")), new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")))).isFalse();

        assertThat(verifier.process(new Comparison(EQUAL, new Reference(BIGINT, "x"), new Reference(BIGINT, "y")), new Comparison(EQUAL, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new Comparison(EQUAL, new Reference(BIGINT, "x"), new Reference(BIGINT, "y")), new Comparison(EQUAL, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")))).isTrue();
        assertThat(verifier.process(new Comparison(EQUAL, new Reference(BIGINT, "y"), new Reference(BIGINT, "x")), new Comparison(EQUAL, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new Comparison(EQUAL, new Reference(BIGINT, "y"), new Reference(BIGINT, "x")), new Comparison(EQUAL, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")))).isTrue();
        assertThat(verifier.process(new Comparison(NOT_EQUAL, new Reference(BIGINT, "x"), new Reference(BIGINT, "y")), new Comparison(NOT_EQUAL, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new Comparison(NOT_EQUAL, new Reference(BIGINT, "x"), new Reference(BIGINT, "y")), new Comparison(NOT_EQUAL, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")))).isTrue();
        assertThat(verifier.process(new Comparison(NOT_EQUAL, new Reference(BIGINT, "y"), new Reference(BIGINT, "x")), new Comparison(NOT_EQUAL, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new Comparison(NOT_EQUAL, new Reference(BIGINT, "y"), new Reference(BIGINT, "x")), new Comparison(NOT_EQUAL, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")))).isTrue();

        assertThat(verifier.process(new Comparison(IS_DISTINCT_FROM, new Reference(BIGINT, "x"), new Reference(BIGINT, "y")), new Comparison(IS_DISTINCT_FROM, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new Comparison(IS_DISTINCT_FROM, new Reference(BIGINT, "x"), new Reference(BIGINT, "y")), new Comparison(IS_DISTINCT_FROM, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")))).isTrue();
        assertThat(verifier.process(new Comparison(IS_DISTINCT_FROM, new Reference(BIGINT, "y"), new Reference(BIGINT, "x")), new Comparison(IS_DISTINCT_FROM, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))).isTrue();
        assertThat(verifier.process(new Comparison(IS_DISTINCT_FROM, new Reference(BIGINT, "y"), new Reference(BIGINT, "x")), new Comparison(IS_DISTINCT_FROM, new Reference(BIGINT, "b"), new Reference(BIGINT, "a")))).isTrue();
    }
}
