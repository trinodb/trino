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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.ir.TestingIr.between;
import static io.trino.sql.ir.TestingIr.comparison;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSortExpressionExtractor
{
    private static final Set<Symbol> BUILD_SYMBOLS = ImmutableSet.of(new Symbol(BIGINT, "b1"), new Symbol(BIGINT, "b2"));

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", fromTypes(BIGINT));
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));

    @Test
    public void testGetSortExpression()
    {
        assertGetSortExpression(
                comparison(GREATER_THAN, new Reference(BIGINT, "p1"), new Reference(BIGINT, "b1")),
                "b1");

        assertGetSortExpression(
                comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "b2"), new Reference(BIGINT, "p1")),
                "b2");

        assertGetSortExpression(
                comparison(GREATER_THAN, new Reference(BIGINT, "b2"), new Reference(BIGINT, "p1")),
                "b2");

        assertGetSortExpression(
                new Logical(AND, ImmutableList.of(comparison(GREATER_THAN, new Reference(BIGINT, "b2"), new Call(RANDOM, ImmutableList.of(new Reference(BIGINT, "p1")))), comparison(GREATER_THAN, new Reference(BIGINT, "b1"), new Reference(BIGINT, "p1")))),
                "b1",
                comparison(GREATER_THAN, new Reference(BIGINT, "b1"), new Reference(BIGINT, "p1")));

        assertNoSortExpression(comparison(GREATER_THAN, new Reference(BIGINT, "b1"), new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "p1"), new Reference(BIGINT, "b2")))));

        assertNoSortExpression(new Logical(OR, ImmutableList.of(comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "b1"), new Reference(BIGINT, "p1")), comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "b2"), new Reference(BIGINT, "p1")))));

        assertGetSortExpression(
                new Logical(AND, ImmutableList.of(comparison(GREATER_THAN, new Reference(BIGINT, "b1"), new Reference(BIGINT, "p1")), comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "b1"), new Reference(BIGINT, "p1")))),
                "b1");

        assertGetSortExpression(
                new Logical(AND, ImmutableList.of(comparison(GREATER_THAN, new Reference(BIGINT, "b1"), new Reference(BIGINT, "p1")), comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "b1"), new Reference(BIGINT, "p1")), comparison(GREATER_THAN, new Reference(BIGINT, "b2"), new Reference(BIGINT, "p1")))),
                "b1",
                comparison(GREATER_THAN, new Reference(BIGINT, "b1"), new Reference(BIGINT, "p1")),
                comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "b1"), new Reference(BIGINT, "p1")));

        assertGetSortExpression(
                new Logical(AND, ImmutableList.of(comparison(GREATER_THAN, new Reference(BIGINT, "b1"), new Reference(BIGINT, "p1")), comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "b1"), new Reference(BIGINT, "p1")), comparison(GREATER_THAN, new Reference(BIGINT, "b2"), new Reference(BIGINT, "p1")), comparison(LESS_THAN, new Reference(BIGINT, "b2"), new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "p1"), new Constant(BIGINT, 10L)))), comparison(GREATER_THAN, new Reference(BIGINT, "b2"), new Reference(BIGINT, "p2")))),
                "b2",
                comparison(GREATER_THAN, new Reference(BIGINT, "b2"), new Reference(BIGINT, "p1")),
                comparison(LESS_THAN, new Reference(BIGINT, "b2"), new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "p1"), new Constant(BIGINT, 10L)))),
                comparison(GREATER_THAN, new Reference(BIGINT, "b2"), new Reference(BIGINT, "p2")));

        assertGetSortExpression(
                new Logical(AND, ImmutableList.of(
                        between(new Reference(BIGINT, "p1"), new Reference(BIGINT, "b1"), new Reference(BIGINT, "b2")),
                        comparison(
                                LESS_THAN,
                                new Reference(BIGINT, "b2"),
                                new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "p2"), new Constant(BIGINT, 1L)))))),
                "b2",
                comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "p1"), new Reference(BIGINT, "b2")),
                comparison(
                        LESS_THAN,
                        new Reference(BIGINT, "b2"),
                        new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "p2"), new Constant(BIGINT, 1L)))));

        assertGetSortExpression(
                between(new Reference(BIGINT, "p1"), new Reference(BIGINT, "p2"), new Reference(BIGINT, "b1")),
                "b1",
                comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "p1"), new Reference(BIGINT, "b1")));

        assertGetSortExpression(
                between(new Reference(BIGINT, "b1"), new Reference(BIGINT, "p1"), new Reference(BIGINT, "p2")),
                "b1",
                comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "b1"), new Reference(BIGINT, "p1")),
                comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "b1"), new Reference(BIGINT, "p2")));

        assertGetSortExpression(
                new Logical(AND, ImmutableList.of(comparison(GREATER_THAN, new Reference(BIGINT, "b1"), new Reference(BIGINT, "p1")), between(new Reference(BIGINT, "p1"), new Reference(BIGINT, "b1"), new Reference(BIGINT, "b2")))),
                "b1",
                comparison(GREATER_THAN, new Reference(BIGINT, "b1"), new Reference(BIGINT, "p1")),
                comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "p1"), new Reference(BIGINT, "b1")));
    }

    private void assertNoSortExpression(Expression expression)
    {
        Optional<SortExpressionContext> actual = SortExpressionExtractor.extractSortExpression(BUILD_SYMBOLS, expression);
        assertThat(actual).isEqualTo(Optional.empty());
    }

    private void assertGetSortExpression(Expression expression, String expectedSymbol)
    {
        // for now we expect that search expressions contain all the conjuncts from filterExpression as more complex cases are not supported yet.
        assertGetSortExpression(expression, expectedSymbol, extractConjuncts(expression));
    }

    private void assertGetSortExpression(Expression expression, String expectedSymbol, Expression... searchExpressions)
    {
        assertGetSortExpression(expression, expectedSymbol, Arrays.asList(searchExpressions));
    }

    private void assertGetSortExpression(Expression expression, String expectedSymbol, List<Expression> searchExpressions)
    {
        Optional<SortExpressionContext> expected = Optional.of(new SortExpressionContext(new Reference(BIGINT, expectedSymbol), searchExpressions));
        Optional<SortExpressionContext> actual = SortExpressionExtractor.extractSortExpression(BUILD_SYMBOLS, expression);
        assertThat(actual).isEqualTo(expected);
    }
}
