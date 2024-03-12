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
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.SymbolReference;
import io.trino.transaction.TestingTransactionManager;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;
import static io.trino.sql.tree.LogicalExpression.Operator.OR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSortExpressionExtractor
{
    private static final TestingTransactionManager TRANSACTION_MANAGER = new TestingTransactionManager();
    private static final PlannerContext PLANNER_CONTEXT = plannerContextBuilder()
            .withTransactionManager(TRANSACTION_MANAGER)
            .build();
    private static final Set<Symbol> BUILD_SYMBOLS = ImmutableSet.of(new Symbol("b1"), new Symbol("b2"));

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction SIN = FUNCTIONS.resolveFunction("sin", fromTypes(DOUBLE));
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", fromTypes(BIGINT));

    @Test
    public void testGetSortExpression()
    {
        assertGetSortExpression(
                new ComparisonExpression(GREATER_THAN, new SymbolReference("p1"), new SymbolReference("b1")),
                "b1");

        assertGetSortExpression(
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("b2"), new SymbolReference("p1")),
                "b2");

        assertGetSortExpression(
                new ComparisonExpression(GREATER_THAN, new SymbolReference("b2"), new SymbolReference("p1")),
                "b2");

        assertGetSortExpression(
                new ComparisonExpression(GREATER_THAN, new SymbolReference("b2"), new FunctionCall(SIN.toQualifiedName(), ImmutableList.of(new SymbolReference("p1")))),
                "b2");

        assertNoSortExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("b2"), new FunctionCall(RANDOM.toQualifiedName(), ImmutableList.of(new SymbolReference("p1")))));

        assertGetSortExpression(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("b2"), new FunctionCall(RANDOM.toQualifiedName(), ImmutableList.of(new SymbolReference("p1")))), new ComparisonExpression(GREATER_THAN, new SymbolReference("b2"), new SymbolReference("p1")))),
                "b2",
                new ComparisonExpression(GREATER_THAN, new SymbolReference("b2"), new SymbolReference("p1")));

        assertGetSortExpression(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("b2"), new FunctionCall(RANDOM.toQualifiedName(), ImmutableList.of(new SymbolReference("p1")))), new ComparisonExpression(GREATER_THAN, new SymbolReference("b1"), new SymbolReference("p1")))),
                "b1",
                new ComparisonExpression(GREATER_THAN, new SymbolReference("b1"), new SymbolReference("p1")));

        assertNoSortExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("b1"), new ArithmeticBinaryExpression(ADD, new SymbolReference("p1"), new SymbolReference("b2"))));

        assertNoSortExpression(new ComparisonExpression(GREATER_THAN, new FunctionCall(SIN.toQualifiedName(), ImmutableList.of(new SymbolReference("b1"))), new SymbolReference("p1")));

        assertNoSortExpression(new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("b1"), new SymbolReference("p1")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("b2"), new SymbolReference("p1")))));

        assertNoSortExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new FunctionCall(SIN.toQualifiedName(), ImmutableList.of(new SymbolReference("b2"))), new SymbolReference("p1")), new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("b2"), new SymbolReference("p1")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("b2"), new ArithmeticBinaryExpression(ADD, new SymbolReference("p1"), new LongLiteral("10"))))))));

        assertGetSortExpression(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new FunctionCall(SIN.toQualifiedName(), ImmutableList.of(new SymbolReference("b2"))), new SymbolReference("p1")), new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("b2"), new SymbolReference("p1")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("b2"), new ArithmeticBinaryExpression(ADD, new SymbolReference("p1"), new LongLiteral("10"))))))),
                "b2",
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("b2"), new SymbolReference("p1")),
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("b2"), new ArithmeticBinaryExpression(ADD, new SymbolReference("p1"), new LongLiteral("10"))));

        assertGetSortExpression(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("b1"), new SymbolReference("p1")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("b1"), new SymbolReference("p1")))),
                "b1");

        assertGetSortExpression(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("b1"), new SymbolReference("p1")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("b1"), new SymbolReference("p1")), new ComparisonExpression(GREATER_THAN, new SymbolReference("b2"), new SymbolReference("p1")))),
                "b1",
                new ComparisonExpression(GREATER_THAN, new SymbolReference("b1"), new SymbolReference("p1")),
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("b1"), new SymbolReference("p1")));

        assertGetSortExpression(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("b1"), new SymbolReference("p1")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("b1"), new SymbolReference("p1")), new ComparisonExpression(GREATER_THAN, new SymbolReference("b2"), new SymbolReference("p1")), new ComparisonExpression(LESS_THAN, new SymbolReference("b2"), new ArithmeticBinaryExpression(ADD, new SymbolReference("p1"), new LongLiteral("10"))), new ComparisonExpression(GREATER_THAN, new SymbolReference("b2"), new SymbolReference("p2")))),
                "b2",
                new ComparisonExpression(GREATER_THAN, new SymbolReference("b2"), new SymbolReference("p1")),
                new ComparisonExpression(LESS_THAN, new SymbolReference("b2"), new ArithmeticBinaryExpression(ADD, new SymbolReference("p1"), new LongLiteral("10"))),
                new ComparisonExpression(GREATER_THAN, new SymbolReference("b2"), new SymbolReference("p2")));

        assertGetSortExpression(
                new BetweenPredicate(new SymbolReference("p1"), new SymbolReference("b1"), new SymbolReference("b2")),
                "b1",
                new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("p1"), new SymbolReference("b1")));

        assertGetSortExpression(
                new BetweenPredicate(new SymbolReference("p1"), new SymbolReference("p2"), new SymbolReference("b1")),
                "b1",
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("p1"), new SymbolReference("b1")));

        assertGetSortExpression(
                new BetweenPredicate(new SymbolReference("b1"), new SymbolReference("p1"), new SymbolReference("p2")),
                "b1",
                new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("b1"), new SymbolReference("p1")));

        assertGetSortExpression(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("b1"), new SymbolReference("p1")), new BetweenPredicate(new SymbolReference("p1"), new SymbolReference("b1"), new SymbolReference("b2")))),
                "b1",
                new ComparisonExpression(GREATER_THAN, new SymbolReference("b1"), new SymbolReference("p1")),
                new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("p1"), new SymbolReference("b1")));
    }

    private void assertNoSortExpression(Expression expression)
    {
        Optional<SortExpressionContext> actual = SortExpressionExtractor.extractSortExpression(PLANNER_CONTEXT.getMetadata(), BUILD_SYMBOLS, expression);
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
        Optional<SortExpressionContext> expected = Optional.of(new SortExpressionContext(new SymbolReference(expectedSymbol), searchExpressions));
        Optional<SortExpressionContext> actual = SortExpressionExtractor.extractSortExpression(PLANNER_CONTEXT.getMetadata(), BUILD_SYMBOLS, expression);
        assertThat(actual).isEqualTo(expected);
    }
}
