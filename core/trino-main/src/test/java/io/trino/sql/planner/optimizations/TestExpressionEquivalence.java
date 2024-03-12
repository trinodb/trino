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
package io.trino.sql.planner.optimizations;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import io.trino.transaction.TestingTransactionManager;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;
import static io.trino.sql.tree.LogicalExpression.Operator.OR;
import static io.trino.testing.TransactionBuilder.transaction;
import static java.lang.String.format;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExpressionEquivalence
{
    private static final TestingTransactionManager TRANSACTION_MANAGER = new TestingTransactionManager();
    private static final PlannerContext PLANNER_CONTEXT = plannerContextBuilder()
            .withTransactionManager(TRANSACTION_MANAGER)
            .build();
    private static final ExpressionEquivalence EQUIVALENCE = new ExpressionEquivalence(
            PLANNER_CONTEXT.getMetadata(),
            PLANNER_CONTEXT.getFunctionManager(),
            PLANNER_CONTEXT.getTypeManager(),
            new IrTypeAnalyzer(PLANNER_CONTEXT));

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction MOD = FUNCTIONS.resolveFunction("mod", fromTypes(INTEGER, INTEGER));

    @Test
    public void testEquivalent()
    {
        assertEquivalent(
                new Cast(new NullLiteral(), dataType("bigint")),
                new Cast(new NullLiteral(), dataType("bigint")));
        assertEquivalent(
                new ComparisonExpression(LESS_THAN, new SymbolReference("a_bigint"), new SymbolReference("b_double")),
                new ComparisonExpression(GREATER_THAN, new SymbolReference("b_double"), new SymbolReference("a_bigint")));
        assertEquivalent(
                TRUE_LITERAL,
                TRUE_LITERAL);
        assertEquivalent(
                new LongLiteral("4"),
                new LongLiteral("4"));
        assertEquivalent(
                new DecimalLiteral("4.4"),
                new DecimalLiteral("4.4"));
        assertEquivalent(
                new StringLiteral("foo"),
                new StringLiteral("foo"));

        assertEquivalent(
                new ComparisonExpression(EQUAL, new LongLiteral("4"), new LongLiteral("5")),
                new ComparisonExpression(EQUAL, new LongLiteral("5"), new LongLiteral("4")));
        assertEquivalent(
                new ComparisonExpression(EQUAL, new DecimalLiteral("4.4"), new DecimalLiteral("5.5")),
                new ComparisonExpression(EQUAL, new DecimalLiteral("5.5"), new DecimalLiteral("4.4")));
        assertEquivalent(
                new ComparisonExpression(EQUAL, new StringLiteral("foo"), new StringLiteral("bar")),
                new ComparisonExpression(EQUAL, new StringLiteral("bar"), new StringLiteral("foo")));
        assertEquivalent(
                new ComparisonExpression(NOT_EQUAL, new LongLiteral("4"), new LongLiteral("5")),
                new ComparisonExpression(NOT_EQUAL, new LongLiteral("5"), new LongLiteral("4")));
        assertEquivalent(
                new ComparisonExpression(IS_DISTINCT_FROM, new LongLiteral("4"), new LongLiteral("5")),
                new ComparisonExpression(IS_DISTINCT_FROM, new LongLiteral("5"), new LongLiteral("4")));
        assertEquivalent(
                new ComparisonExpression(LESS_THAN, new LongLiteral("4"), new LongLiteral("5")),
                new ComparisonExpression(GREATER_THAN, new LongLiteral("5"), new LongLiteral("4")));
        assertEquivalent(
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new LongLiteral("4"), new LongLiteral("5")),
                new ComparisonExpression(GREATER_THAN_OR_EQUAL, new LongLiteral("5"), new LongLiteral("4")));
        assertEquivalent(
                new ComparisonExpression(EQUAL, new GenericLiteral("TIMESTAMP", "2020-05-10 12:34:56.123456789"), new GenericLiteral("TIMESTAMP", "2021-05-10 12:34:56.123456789")),
                new ComparisonExpression(EQUAL, new GenericLiteral("TIMESTAMP", "2021-05-10 12:34:56.123456789"), new GenericLiteral("TIMESTAMP", "2020-05-10 12:34:56.123456789")));
        assertEquivalent(
                new ComparisonExpression(EQUAL, new GenericLiteral("TIMESTAMP", "2020-05-10 12:34:56.123456789 +8"), new GenericLiteral("TIMESTAMP", "2021-05-10 12:34:56.123456789 +8")),
                new ComparisonExpression(EQUAL, new GenericLiteral("TIMESTAMP", "2021-05-10 12:34:56.123456789 +8"), new GenericLiteral("TIMESTAMP", "2020-05-10 12:34:56.123456789 +8")));

        assertEquivalent(
                new FunctionCall(MOD.toQualifiedName(), ImmutableList.of(new LongLiteral("4"), new LongLiteral("5"))),
                new FunctionCall(MOD.toQualifiedName(), ImmutableList.of(new LongLiteral("4"), new LongLiteral("5"))));

        assertEquivalent(
                new SymbolReference("a_bigint"),
                new SymbolReference("a_bigint"));
        assertEquivalent(
                new ComparisonExpression(EQUAL, new SymbolReference("a_bigint"), new SymbolReference("b_bigint")),
                new ComparisonExpression(EQUAL, new SymbolReference("b_bigint"), new SymbolReference("a_bigint")));
        assertEquivalent(
                new ComparisonExpression(LESS_THAN, new SymbolReference("a_bigint"), new SymbolReference("b_bigint")),
                new ComparisonExpression(GREATER_THAN, new SymbolReference("b_bigint"), new SymbolReference("a_bigint")));

        assertEquivalent(
                new ComparisonExpression(LESS_THAN, new SymbolReference("a_bigint"), new SymbolReference("b_double")),
                new ComparisonExpression(GREATER_THAN, new SymbolReference("b_double"), new SymbolReference("a_bigint")));

        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(TRUE_LITERAL, FALSE_LITERAL)),
                new LogicalExpression(AND, ImmutableList.of(FALSE_LITERAL, TRUE_LITERAL)));
        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new LongLiteral("4"), new LongLiteral("5")), new ComparisonExpression(LESS_THAN, new LongLiteral("6"), new LongLiteral("7")))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new LongLiteral("7"), new LongLiteral("6")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new LongLiteral("5"), new LongLiteral("4")))));
        assertEquivalent(
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new LongLiteral("4"), new LongLiteral("5")), new ComparisonExpression(LESS_THAN, new LongLiteral("6"), new LongLiteral("7")))),
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new LongLiteral("7"), new LongLiteral("6")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new LongLiteral("5"), new LongLiteral("4")))));
        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a_bigint"), new SymbolReference("b_bigint")), new ComparisonExpression(LESS_THAN, new SymbolReference("c_bigint"), new SymbolReference("d_bigint")))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("d_bigint"), new SymbolReference("c_bigint")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("b_bigint"), new SymbolReference("a_bigint")))));
        assertEquivalent(
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a_bigint"), new SymbolReference("b_bigint")), new ComparisonExpression(LESS_THAN, new SymbolReference("c_bigint"), new SymbolReference("d_bigint")))),
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("d_bigint"), new SymbolReference("c_bigint")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("b_bigint"), new SymbolReference("a_bigint")))));

        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new LongLiteral("4"), new LongLiteral("5")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new LongLiteral("4"), new LongLiteral("5")))),
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new LongLiteral("4"), new LongLiteral("5")));
        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new LongLiteral("4"), new LongLiteral("5")), new ComparisonExpression(LESS_THAN, new LongLiteral("6"), new LongLiteral("7")))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new LongLiteral("7"), new LongLiteral("6")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new LongLiteral("5"), new LongLiteral("4")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new LongLiteral("5"), new LongLiteral("4")))));
        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new LongLiteral("2"), new LongLiteral("3")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new LongLiteral("4"), new LongLiteral("5")), new ComparisonExpression(LESS_THAN, new LongLiteral("6"), new LongLiteral("7")))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new LongLiteral("7"), new LongLiteral("6")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new LongLiteral("5"), new LongLiteral("4")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new LongLiteral("3"), new LongLiteral("2")))));

        assertEquivalent(
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new LongLiteral("4"), new LongLiteral("5")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new LongLiteral("4"), new LongLiteral("5")))),
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new LongLiteral("4"), new LongLiteral("5")));
        assertEquivalent(
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new LongLiteral("4"), new LongLiteral("5")), new ComparisonExpression(LESS_THAN, new LongLiteral("6"), new LongLiteral("7")))),
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new LongLiteral("7"), new LongLiteral("6")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new LongLiteral("5"), new LongLiteral("4")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new LongLiteral("5"), new LongLiteral("4")))));
        assertEquivalent(
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new LongLiteral("2"), new LongLiteral("3")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new LongLiteral("4"), new LongLiteral("5")), new ComparisonExpression(LESS_THAN, new LongLiteral("6"), new LongLiteral("7")))),
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new LongLiteral("7"), new LongLiteral("6")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new LongLiteral("5"), new LongLiteral("4")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new LongLiteral("3"), new LongLiteral("2")))));

        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference("a_boolean"), new SymbolReference("b_boolean"), new SymbolReference("c_boolean"))),
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference("c_boolean"), new SymbolReference("b_boolean"), new SymbolReference("a_boolean"))));
        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("a_boolean"), new SymbolReference("b_boolean"))), new SymbolReference("c_boolean"))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("c_boolean"), new SymbolReference("b_boolean"))), new SymbolReference("a_boolean"))));
        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference("a_boolean"), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("b_boolean"), new SymbolReference("c_boolean"))))),
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference("a_boolean"), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("c_boolean"), new SymbolReference("b_boolean"))), new SymbolReference("a_boolean"))));

        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("a_boolean"), new SymbolReference("b_boolean"), new SymbolReference("c_boolean"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("d_boolean"), new SymbolReference("e_boolean"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("f_boolean"), new SymbolReference("g_boolean"), new SymbolReference("h_boolean"))))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("h_boolean"), new SymbolReference("g_boolean"), new SymbolReference("f_boolean"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("b_boolean"), new SymbolReference("a_boolean"), new SymbolReference("c_boolean"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("e_boolean"), new SymbolReference("d_boolean"))))));

        assertEquivalent(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("a_boolean"), new SymbolReference("b_boolean"), new SymbolReference("c_boolean"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("d_boolean"), new SymbolReference("e_boolean"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("f_boolean"), new SymbolReference("g_boolean"), new SymbolReference("h_boolean"))))),
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("h_boolean"), new SymbolReference("g_boolean"), new SymbolReference("f_boolean"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("b_boolean"), new SymbolReference("a_boolean"), new SymbolReference("c_boolean"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("e_boolean"), new SymbolReference("d_boolean"))))));
    }

    private static void assertEquivalent(Expression leftExpression, Expression rightExpression)
    {
        Set<Symbol> symbols = extractUnique(ImmutableList.of(leftExpression, rightExpression));
        TypeProvider types = TypeProvider.copyOf(symbols.stream()
                .collect(toMap(identity(), TestExpressionEquivalence::generateType)));

        assertThat(areExpressionEquivalent(leftExpression, rightExpression, types))
                .describedAs(format("Expected (%s) and (%s) to be equivalent", leftExpression, rightExpression))
                .isTrue();
        assertThat(areExpressionEquivalent(rightExpression, leftExpression, types))
                .describedAs(format("Expected (%s) and (%s) to be equivalent", rightExpression, leftExpression))
                .isTrue();
    }

    @Test
    public void testNotEquivalent()
    {
        assertNotEquivalent(
                new Cast(new NullLiteral(), dataType("boolean")),
                FALSE_LITERAL);
        assertNotEquivalent(
                FALSE_LITERAL,
                new Cast(new NullLiteral(), dataType("boolean")));
        assertNotEquivalent(
                TRUE_LITERAL,
                FALSE_LITERAL);
        assertNotEquivalent(
                new LongLiteral("4"),
                new LongLiteral("5"));
        assertNotEquivalent(
                new DecimalLiteral("4.4"),
                new DecimalLiteral("5.5"));
        assertNotEquivalent(
                new StringLiteral("'foo'"),
                new StringLiteral("'bar'"));

        assertNotEquivalent(
                new ComparisonExpression(EQUAL, new LongLiteral("4"), new LongLiteral("5")),
                new ComparisonExpression(EQUAL, new LongLiteral("5"), new LongLiteral("6")));
        assertNotEquivalent(
                new ComparisonExpression(NOT_EQUAL, new LongLiteral("4"), new LongLiteral("5")),
                new ComparisonExpression(NOT_EQUAL, new LongLiteral("5"), new LongLiteral("6")));
        assertNotEquivalent(
                new ComparisonExpression(IS_DISTINCT_FROM, new LongLiteral("4"), new LongLiteral("5")),
                new ComparisonExpression(IS_DISTINCT_FROM, new LongLiteral("5"), new LongLiteral("6")));
        assertNotEquivalent(
                new ComparisonExpression(LESS_THAN, new LongLiteral("4"), new LongLiteral("5")),
                new ComparisonExpression(GREATER_THAN, new LongLiteral("5"), new LongLiteral("6")));
        assertNotEquivalent(
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new LongLiteral("4"), new LongLiteral("5")),
                new ComparisonExpression(GREATER_THAN_OR_EQUAL, new LongLiteral("5"), new LongLiteral("6")));

        assertNotEquivalent(
                new FunctionCall(MOD.toQualifiedName(), ImmutableList.of(new LongLiteral("4"), new LongLiteral("5"))),
                new FunctionCall(MOD.toQualifiedName(), ImmutableList.of(new LongLiteral("5"), new LongLiteral("4"))));

        assertNotEquivalent(
                new SymbolReference("a_bigint"),
                new SymbolReference("b_bigint"));
        assertNotEquivalent(
                new ComparisonExpression(EQUAL, new SymbolReference("a_bigint"), new SymbolReference("b_bigint")),
                new ComparisonExpression(EQUAL, new SymbolReference("b_bigint"), new SymbolReference("c_bigint")));
        assertNotEquivalent(
                new ComparisonExpression(LESS_THAN, new SymbolReference("a_bigint"), new SymbolReference("b_bigint")),
                new ComparisonExpression(GREATER_THAN, new SymbolReference("b_bigint"), new SymbolReference("c_bigint")));

        assertNotEquivalent(
                new ComparisonExpression(LESS_THAN, new SymbolReference("a_bigint"), new SymbolReference("b_double")),
                new ComparisonExpression(GREATER_THAN, new SymbolReference("b_double"), new SymbolReference("c_bigint")));

        assertNotEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new LongLiteral("4"), new LongLiteral("5")), new ComparisonExpression(LESS_THAN, new LongLiteral("6"), new LongLiteral("7")))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new LongLiteral("7"), new LongLiteral("6")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new LongLiteral("5"), new LongLiteral("6")))));
        assertNotEquivalent(
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new LongLiteral("4"), new LongLiteral("5")), new ComparisonExpression(LESS_THAN, new LongLiteral("6"), new LongLiteral("7")))),
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new LongLiteral("7"), new LongLiteral("6")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new LongLiteral("5"), new LongLiteral("6")))));
        assertNotEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a_bigint"), new SymbolReference("b_bigint")), new ComparisonExpression(LESS_THAN, new SymbolReference("c_bigint"), new SymbolReference("d_bigint")))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("d_bigint"), new SymbolReference("c_bigint")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("b_bigint"), new SymbolReference("c_bigint")))));
        assertNotEquivalent(
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("a_bigint"), new SymbolReference("b_bigint")), new ComparisonExpression(LESS_THAN, new SymbolReference("c_bigint"), new SymbolReference("d_bigint")))),
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("d_bigint"), new SymbolReference("c_bigint")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("b_bigint"), new SymbolReference("c_bigint")))));

        assertNotEquivalent(
                new Cast(new GenericLiteral("TIME", "12:34:56.123 +00:00"), dataType("varchar")),
                new Cast(new GenericLiteral("TIME", "14:34:56.123 +02:00"), dataType("varchar")));
        assertNotEquivalent(
                new Cast(new GenericLiteral("TIME", "12:34:56.123456 +00:00"), dataType("varchar")),
                new Cast(new GenericLiteral("TIME", "14:34:56.123456 +02:00"), dataType("varchar")));
        assertNotEquivalent(
                new Cast(new GenericLiteral("TIME", "12:34:56.123456789 +00:00"), dataType("varchar")),
                new Cast(new GenericLiteral("TIME", "14:34:56.123456789 +02:00"), dataType("varchar")));
        assertNotEquivalent(
                new Cast(new GenericLiteral("TIME", "12:34:56.123456789012 +00:00"), dataType("varchar")),
                new Cast(new GenericLiteral("TIME", "14:34:56.123456789012 +02:00"), dataType("varchar")));

        assertNotEquivalent(
                new Cast(new GenericLiteral("TIMESTAMP", "2020-05-10 12:34:56.123 Europe/Warsaw"), dataType("varchar")),
                new Cast(new GenericLiteral("TIMESTAMP", "2020-05-10 12:34:56.123 Europe/Paris"), dataType("varchar")));
        assertNotEquivalent(
                new Cast(new GenericLiteral("TIMESTAMP", "2020-05-10 12:34:56.123456 Europe/Warsaw"), dataType("varchar")),
                new Cast(new GenericLiteral("TIMESTAMP", "2020-05-10 12:34:56.123456 Europe/Paris"), dataType("varchar")));
        assertNotEquivalent(
                new Cast(new GenericLiteral("TIMESTAMP", "2020-05-10 12:34:56.123456789 Europe/Warsaw"), dataType("varchar")),
                new Cast(new GenericLiteral("TIMESTAMP", "2020-05-10 12:34:56.123456789 Europe/Paris"), dataType("varchar")));
        assertNotEquivalent(
                new Cast(new GenericLiteral("TIMESTAMP", "2020-05-10 12:34:56.123456789012 Europe/Warsaw"), dataType("varchar")),
                new Cast(new GenericLiteral("TIMESTAMP", "2020-05-10 12:34:56.123456789012 Europe/Paris"), dataType("varchar")));
    }

    private static void assertNotEquivalent(Expression leftExpression, Expression rightExpression)
    {
        Set<Symbol> symbols = extractUnique(ImmutableList.of(leftExpression, rightExpression));
        TypeProvider types = TypeProvider.copyOf(symbols.stream()
                .collect(toMap(identity(), TestExpressionEquivalence::generateType)));

        assertThat(areExpressionEquivalent(leftExpression, rightExpression, types))
                .describedAs(format("Expected (%s) and (%s) to not be equivalent", leftExpression, rightExpression))
                .isFalse();
        assertThat(areExpressionEquivalent(rightExpression, leftExpression, types))
                .describedAs(format("Expected (%s) and (%s) to not be equivalent", rightExpression, leftExpression))
                .isFalse();
    }

    private static boolean areExpressionEquivalent(Expression leftExpression, Expression rightExpression, TypeProvider types)
    {
        TransactionManager transactionManager = new TestingTransactionManager();
        Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
        return transaction(transactionManager, metadata, new AllowAllAccessControl())
                .singleStatement()
                .execute(TEST_SESSION, transactionSession -> {
                    return EQUIVALENCE.areExpressionsEquivalent(transactionSession, leftExpression, rightExpression, types);
                });
    }

    private static Type generateType(Symbol symbol)
    {
        String typeName = Splitter.on('_').limit(2).splitToList(symbol.getName()).get(1);
        return PLANNER_CONTEXT.getTypeManager().getType(new TypeSignature(typeName, ImmutableList.of()));
    }
}
