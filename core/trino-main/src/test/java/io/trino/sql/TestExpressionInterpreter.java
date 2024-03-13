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
package io.trino.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.Type;
import io.trino.sql.planner.IrExpressionInterpreter;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolResolver;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.assertions.SymbolAliases;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.WhenClause;
import io.trino.transaction.TestingTransactionManager;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ExpressionTestUtils.assertExpressionEquals;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.DIVIDE;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.MULTIPLY;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.SUBTRACT;
import static io.trino.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static io.trino.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;
import static io.trino.sql.tree.LogicalExpression.Operator.OR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Locale.ENGLISH;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExpressionInterpreter
{
    private static final TypeProvider SYMBOL_TYPES = TypeProvider.copyOf(ImmutableMap.<Symbol, Type>builder()
            .put(new Symbol("bound_value"), INTEGER)
            .put(new Symbol("unbound_value"), INTEGER)
            .buildOrThrow());

    private static final SymbolResolver INPUTS = symbol -> {
        if (symbol.getName().toLowerCase(ENGLISH).equals("bound_value")) {
            return 1234L;
        }

        return symbol.toSymbolReference();
    };

    private static final TestingTransactionManager TRANSACTION_MANAGER = new TestingTransactionManager();
    private static final PlannerContext PLANNER_CONTEXT = plannerContextBuilder()
            .withTransactionManager(TRANSACTION_MANAGER)
            .build();

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ABS = FUNCTIONS.resolveFunction("abs", fromTypes(BIGINT));
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", fromTypes());

    @Test
    public void testAnd()
    {
        assertOptimizedEquals(
                new LogicalExpression(AND, ImmutableList.of(TRUE_LITERAL, FALSE_LITERAL)),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new LogicalExpression(AND, ImmutableList.of(FALSE_LITERAL, TRUE_LITERAL)),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new LogicalExpression(AND, ImmutableList.of(FALSE_LITERAL, FALSE_LITERAL)),
                FALSE_LITERAL);

        assertOptimizedEquals(
                new LogicalExpression(AND, ImmutableList.of(TRUE_LITERAL, new NullLiteral())),
                new NullLiteral());
        assertOptimizedEquals(
                new LogicalExpression(AND, ImmutableList.of(FALSE_LITERAL, new NullLiteral())),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new LogicalExpression(AND, ImmutableList.of(new NullLiteral(), TRUE_LITERAL)),
                new NullLiteral());
        assertOptimizedEquals(
                new LogicalExpression(AND, ImmutableList.of(new NullLiteral(), FALSE_LITERAL)),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new LogicalExpression(AND, ImmutableList.of(new NullLiteral(), new NullLiteral())),
                new NullLiteral());
    }

    @Test
    public void testOr()
    {
        assertOptimizedEquals(
                new LogicalExpression(OR, ImmutableList.of(TRUE_LITERAL, TRUE_LITERAL)),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new LogicalExpression(OR, ImmutableList.of(TRUE_LITERAL, FALSE_LITERAL)),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new LogicalExpression(OR, ImmutableList.of(FALSE_LITERAL, TRUE_LITERAL)),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new LogicalExpression(OR, ImmutableList.of(FALSE_LITERAL, FALSE_LITERAL)),
                FALSE_LITERAL);

        assertOptimizedEquals(
                new LogicalExpression(OR, ImmutableList.of(TRUE_LITERAL, new NullLiteral())),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new LogicalExpression(OR, ImmutableList.of(new NullLiteral(), TRUE_LITERAL)),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new LogicalExpression(OR, ImmutableList.of(new NullLiteral(), new NullLiteral())),
                new NullLiteral());

        assertOptimizedEquals(
                new LogicalExpression(OR, ImmutableList.of(FALSE_LITERAL, new NullLiteral())),
                new NullLiteral());
        assertOptimizedEquals(
                new LogicalExpression(OR, ImmutableList.of(new NullLiteral(), FALSE_LITERAL)),
                new NullLiteral());
    }

    @Test
    public void testComparison()
    {
        assertOptimizedEquals(
                new ComparisonExpression(EQUAL, new NullLiteral(), new NullLiteral()),
                new NullLiteral());

        assertOptimizedEquals(
                new ComparisonExpression(EQUAL, new StringLiteral("a"), new StringLiteral("b")),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new ComparisonExpression(EQUAL, new StringLiteral("a"), new StringLiteral("a")),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new ComparisonExpression(EQUAL, new StringLiteral("a"), new NullLiteral()),
                new NullLiteral());
        assertOptimizedEquals(
                new ComparisonExpression(EQUAL, new NullLiteral(), new StringLiteral("a")),
                new NullLiteral());
        assertOptimizedEquals(
                new ComparisonExpression(EQUAL, new SymbolReference("bound_value"), new LongLiteral("1234")),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new ComparisonExpression(EQUAL, new SymbolReference("bound_value"), new LongLiteral("1")),
                FALSE_LITERAL);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertOptimizedEquals(
                new ComparisonExpression(IS_DISTINCT_FROM, new NullLiteral(), new NullLiteral()),
                FALSE_LITERAL);

        assertOptimizedEquals(
                new ComparisonExpression(IS_DISTINCT_FROM, new LongLiteral("3"), new LongLiteral("4")),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new ComparisonExpression(IS_DISTINCT_FROM, new LongLiteral("3"), new LongLiteral("3")),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new ComparisonExpression(IS_DISTINCT_FROM, new LongLiteral("3"), new NullLiteral()),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new ComparisonExpression(IS_DISTINCT_FROM, new NullLiteral(), new LongLiteral("3")),
                TRUE_LITERAL);

        assertOptimizedMatches(
                new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("unbound_value"), new LongLiteral("1")),
                new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("unbound_value"), new LongLiteral("1")));
        assertOptimizedMatches(
                new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("unbound_value"), new NullLiteral()),
                new IsNotNullPredicate(new SymbolReference("unbound_value")));
        assertOptimizedMatches(
                new ComparisonExpression(IS_DISTINCT_FROM, new NullLiteral(), new SymbolReference("unbound_value")),
                new IsNotNullPredicate(new SymbolReference("unbound_value")));
    }

    @Test
    public void testIsNull()
    {
        assertOptimizedEquals(
                new IsNullPredicate(new NullLiteral()),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new IsNullPredicate(new LongLiteral("1")),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new IsNullPredicate(new ArithmeticBinaryExpression(ADD, new NullLiteral(), new LongLiteral("1"))),
                TRUE_LITERAL);
    }

    @Test
    public void testIsNotNull()
    {
        assertOptimizedEquals(
                new IsNotNullPredicate(new NullLiteral()),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new IsNotNullPredicate(new LongLiteral("1")),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new IsNotNullPredicate(new ArithmeticBinaryExpression(ADD, new NullLiteral(), new LongLiteral("1"))),
                FALSE_LITERAL);
    }

    @Test
    public void testNullIf()
    {
        assertOptimizedEquals(
                new NullIfExpression(new StringLiteral("a"), new StringLiteral("a")),
                new NullLiteral());
        assertOptimizedEquals(
                new NullIfExpression(new StringLiteral("a"), new StringLiteral("b")),
                new StringLiteral("a"));
        assertOptimizedEquals(
                new NullIfExpression(new NullLiteral(), new StringLiteral("b")),
                new NullLiteral());
        assertOptimizedEquals(
                new NullIfExpression(new StringLiteral("a"), new NullLiteral()),
                new StringLiteral("a"));
        assertOptimizedEquals(
                new NullIfExpression(new SymbolReference("unbound_value"), new LongLiteral("1")),
                new NullIfExpression(new SymbolReference("unbound_value"), new LongLiteral("1")));
    }

    @Test
    public void testNegative()
    {
        assertOptimizedEquals(
                new ArithmeticUnaryExpression(MINUS, new LongLiteral("1")),
                new LongLiteral("-1"));
        assertOptimizedEquals(
                new ArithmeticUnaryExpression(MINUS, new ArithmeticBinaryExpression(ADD, new SymbolReference("unbound_value"), new LongLiteral("1"))),
                new ArithmeticUnaryExpression(MINUS, new ArithmeticBinaryExpression(ADD, new SymbolReference("unbound_value"), new LongLiteral("1"))));
    }

    @Test
    public void testNot()
    {
        assertOptimizedEquals(
                new NotExpression(TRUE_LITERAL),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new NotExpression(FALSE_LITERAL),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new NotExpression(new NullLiteral()),
                new NullLiteral());
        assertOptimizedEquals(
                new NotExpression(new ComparisonExpression(EQUAL, new SymbolReference("unbound_value"), new LongLiteral("1"))),
                new NotExpression(new ComparisonExpression(EQUAL, new SymbolReference("unbound_value"), new LongLiteral("1"))));
    }

    @Test
    public void testFunctionCall()
    {
        assertOptimizedEquals(
                new FunctionCall(ABS.toQualifiedName(), ImmutableList.of(new LongLiteral("-5"))),
                new LongLiteral("5"));
        assertOptimizedEquals(
                new FunctionCall(ABS.toQualifiedName(), ImmutableList.of(new SymbolReference("unbound_value"))),
                new FunctionCall(ABS.toQualifiedName(), ImmutableList.of(new SymbolReference("unbound_value"))));
    }

    @Test
    public void testNonDeterministicFunctionCall()
    {
        assertOptimizedEquals(
                new FunctionCall(RANDOM.toQualifiedName(), ImmutableList.of()),
                new FunctionCall(RANDOM.toQualifiedName(), ImmutableList.of()));
    }

    @Test
    public void testBetween()
    {
        assertOptimizedEquals(
                new BetweenPredicate(new LongLiteral("3"), new LongLiteral("2"), new LongLiteral("4")),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new BetweenPredicate(new LongLiteral("2"), new LongLiteral("3"), new LongLiteral("4")),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new BetweenPredicate(new NullLiteral(), new LongLiteral("2"), new LongLiteral("4")),
                new NullLiteral());
        assertOptimizedEquals(
                new BetweenPredicate(new LongLiteral("3"), new NullLiteral(), new LongLiteral("4")),
                new NullLiteral());
        assertOptimizedEquals(
                new BetweenPredicate(new LongLiteral("3"), new LongLiteral("2"), new NullLiteral()),
                new NullLiteral());
        assertOptimizedEquals(
                new BetweenPredicate(new LongLiteral("2"), new LongLiteral("3"), new NullLiteral()),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new BetweenPredicate(new LongLiteral("8"), new NullLiteral(), new LongLiteral("6")),
                FALSE_LITERAL);

        assertOptimizedEquals(
                new BetweenPredicate(new SymbolReference("bound_value"), new LongLiteral("1000"), new LongLiteral("2000")),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new BetweenPredicate(new SymbolReference("bound_value"), new LongLiteral("3"), new LongLiteral("4")),
                FALSE_LITERAL);
    }

    @Test
    public void testIn()
    {
        assertOptimizedEquals(
                new InPredicate(new LongLiteral("3"), new InListExpression(ImmutableList.of(new LongLiteral("2"), new LongLiteral("4"), new LongLiteral("3"), new LongLiteral("5")))),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new InPredicate(new LongLiteral("3"), new InListExpression(ImmutableList.of(new LongLiteral("2"), new LongLiteral("4"), new LongLiteral("9"), new LongLiteral("5")))),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new InPredicate(new LongLiteral("3"), new InListExpression(ImmutableList.of(new LongLiteral("2"), new Cast(new NullLiteral(), dataType("integer")), new LongLiteral("3"), new LongLiteral("5")))),
                TRUE_LITERAL);

        assertOptimizedEquals(
                new InPredicate(new Cast(new NullLiteral(), dataType("integer")), new InListExpression(ImmutableList.of(new LongLiteral("2"), new Cast(new NullLiteral(), dataType("integer")), new LongLiteral("3"), new LongLiteral("5")))),
                new NullLiteral());
        assertOptimizedEquals(
                new InPredicate(new LongLiteral("3"), new InListExpression(ImmutableList.of(new LongLiteral("2"), new Cast(new NullLiteral(), dataType("integer"))))),
                new NullLiteral());

        assertOptimizedEquals(
                new InPredicate(new SymbolReference("bound_value"), new InListExpression(ImmutableList.of(new LongLiteral("2"), new LongLiteral("1234"), new LongLiteral("3"), new LongLiteral("5")))),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new InPredicate(new SymbolReference("bound_value"), new InListExpression(ImmutableList.of(new LongLiteral("2"), new LongLiteral("4"), new LongLiteral("3"), new LongLiteral("5")))),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new InPredicate(new LongLiteral("1234"), new InListExpression(ImmutableList.of(new LongLiteral("2"), new SymbolReference("bound_value"), new LongLiteral("3"), new LongLiteral("5")))),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new InPredicate(new LongLiteral("99"), new InListExpression(ImmutableList.of(new LongLiteral("2"), new SymbolReference("bound_value"), new LongLiteral("3"), new LongLiteral("5")))),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new InPredicate(new SymbolReference("bound_value"), new InListExpression(ImmutableList.of(new LongLiteral("2"), new SymbolReference("bound_value"), new LongLiteral("3"), new LongLiteral("5")))),
                TRUE_LITERAL);

        assertOptimizedEquals(
                new InPredicate(new SymbolReference("unbound_value"), new InListExpression(ImmutableList.of(new LongLiteral("1")))),
                new ComparisonExpression(EQUAL, new SymbolReference("unbound_value"), new LongLiteral("1")));

        assertOptimizedEquals(
                new InPredicate(new LongLiteral("3"), new InListExpression(ImmutableList.of(new LongLiteral("2"), new LongLiteral("4"), new LongLiteral("3"), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("5"), new LongLiteral("0"))))),
                new InPredicate(new LongLiteral("3"), new InListExpression(ImmutableList.of(new LongLiteral("2"), new LongLiteral("4"), new LongLiteral("3"), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("5"), new LongLiteral("0"))))));
        assertOptimizedEquals(
                new InPredicate(new Cast(new NullLiteral(), dataType("integer")), new InListExpression(ImmutableList.of(new LongLiteral("2"), new LongLiteral("4"), new LongLiteral("3"), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("5"), new LongLiteral("0"))))),
                new InPredicate(new Cast(new NullLiteral(), dataType("integer")), new InListExpression(ImmutableList.of(new LongLiteral("2"), new LongLiteral("4"), new LongLiteral("3"), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("5"), new LongLiteral("0"))))));
        assertOptimizedEquals(
                new InPredicate(new LongLiteral("3"), new InListExpression(ImmutableList.of(new LongLiteral("2"), new LongLiteral("4"), new LongLiteral("3"), new Cast(new NullLiteral(), dataType("integer")), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("5"), new LongLiteral("0"))))),
                new InPredicate(new LongLiteral("3"), new InListExpression(ImmutableList.of(new LongLiteral("2"), new LongLiteral("4"), new LongLiteral("3"), new Cast(new NullLiteral(), dataType("integer")), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("5"), new LongLiteral("0"))))));
        assertOptimizedEquals(
                new InPredicate(new Cast(new NullLiteral(), dataType("integer")), new InListExpression(ImmutableList.of(new LongLiteral("2"), new LongLiteral("4"), new Cast(new NullLiteral(), dataType("integer")), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("5"), new LongLiteral("0"))))),
                new InPredicate(new Cast(new NullLiteral(), dataType("integer")), new InListExpression(ImmutableList.of(new LongLiteral("2"), new LongLiteral("4"), new Cast(new NullLiteral(), dataType("integer")), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("5"), new LongLiteral("0"))))));
        assertOptimizedEquals(
                new InPredicate(new LongLiteral("3"), new InListExpression(ImmutableList.of(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("5"), new LongLiteral("0")), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("5"), new LongLiteral("0"))))),
                new InPredicate(new LongLiteral("3"), new InListExpression(ImmutableList.of(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("5"), new LongLiteral("0")), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("5"), new LongLiteral("0"))))));
        assertTrinoExceptionThrownBy(() -> evaluate(new InPredicate(new LongLiteral("3"), new InListExpression(ImmutableList.of(new LongLiteral("2"), new LongLiteral("4"), new LongLiteral("3"), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("5"), new LongLiteral("0")))))))
                .hasErrorCode(DIVISION_BY_ZERO);

        assertOptimizedEquals(
                new InPredicate(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new InListExpression(ImmutableList.of(new LongLiteral("2"), new LongLiteral("4"), new LongLiteral("3"), new LongLiteral("5")))),
                new InPredicate(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new InListExpression(ImmutableList.of(new LongLiteral("2"), new LongLiteral("4"), new LongLiteral("3"), new LongLiteral("5")))));
        assertOptimizedEquals(
                new InPredicate(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new InListExpression(ImmutableList.of(new LongLiteral("2"), new LongLiteral("4"), new LongLiteral("2"), new LongLiteral("4")))),
                new InPredicate(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new InListExpression(ImmutableList.of(new LongLiteral("2"), new LongLiteral("4")))));
        assertOptimizedEquals(
                new InPredicate(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new InListExpression(ImmutableList.of(new LongLiteral("2"), new LongLiteral("2")))),
                new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("2")));
    }

    @Test
    public void testCastOptimization()
    {
        assertOptimizedEquals(
                new Cast(new SymbolReference("bound_value"), dataType("varchar")),
                new StringLiteral("1234"));
        assertOptimizedMatches(
                new Cast(new SymbolReference("unbound_value"), dataType("integer")),
                new SymbolReference("unbound_value"));
    }

    @Test
    public void testTryCast()
    {
        assertOptimizedEquals(
                new Cast(new NullLiteral(), dataType("bigint"), true),
                new NullLiteral());
        assertOptimizedEquals(
                new Cast(new LongLiteral("123"), dataType("bigint"), true),
                new LongLiteral("123"));
        assertOptimizedEquals(
                new Cast(new NullLiteral(), dataType("integer"), true),
                new NullLiteral());
        assertOptimizedEquals(
                new Cast(new LongLiteral("123"), dataType("integer"), true),
                new LongLiteral("123"));
    }

    @Test
    public void testSearchCase()
    {
        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(TRUE_LITERAL, new LongLiteral("33"))),
                        Optional.empty()),
                new LongLiteral("33"));
        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(FALSE_LITERAL, new LongLiteral("1"))),
                        Optional.of(new LongLiteral("33"))),
                new LongLiteral("33"));

        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("bound_value"), new LongLiteral("1234")), new LongLiteral("33"))),
                        Optional.empty()),
                new LongLiteral("33"));
        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(TRUE_LITERAL, new SymbolReference("bound_value"))),
                        Optional.empty()),
                new LongLiteral("1234"));
        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(FALSE_LITERAL, new LongLiteral("1"))),
                        Optional.of(new SymbolReference("bound_value"))),
                new LongLiteral("1234"));

        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("bound_value"), new LongLiteral("1234")), new LongLiteral("33"))),
                        Optional.of(new SymbolReference("unbound_value"))),
                new LongLiteral("33"));

        assertOptimizedMatches(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("0")), new LongLiteral("1"))),
                        Optional.empty()),
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("0")), new LongLiteral("1"))),
                        Optional.empty()));

        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(TRUE_LITERAL, new StringLiteral("a")), new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("0")), new StringLiteral("b"))),
                        Optional.of(new StringLiteral("c"))),
                new StringLiteral("a"));
        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("0")), new StringLiteral("a")), new WhenClause(TRUE_LITERAL, new StringLiteral("b"))),
                        Optional.of(new StringLiteral("c"))),
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("0")), new StringLiteral("a"))),
                        Optional.of(new StringLiteral("b"))));
        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("0")), new StringLiteral("a")), new WhenClause(FALSE_LITERAL, new StringLiteral("b"))),
                        Optional.of(new StringLiteral("c"))),
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("0")), new StringLiteral("a"))),
                        Optional.of(new StringLiteral("c"))));
        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("0")), new StringLiteral("a")),
                        new WhenClause(FALSE_LITERAL, new StringLiteral("b"))),
                        Optional.empty()),
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("0")), new StringLiteral("a"))),
                        Optional.empty()));
        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(TRUE_LITERAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0"))),
                        new WhenClause(FALSE_LITERAL, new LongLiteral("1"))),
                        Optional.empty()),
                new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")));
        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(FALSE_LITERAL, new LongLiteral("1")), new WhenClause(FALSE_LITERAL, new LongLiteral("2"))),
                        Optional.of(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")))),
                new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")));

        assertEvaluatedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(FALSE_LITERAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0"))), new WhenClause(TRUE_LITERAL, new LongLiteral("1"))),
                        Optional.of(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")))),
                new LongLiteral("1"));
        assertEvaluatedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(TRUE_LITERAL, new LongLiteral("1")), new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("0")), new LongLiteral("2"))),
                        Optional.of(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")))),
                new LongLiteral("1"));
    }

    @Test
    public void testSimpleCase()
    {
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new LongLiteral("1"),
                        ImmutableList.of(
                                new WhenClause(new LongLiteral("1"), new LongLiteral("33")),
                                new WhenClause(new LongLiteral("1"), new LongLiteral("34"))),
                        Optional.empty()),
                new LongLiteral("33"));

        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new Cast(new NullLiteral(), dataType("boolean")),
                        ImmutableList.of(
                                new WhenClause(TRUE_LITERAL, new LongLiteral("33"))),
                        Optional.empty()),
                new NullLiteral());
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new Cast(new NullLiteral(), dataType("boolean")),
                        ImmutableList.of(
                                new WhenClause(TRUE_LITERAL, new LongLiteral("33"))),
                        Optional.of(new LongLiteral("33"))),
                new LongLiteral("33"));
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new LongLiteral("33"),
                        ImmutableList.of(
                                new WhenClause(new Cast(new NullLiteral(), dataType("integer")), new LongLiteral("1"))),
                        Optional.of(new LongLiteral("33"))),
                new LongLiteral("33"));

        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new SymbolReference("bound_value"),
                        ImmutableList.of(
                                new WhenClause(new LongLiteral("1234"), new LongLiteral("33"))),
                        Optional.empty()),
                new LongLiteral("33"));
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new LongLiteral("1234"),
                        ImmutableList.of(
                                new WhenClause(new SymbolReference("bound_value"), new LongLiteral("33"))),
                        Optional.empty()),
                new LongLiteral("33"));
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        TRUE_LITERAL,
                        ImmutableList.of(
                                new WhenClause(TRUE_LITERAL, new SymbolReference("bound_value"))),
                        Optional.empty()),
                new LongLiteral("1234"));
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        TRUE_LITERAL,
                        ImmutableList.of(
                                new WhenClause(FALSE_LITERAL, new LongLiteral("1"))),
                        Optional.of(new SymbolReference("bound_value"))),
                new LongLiteral("1234"));

        assertOptimizedEquals(
                new SimpleCaseExpression(
                        TRUE_LITERAL,
                        ImmutableList.of(
                                new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("unbound_value"), new LongLiteral("1")), new LongLiteral("1")),
                                new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("0")), new LongLiteral("2"))),
                        Optional.of(new LongLiteral("33"))),
                new SimpleCaseExpression(
                        TRUE_LITERAL,
                        ImmutableList.of(
                                new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("unbound_value"), new LongLiteral("1")), new LongLiteral("1")),
                                new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("0")), new LongLiteral("2"))),
                        Optional.of(new LongLiteral("33"))));

        assertOptimizedMatches(
                new SimpleCaseExpression(
                        new LongLiteral("1"),
                        ImmutableList.of(
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("1")),
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("2"))),
                        Optional.of(new LongLiteral("1"))),
                new SimpleCaseExpression(
                        new LongLiteral("1"),
                        ImmutableList.of(
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("1")),
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("2"))),
                        Optional.of(new LongLiteral("1"))));

        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new Cast(new NullLiteral(), dataType("integer")),
                        ImmutableList.of(
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")))),
                        Optional.of(new LongLiteral("1"))),
                new LongLiteral("1"));
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new Cast(new NullLiteral(), dataType("integer")),
                        ImmutableList.of(
                                new WhenClause(new LongLiteral("1"), new LongLiteral("2"))),
                        Optional.of(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")))),
                new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")));
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")),
                        ImmutableList.of(
                                new WhenClause(new LongLiteral("1"), new LongLiteral("2"))),
                        Optional.of(new LongLiteral("3"))),
                new SimpleCaseExpression(
                        new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")),
                        ImmutableList.of(
                                new WhenClause(new LongLiteral("1"), new LongLiteral("2"))),
                        Optional.of(new LongLiteral("3"))));
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new LongLiteral("1"),
                        ImmutableList.of(
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("2"))),
                        Optional.of(new LongLiteral("3"))),
                new SimpleCaseExpression(
                        new LongLiteral("1"),
                        ImmutableList.of(
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("2"))),
                        Optional.of(new LongLiteral("3"))));
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new LongLiteral("1"),
                        ImmutableList.of(
                                new WhenClause(new LongLiteral("2"), new LongLiteral("2")),
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("3"))),
                        Optional.of(new LongLiteral("4"))),
                new SimpleCaseExpression(
                        new LongLiteral("1"),
                        ImmutableList.of(
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("3"))),
                        Optional.of(new LongLiteral("4"))));
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new LongLiteral("1"),
                        ImmutableList.of(
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("2"))),
                        Optional.empty()),
                new SimpleCaseExpression(
                        new LongLiteral("1"),
                        ImmutableList.of(
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("2"))),
                        Optional.empty()));
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new LongLiteral("1"),
                        ImmutableList.of(
                                new WhenClause(new LongLiteral("2"), new LongLiteral("2")),
                                new WhenClause(new LongLiteral("3"), new LongLiteral("3"))),
                        Optional.empty()),
                new NullLiteral());

        assertEvaluatedEquals(
                new SimpleCaseExpression(
                        new Cast(new NullLiteral(), dataType("integer")),
                        ImmutableList.of(
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")))),
                        Optional.of(new LongLiteral("1"))),
                new LongLiteral("1"));
        assertEvaluatedEquals(
                new SimpleCaseExpression(
                        new LongLiteral("1"),
                        ImmutableList.of(
                                new WhenClause(new LongLiteral("2"), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")))),
                        Optional.of(new LongLiteral("3"))),
                new LongLiteral("3"));
        assertEvaluatedEquals(
                new SimpleCaseExpression(
                        new LongLiteral("1"),
                        ImmutableList.of(
                                new WhenClause(new LongLiteral("1"), new LongLiteral("2")),
                                new WhenClause(new LongLiteral("1"), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")))),
                        Optional.empty()),
                new LongLiteral("2"));
        assertEvaluatedEquals(
                new SimpleCaseExpression(
                        new LongLiteral("1"),
                        ImmutableList.of(
                                new WhenClause(new LongLiteral("1"), new LongLiteral("2"))),
                        Optional.of(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")))),
                new LongLiteral("2"));
    }

    @Test
    public void testCoalesce()
    {
        assertOptimizedEquals(
                new CoalesceExpression(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("unbound_value"), new ArithmeticBinaryExpression(MULTIPLY, new LongLiteral("2"), new LongLiteral("3"))), new ArithmeticBinaryExpression(SUBTRACT, new LongLiteral("1"), new LongLiteral("1")), new Cast(new NullLiteral(), dataType("integer"))),
                new CoalesceExpression(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("unbound_value"), new LongLiteral("6")), new LongLiteral("0")));
        assertOptimizedMatches(
                new CoalesceExpression(new SymbolReference("unbound_value"), new SymbolReference("unbound_value")),
                new SymbolReference("unbound_value"));
        assertOptimizedEquals(
                new CoalesceExpression(new LongLiteral("6"), new SymbolReference("unbound_value")),
                new LongLiteral("6"));
        assertOptimizedMatches(
                new CoalesceExpression(new FunctionCall(RANDOM.toQualifiedName(), ImmutableList.of()), new FunctionCall(RANDOM.toQualifiedName(), ImmutableList.of()), new DoubleLiteral("5.0")),
                new CoalesceExpression(new FunctionCall(RANDOM.toQualifiedName(), ImmutableList.of()), new FunctionCall(RANDOM.toQualifiedName(), ImmutableList.of()), new DoubleLiteral("5.0")));

        assertOptimizedEquals(
                new CoalesceExpression(new NullLiteral(), new CoalesceExpression(new NullLiteral(), new NullLiteral())),
                new NullLiteral());
        assertOptimizedEquals(
                new CoalesceExpression(new Cast(new NullLiteral(), dataType("integer")), new CoalesceExpression(new Cast(new NullLiteral(), dataType("integer")), new CoalesceExpression(new Cast(new NullLiteral(), dataType("integer")), new Cast(new NullLiteral(), dataType("integer")), new LongLiteral("1")))),
                new LongLiteral("1"));
        assertOptimizedEquals(
                new CoalesceExpression(new LongLiteral("1"), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0"))),
                new LongLiteral("1"));
        assertOptimizedEquals(
                new CoalesceExpression(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("1")),
                new CoalesceExpression(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("1")));
        assertOptimizedEquals(
                new CoalesceExpression(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("1"), new Cast(new NullLiteral(), dataType("integer"))),
                new CoalesceExpression(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("1")));
        assertOptimizedEquals(
                new CoalesceExpression(new LongLiteral("1"), new CoalesceExpression(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("2"))),
                new LongLiteral("1"));
        assertOptimizedEquals(
                new CoalesceExpression(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new Cast(new NullLiteral(), dataType("integer")), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("1"), new LongLiteral("0")), new Cast(new NullLiteral(), dataType("integer")), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0"))),
                new CoalesceExpression(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("1"), new LongLiteral("0"))));
        assertOptimizedEquals(
                new CoalesceExpression(new FunctionCall(RANDOM.toQualifiedName(), ImmutableList.of()), new FunctionCall(RANDOM.toQualifiedName(), ImmutableList.of()), new DoubleLiteral("1.0"), new FunctionCall(RANDOM.toQualifiedName(), ImmutableList.of())),
                new CoalesceExpression(new FunctionCall(RANDOM.toQualifiedName(), ImmutableList.of()), new FunctionCall(RANDOM.toQualifiedName(), ImmutableList.of()), new DoubleLiteral("1.0")));

        assertEvaluatedEquals(
                new CoalesceExpression(new LongLiteral("1"), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0"))),
                new LongLiteral("1"));
        assertTrinoExceptionThrownBy(() -> evaluate(new CoalesceExpression(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("1"))))
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testIf()
    {
        assertOptimizedEquals(
                new IfExpression(new ComparisonExpression(EQUAL, new LongLiteral("2"), new LongLiteral("2")), new LongLiteral("3"), new LongLiteral("4")),
                new LongLiteral("3"));
        assertOptimizedEquals(
                new IfExpression(new ComparisonExpression(EQUAL, new LongLiteral("1"), new LongLiteral("2")), new LongLiteral("3"), new LongLiteral("4")),
                new LongLiteral("4"));

        assertOptimizedEquals(
                new IfExpression(TRUE_LITERAL, new LongLiteral("3"), new LongLiteral("4")),
                new LongLiteral("3"));
        assertOptimizedEquals(
                new IfExpression(FALSE_LITERAL, new LongLiteral("3"), new LongLiteral("4")),
                new LongLiteral("4"));
        assertOptimizedEquals(
                new IfExpression(new Cast(new NullLiteral(), dataType("boolean")), new LongLiteral("3"), new LongLiteral("4")),
                new LongLiteral("4"));

        assertOptimizedEquals(
                new IfExpression(TRUE_LITERAL, new LongLiteral("3"), new Cast(new NullLiteral(), dataType("integer"))),
                new LongLiteral("3"));
        assertOptimizedEquals(
                new IfExpression(FALSE_LITERAL, new LongLiteral("3"), new Cast(new NullLiteral(), dataType("integer"))),
                new NullLiteral());
        assertOptimizedEquals(
                new IfExpression(TRUE_LITERAL, new Cast(new NullLiteral(), dataType("integer")), new LongLiteral("4")),
                new NullLiteral());
        assertOptimizedEquals(
                new IfExpression(FALSE_LITERAL, new Cast(new NullLiteral(), dataType("integer")), new LongLiteral("4")),
                new LongLiteral("4"));
        assertOptimizedEquals(
                new IfExpression(TRUE_LITERAL, new Cast(new NullLiteral(), dataType("integer")), new Cast(new NullLiteral(), dataType("integer"))),
                new NullLiteral());
        assertOptimizedEquals(
                new IfExpression(FALSE_LITERAL, new Cast(new NullLiteral(), dataType("integer")), new Cast(new NullLiteral(), dataType("integer"))),
                new NullLiteral());

        assertOptimizedEquals(
                new IfExpression(TRUE_LITERAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0"))),
                new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")));
        assertOptimizedEquals(
                new IfExpression(TRUE_LITERAL, new LongLiteral("1"), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0"))),
                new LongLiteral("1"));
        assertOptimizedEquals(
                new IfExpression(FALSE_LITERAL, new LongLiteral("1"), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0"))),
                new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")));
        assertOptimizedEquals(
                new IfExpression(FALSE_LITERAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("1")),
                new LongLiteral("1"));
        assertOptimizedEquals(
                new IfExpression(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("0")), new LongLiteral("1"), new LongLiteral("2")),
                new IfExpression(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("0")), new LongLiteral("1"), new LongLiteral("2")));

        assertEvaluatedEquals(
                new IfExpression(TRUE_LITERAL, new LongLiteral("1"), new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0"))),
                new LongLiteral("1"));
        assertEvaluatedEquals(
                new IfExpression(FALSE_LITERAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("1")),
                new LongLiteral("1"));
        assertTrinoExceptionThrownBy(() -> evaluate(new IfExpression(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("0")), new LongLiteral("1"), new LongLiteral("2"))))
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testOptimizeDivideByZero()
    {
        assertOptimizedEquals(
                new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")),
                new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")));

        assertTrinoExceptionThrownBy(() -> evaluate(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0"))))
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testRowSubscript()
    {
        assertOptimizedEquals(
                new SubscriptExpression(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("a"), TRUE_LITERAL)), new LongLiteral("3")),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new SubscriptExpression(new SubscriptExpression(new SubscriptExpression(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("a"), new Row(ImmutableList.of(new LongLiteral("2"), new StringLiteral("b"), new Row(ImmutableList.of(new LongLiteral("3"), new StringLiteral("c"))))))), new LongLiteral("3")), new LongLiteral("3")), new LongLiteral("2")),
                new StringLiteral("c"));

        assertOptimizedEquals(
                new SubscriptExpression(new Row(ImmutableList.of(new LongLiteral("1"), new NullLiteral())), new LongLiteral("2")),
                new NullLiteral());
        assertOptimizedEquals(
                new SubscriptExpression(new Row(ImmutableList.of(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("1"))), new LongLiteral("1")),
                new SubscriptExpression(new Row(ImmutableList.of(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("1"))), new LongLiteral("1")));
        assertOptimizedEquals(
                new SubscriptExpression(new Row(ImmutableList.of(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("1"))), new LongLiteral("2")),
                new SubscriptExpression(new Row(ImmutableList.of(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("1"))), new LongLiteral("2")));

        assertTrinoExceptionThrownBy(() -> evaluate(new SubscriptExpression(new Row(ImmutableList.of(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("1"))), new LongLiteral("2"))))
                .hasErrorCode(DIVISION_BY_ZERO);
        assertTrinoExceptionThrownBy(() -> evaluate(new SubscriptExpression(new Row(ImmutableList.of(new ArithmeticBinaryExpression(DIVIDE, new LongLiteral("0"), new LongLiteral("0")), new LongLiteral("1"))), new LongLiteral("2"))))
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    private static void assertOptimizedEquals(Expression actual, Expression expected)
    {
        assertThat(optimize(actual)).isEqualTo(optimize(expected));
    }

    private static void assertOptimizedMatches(Expression actual, Expression expected)
    {
        Expression actualOptimized = (Expression) optimize(actual);

        SymbolAliases.Builder aliases = SymbolAliases.builder()
                .putAll(SYMBOL_TYPES.allTypes().keySet().stream()
                        .map(Symbol::getName)
                        .collect(toImmutableMap(identity(), SymbolReference::new)));

        assertExpressionEquals(actualOptimized, expected, aliases.build());
    }

    static Object optimize(Expression parsedExpression)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = new IrTypeAnalyzer(PLANNER_CONTEXT).getTypes(TEST_SESSION, SYMBOL_TYPES, parsedExpression);
        IrExpressionInterpreter interpreter = new IrExpressionInterpreter(parsedExpression, PLANNER_CONTEXT, TEST_SESSION, expressionTypes);
        return interpreter.optimize(INPUTS);
    }

    private static void assertEvaluatedEquals(Expression actual, Expression expected)
    {
        assertThat(evaluate(actual)).isEqualTo(evaluate(expected));
    }

    private static Object evaluate(Expression expression)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = new IrTypeAnalyzer(PLANNER_CONTEXT).getTypes(TEST_SESSION, SYMBOL_TYPES, expression);
        IrExpressionInterpreter interpreter = new IrExpressionInterpreter(expression, PLANNER_CONTEXT, TEST_SESSION, expressionTypes);

        return interpreter.evaluate(INPUTS);
    }
}
