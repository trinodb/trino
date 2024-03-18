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
import io.airlift.slice.Slices;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.ArithmeticBinaryExpression;
import io.trino.sql.ir.ArithmeticUnaryExpression;
import io.trino.sql.ir.BetweenPredicate;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.CoalesceExpression;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FunctionCall;
import io.trino.sql.ir.IfExpression;
import io.trino.sql.ir.InPredicate;
import io.trino.sql.ir.IsNullPredicate;
import io.trino.sql.ir.LogicalExpression;
import io.trino.sql.ir.NodeRef;
import io.trino.sql.ir.NotExpression;
import io.trino.sql.ir.NullIfExpression;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.SearchedCaseExpression;
import io.trino.sql.ir.SimpleCaseExpression;
import io.trino.sql.ir.SubscriptExpression;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.IrExpressionInterpreter;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolResolver;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.assertions.SymbolAliases;
import io.trino.transaction.TestingTransactionManager;
import io.trino.type.UnknownType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ExpressionTestUtils.assertExpressionEquals;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.DIVIDE;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.MULTIPLY;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.SUBTRACT;
import static io.trino.sql.ir.ArithmeticUnaryExpression.Sign.MINUS;
import static io.trino.sql.ir.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.ir.LogicalExpression.Operator.AND;
import static io.trino.sql.ir.LogicalExpression.Operator.OR;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
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
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction SUBTRACT_INTEGER = FUNCTIONS.resolveOperator(OperatorType.SUBTRACT, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction MULTIPLY_INTEGER = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction DIVIDE_INTEGER = FUNCTIONS.resolveOperator(OperatorType.DIVIDE, ImmutableList.of(INTEGER, INTEGER));

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
                new LogicalExpression(AND, ImmutableList.of(TRUE_LITERAL, new Constant(UnknownType.UNKNOWN, null))),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new LogicalExpression(AND, ImmutableList.of(FALSE_LITERAL, new Constant(UnknownType.UNKNOWN, null))),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new LogicalExpression(AND, ImmutableList.of(new Constant(UnknownType.UNKNOWN, null), TRUE_LITERAL)),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new LogicalExpression(AND, ImmutableList.of(new Constant(UnknownType.UNKNOWN, null), FALSE_LITERAL)),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new LogicalExpression(AND, ImmutableList.of(new Constant(UnknownType.UNKNOWN, null), new Constant(UnknownType.UNKNOWN, null))),
                new Constant(UnknownType.UNKNOWN, null));
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
                new LogicalExpression(OR, ImmutableList.of(TRUE_LITERAL, new Constant(UnknownType.UNKNOWN, null))),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new LogicalExpression(OR, ImmutableList.of(new Constant(UnknownType.UNKNOWN, null), TRUE_LITERAL)),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new LogicalExpression(OR, ImmutableList.of(new Constant(UnknownType.UNKNOWN, null), new Constant(UnknownType.UNKNOWN, null))),
                new Constant(UnknownType.UNKNOWN, null));

        assertOptimizedEquals(
                new LogicalExpression(OR, ImmutableList.of(FALSE_LITERAL, new Constant(UnknownType.UNKNOWN, null))),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new LogicalExpression(OR, ImmutableList.of(new Constant(UnknownType.UNKNOWN, null), FALSE_LITERAL)),
                new Constant(UnknownType.UNKNOWN, null));
    }

    @Test
    public void testComparison()
    {
        assertOptimizedEquals(
                new ComparisonExpression(EQUAL, new Constant(UnknownType.UNKNOWN, null), new Constant(UnknownType.UNKNOWN, null)),
                new Constant(UnknownType.UNKNOWN, null));

        assertOptimizedEquals(
                new ComparisonExpression(EQUAL, new Constant(VARCHAR, Slices.utf8Slice("a")), new Constant(VARCHAR, Slices.utf8Slice("b"))),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new ComparisonExpression(EQUAL, new Constant(VARCHAR, Slices.utf8Slice("a")), new Constant(VARCHAR, Slices.utf8Slice("a"))),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new ComparisonExpression(EQUAL, new Constant(VARCHAR, Slices.utf8Slice("a")), new Constant(UnknownType.UNKNOWN, null)),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new ComparisonExpression(EQUAL, new Constant(UnknownType.UNKNOWN, null), new Constant(VARCHAR, Slices.utf8Slice("a"))),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new ComparisonExpression(EQUAL, new SymbolReference("bound_value"), new Constant(INTEGER, 1234L)),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new ComparisonExpression(EQUAL, new SymbolReference("bound_value"), new Constant(INTEGER, 1L)),
                FALSE_LITERAL);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertOptimizedEquals(
                new ComparisonExpression(IS_DISTINCT_FROM, new Constant(UnknownType.UNKNOWN, null), new Constant(UnknownType.UNKNOWN, null)),
                FALSE_LITERAL);

        assertOptimizedEquals(
                new ComparisonExpression(IS_DISTINCT_FROM, new Constant(INTEGER, 3L), new Constant(INTEGER, 4L)),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new ComparisonExpression(IS_DISTINCT_FROM, new Constant(INTEGER, 3L), new Constant(INTEGER, 3L)),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new ComparisonExpression(IS_DISTINCT_FROM, new Constant(INTEGER, 3L), new Constant(UnknownType.UNKNOWN, null)),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new ComparisonExpression(IS_DISTINCT_FROM, new Constant(UnknownType.UNKNOWN, null), new Constant(INTEGER, 3L)),
                TRUE_LITERAL);

        assertOptimizedMatches(
                new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("unbound_value"), new Constant(INTEGER, 1L)),
                new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("unbound_value"), new Constant(INTEGER, 1L)));
        assertOptimizedMatches(
                new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("unbound_value"), new Constant(UnknownType.UNKNOWN, null)),
                new NotExpression(new IsNullPredicate(new SymbolReference("unbound_value"))));
        assertOptimizedMatches(
                new ComparisonExpression(IS_DISTINCT_FROM, new Constant(UnknownType.UNKNOWN, null), new SymbolReference("unbound_value")),
                new NotExpression(new IsNullPredicate(new SymbolReference("unbound_value"))));
    }

    @Test
    public void testIsNull()
    {
        assertOptimizedEquals(
                new IsNullPredicate(new Constant(UnknownType.UNKNOWN, null)),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new IsNullPredicate(new Constant(INTEGER, 1L)),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new IsNullPredicate(new ArithmeticBinaryExpression(ADD_INTEGER, ADD, new Constant(INTEGER, null), new Constant(INTEGER, 1L))),
                TRUE_LITERAL);
    }

    @Test
    public void testIsNotNull()
    {
        assertOptimizedEquals(
                new NotExpression(new IsNullPredicate(new Constant(UnknownType.UNKNOWN, null))),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new NotExpression(new IsNullPredicate(new Constant(INTEGER, 1L))),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new NotExpression(new IsNullPredicate(new ArithmeticBinaryExpression(ADD_INTEGER, ADD, new Constant(INTEGER, null), new Constant(INTEGER, 1L)))),
                FALSE_LITERAL);
    }

    @Test
    public void testNullIf()
    {
        assertOptimizedEquals(
                new NullIfExpression(new Constant(VARCHAR, Slices.utf8Slice("a")), new Constant(VARCHAR, Slices.utf8Slice("a"))),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new NullIfExpression(new Constant(VARCHAR, Slices.utf8Slice("a")), new Constant(VARCHAR, Slices.utf8Slice("b"))),
                new Constant(VARCHAR, Slices.utf8Slice("a")));
        assertOptimizedEquals(
                new NullIfExpression(new Constant(UnknownType.UNKNOWN, null), new Constant(VARCHAR, Slices.utf8Slice("b"))),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new NullIfExpression(new Constant(VARCHAR, Slices.utf8Slice("a")), new Constant(UnknownType.UNKNOWN, null)),
                new Constant(VARCHAR, Slices.utf8Slice("a")));
        assertOptimizedEquals(
                new NullIfExpression(new SymbolReference("unbound_value"), new Constant(INTEGER, 1L)),
                new NullIfExpression(new SymbolReference("unbound_value"), new Constant(INTEGER, 1L)));
    }

    @Test
    public void testNegative()
    {
        assertOptimizedEquals(
                new ArithmeticUnaryExpression(MINUS, new Constant(INTEGER, 1L)),
                new Constant(INTEGER, -1L));
        assertOptimizedEquals(
                new ArithmeticUnaryExpression(MINUS, new ArithmeticBinaryExpression(ADD_INTEGER, ADD, new SymbolReference("unbound_value"), new Constant(INTEGER, 1L))),
                new ArithmeticUnaryExpression(MINUS, new ArithmeticBinaryExpression(ADD_INTEGER, ADD, new SymbolReference("unbound_value"), new Constant(INTEGER, 1L))));
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
                new NotExpression(new Constant(UnknownType.UNKNOWN, null)),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new NotExpression(new ComparisonExpression(EQUAL, new SymbolReference("unbound_value"), new Constant(INTEGER, 1L))),
                new NotExpression(new ComparisonExpression(EQUAL, new SymbolReference("unbound_value"), new Constant(INTEGER, 1L))));
    }

    @Test
    public void testFunctionCall()
    {
        assertOptimizedEquals(
                new FunctionCall(ABS, ImmutableList.of(new Constant(INTEGER, 5L))),
                new Constant(INTEGER, 5L));
        assertOptimizedEquals(
                new FunctionCall(ABS, ImmutableList.of(new SymbolReference("unbound_value"))),
                new FunctionCall(ABS, ImmutableList.of(new SymbolReference("unbound_value"))));
    }

    @Test
    public void testNonDeterministicFunctionCall()
    {
        assertOptimizedEquals(
                new FunctionCall(RANDOM, ImmutableList.of()),
                new FunctionCall(RANDOM, ImmutableList.of()));
    }

    @Test
    public void testBetween()
    {
        assertOptimizedEquals(
                new BetweenPredicate(new Constant(INTEGER, 3L), new Constant(INTEGER, 2L), new Constant(INTEGER, 4L)),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new BetweenPredicate(new Constant(INTEGER, 2L), new Constant(INTEGER, 3L), new Constant(INTEGER, 4L)),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new BetweenPredicate(new Constant(UnknownType.UNKNOWN, null), new Constant(INTEGER, 2L), new Constant(INTEGER, 4L)),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new BetweenPredicate(new Constant(INTEGER, 3L), new Constant(UnknownType.UNKNOWN, null), new Constant(INTEGER, 4L)),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new BetweenPredicate(new Constant(INTEGER, 3L), new Constant(INTEGER, 2L), new Constant(UnknownType.UNKNOWN, null)),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new BetweenPredicate(new Constant(INTEGER, 2L), new Constant(INTEGER, 3L), new Constant(UnknownType.UNKNOWN, null)),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new BetweenPredicate(new Constant(INTEGER, 8L), new Constant(UnknownType.UNKNOWN, null), new Constant(INTEGER, 6L)),
                FALSE_LITERAL);

        assertOptimizedEquals(
                new BetweenPredicate(new SymbolReference("bound_value"), new Constant(INTEGER, 1000L), new Constant(INTEGER, 2000L)),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new BetweenPredicate(new SymbolReference("bound_value"), new Constant(INTEGER, 3L), new Constant(INTEGER, 4L)),
                FALSE_LITERAL);
    }

    @Test
    public void testIn()
    {
        assertOptimizedEquals(
                new InPredicate(new Constant(INTEGER, 3L), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new InPredicate(new Constant(INTEGER, 3L), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 9L), new Constant(INTEGER, 5L))),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new InPredicate(new Constant(INTEGER, 3L), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, null), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))),
                TRUE_LITERAL);

        assertOptimizedEquals(
                new InPredicate(new Constant(INTEGER, null), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, null), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new InPredicate(new Constant(INTEGER, 3L), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, null))),
                new Constant(UnknownType.UNKNOWN, null));

        assertOptimizedEquals(
                new InPredicate(new SymbolReference("bound_value"), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 1234L), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new InPredicate(new SymbolReference("bound_value"), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new InPredicate(new Constant(INTEGER, 1234L), ImmutableList.of(new Constant(INTEGER, 2L), new SymbolReference("bound_value"), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new InPredicate(new Constant(INTEGER, 99L), ImmutableList.of(new Constant(INTEGER, 2L), new SymbolReference("bound_value"), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))),
                FALSE_LITERAL);
        assertOptimizedEquals(
                new InPredicate(new SymbolReference("bound_value"), ImmutableList.of(new Constant(INTEGER, 2L), new SymbolReference("bound_value"), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))),
                TRUE_LITERAL);

        assertOptimizedEquals(
                new InPredicate(new SymbolReference("unbound_value"), ImmutableList.of(new Constant(INTEGER, 1L))),
                new ComparisonExpression(EQUAL, new SymbolReference("unbound_value"), new Constant(INTEGER, 1L)));

        assertOptimizedEquals(
                new InPredicate(new Constant(INTEGER, 3L), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 5L), new Constant(INTEGER, 0L)))),
                new InPredicate(new Constant(INTEGER, 3L), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 5L), new Constant(INTEGER, 0L)))));
        assertOptimizedEquals(
                new InPredicate(new Constant(INTEGER, null), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 5L), new Constant(INTEGER, 0L)))),
                new InPredicate(new Constant(INTEGER, null), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 5L), new Constant(INTEGER, 0L)))));
        assertOptimizedEquals(
                new InPredicate(new Constant(INTEGER, 3L), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new Constant(INTEGER, null), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 5L), new Constant(INTEGER, 0L)))),
                new InPredicate(new Constant(INTEGER, 3L), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new Constant(INTEGER, null), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 5L), new Constant(INTEGER, 0L)))));
        assertOptimizedEquals(
                new InPredicate(new Constant(INTEGER, null), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, null), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 5L), new Constant(INTEGER, 0L)))),
                new InPredicate(new Constant(INTEGER, null), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, null), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 5L), new Constant(INTEGER, 0L)))));
        assertOptimizedEquals(
                new InPredicate(new Constant(INTEGER, 3L), ImmutableList.of(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 5L), new Constant(INTEGER, 0L)), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 5L), new Constant(INTEGER, 0L)))),
                new InPredicate(new Constant(INTEGER, 3L), ImmutableList.of(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 5L), new Constant(INTEGER, 0L)), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 5L), new Constant(INTEGER, 0L)))));
        assertTrinoExceptionThrownBy(() -> evaluate(new InPredicate(new Constant(INTEGER, 3L), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 5L), new Constant(INTEGER, 0L))))))
                .hasErrorCode(DIVISION_BY_ZERO);

        assertOptimizedEquals(
                new InPredicate(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))),
                new InPredicate(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))));
        assertOptimizedEquals(
                new InPredicate(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 2L), new Constant(INTEGER, 4L))),
                new InPredicate(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L))));
        assertOptimizedEquals(
                new InPredicate(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 2L))),
                new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 2L)));
    }

    @Test
    public void testCastOptimization()
    {
        assertOptimizedEquals(
                new Cast(new SymbolReference("bound_value"), VARCHAR),
                new Constant(VARCHAR, Slices.utf8Slice("1234")));
        assertOptimizedMatches(
                new Cast(new SymbolReference("unbound_value"), INTEGER),
                new SymbolReference("unbound_value"));
    }

    @Test
    public void testTryCast()
    {
        assertOptimizedEquals(
                new Cast(new Constant(UnknownType.UNKNOWN, null), BIGINT, true),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new Cast(new Constant(INTEGER, 123L), BIGINT, true),
                new Constant(INTEGER, 123L));
        assertOptimizedEquals(
                new Cast(new Constant(UnknownType.UNKNOWN, null), INTEGER, true),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new Cast(new Constant(INTEGER, 123L), INTEGER, true),
                new Constant(INTEGER, 123L));
    }

    @Test
    public void testSearchCase()
    {
        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(TRUE_LITERAL, new Constant(INTEGER, 33L))),
                        Optional.empty()),
                new Constant(INTEGER, 33L));
        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(FALSE_LITERAL, new Constant(INTEGER, 1L))),
                        Optional.of(new Constant(INTEGER, 33L))),
                new Constant(INTEGER, 33L));

        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("bound_value"), new Constant(INTEGER, 1234L)), new Constant(INTEGER, 33L))),
                        Optional.empty()),
                new Constant(INTEGER, 33L));
        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(TRUE_LITERAL, new SymbolReference("bound_value"))),
                        Optional.empty()),
                new Constant(INTEGER, 1234L));
        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(FALSE_LITERAL, new Constant(INTEGER, 1L))),
                        Optional.of(new SymbolReference("bound_value"))),
                new Constant(INTEGER, 1234L));

        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("bound_value"), new Constant(INTEGER, 1234L)), new Constant(INTEGER, 33L))),
                        Optional.of(new SymbolReference("unbound_value"))),
                new Constant(INTEGER, 33L));

        assertOptimizedMatches(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L))),
                        Optional.empty()),
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L))),
                        Optional.empty()));

        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(TRUE_LITERAL, new Constant(VARCHAR, Slices.utf8Slice("a"))), new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 0L)), new Constant(VARCHAR, Slices.utf8Slice("b")))),
                        Optional.of(new Constant(VARCHAR, Slices.utf8Slice("c")))),
                new Constant(VARCHAR, Slices.utf8Slice("a")));
        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 0L)), new Constant(VARCHAR, Slices.utf8Slice("a"))), new WhenClause(TRUE_LITERAL, new Constant(VARCHAR, Slices.utf8Slice("b")))),
                        Optional.of(new Constant(VARCHAR, Slices.utf8Slice("c")))),
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 0L)), new Constant(VARCHAR, Slices.utf8Slice("a")))),
                        Optional.of(new Constant(VARCHAR, Slices.utf8Slice("b")))));
        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 0L)), new Constant(VARCHAR, Slices.utf8Slice("a"))), new WhenClause(FALSE_LITERAL, new Constant(VARCHAR, Slices.utf8Slice("b")))),
                        Optional.of(new Constant(VARCHAR, Slices.utf8Slice("c")))),
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 0L)), new Constant(VARCHAR, Slices.utf8Slice("a")))),
                        Optional.of(new Constant(VARCHAR, Slices.utf8Slice("c")))));
        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 0L)), new Constant(VARCHAR, Slices.utf8Slice("a"))),
                        new WhenClause(FALSE_LITERAL, new Constant(VARCHAR, Slices.utf8Slice("b")))),
                        Optional.empty()),
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 0L)), new Constant(VARCHAR, Slices.utf8Slice("a")))),
                        Optional.empty()));
        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(TRUE_LITERAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))),
                        new WhenClause(FALSE_LITERAL, new Constant(INTEGER, 1L))),
                        Optional.empty()),
                new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)));
        assertOptimizedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(FALSE_LITERAL, new Constant(INTEGER, 1L)), new WhenClause(FALSE_LITERAL, new Constant(INTEGER, 2L))),
                        Optional.of(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)));

        assertEvaluatedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(FALSE_LITERAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new WhenClause(TRUE_LITERAL, new Constant(INTEGER, 1L))),
                        Optional.of(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                new Constant(INTEGER, 1L));
        assertEvaluatedEquals(
                new SearchedCaseExpression(ImmutableList.of(
                        new WhenClause(TRUE_LITERAL, new Constant(INTEGER, 1L)), new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 0L)), new Constant(INTEGER, 2L))),
                        Optional.of(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                new Constant(INTEGER, 1L));
    }

    @Test
    public void testSimpleCase()
    {
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 1L), new Constant(INTEGER, 33L)),
                                new WhenClause(new Constant(INTEGER, 1L), new Constant(INTEGER, 34L))),
                        Optional.empty()),
                new Constant(INTEGER, 33L));

        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new Constant(BOOLEAN, null),
                        ImmutableList.of(
                                new WhenClause(TRUE_LITERAL, new Constant(INTEGER, 33L))),
                        Optional.empty()),
                new Constant(UnknownType.UNKNOWN, null));
        for (SimpleCaseExpression simpleCaseExpression : Arrays.asList(new SimpleCaseExpression(
                        new Constant(BOOLEAN, null),
                        ImmutableList.of(
                                new WhenClause(TRUE_LITERAL, new Constant(INTEGER, 33L))),
                        Optional.of(new Constant(INTEGER, 33L))),
                new SimpleCaseExpression(
                        new Constant(INTEGER, 33L),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, null), new Constant(INTEGER, 1L))),
                        Optional.of(new Constant(INTEGER, 33L))),
                new SimpleCaseExpression(
                        new SymbolReference("bound_value"),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 1234L), new Constant(INTEGER, 33L))),
                        Optional.empty()),
                new SimpleCaseExpression(
                        new Constant(INTEGER, 1234L),
                        ImmutableList.of(
                                new WhenClause(new SymbolReference("bound_value"), new Constant(INTEGER, 33L))),
                        Optional.empty()))) {
            assertOptimizedEquals(
                    simpleCaseExpression,
                    new Constant(INTEGER, 33L));
        }

        assertOptimizedEquals(
                new SimpleCaseExpression(
                        TRUE_LITERAL,
                        ImmutableList.of(
                                new WhenClause(TRUE_LITERAL, new SymbolReference("bound_value"))),
                        Optional.empty()),
                new Constant(INTEGER, 1234L));
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        TRUE_LITERAL,
                        ImmutableList.of(
                                new WhenClause(FALSE_LITERAL, new Constant(INTEGER, 1L))),
                        Optional.of(new SymbolReference("bound_value"))),
                new Constant(INTEGER, 1234L));

        assertOptimizedEquals(
                new SimpleCaseExpression(
                        TRUE_LITERAL,
                        ImmutableList.of(
                                new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("unbound_value"), new Constant(INTEGER, 1L)), new Constant(INTEGER, 1L)),
                                new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 0L)), new Constant(INTEGER, 2L))),
                        Optional.of(new Constant(INTEGER, 33L))),
                new SimpleCaseExpression(
                        TRUE_LITERAL,
                        ImmutableList.of(
                                new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("unbound_value"), new Constant(INTEGER, 1L)), new Constant(INTEGER, 1L)),
                                new WhenClause(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 0L)), new Constant(INTEGER, 2L))),
                        Optional.of(new Constant(INTEGER, 33L))));

        assertOptimizedMatches(
                new SimpleCaseExpression(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L)),
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 2L))),
                        Optional.of(new Constant(INTEGER, 1L))),
                new SimpleCaseExpression(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L)),
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 2L))),
                        Optional.of(new Constant(INTEGER, 1L))));

        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new Constant(INTEGER, null),
                        ImmutableList.of(
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                        Optional.of(new Constant(INTEGER, 1L))),
                new Constant(INTEGER, 1L));
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new Constant(INTEGER, null),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 1L), new Constant(INTEGER, 2L))),
                        Optional.of(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)));
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 1L), new Constant(INTEGER, 2L))),
                        Optional.of(new Constant(INTEGER, 3L))),
                new SimpleCaseExpression(
                        new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 1L), new Constant(INTEGER, 2L))),
                        Optional.of(new Constant(INTEGER, 3L))));
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 2L))),
                        Optional.of(new Constant(INTEGER, 3L))),
                new SimpleCaseExpression(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 2L))),
                        Optional.of(new Constant(INTEGER, 3L))));
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 2L), new Constant(INTEGER, 2L)),
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 3L))),
                        Optional.of(new Constant(INTEGER, 4L))),
                new SimpleCaseExpression(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 3L))),
                        Optional.of(new Constant(INTEGER, 4L))));
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 2L))),
                        Optional.empty()),
                new SimpleCaseExpression(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 2L))),
                        Optional.empty()));
        assertOptimizedEquals(
                new SimpleCaseExpression(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 2L), new Constant(INTEGER, 2L)),
                                new WhenClause(new Constant(INTEGER, 3L), new Constant(INTEGER, 3L))),
                        Optional.empty()),
                new Constant(UnknownType.UNKNOWN, null));

        assertEvaluatedEquals(
                new SimpleCaseExpression(
                        new Constant(INTEGER, null),
                        ImmutableList.of(
                                new WhenClause(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                        Optional.of(new Constant(INTEGER, 1L))),
                new Constant(INTEGER, 1L));
        assertEvaluatedEquals(
                new SimpleCaseExpression(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 2L), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                        Optional.of(new Constant(INTEGER, 3L))),
                new Constant(INTEGER, 3L));
        assertEvaluatedEquals(
                new SimpleCaseExpression(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 1L), new Constant(INTEGER, 2L)),
                                new WhenClause(new Constant(INTEGER, 1L), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                        Optional.empty()),
                new Constant(INTEGER, 2L));
        assertEvaluatedEquals(
                new SimpleCaseExpression(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 1L), new Constant(INTEGER, 2L))),
                        Optional.of(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                new Constant(INTEGER, 2L));
    }

    @Test
    public void testCoalesce()
    {
        assertOptimizedEquals(
                new CoalesceExpression(new ArithmeticBinaryExpression(MULTIPLY_INTEGER, MULTIPLY, new SymbolReference("unbound_value"), new ArithmeticBinaryExpression(MULTIPLY_INTEGER, MULTIPLY, new Constant(INTEGER, 2L), new Constant(INTEGER, 3L))), new ArithmeticBinaryExpression(SUBTRACT_INTEGER, SUBTRACT, new Constant(INTEGER, 1L), new Constant(INTEGER, 1L)), new Constant(INTEGER, null)),
                new CoalesceExpression(new ArithmeticBinaryExpression(MULTIPLY_INTEGER, MULTIPLY, new SymbolReference("unbound_value"), new Constant(INTEGER, 6L)), new Constant(INTEGER, 0L)));
        assertOptimizedMatches(
                new CoalesceExpression(new SymbolReference("unbound_value"), new SymbolReference("unbound_value")),
                new SymbolReference("unbound_value"));
        assertOptimizedEquals(
                new CoalesceExpression(new Constant(INTEGER, 6L), new SymbolReference("unbound_value")),
                new Constant(INTEGER, 6L));
        assertOptimizedMatches(
                new CoalesceExpression(new FunctionCall(RANDOM, ImmutableList.of()), new FunctionCall(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 5.0)),
                new CoalesceExpression(new FunctionCall(RANDOM, ImmutableList.of()), new FunctionCall(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 5.0)));

        assertOptimizedEquals(
                new CoalesceExpression(new Constant(UnknownType.UNKNOWN, null), new CoalesceExpression(new Constant(UnknownType.UNKNOWN, null), new Constant(UnknownType.UNKNOWN, null))),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new CoalesceExpression(new Constant(INTEGER, null), new CoalesceExpression(new Constant(INTEGER, null), new CoalesceExpression(new Constant(INTEGER, null), new Constant(INTEGER, null), new Constant(INTEGER, 1L)))),
                new Constant(INTEGER, 1L));
        assertOptimizedEquals(
                new CoalesceExpression(new Constant(INTEGER, 1L), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))),
                new Constant(INTEGER, 1L));
        assertOptimizedEquals(
                new CoalesceExpression(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L)),
                new CoalesceExpression(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L)));
        assertOptimizedEquals(
                new CoalesceExpression(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L), new Constant(INTEGER, null)),
                new CoalesceExpression(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L)));
        assertOptimizedEquals(
                new CoalesceExpression(new Constant(INTEGER, 1L), new CoalesceExpression(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 2L))),
                new Constant(INTEGER, 1L));
        assertOptimizedEquals(
                new CoalesceExpression(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, null), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 1L), new Constant(INTEGER, 0L)), new Constant(INTEGER, null), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))),
                new CoalesceExpression(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 1L), new Constant(INTEGER, 0L))));
        assertOptimizedEquals(
                new CoalesceExpression(new FunctionCall(RANDOM, ImmutableList.of()), new FunctionCall(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 1.0), new FunctionCall(RANDOM, ImmutableList.of())),
                new CoalesceExpression(new FunctionCall(RANDOM, ImmutableList.of()), new FunctionCall(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 1.0)));

        assertEvaluatedEquals(
                new CoalesceExpression(new Constant(INTEGER, 1L), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))),
                new Constant(INTEGER, 1L));
        assertTrinoExceptionThrownBy(() -> evaluate(new CoalesceExpression(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L))))
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testIf()
    {
        assertOptimizedEquals(
                new IfExpression(new ComparisonExpression(EQUAL, new Constant(INTEGER, 2L), new Constant(INTEGER, 2L)), new Constant(INTEGER, 3L), new Constant(INTEGER, 4L)),
                new Constant(INTEGER, 3L));
        assertOptimizedEquals(
                new IfExpression(new ComparisonExpression(EQUAL, new Constant(INTEGER, 1L), new Constant(INTEGER, 2L)), new Constant(INTEGER, 3L), new Constant(INTEGER, 4L)),
                new Constant(INTEGER, 4L));

        assertOptimizedEquals(
                new IfExpression(TRUE_LITERAL, new Constant(INTEGER, 3L), new Constant(INTEGER, 4L)),
                new Constant(INTEGER, 3L));
        assertOptimizedEquals(
                new IfExpression(FALSE_LITERAL, new Constant(INTEGER, 3L), new Constant(INTEGER, 4L)),
                new Constant(INTEGER, 4L));
        assertOptimizedEquals(
                new IfExpression(new Constant(BOOLEAN, null), new Constant(INTEGER, 3L), new Constant(INTEGER, 4L)),
                new Constant(INTEGER, 4L));

        assertOptimizedEquals(
                new IfExpression(TRUE_LITERAL, new Constant(INTEGER, 3L), new Constant(INTEGER, null)),
                new Constant(INTEGER, 3L));
        assertOptimizedEquals(
                new IfExpression(FALSE_LITERAL, new Constant(INTEGER, 3L), new Constant(INTEGER, null)),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new IfExpression(TRUE_LITERAL, new Constant(INTEGER, null), new Constant(INTEGER, 4L)),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new IfExpression(FALSE_LITERAL, new Constant(INTEGER, null), new Constant(INTEGER, 4L)),
                new Constant(INTEGER, 4L));
        assertOptimizedEquals(
                new IfExpression(TRUE_LITERAL, new Constant(INTEGER, null), new Constant(INTEGER, null)),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new IfExpression(FALSE_LITERAL, new Constant(INTEGER, null), new Constant(INTEGER, null)),
                new Constant(UnknownType.UNKNOWN, null));

        assertOptimizedEquals(
                new IfExpression(TRUE_LITERAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))),
                new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)));
        assertOptimizedEquals(
                new IfExpression(TRUE_LITERAL, new Constant(INTEGER, 1L), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))),
                new Constant(INTEGER, 1L));
        assertOptimizedEquals(
                new IfExpression(FALSE_LITERAL, new Constant(INTEGER, 1L), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))),
                new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)));
        assertOptimizedEquals(
                new IfExpression(FALSE_LITERAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L)),
                new Constant(INTEGER, 1L));
        assertOptimizedEquals(
                new IfExpression(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L), new Constant(INTEGER, 2L)),
                new IfExpression(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L), new Constant(INTEGER, 2L)));

        assertEvaluatedEquals(
                new IfExpression(TRUE_LITERAL, new Constant(INTEGER, 1L), new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))),
                new Constant(INTEGER, 1L));
        assertEvaluatedEquals(
                new IfExpression(FALSE_LITERAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L)),
                new Constant(INTEGER, 1L));
        assertTrinoExceptionThrownBy(() -> evaluate(new IfExpression(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L), new Constant(INTEGER, 2L))))
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testOptimizeDivideByZero()
    {
        assertOptimizedEquals(
                new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)),
                new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)));

        assertTrinoExceptionThrownBy(() -> evaluate(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))))
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testRowSubscript()
    {
        assertOptimizedEquals(
                new SubscriptExpression(new Row(ImmutableList.of(new Constant(INTEGER, 1L), new Constant(VARCHAR, Slices.utf8Slice("a")), TRUE_LITERAL)), new Constant(INTEGER, 3L)),
                TRUE_LITERAL);
        assertOptimizedEquals(
                new SubscriptExpression(new SubscriptExpression(new SubscriptExpression(new Row(ImmutableList.of(new Constant(INTEGER, 1L), new Constant(VARCHAR, Slices.utf8Slice("a")), new Row(ImmutableList.of(new Constant(INTEGER, 2L), new Constant(VARCHAR, Slices.utf8Slice("b")), new Row(ImmutableList.of(new Constant(INTEGER, 3L), new Constant(VARCHAR, Slices.utf8Slice("c")))))))), new Constant(INTEGER, 3L)), new Constant(INTEGER, 3L)), new Constant(INTEGER, 2L)),
                new Constant(VARCHAR, Slices.utf8Slice("c")));

        assertOptimizedEquals(
                new SubscriptExpression(new Row(ImmutableList.of(new Constant(INTEGER, 1L), new Constant(UnknownType.UNKNOWN, null))), new Constant(INTEGER, 2L)),
                new Constant(UnknownType.UNKNOWN, null));
        assertOptimizedEquals(
                new SubscriptExpression(new Row(ImmutableList.of(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L))), new Constant(INTEGER, 1L)),
                new SubscriptExpression(new Row(ImmutableList.of(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L))), new Constant(INTEGER, 1L)));
        assertOptimizedEquals(
                new SubscriptExpression(new Row(ImmutableList.of(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L))), new Constant(INTEGER, 2L)),
                new SubscriptExpression(new Row(ImmutableList.of(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L))), new Constant(INTEGER, 2L)));

        assertTrinoExceptionThrownBy(() -> evaluate(new SubscriptExpression(new Row(ImmutableList.of(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L))), new Constant(INTEGER, 2L))))
                .hasErrorCode(DIVISION_BY_ZERO);
        assertTrinoExceptionThrownBy(() -> evaluate(new SubscriptExpression(new Row(ImmutableList.of(new ArithmeticBinaryExpression(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L))), new Constant(INTEGER, 2L))))
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
        Map<NodeRef<Expression>, Type> expressionTypes = new IrTypeAnalyzer(PLANNER_CONTEXT).getTypes(SYMBOL_TYPES, parsedExpression);
        IrExpressionInterpreter interpreter = new IrExpressionInterpreter(parsedExpression, PLANNER_CONTEXT, TEST_SESSION, expressionTypes);
        return interpreter.optimize(INPUTS);
    }

    private static void assertEvaluatedEquals(Expression actual, Expression expected)
    {
        assertThat(evaluate(actual)).isEqualTo(evaluate(expected));
    }

    private static Object evaluate(Expression expression)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = new IrTypeAnalyzer(PLANNER_CONTEXT).getTypes(SYMBOL_TYPES, expression);
        IrExpressionInterpreter interpreter = new IrExpressionInterpreter(expression, PLANNER_CONTEXT, TEST_SESSION, expressionTypes);

        return interpreter.evaluate(INPUTS);
    }
}
