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
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.IrExpressionInterpreter;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolResolver;
import io.trino.sql.planner.assertions.SymbolAliases;
import io.trino.transaction.TestingTransactionManager;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ExpressionTestUtils.assertExpressionEquals;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExpressionInterpreter
{
    private static final Set<Symbol> SYMBOLS = ImmutableSet.of(
            new Symbol(INTEGER, "bound_value"),
            new Symbol(INTEGER, "unbound_value"));

    private static final SymbolResolver INPUTS = symbol -> {
        if (symbol.name().toLowerCase(ENGLISH).equals("bound_value")) {
            return Optional.of(new Constant(INTEGER, 1234L));
        }

        return Optional.empty();
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
    private static final ResolvedFunction NEGATION_INTEGER = FUNCTIONS.resolveOperator(OperatorType.NEGATION, ImmutableList.of(INTEGER));

    @Test
    public void testAnd()
    {
        assertOptimizedEquals(
                new Logical(AND, ImmutableList.of(TRUE, FALSE)),
                FALSE);
        assertOptimizedEquals(
                new Logical(AND, ImmutableList.of(FALSE, TRUE)),
                FALSE);
        assertOptimizedEquals(
                new Logical(AND, ImmutableList.of(FALSE, FALSE)),
                FALSE);

        assertOptimizedEquals(
                new Logical(AND, ImmutableList.of(TRUE, new Constant(BOOLEAN, null))),
                new Constant(BOOLEAN, null));
        assertOptimizedEquals(
                new Logical(AND, ImmutableList.of(FALSE, new Constant(BOOLEAN, null))),
                FALSE);
        assertOptimizedEquals(
                new Logical(AND, ImmutableList.of(new Constant(BOOLEAN, null), TRUE)),
                new Constant(BOOLEAN, null));
        assertOptimizedEquals(
                new Logical(AND, ImmutableList.of(new Constant(BOOLEAN, null), FALSE)),
                FALSE);
        assertOptimizedEquals(
                new Logical(AND, ImmutableList.of(new Constant(BOOLEAN, null), new Constant(BOOLEAN, null))),
                new Constant(BOOLEAN, null));
    }

    @Test
    public void testOr()
    {
        assertOptimizedEquals(
                new Logical(OR, ImmutableList.of(TRUE, TRUE)),
                TRUE);
        assertOptimizedEquals(
                new Logical(OR, ImmutableList.of(TRUE, FALSE)),
                TRUE);
        assertOptimizedEquals(
                new Logical(OR, ImmutableList.of(FALSE, TRUE)),
                TRUE);
        assertOptimizedEquals(
                new Logical(OR, ImmutableList.of(FALSE, FALSE)),
                FALSE);

        assertOptimizedEquals(
                new Logical(OR, ImmutableList.of(TRUE, new Constant(BOOLEAN, null))),
                TRUE);
        assertOptimizedEquals(
                new Logical(OR, ImmutableList.of(new Constant(BOOLEAN, null), TRUE)),
                TRUE);
        assertOptimizedEquals(
                new Logical(OR, ImmutableList.of(new Constant(BOOLEAN, null), new Constant(BOOLEAN, null))),
                new Constant(BOOLEAN, null));

        assertOptimizedEquals(
                new Logical(OR, ImmutableList.of(FALSE, new Constant(BOOLEAN, null))),
                new Constant(BOOLEAN, null));
        assertOptimizedEquals(
                new Logical(OR, ImmutableList.of(new Constant(BOOLEAN, null), FALSE)),
                new Constant(BOOLEAN, null));
    }

    @Test
    public void testComparison()
    {
        assertOptimizedEquals(
                new Comparison(EQUAL, new Constant(UNKNOWN, null), new Constant(UNKNOWN, null)),
                new Constant(BOOLEAN, null));

        assertOptimizedEquals(
                new Comparison(EQUAL, new Constant(VARCHAR, Slices.utf8Slice("a")), new Constant(VARCHAR, Slices.utf8Slice("b"))),
                FALSE);
        assertOptimizedEquals(
                new Comparison(EQUAL, new Constant(VARCHAR, Slices.utf8Slice("a")), new Constant(VARCHAR, Slices.utf8Slice("a"))),
                TRUE);
        assertOptimizedEquals(
                new Comparison(EQUAL, new Constant(VARCHAR, Slices.utf8Slice("a")), new Constant(VARCHAR, null)),
                new Constant(BOOLEAN, null));
        assertOptimizedEquals(
                new Comparison(EQUAL, new Constant(VARCHAR, null), new Constant(VARCHAR, Slices.utf8Slice("a"))),
                new Constant(BOOLEAN, null));
        assertOptimizedEquals(
                new Comparison(EQUAL, new Reference(INTEGER, "bound_value"), new Constant(INTEGER, 1234L)),
                TRUE);
        assertOptimizedEquals(
                new Comparison(EQUAL, new Reference(INTEGER, "bound_value"), new Constant(INTEGER, 1L)),
                FALSE);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertOptimizedEquals(
                new Comparison(IS_DISTINCT_FROM, new Constant(UNKNOWN, null), new Constant(UNKNOWN, null)),
                FALSE);

        assertOptimizedEquals(
                new Comparison(IS_DISTINCT_FROM, new Constant(INTEGER, 3L), new Constant(INTEGER, 4L)),
                TRUE);
        assertOptimizedEquals(
                new Comparison(IS_DISTINCT_FROM, new Constant(INTEGER, 3L), new Constant(INTEGER, 3L)),
                FALSE);
        assertOptimizedEquals(
                new Comparison(IS_DISTINCT_FROM, new Constant(INTEGER, 3L), new Constant(INTEGER, null)),
                TRUE);
        assertOptimizedEquals(
                new Comparison(IS_DISTINCT_FROM, new Constant(INTEGER, null), new Constant(INTEGER, 3L)),
                TRUE);

        assertOptimizedMatches(
                new Comparison(IS_DISTINCT_FROM, new Reference(INTEGER, "unbound_value"), new Constant(INTEGER, 1L)),
                new Comparison(IS_DISTINCT_FROM, new Reference(INTEGER, "unbound_value"), new Constant(INTEGER, 1L)));
        assertOptimizedMatches(
                new Comparison(IS_DISTINCT_FROM, new Reference(INTEGER, "unbound_value"), new Constant(INTEGER, null)),
                new Not(new IsNull(new Reference(INTEGER, "unbound_value"))));
        assertOptimizedMatches(
                new Comparison(IS_DISTINCT_FROM, new Constant(INTEGER, null), new Reference(INTEGER, "unbound_value")),
                new Not(new IsNull(new Reference(INTEGER, "unbound_value"))));
    }

    @Test
    public void testIsNull()
    {
        assertOptimizedEquals(
                new IsNull(new Constant(UNKNOWN, null)),
                TRUE);
        assertOptimizedEquals(
                new IsNull(new Constant(INTEGER, 1L)),
                FALSE);
        assertOptimizedEquals(
                new IsNull(new Call(ADD_INTEGER, ImmutableList.of(new Constant(INTEGER, null), new Constant(INTEGER, 1L)))),
                TRUE);
    }

    @Test
    public void testIsNotNull()
    {
        assertOptimizedEquals(
                new Not(new IsNull(new Constant(UNKNOWN, null))),
                FALSE);
        assertOptimizedEquals(
                new Not(new IsNull(new Constant(INTEGER, 1L))),
                TRUE);
        assertOptimizedEquals(
                new Not(new IsNull(new Call(ADD_INTEGER, ImmutableList.of(new Constant(INTEGER, null), new Constant(INTEGER, 1L))))),
                FALSE);
    }

    @Test
    public void testNullIf()
    {
        assertOptimizedEquals(
                new NullIf(new Constant(VARCHAR, Slices.utf8Slice("a")), new Constant(VARCHAR, Slices.utf8Slice("a"))),
                new Constant(VARCHAR, null));
        assertOptimizedEquals(
                new NullIf(new Constant(VARCHAR, Slices.utf8Slice("a")), new Constant(VARCHAR, Slices.utf8Slice("b"))),
                new Constant(VARCHAR, Slices.utf8Slice("a")));
        assertOptimizedEquals(
                new NullIf(new Constant(VARCHAR, null), new Constant(VARCHAR, Slices.utf8Slice("b"))),
                new Constant(VARCHAR, null));
        assertOptimizedEquals(
                new NullIf(new Constant(VARCHAR, Slices.utf8Slice("a")), new Constant(VARCHAR, null)),
                new Constant(VARCHAR, Slices.utf8Slice("a")));
        assertOptimizedEquals(
                new NullIf(new Reference(INTEGER, "unbound_value"), new Constant(INTEGER, 1L)),
                new NullIf(new Reference(INTEGER, "unbound_value"), new Constant(INTEGER, 1L)));
    }

    @Test
    public void testNegative()
    {
        assertOptimizedEquals(
                new Call(NEGATION_INTEGER, ImmutableList.of(new Constant(INTEGER, 1L))),
                new Constant(INTEGER, -1L));
        assertOptimizedEquals(
                new Call(NEGATION_INTEGER, ImmutableList.of(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "unbound_value"), new Constant(INTEGER, 1L))))),
                new Call(NEGATION_INTEGER, ImmutableList.of(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "unbound_value"), new Constant(INTEGER, 1L))))));
    }

    @Test
    public void testNot()
    {
        assertOptimizedEquals(
                new Not(TRUE),
                FALSE);
        assertOptimizedEquals(
                new Not(FALSE),
                TRUE);
        assertOptimizedEquals(
                new Not(new Constant(BOOLEAN, null)),
                new Constant(BOOLEAN, null));
        assertOptimizedEquals(
                new Not(new Comparison(EQUAL, new Reference(INTEGER, "unbound_value"), new Constant(INTEGER, 1L))),
                new Not(new Comparison(EQUAL, new Reference(INTEGER, "unbound_value"), new Constant(INTEGER, 1L))));
    }

    @Test
    public void testFunctionCall()
    {
        assertOptimizedEquals(
                new Call(ABS, ImmutableList.of(new Constant(BIGINT, 5L))),
                new Constant(BIGINT, 5L));
        assertOptimizedEquals(
                new Call(ABS, ImmutableList.of(new Reference(BIGINT, "unbound_value"))),
                new Call(ABS, ImmutableList.of(new Reference(BIGINT, "unbound_value"))));
    }

    @Test
    public void testNonDeterministicFunctionCall()
    {
        assertOptimizedEquals(
                new Call(RANDOM, ImmutableList.of()),
                new Call(RANDOM, ImmutableList.of()));
    }

    @Test
    public void testBetween()
    {
        assertOptimizedEquals(
                new Between(new Constant(INTEGER, 3L), new Constant(INTEGER, 2L), new Constant(INTEGER, 4L)),
                TRUE);
        assertOptimizedEquals(
                new Between(new Constant(INTEGER, 2L), new Constant(INTEGER, 3L), new Constant(INTEGER, 4L)),
                FALSE);
        assertOptimizedEquals(
                new Between(new Constant(INTEGER, null), new Constant(INTEGER, 2L), new Constant(INTEGER, 4L)),
                new Constant(BOOLEAN, null));
        assertOptimizedEquals(
                new Between(new Constant(INTEGER, 3L), new Constant(INTEGER, null), new Constant(INTEGER, 4L)),
                new Constant(BOOLEAN, null));
        assertOptimizedEquals(
                new Between(new Constant(INTEGER, 3L), new Constant(INTEGER, 2L), new Constant(INTEGER, null)),
                new Constant(BOOLEAN, null));
        assertOptimizedEquals(
                new Between(new Constant(INTEGER, 2L), new Constant(INTEGER, 3L), new Constant(INTEGER, null)),
                FALSE);
        assertOptimizedEquals(
                new Between(new Constant(INTEGER, 8L), new Constant(INTEGER, null), new Constant(INTEGER, 6L)),
                FALSE);

        assertOptimizedEquals(
                new Between(new Reference(INTEGER, "bound_value"), new Constant(INTEGER, 1000L), new Constant(INTEGER, 2000L)),
                TRUE);
        assertOptimizedEquals(
                new Between(new Reference(INTEGER, "bound_value"), new Constant(INTEGER, 3L), new Constant(INTEGER, 4L)),
                FALSE);

        assertOptimizedEquals(
                new Between(new Reference(INTEGER, "unbound_value"), new Constant(INTEGER, 3L), new Constant(INTEGER, 0L)),
                ifExpression(new Not(new IsNull(new Reference(INTEGER, "unbound_value"))), FALSE));
    }

    @Test
    public void testIn()
    {
        assertOptimizedEquals(
                new In(new Constant(INTEGER, 3L), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))),
                TRUE);
        assertOptimizedEquals(
                new In(new Constant(INTEGER, 3L), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 9L), new Constant(INTEGER, 5L))),
                FALSE);
        assertOptimizedEquals(
                new In(new Constant(INTEGER, 3L), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, null), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))),
                TRUE);

        assertOptimizedEquals(
                new In(new Constant(INTEGER, null), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, null), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))),
                new Constant(BOOLEAN, null));
        assertOptimizedEquals(
                new In(new Constant(INTEGER, 3L), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, null))),
                new Constant(BOOLEAN, null));

        assertOptimizedEquals(
                new In(new Reference(INTEGER, "bound_value"), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 1234L), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))),
                TRUE);
        assertOptimizedEquals(
                new In(new Reference(INTEGER, "bound_value"), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))),
                FALSE);
        assertOptimizedEquals(
                new In(new Constant(INTEGER, 1234L), ImmutableList.of(new Constant(INTEGER, 2L), new Reference(INTEGER, "bound_value"), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))),
                TRUE);
        assertOptimizedEquals(
                new In(new Constant(INTEGER, 99L), ImmutableList.of(new Constant(INTEGER, 2L), new Reference(INTEGER, "bound_value"), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))),
                FALSE);
        assertOptimizedEquals(
                new In(new Reference(INTEGER, "bound_value"), ImmutableList.of(new Constant(INTEGER, 2L), new Reference(INTEGER, "bound_value"), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))),
                TRUE);

        assertOptimizedEquals(
                new In(new Reference(INTEGER, "unbound_value"), ImmutableList.of(new Constant(INTEGER, 1L))),
                new Comparison(EQUAL, new Reference(INTEGER, "unbound_value"), new Constant(INTEGER, 1L)));

        assertOptimizedEquals(
                new In(new Constant(INTEGER, 3L), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 5L), new Constant(INTEGER, 0L))))),
                new In(new Constant(INTEGER, 3L), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 5L), new Constant(INTEGER, 0L))))));
        assertOptimizedEquals(
                new In(new Constant(INTEGER, null), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 5L), new Constant(INTEGER, 0L))))),
                new In(new Constant(INTEGER, null), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 5L), new Constant(INTEGER, 0L))))));
        assertOptimizedEquals(
                new In(new Constant(INTEGER, 3L), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new Constant(INTEGER, null), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 5L), new Constant(INTEGER, 0L))))),
                new In(new Constant(INTEGER, 3L), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new Constant(INTEGER, null), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 5L), new Constant(INTEGER, 0L))))));
        assertOptimizedEquals(
                new In(new Constant(INTEGER, null), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, null), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 5L), new Constant(INTEGER, 0L))))),
                new In(new Constant(INTEGER, null), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, null), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 5L), new Constant(INTEGER, 0L))))));
        assertOptimizedEquals(
                new In(new Constant(INTEGER, 3L), ImmutableList.of(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 5L), new Constant(INTEGER, 0L))), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 5L), new Constant(INTEGER, 0L))))),
                new In(new Constant(INTEGER, 3L), ImmutableList.of(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 5L), new Constant(INTEGER, 0L))), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 5L), new Constant(INTEGER, 0L))))));
        assertTrinoExceptionThrownBy(() -> evaluate(new In(new Constant(INTEGER, 3L), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 5L), new Constant(INTEGER, 0L)))))))
                .hasErrorCode(DIVISION_BY_ZERO);

        assertOptimizedEquals(
                new In(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))),
                new In(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 3L), new Constant(INTEGER, 5L))));
        assertOptimizedEquals(
                new In(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L), new Constant(INTEGER, 2L), new Constant(INTEGER, 4L))),
                new In(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 4L))));
        assertOptimizedEquals(
                new In(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 2L))),
                new Comparison(EQUAL, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 2L)));
    }

    @Test
    public void testCastOptimization()
    {
        assertOptimizedEquals(
                new Cast(new Reference(INTEGER, "bound_value"), VARCHAR),
                new Constant(VARCHAR, Slices.utf8Slice("1234")));
        assertOptimizedMatches(
                new Cast(new Reference(INTEGER, "unbound_value"), INTEGER),
                new Reference(INTEGER, "unbound_value"));
    }

    @Test
    public void testTryCast()
    {
        assertOptimizedEquals(
                new Cast(new Constant(UNKNOWN, null), BIGINT, true),
                new Constant(BIGINT, null));
        assertOptimizedEquals(
                new Cast(new Constant(INTEGER, 123L), BIGINT, true),
                new Constant(BIGINT, 123L));
        assertOptimizedEquals(
                new Cast(new Constant(UNKNOWN, null), INTEGER, true),
                new Constant(INTEGER, null));
        assertOptimizedEquals(
                new Cast(new Constant(INTEGER, 123L), INTEGER, true),
                new Constant(INTEGER, 123L));
    }

    @Test
    public void testSearchCase()
    {
        assertOptimizedEquals(
                new Case(ImmutableList.of(
                        new WhenClause(TRUE, new Constant(INTEGER, 33L))),
                        new Constant(INTEGER, null)),
                new Constant(INTEGER, 33L));
        assertOptimizedEquals(
                new Case(ImmutableList.of(
                        new WhenClause(FALSE, new Constant(INTEGER, 1L))),
                        new Constant(INTEGER, 33L)),
                new Constant(INTEGER, 33L));

        assertOptimizedEquals(
                new Case(ImmutableList.of(
                        new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "bound_value"), new Constant(INTEGER, 1234L)), new Constant(INTEGER, 33L))),
                        new Constant(INTEGER, null)),
                new Constant(INTEGER, 33L));
        assertOptimizedEquals(
                new Case(ImmutableList.of(
                        new WhenClause(TRUE, new Reference(INTEGER, "bound_value"))),
                        new Constant(INTEGER, null)),
                new Constant(INTEGER, 1234L));
        assertOptimizedEquals(
                new Case(ImmutableList.of(
                        new WhenClause(FALSE, new Constant(INTEGER, 1L))),
                        new Reference(INTEGER, "bound_value")),
                new Constant(INTEGER, 1234L));

        assertOptimizedEquals(
                new Case(ImmutableList.of(
                        new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "bound_value"), new Constant(INTEGER, 1234L)), new Constant(INTEGER, 33L))),
                        new Reference(INTEGER, "unbound_value")),
                new Constant(INTEGER, 33L));

        assertOptimizedMatches(
                new Case(ImmutableList.of(
                        new WhenClause(new Comparison(EQUAL, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L))),
                        new Constant(INTEGER, null)),
                new Case(ImmutableList.of(
                        new WhenClause(new Comparison(EQUAL, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L))),
                        new Constant(INTEGER, null)));

        assertOptimizedEquals(
                new Case(ImmutableList.of(
                        new WhenClause(TRUE, new Constant(VARCHAR, Slices.utf8Slice("a"))), new WhenClause(new Comparison(EQUAL, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 0L)), new Constant(VARCHAR, Slices.utf8Slice("b")))),
                        new Constant(VARCHAR, Slices.utf8Slice("c"))),
                new Constant(VARCHAR, Slices.utf8Slice("a")));
        assertOptimizedEquals(
                new Case(ImmutableList.of(
                        new WhenClause(new Comparison(EQUAL, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 0L)), new Constant(VARCHAR, Slices.utf8Slice("a"))), new WhenClause(TRUE, new Constant(VARCHAR, Slices.utf8Slice("b")))),
                        new Constant(VARCHAR, Slices.utf8Slice("c"))),
                new Case(ImmutableList.of(
                        new WhenClause(new Comparison(EQUAL, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 0L)), new Constant(VARCHAR, Slices.utf8Slice("a")))),
                        new Constant(VARCHAR, Slices.utf8Slice("b"))));
        assertOptimizedEquals(
                new Case(ImmutableList.of(
                        new WhenClause(new Comparison(EQUAL, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 0L)), new Constant(VARCHAR, Slices.utf8Slice("a"))), new WhenClause(FALSE, new Constant(VARCHAR, Slices.utf8Slice("b")))),
                        new Constant(VARCHAR, Slices.utf8Slice("c"))),
                new Case(ImmutableList.of(
                        new WhenClause(new Comparison(EQUAL, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 0L)), new Constant(VARCHAR, Slices.utf8Slice("a")))),
                        new Constant(VARCHAR, Slices.utf8Slice("c"))));
        assertOptimizedEquals(
                new Case(ImmutableList.of(
                        new WhenClause(new Comparison(EQUAL, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 0L)), new Constant(VARCHAR, Slices.utf8Slice("a"))),
                        new WhenClause(FALSE, new Constant(VARCHAR, Slices.utf8Slice("b")))),
                        new Constant(VARCHAR, null)),
                new Case(ImmutableList.of(
                        new WhenClause(new Comparison(EQUAL, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 0L)), new Constant(VARCHAR, Slices.utf8Slice("a")))),
                        new Constant(VARCHAR, null)));
        assertOptimizedEquals(
                new Case(ImmutableList.of(
                        new WhenClause(TRUE, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                        new WhenClause(FALSE, new Constant(INTEGER, 1L))),
                        new Constant(INTEGER, null)),
                new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))));
        assertOptimizedEquals(
                new Case(ImmutableList.of(
                        new WhenClause(FALSE, new Constant(INTEGER, 1L)), new WhenClause(FALSE, new Constant(INTEGER, 2L))),
                        new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))));

        assertEvaluatedEquals(
                new Case(ImmutableList.of(
                        new WhenClause(FALSE, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))), new WhenClause(TRUE, new Constant(INTEGER, 1L))),
                        new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                new Constant(INTEGER, 1L));
        assertEvaluatedEquals(
                new Case(ImmutableList.of(
                        new WhenClause(TRUE, new Constant(INTEGER, 1L))),
                        new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                new Constant(INTEGER, 1L));
    }

    @Test
    public void testSimpleCase()
    {
        assertOptimizedEquals(
                new Switch(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 1L), new Constant(INTEGER, 33L)),
                                new WhenClause(new Constant(INTEGER, 1L), new Constant(INTEGER, 34L))),
                        new Constant(INTEGER, null)),
                new Constant(INTEGER, 33L));

        assertOptimizedEquals(
                new Switch(
                        new Constant(BOOLEAN, null),
                        ImmutableList.of(
                                new WhenClause(TRUE, new Constant(INTEGER, 33L))),
                        new Constant(INTEGER, null)),
                new Constant(INTEGER, null));
        for (Switch aSwitch : Arrays.asList(new Switch(
                        new Constant(BOOLEAN, null),
                        ImmutableList.of(
                                new WhenClause(TRUE, new Constant(INTEGER, 33L))),
                        new Constant(INTEGER, 33L)),
                new Switch(
                        new Constant(INTEGER, 33L),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, null), new Constant(INTEGER, 1L))),
                        new Constant(INTEGER, 33L)),
                new Switch(
                        new Reference(INTEGER, "bound_value"),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 1234L), new Constant(INTEGER, 33L))),
                        new Constant(INTEGER, null)),
                new Switch(
                        new Constant(INTEGER, 1234L),
                        ImmutableList.of(
                                new WhenClause(new Reference(INTEGER, "bound_value"), new Constant(INTEGER, 33L))),
                        new Constant(INTEGER, null)))) {
            assertOptimizedEquals(
                    aSwitch,
                    new Constant(INTEGER, 33L));
        }

        assertOptimizedEquals(
                new Switch(
                        TRUE,
                        ImmutableList.of(
                                new WhenClause(TRUE, new Reference(INTEGER, "bound_value"))),
                        new Constant(INTEGER, null)),
                new Constant(INTEGER, 1234L));
        assertOptimizedEquals(
                new Switch(
                        TRUE,
                        ImmutableList.of(
                                new WhenClause(FALSE, new Constant(INTEGER, 1L))),
                        new Reference(INTEGER, "bound_value")),
                new Constant(INTEGER, 1234L));

        assertOptimizedEquals(
                new Switch(
                        TRUE,
                        ImmutableList.of(
                                new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "unbound_value"), new Constant(INTEGER, 1L)), new Constant(INTEGER, 1L)),
                                new WhenClause(new Comparison(EQUAL, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 0L)), new Constant(INTEGER, 2L))),
                        new Constant(INTEGER, 33L)),
                new Switch(
                        TRUE,
                        ImmutableList.of(
                                new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "unbound_value"), new Constant(INTEGER, 1L)), new Constant(INTEGER, 1L)),
                                new WhenClause(new Comparison(EQUAL, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 0L)), new Constant(INTEGER, 2L))),
                        new Constant(INTEGER, 33L)));

        assertOptimizedMatches(
                new Switch(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 1L)),
                                new WhenClause(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 2L))),
                        new Constant(INTEGER, 1L)),
                new Switch(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 1L)),
                                new WhenClause(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 2L))),
                        new Constant(INTEGER, 1L)));

        assertOptimizedEquals(
                new Switch(
                        new Constant(INTEGER, null),
                        ImmutableList.of(
                                new WhenClause(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))))),
                        new Constant(INTEGER, 1L)),
                new Constant(INTEGER, 1L));
        assertOptimizedEquals(
                new Switch(
                        new Constant(INTEGER, null),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 1L), new Constant(INTEGER, 2L))),
                        new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))));
        assertOptimizedEquals(
                new Switch(
                        new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 1L), new Constant(INTEGER, 2L))),
                        new Constant(INTEGER, 3L)),
                new Switch(
                        new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 1L), new Constant(INTEGER, 2L))),
                        new Constant(INTEGER, 3L)));
        assertOptimizedEquals(
                new Switch(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 2L))),
                        new Constant(INTEGER, 3L)),
                new Switch(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 2L))),
                        new Constant(INTEGER, 3L)));
        assertOptimizedEquals(
                new Switch(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 2L), new Constant(INTEGER, 2L)),
                                new WhenClause(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 3L))),
                        new Constant(INTEGER, 4L)),
                new Switch(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 3L))),
                        new Constant(INTEGER, 4L)));
        assertOptimizedEquals(
                new Switch(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 2L))),
                        new Constant(INTEGER, null)),
                new Switch(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 2L))),
                        new Constant(INTEGER, null)));
        assertOptimizedEquals(
                new Switch(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 2L), new Constant(INTEGER, 2L)),
                                new WhenClause(new Constant(INTEGER, 3L), new Constant(INTEGER, 3L))),
                        new Constant(INTEGER, null)),
                new Constant(INTEGER, null));

        assertEvaluatedEquals(
                new Switch(
                        new Constant(INTEGER, null),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 1L), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))))),
                        new Constant(INTEGER, 1L)),
                new Constant(INTEGER, 1L));
        assertEvaluatedEquals(
                new Switch(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 2L), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))))),
                        new Constant(INTEGER, 3L)),
                new Constant(INTEGER, 3L));
        assertEvaluatedEquals(
                new Switch(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 1L), new Constant(INTEGER, 2L)),
                                new WhenClause(new Constant(INTEGER, 1L), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))))),
                        new Constant(INTEGER, null)),
                new Constant(INTEGER, 2L));
        assertEvaluatedEquals(
                new Switch(
                        new Constant(INTEGER, 1L),
                        ImmutableList.of(
                                new WhenClause(new Constant(INTEGER, 1L), new Constant(INTEGER, 2L))),
                        new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                new Constant(INTEGER, 2L));
    }

    @Test
    public void testCoalesce()
    {
        assertOptimizedEquals(
                new Coalesce(new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "unbound_value"), new Call(MULTIPLY_INTEGER, ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 3L))))), new Call(SUBTRACT_INTEGER, ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 1L))), new Constant(INTEGER, null)),
                new Coalesce(new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "unbound_value"), new Constant(INTEGER, 6L))), new Constant(INTEGER, 0L)));
        assertOptimizedMatches(
                new Coalesce(new Reference(INTEGER, "unbound_value"), new Reference(INTEGER, "unbound_value")),
                new Reference(INTEGER, "unbound_value"));
        assertOptimizedEquals(
                new Coalesce(new Constant(INTEGER, 6L), new Reference(INTEGER, "unbound_value")),
                new Constant(INTEGER, 6L));
        assertOptimizedMatches(
                new Coalesce(new Call(RANDOM, ImmutableList.of()), new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 5.0)),
                new Coalesce(new Call(RANDOM, ImmutableList.of()), new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 5.0)));

        assertOptimizedEquals(
                new Coalesce(new Constant(UNKNOWN, null), new Coalesce(new Constant(UNKNOWN, null), new Constant(UNKNOWN, null))),
                new Constant(UNKNOWN, null));
        assertOptimizedEquals(
                new Coalesce(new Constant(INTEGER, null), new Coalesce(new Constant(INTEGER, null), new Coalesce(new Constant(INTEGER, null), new Constant(INTEGER, null), new Constant(INTEGER, 1L)))),
                new Constant(INTEGER, 1L));
        assertOptimizedEquals(
                new Coalesce(new Constant(INTEGER, 1L), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                new Constant(INTEGER, 1L));
        assertOptimizedEquals(
                new Coalesce(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 1L)),
                new Coalesce(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 1L)));
        assertOptimizedEquals(
                new Coalesce(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 1L), new Constant(INTEGER, null)),
                new Coalesce(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 1L)));
        assertOptimizedEquals(
                new Coalesce(new Constant(INTEGER, 1L), new Coalesce(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 2L))),
                new Constant(INTEGER, 1L));
        assertOptimizedEquals(
                new Coalesce(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, null), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 0L))), new Constant(INTEGER, null), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                new Coalesce(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 0L)))));
        assertOptimizedEquals(
                new Coalesce(new Call(RANDOM, ImmutableList.of()), new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 1.0), new Call(RANDOM, ImmutableList.of())),
                new Coalesce(new Call(RANDOM, ImmutableList.of()), new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 1.0)));

        assertEvaluatedEquals(
                new Coalesce(new Constant(INTEGER, 1L), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                new Constant(INTEGER, 1L));
        assertTrinoExceptionThrownBy(() -> evaluate(new Coalesce(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 1L))))
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testIf()
    {
        assertOptimizedEquals(
                ifExpression(new Comparison(EQUAL, new Constant(INTEGER, 2L), new Constant(INTEGER, 2L)), new Constant(INTEGER, 3L), new Constant(INTEGER, 4L)),
                new Constant(INTEGER, 3L));
        assertOptimizedEquals(
                ifExpression(new Comparison(EQUAL, new Constant(INTEGER, 1L), new Constant(INTEGER, 2L)), new Constant(INTEGER, 3L), new Constant(INTEGER, 4L)),
                new Constant(INTEGER, 4L));

        assertOptimizedEquals(
                ifExpression(TRUE, new Constant(INTEGER, 3L), new Constant(INTEGER, 4L)),
                new Constant(INTEGER, 3L));
        assertOptimizedEquals(
                ifExpression(FALSE, new Constant(INTEGER, 3L), new Constant(INTEGER, 4L)),
                new Constant(INTEGER, 4L));
        assertOptimizedEquals(
                ifExpression(new Constant(BOOLEAN, null), new Constant(INTEGER, 3L), new Constant(INTEGER, 4L)),
                new Constant(INTEGER, 4L));

        assertOptimizedEquals(
                ifExpression(TRUE, new Constant(INTEGER, 3L), new Constant(INTEGER, null)),
                new Constant(INTEGER, 3L));
        assertOptimizedEquals(
                ifExpression(FALSE, new Constant(INTEGER, 3L), new Constant(INTEGER, null)),
                new Constant(INTEGER, null));
        assertOptimizedEquals(
                ifExpression(TRUE, new Constant(INTEGER, null), new Constant(INTEGER, 4L)),
                new Constant(INTEGER, null));
        assertOptimizedEquals(
                ifExpression(FALSE, new Constant(INTEGER, null), new Constant(INTEGER, 4L)),
                new Constant(INTEGER, 4L));
        assertOptimizedEquals(
                ifExpression(TRUE, new Constant(INTEGER, null), new Constant(INTEGER, null)),
                new Constant(INTEGER, null));
        assertOptimizedEquals(
                ifExpression(FALSE, new Constant(INTEGER, null), new Constant(INTEGER, null)),
                new Constant(INTEGER, null));

        assertOptimizedEquals(
                ifExpression(TRUE, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))));
        assertOptimizedEquals(
                ifExpression(TRUE, new Constant(INTEGER, 1L), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                new Constant(INTEGER, 1L));
        assertOptimizedEquals(
                ifExpression(FALSE, new Constant(INTEGER, 1L), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))));
        assertOptimizedEquals(
                ifExpression(FALSE, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 1L)),
                new Constant(INTEGER, 1L));
        assertOptimizedEquals(
                ifExpression(new Comparison(EQUAL, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L), new Constant(INTEGER, 2L)),
                ifExpression(new Comparison(EQUAL, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L), new Constant(INTEGER, 2L)));

        assertEvaluatedEquals(
                ifExpression(TRUE, new Constant(INTEGER, 1L), new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))),
                new Constant(INTEGER, 1L));
        assertEvaluatedEquals(
                ifExpression(FALSE, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 1L)),
                new Constant(INTEGER, 1L));
        assertTrinoExceptionThrownBy(() -> evaluate(ifExpression(new Comparison(EQUAL, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 0L)), new Constant(INTEGER, 1L), new Constant(INTEGER, 2L))))
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testOptimizeDivideByZero()
    {
        assertOptimizedEquals(
                new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))),
                new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))));

        assertTrinoExceptionThrownBy(() -> evaluate(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L)))))
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testRowSubscript()
    {
        assertOptimizedEquals(
                new FieldReference(new Row(ImmutableList.of(new Constant(INTEGER, 1L), new Constant(VARCHAR, Slices.utf8Slice("a")), TRUE)), 2),
                TRUE);
        assertOptimizedEquals(
                new FieldReference(
                        new FieldReference(
                                new FieldReference(
                                        new Row(ImmutableList.of(
                                                new Constant(INTEGER, 1L),
                                                new Constant(VARCHAR, Slices.utf8Slice("a")),
                                                new Row(ImmutableList.of(
                                                        new Constant(INTEGER, 2L),
                                                        new Constant(VARCHAR, Slices.utf8Slice("b")),
                                                        new Row(ImmutableList.of(new Constant(INTEGER, 3L), new Constant(VARCHAR, Slices.utf8Slice("c")))))))),
                                        2),
                                2),
                        1),
                new Constant(VARCHAR, Slices.utf8Slice("c")));

        assertOptimizedEquals(
                new FieldReference(new Row(ImmutableList.of(new Constant(INTEGER, 1L), new Constant(UNKNOWN, null))), 1),
                new Constant(UNKNOWN, null));
        assertOptimizedEquals(
                new FieldReference(new Row(ImmutableList.of(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 1L))), 0),
                new FieldReference(new Row(ImmutableList.of(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 1L))), 0));
        assertOptimizedEquals(
                new FieldReference(new Row(ImmutableList.of(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 1L))), 1),
                new FieldReference(new Row(ImmutableList.of(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 1L))), 1));

        assertTrinoExceptionThrownBy(() -> evaluate(new FieldReference(new Row(ImmutableList.of(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 1L))), 1)))
                .hasErrorCode(DIVISION_BY_ZERO);
        assertTrinoExceptionThrownBy(() -> evaluate(new FieldReference(new Row(ImmutableList.of(new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 0L), new Constant(INTEGER, 0L))), new Constant(INTEGER, 1L))), 1)))
                .hasErrorCode(DIVISION_BY_ZERO);

        assertOptimizedEquals(
                new FieldReference(new Row(ImmutableList.of(new Reference(INTEGER, "unbound_value"), new Constant(INTEGER, 5L))), 0),
                new Reference(INTEGER, "unbound_value"));
    }

    private static void assertOptimizedEquals(Expression actual, Expression expected)
    {
        assertThat(optimize(actual)).isEqualTo(optimize(expected));
    }

    private static void assertOptimizedMatches(Expression actual, Expression expected)
    {
        Expression actualOptimized = (Expression) optimize(actual);

        SymbolAliases.Builder aliases = SymbolAliases.builder();

        for (Symbol symbol : SYMBOLS) {
            aliases.put(symbol.name(), symbol.toSymbolReference());
        }

        assertExpressionEquals(actualOptimized, expected, aliases.build());
    }

    static Object optimize(Expression parsedExpression)
    {
        IrExpressionInterpreter interpreter = new IrExpressionInterpreter(parsedExpression, PLANNER_CONTEXT, TEST_SESSION);
        return interpreter.optimize(INPUTS);
    }

    private static void assertEvaluatedEquals(Expression actual, Expression expected)
    {
        assertThat(evaluate(actual)).isEqualTo(evaluate(expected));
    }

    private static Object evaluate(Expression expression)
    {
        IrExpressionInterpreter interpreter = new IrExpressionInterpreter(expression, PLANNER_CONTEXT, TEST_SESSION);

        return interpreter.evaluate();
    }
}
