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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.assertions.SymbolAliases;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.Test;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.ExpressionTestUtils.assertExpressionEquals;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.NOT_EQUAL;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.sql.planner.iterative.rule.CanonicalizeExpressionRewriter.rewrite;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;

public class TestCanonicalizeExpressionRewriter
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction MULTIPLY_INTEGER = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(INTEGER, INTEGER));

    private static final TransactionManager TRANSACTION_MANAGER = createTestTransactionManager();
    private static final PlannerContext PLANNER_CONTEXT = plannerContextBuilder()
            .withTransactionManager(TRANSACTION_MANAGER)
            .build();
    private static final AllowAllAccessControl ACCESS_CONTROL = new AllowAllAccessControl();

    @Test
    public void testRewriteIsNotNullPredicate()
    {
        assertRewritten(
                new Not(new IsNull(new Reference(BIGINT, "x"))),
                new Not(new IsNull(new Reference(BIGINT, "x"))));
    }

    @Test
    public void testRewriteIfExpression()
    {
        assertRewritten(
                ifExpression(new Comparison(EQUAL, new Reference(INTEGER, "x"), new Constant(INTEGER, 0L)), new Constant(INTEGER, 0L), new Constant(INTEGER, 1L)),
                new Case(ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "x"), new Constant(INTEGER, 0L)), new Constant(INTEGER, 0L))), new Constant(INTEGER, 1L)));
    }

    @Test
    public void testCanonicalizeArithmetic()
    {
        assertRewritten(
                new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 1L))),
                new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 1L))));

        assertRewritten(
                new Call(ADD_INTEGER, ImmutableList.of(new Constant(INTEGER, 1L), new Reference(INTEGER, "a"))),
                new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 1L))));

        assertRewritten(
                new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 1L))),
                new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 1L))));

        assertRewritten(
                new Call(MULTIPLY_INTEGER, ImmutableList.of(new Constant(INTEGER, 1L), new Reference(INTEGER, "a"))),
                new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 1L))));
    }

    @Test
    public void testCanonicalizeComparison()
    {
        assertRewritten(
                new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)),
                new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)));

        assertRewritten(
                new Comparison(EQUAL, new Constant(INTEGER, 1L), new Reference(INTEGER, "a")),
                new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)));

        assertRewritten(
                new Comparison(NOT_EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)),
                new Comparison(NOT_EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)));

        assertRewritten(
                new Comparison(NOT_EQUAL, new Constant(INTEGER, 1L), new Reference(INTEGER, "a")),
                new Comparison(NOT_EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)));

        assertRewritten(
                new Comparison(GREATER_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)),
                new Comparison(GREATER_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)));

        assertRewritten(
                new Comparison(GREATER_THAN, new Constant(INTEGER, 1L), new Reference(INTEGER, "a")),
                new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)));

        assertRewritten(
                new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)),
                new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)));

        assertRewritten(
                new Comparison(LESS_THAN, new Constant(INTEGER, 1L), new Reference(INTEGER, "a")),
                new Comparison(GREATER_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)));

        assertRewritten(
                new Comparison(GREATER_THAN_OR_EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)),
                new Comparison(GREATER_THAN_OR_EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)));

        assertRewritten(
                new Comparison(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 1L), new Reference(INTEGER, "a")),
                new Comparison(LESS_THAN_OR_EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)));

        assertRewritten(
                new Comparison(LESS_THAN_OR_EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)),
                new Comparison(LESS_THAN_OR_EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)));

        assertRewritten(
                new Comparison(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 1L), new Reference(INTEGER, "a")),
                new Comparison(GREATER_THAN_OR_EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)));

        assertRewritten(
                new Comparison(IS_DISTINCT_FROM, new Constant(INTEGER, 1L), new Reference(INTEGER, "a")),
                new Comparison(IS_DISTINCT_FROM, new Constant(INTEGER, 1L), new Reference(INTEGER, "a")));

        assertRewritten(
                new Comparison(IS_DISTINCT_FROM, new Constant(INTEGER, 1L), new Reference(INTEGER, "a")),
                new Comparison(IS_DISTINCT_FROM, new Constant(INTEGER, 1L), new Reference(INTEGER, "a")));
    }

    @Test
    public void testCanonicalizeRewriteDateFunctionToCast()
    {
        assertCanonicalizedDate(createTimestampType(3), "ts");
        assertCanonicalizedDate(createTimestampWithTimeZoneType(3), "tstz");
        assertCanonicalizedDate(createVarcharType(100), "v");
    }

    private static void assertCanonicalizedDate(Type type, String symbolName)
    {
        Call date = new Call(
                PLANNER_CONTEXT.getMetadata().resolveBuiltinFunction("date", fromTypes(type)),
                ImmutableList.of(new Reference(type, symbolName)));
        assertRewritten(date, new Cast(new Reference(VARCHAR, symbolName), DATE));
    }

    private static void assertRewritten(Expression from, Expression to)
    {
        assertExpressionEquals(
                transaction(TRANSACTION_MANAGER, PLANNER_CONTEXT.getMetadata(), ACCESS_CONTROL).execute(TEST_SESSION, transactedSession -> {
                    return rewrite(from, PLANNER_CONTEXT);
                }),
                to,
                SymbolAliases.builder()
                        .put("x", new Reference(BIGINT, "x"))
                        .put("a", new Reference(BIGINT, "a"))
                        .put("ts", new Reference(createTimestampType(3), "ts"))
                        .put("tstz", new Reference(createTimestampWithTimeZoneType(3), "tstz"))
                        .put("v", new Reference(createVarcharType(100), "v"))
                        .build());
    }
}
