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
import com.google.common.collect.ImmutableMap;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.assertions.SymbolAliases;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.SymbolReference;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.Test;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.ExpressionTestUtils.assertExpressionEquals;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.sql.planner.iterative.rule.CanonicalizeExpressionRewriter.rewrite;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;

public class TestCanonicalizeExpressionRewriter
{
    private static final TransactionManager TRANSACTION_MANAGER = createTestTransactionManager();
    private static final PlannerContext PLANNER_CONTEXT = plannerContextBuilder()
            .withTransactionManager(TRANSACTION_MANAGER)
            .build();
    private static final IrTypeAnalyzer TYPE_ANALYZER = new IrTypeAnalyzer(PLANNER_CONTEXT);
    private static final AllowAllAccessControl ACCESS_CONTROL = new AllowAllAccessControl();

    @Test
    public void testRewriteIsNotNullPredicate()
    {
        assertRewritten("x is NOT NULL", "NOT (x IS NULL)");
    }

    @Test
    public void testRewriteIfExpression()
    {
        assertRewritten("IF(x = 0, 0, 1)", "CASE WHEN x = 0 THEN 0 ELSE 1 END");
    }

    @Test
    public void testCanonicalizeArithmetic()
    {
        assertRewritten("a + 1", "a + 1");
        assertRewritten("1 + a", "a + 1");

        assertRewritten("a * 1", "a * 1");
        assertRewritten("1 * a", "a * 1");
    }

    @Test
    public void testCanonicalizeComparison()
    {
        assertRewritten("a = 1", "a = 1");
        assertRewritten("1 = a", "a = 1");

        assertRewritten("a <> 1", "a <> 1");
        assertRewritten("1 <> a", "a <> 1");

        assertRewritten("a > 1", "a > 1");
        assertRewritten("1 > a", "a < 1");

        assertRewritten("a < 1", "a < 1");
        assertRewritten("1 < a", "a > 1");

        assertRewritten("a >= 1", "a >= 1");
        assertRewritten("1 >= a", "a <= 1");

        assertRewritten("a <= 1", "a <= 1");
        assertRewritten("1 <= a", "a >= 1");

        assertRewritten("a IS DISTINCT FROM 1", "a IS DISTINCT FROM 1");
        assertRewritten("1 IS DISTINCT FROM a", "a IS DISTINCT FROM 1");
    }

    @Test
    public void testTypedLiteral()
    {
        // typed literals are encoded as Cast(Literal) in current IR

        assertRewritten("a = CAST(1 AS decimal(5,2))", "a = CAST(1 AS decimal(5,2))");
        assertRewritten("CAST(1 AS decimal(5,2)) = a", "a = CAST(1 AS decimal(5,2))");

        assertRewritten("a + CAST(1 AS decimal(5,2))", "a + CAST(1 AS decimal(5,2))");
        assertRewritten("CAST(1 AS decimal(5,2)) + a", "a + CAST(1 AS decimal(5,2))");
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
        FunctionCall date = new FunctionCall(
                PLANNER_CONTEXT.getMetadata().resolveBuiltinFunction("date", fromTypes(type)).toQualifiedName(),
                ImmutableList.of(new SymbolReference(symbolName)));
        assertRewritten(date, "CAST(" + symbolName + " as DATE)");
    }

    private static void assertRewritten(String from, String to)
    {
        assertRewritten(PlanBuilder.expression(from), to);
    }

    private static void assertRewritten(Expression from, String to)
    {
        assertExpressionEquals(
                transaction(TRANSACTION_MANAGER, PLANNER_CONTEXT.getMetadata(), ACCESS_CONTROL).execute(TEST_SESSION, transactedSession -> {
                    return rewrite(
                            from,
                            transactedSession,
                            PLANNER_CONTEXT,
                            TYPE_ANALYZER,
                                    TypeProvider.copyOf(ImmutableMap.<Symbol, Type>builder()
                                            .put(new Symbol("x"), BIGINT)
                                            .put(new Symbol("a"), BIGINT)
                                            .put(new Symbol("ts"), createTimestampType(3))
                                            .put(new Symbol("tstz"), createTimestampWithTimeZoneType(3))
                                            .put(new Symbol("v"), createVarcharType(100))
                                            .buildOrThrow()));
                }),
                PlanBuilder.expression(to),
                SymbolAliases.builder()
                        .put("x", new SymbolReference("x"))
                        .put("a", new SymbolReference("a"))
                        .put("ts", new SymbolReference("ts"))
                        .put("tstz", new SymbolReference("tstz"))
                        .put("v", new SymbolReference("v"))
                        .build());
    }
}
