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

import com.google.common.collect.ImmutableMap;
import io.trino.FeaturesConfig;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.assertions.SymbolAliases;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.tree.SymbolReference;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.Test;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.ExpressionTestUtils.assertExpressionEquals;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.sql.planner.iterative.rule.CanonicalizeExpressionRewriter.rewrite;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.trino.transaction.TransactionBuilder.transaction;

public class TestCanonicalizeExpressionRewriter
{
    private static final TransactionManager TRANSACTION_MANAGER = createTestTransactionManager();
    private static final PlannerContext PLANNER_CONTEXT = plannerContextBuilder().withMetadata(createTestMetadataManager(TRANSACTION_MANAGER, new FeaturesConfig())).build();
    private static final TypeAnalyzer TYPE_ANALYZER = createTestingTypeAnalyzer(PLANNER_CONTEXT);
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
    public void testRewriteYearExtract()
    {
        assertRewritten("EXTRACT(YEAR FROM DATE '2017-07-20')", "year(DATE '2017-07-20')");
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
        assertRewritten("date(ts)", "CAST(ts as DATE)");
        assertRewritten("date(tstz)", "CAST(tstz as DATE)");
        assertRewritten("date(v)", "CAST(v as DATE)");
    }

    private static void assertRewritten(String from, String to)
    {
        assertExpressionEquals(
                transaction(TRANSACTION_MANAGER, ACCESS_CONTROL).execute(TEST_SESSION, transactedSession -> {
                    return rewrite(
                            PlanBuilder.expression(from),
                            transactedSession,
                            PLANNER_CONTEXT.getMetadata(),
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
