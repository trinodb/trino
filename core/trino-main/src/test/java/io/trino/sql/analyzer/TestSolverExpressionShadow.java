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
package io.trino.sql.analyzer;

import com.google.common.base.VerifyException;
import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.NodeRef;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.ExpressionAnalyzer.analyzeExpressions;
import static io.trino.sql.analyzer.QueryType.OTHERS;
import static io.trino.sql.analyzer.StatementAnalyzerFactory.createTestingStatementAnalyzerFactory;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/// With the expression shadow enabled, every expression analyzed below is re-typed by the
/// solver-based checker and the per-node types and coercions must agree — a divergence throws
/// out of analysis itself. The corpus leans on analyzer behavior the resolution shadow never
/// sees: special-form typing (CASE, IF, COALESCE, NULLIF, BETWEEN, IN), branch coercions to a
/// common supertype, literal typing, row construction and comparison, and boolean obligations.
// The static shadow toggle is process-global; concurrent classes must not observe the flip
@Isolated
@TestInstance(PER_CLASS)
class TestSolverExpressionShadow
{
    private static final List<String> CORPUS = List.of(
            "1 + 2",
            "1 + CAST(2 AS bigint)",
            "abs(-1) + length('abc')",
            "CASE WHEN true THEN 1 ELSE CAST(2 AS bigint) END",
            "CASE 1 WHEN CAST(1 AS bigint) THEN 'a' WHEN 2 THEN 'bc' END",
            "IF(1 < 2, 1.5, 2)",
            "COALESCE(NULL, 1, CAST(2 AS bigint))",
            "NULLIF('abc', 'de')",
            "1 BETWEEN CAST(0 AS bigint) AND 2",
            "CAST(1.5 AS decimal(10,2)) IN (1, 2, CAST(3 AS bigint))",
            "NOT (1 = 2 OR 2 < 3)",
            "ROW(1, 'a', 2.5) = ROW(CAST(2 AS bigint), 'bc', 3.5)",
            "sequence(1, 3)[1] + 1",
            "transform(sequence(1, 3), x -> x + 1)[1]",
            "greatest(CAST(NULL AS row(bigint, double)), ROW(CAST(1 AS bigint), 2.5))",
            "1 IS NULL AND 'a' IS NOT NULL",
            "ROW(1 AS a, 'two' AS b, true AS c)",
            "NOT NULL",
            "CASE WHEN NULL THEN 1 ELSE 2 END",
            "CAST(ROW(1, 2) AS row(a bigint, b bigint)) = ROW(1, 2)",
            // a named row column compared against an anonymous ROW literal, nested — the
            // connector-test shape where field names must drop at every level
            "CAST(ROW(1, ROW(2, 3)) AS row(c1 bigint, c2 row(c21 bigint, c22 bigint))) = ROW(-1, ROW(-1, -1))");

    // Forms outside the checker take the analyzer's type as given — the node's subtree goes
    // unverified while the surrounding structure still checks; either way analysis must not throw
    private static final List<String> LEAF_ORACLE = List.of(
            "ROW(1, 2)[1]",
            "ROW(1, 2)[1] + CAST(1 AS bigint)",
            "TRY(1 / 0)",
            "TRY(1 / 0) * 2",
            "CURRENT_DATE > DATE '2020-01-01'",
            "CASE WHEN TRY(1 / 0) IS NULL THEN 1.5 ELSE 2 END");

    // Instance (receiver.method(args)) and static (Type::method(args)) method calls re-typed
    // node-by-node: the receiver and arguments type as usual and the call's own type and coercions
    // must agree, the same as a free-function call — exercised here within larger expressions
    private static final List<String> METHOD_CORPUS = List.of(
            "'abc'.length() + 1",
            "'hello world'.substring(7)",
            "'abc'.reverse() = 'cba'",
            "CAST('a-b-c'.split('-') AS array(varchar)) [1]",
            "varchar::chr(65)",
            "length(varchar::from_utf8(to_utf8('hello')))");

    private final SqlParser parser = new SqlParser();
    private final TransactionManager transactionManager = createTestTransactionManager();
    private final PlannerContext plannerContext = plannerContextBuilder()
            .withTransactionManager(transactionManager)
            .build();
    private final StatementAnalyzerFactory statementAnalyzerFactory = createTestingStatementAnalyzerFactory(
            plannerContext,
            new AllowAllAccessControl(),
            new TablePropertyManager(CatalogServiceProvider.fail()),
            new AnalyzePropertyManager(CatalogServiceProvider.fail()));

    private boolean shadowWasEnabled;

    @BeforeAll
    void enableShadow()
    {
        shadowWasEnabled = SolverExpressionShadow.isEnabled();
        SolverExpressionShadow.setEnabled(true);
    }

    @AfterAll
    void restoreShadow()
    {
        // Restore rather than disable: test JVMs are reused, and a run with the shadow enabled
        // globally must stay shadowed after this class finishes
        SolverExpressionShadow.setEnabled(shadowWasEnabled);
    }

    @Test
    void testAnalysisAgreesUnderShadow()
    {
        for (String sql : CORPUS) {
            assertThatCode(() -> analyze(sql))
                    .as(sql)
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void testMethodCallsAgreeUnderShadow()
    {
        for (String sql : METHOD_CORPUS) {
            assertThatCode(() -> analyze(sql))
                    .as(sql)
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void testUnsupportedFormsVerifyAtTheAnalyzersTypes()
    {
        for (String sql : LEAF_ORACLE) {
            assertThatCode(() -> analyze(sql))
                    .as(sql)
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void testColumnReferenceLeavesVerifyAgainstSuppliedTypes()
    {
        // "x + 1" with x typed by the (fabricated) analysis: the checker takes x as bigint and
        // must agree the sum is bigint; a lying root type is still called out
        ArithmeticBinaryExpression expression = (ArithmeticBinaryExpression) parser.createExpression("x + 1");
        Map<NodeRef<Expression>, Type> consistent = Map.of(
                NodeRef.of(expression), BIGINT,
                NodeRef.of(expression.getLeft()), BIGINT,
                NodeRef.of(expression.getRight()), INTEGER);
        assertThatCode(() -> inTransaction(session -> {
            SolverExpressionShadow.verifyAnalysis(session, plannerContext, expression, Map.of(), consistent, Map.of(NodeRef.of(expression.getRight()), BIGINT));
            return null;
        })).doesNotThrowAnyException();

        Map<NodeRef<Expression>, Type> lied = Map.of(
                NodeRef.of(expression), INTEGER,
                NodeRef.of(expression.getLeft()), BIGINT,
                NodeRef.of(expression.getRight()), INTEGER);
        assertThatThrownBy(() -> inTransaction(session -> {
            SolverExpressionShadow.verifyAnalysis(session, plannerContext, expression, Map.of(), lied, Map.of(NodeRef.of(expression.getRight()), BIGINT));
            return null;
        }))
                .isInstanceOf(VerifyException.class)
                .hasMessageContaining("is integer in the engine but bigint in the solver");
    }

    @Test
    void testDivergenceDetected()
    {
        // A fabricated analysis claiming length('abc') is varchar must be called out
        FunctionCall expression = (FunctionCall) parser.createExpression("length('abc')");
        Map<NodeRef<Expression>, Type> liedTypes = Map.of(
                NodeRef.of(expression), VARCHAR,
                NodeRef.of(expression.getArguments().getFirst().getValue()), createVarcharType(3));
        assertThatThrownBy(() -> inTransaction(session -> {
            SolverExpressionShadow.verifyAnalysis(session, plannerContext, expression, Map.of(), liedTypes, Map.of());
            return null;
        }))
                .isInstanceOf(VerifyException.class)
                .hasMessageContaining("is varchar in the engine but bigint in the solver");
    }

    private void analyze(String sql)
    {
        inTransaction(session -> analyzeExpressions(
                session,
                plannerContext,
                statementAnalyzerFactory,
                new AllowAllAccessControl(),
                List.of(parser.createExpression(sql)),
                Map.of(),
                WarningCollector.NOOP,
                OTHERS));
    }

    private <T> T inTransaction(Function<Session, T> callback)
    {
        return transaction(transactionManager, plannerContext.getMetadata(), new AllowAllAccessControl())
                .singleStatement()
                .execute(TEST_SESSION, callback);
    }
}
