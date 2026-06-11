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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.function.BoundSignature;
import io.trino.sql.PlannerContext;
import io.trino.sql.parser.SqlParser;
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
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.ExpressionAnalyzer.analyzeExpressions;
import static io.trino.sql.analyzer.QueryType.OTHERS;
import static io.trino.sql.analyzer.StatementAnalyzerFactory.createTestingStatementAnalyzerFactory;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/// With the shadow enabled, every function call analyzed below is re-resolved by the type solver
/// and the bound signatures must agree — a divergence throws out of analysis itself. The corpus
/// leans on resolution behavior the solver must reproduce exactly: type-precedence coercion,
/// calculated decimal/varchar signatures, generic instantiation, overload dominance, variadic
/// matching, and lambda-taking functions at their resolved function types.
// The static shadow toggle is process-global; concurrent classes must not observe the flip
@Isolated
@TestInstance(PER_CLASS)
class TestSolverShadow
{
    private static final List<String> CORPUS = List.of(
            "abs(-1)",
            "abs(CAST(-1.5 AS decimal(10,2)))",
            "length('abc')",
            "upper('abc')",
            "upper(CAST('abc' AS varchar))",
            "lower(upper('abc'))",
            "concat('ab', 'cde')",
            "concat('a', 'b', 'c', 'd')",
            "round(CAST(1.5 AS decimal(18,4)))",
            "round(CAST(1.5 AS decimal(18,4)), 2)",
            "mod(CAST(10 AS decimal(10,2)), CAST(3 AS tinyint))",
            "greatest(1, CAST(2 AS bigint))",
            "least(1.5, 2)",
            "sequence(1, 3)",
            "sequence(CAST(1 AS bigint), 5)",
            "element_at(sequence(1, 3), 1)",
            "array_distinct(sequence(1, 3))",
            "contains(sequence(1, 3), CAST(2 AS bigint))",
            "from_unixtime(1.5e0)",
            "transform(sequence(1, 3), x -> x + 1)",
            "transform(sequence(1, 3), x -> x > 2)",
            "filter(sequence(1, 3), x -> x > 2)",
            "any_match(sequence(1, 3), x -> x > 2)",
            "zip_with(sequence(1, 2), sequence(3, 4), (a, b) -> a + b)",
            "reduce(sequence(1, 3), CAST(0 AS bigint), (s, x) -> s + x, s -> s * 2.5e0)",
            "transform(transform(sequence(1, 3), x -> x + 1), y -> y * 1.5e0)",
            // temporal signatures with calculated precision
            "date_trunc('day', TIMESTAMP '2020-01-01 12:34:56')",
            "to_unixtime(TIMESTAMP '2020-01-01 12:34:56.123')",
            // magic-literal argument types: the string argument coerces to the compiled form
            "regexp_like('abc', 'a+')",
            "json_extract_scalar('{\"a\": 1}', '$.a')",
            "split_part('a-b-c', '-', 2)",
            "codepoint('a')",
            "strpos('haystack', 'hay')");

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
        shadowWasEnabled = SolverShadow.isEnabled();
        SolverShadow.setEnabled(true);
    }

    @AfterAll
    void restoreShadow()
    {
        // Restore rather than disable: test JVMs are reused, and a run with the shadow enabled
        // globally must stay shadowed after this class finishes
        SolverShadow.setEnabled(shadowWasEnabled);
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
    void testDivergenceDetected()
    {
        // A fabricated resolution claiming length('abc') returns varchar must be called out
        ResolvedFunction length = inTransaction(_ -> plannerContext.getMetadata()
                .resolveBuiltinFunction(getCharVarcharCoercion(TEST_SESSION), "length", TypeDescriptorProvider.fromTypes(List.of(VARCHAR))));
        ResolvedFunction lied = new ResolvedFunction(
                new BoundSignature(length.signature().getName(), VARCHAR, length.signature().getArgumentTypes()),
                length.catalogHandle(),
                length.functionId(),
                length.functionKind(),
                length.deterministic(),
                length.neverFails(),
                length.functionNullability(),
                length.typeDependencies(),
                length.functionDependencies());
        assertThatThrownBy(() -> inTransaction(session -> {
            SolverShadow.verifyResolution(session, plannerContext.getMetadata(), lied, List.of(VARCHAR));
            return null;
        }))
                .isInstanceOf(VerifyException.class)
                .hasMessageContaining("returns varchar in the engine but bigint in the solver");
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
