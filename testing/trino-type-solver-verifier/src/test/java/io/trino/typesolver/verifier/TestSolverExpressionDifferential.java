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
package io.trino.typesolver.verifier;

import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.lib.SignatureBridge;
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.ExpressionAnalysis;
import io.trino.sql.analyzer.StatementAnalyzerFactory;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.sql.analyzer.ExpressionAnalyzer.analyzeExpressions;
import static io.trino.sql.analyzer.QueryType.OTHERS;
import static io.trino.sql.analyzer.StatementAnalyzerFactory.createTestingStatementAnalyzerFactory;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.trino.typesolver.verifier.SolverExpressionTypeChecker.TypeCheckFailure.Kind.BOOLEAN_REQUIRED;
import static io.trino.typesolver.verifier.SolverExpressionTypeChecker.TypeCheckFailure.Kind.NO_COMMON_TYPE;
import static io.trino.typesolver.verifier.SolverExpressionTypeChecker.TypeCheckFailure.Kind.NO_MATCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Differential validation of [SolverExpressionTypeChecker]: every corpus expression is analyzed
/// by the solver and by Trino's real `ExpressionAnalyzer`, and the two must agree on the full
/// analyzer product — the type of EVERY visited node and the coercions recorded on it, not just
/// the root type. The analyzer is the oracle, so a divergence is either a bridge gap or a checker
/// bug.
///
/// The checker types against the live catalog ([CatalogLibrary]): every function and operator
/// definition is the real registered signature, bridged through [SignatureBridge] — including the
/// calculated decimal/varchar arithmetic the hand-written preset only approximates.
class TestSolverExpressionDifferential
{
    private static final List<String> CORPUS = List.of(
            // literals and arithmetic, with type-precedence coercion
            "1 + 2",
            "1 + CAST(2 AS bigint)",
            "CAST(1 AS smallint) + CAST(2 AS tinyint)",
            "1.5e0 + 2",
            "1 > 2",
            "1 = 2",
            "1 <> 2",
            "CAST(1 AS bigint) > 2",
            // calculated decimal arithmetic from the catalog's real operator signatures
            "1.5 + 2.25",
            "1.5 * 2.25",
            "CAST(1.5 AS decimal(10,2)) + CAST(2 AS decimal(5,0))",
            "CAST(1.5 AS decimal(10,2)) / CAST(2 AS decimal(5,0))",
            "CAST(1.5 AS decimal(20,1)) * CAST(2 AS decimal(20,1))",
            "1.5 + 2",
            "1.5 < 2",
            // calculated functions
            "round(CAST(1.5 AS decimal(18,4)))",
            "round(CAST(1.5 AS decimal(18,4)), 2)",
            "abs(CAST(-1.5 AS decimal(10,2)))",
            "mod(CAST(10 AS decimal(10,2)), CAST(3 AS tinyint))",
            "concat('ab', 'cde')",
            // string functions, including length propagation through parametric varchar
            "length('abc')",
            "upper('abc')",
            "upper(CAST('abc' AS varchar))",
            "lower(upper('abc'))",
            // generic functions over concrete instantiations
            "greatest(1, CAST(2 AS bigint))",
            "element_at(sequence(1, 3), 1)",
            // array construction
            "sequence(1, 3)",
            "sequence(CAST(1 AS bigint), 5)",
            // single-parameter lambdas
            "transform(sequence(1, 3), x -> x + 1)",
            "transform(sequence(1, 3), x -> x > 2)",
            "transform(sequence(1, 3), x -> x * 1.5e0)",
            "filter(sequence(1, 3), x -> x > 2)",
            "any_match(sequence(1, 3), x -> x > 2)",
            "all_match(sequence(1, 3), x -> x > 2)",
            // multi-parameter lambdas
            "zip_with(sequence(1, 2), sequence(3, 4), (a, b) -> a + b)",
            "zip_with(sequence(1, 2), sequence(3, 4), (a, b) -> a > b)",
            // multiple lambdas in one call
            "reduce(sequence(1, 3), CAST(0 AS bigint), (s, x) -> s + x, s -> s * 2.5e0)",
            "reduce(sequence(1, 3), CAST(0 AS bigint), (s, x) -> s + x, s -> s > 5)",
            // nested lambda-taking calls
            "transform(transform(sequence(1, 3), x -> x + 1), y -> y * 1.5e0)",
            "filter(transform(sequence(1, 3), x -> x + 1), y -> y > 2)",
            "any_match(filter(sequence(1, 3), x -> x > 1), y -> y > 2)",
            // analyzer-special-cased syntax: NULL, logical operators, predicates
            "NOT (1 > 2)",
            "1 > 2 AND 2 > 1",
            "1 > 2 OR 2 > 1",
            "1 IS NULL",
            "1 IS NOT NULL",
            "1 BETWEEN 0 AND CAST(5 AS bigint)",
            "1 IN (1, CAST(2 AS bigint))",
            "'ab' IN ('abc', 'x')",
            // branching constructs: every branch flows into one type variable, so the common
            // supertype falls out of the same coercion search a generic signature uses
            "coalesce(NULL, 1)",
            "coalesce(1, CAST(2 AS bigint), CAST(3 AS smallint))",
            "coalesce(1.5, 2)",
            "if(1 > 2, 1, CAST(0 AS bigint))",
            "if(1 > 2, 'ab', 'cde')",
            "nullif(1, CAST(2 AS bigint))",
            "CASE WHEN 1 > 2 THEN 'ab' ELSE 'cde' END",
            "CASE WHEN 1 > 2 THEN 1 WHEN 2 > 1 THEN CAST(2 AS bigint) ELSE 0 END",
            "CASE 1 WHEN CAST(2 AS bigint) THEN 'x' ELSE 'yz' END",
            // subscript through the bridged operator, rows, and || (desugared to concat)
            "sequence(1, 3)[1]",
            "ROW(1, 'ab')",
            "'ab' || 'cde'",
            // lambdas whose bodies use the special-cased syntax
            "transform(sequence(1, 3), x -> if(x > 2, CAST(x AS bigint), 0))",
            "filter(sequence(1, 3), x -> x IS NOT NULL AND x > 1)");

    /// Invalid expressions: both sides must REJECT, and the rejection categories must correspond.
    /// The expected pair is explicit per case — the checker's taxonomy is structural (what failed
    /// in the solve) while the analyzer's is syntactic (which visitor raised), so the mapping is
    /// many-to-many and a lookup table would obscure rather than verify it.
    private static final List<Rejection> REJECTIONS = List.of(
            // unknown function / no matching overload
            new Rejection("no_such_function(1)", FUNCTION_NOT_FOUND, NO_MATCH),
            new Rejection("length(1)", FUNCTION_NOT_FOUND, NO_MATCH),
            new Rejection("abs('ab')", FUNCTION_NOT_FOUND, NO_MATCH),
            // operator mismatches surface as TYPE_MISMATCH in the analyzer
            new Rejection("1 + 'ab'", TYPE_MISMATCH, NO_MATCH),
            new Rejection("sequence(1, 3)[true]", TYPE_MISMATCH, NO_MATCH),
            // lambda obligations: a body that fails to type, and a body of the wrong type
            new Rejection("transform(sequence(1, 3), x -> x + 'a')", TYPE_MISMATCH, NO_MATCH),
            new Rejection("filter(sequence(1, 3), x -> x + 1)", FUNCTION_NOT_FOUND, NO_MATCH),
            // boolean obligations
            new Rejection("1 AND true", TYPE_MISMATCH, BOOLEAN_REQUIRED),
            new Rejection("if(1, 2, 3)", TYPE_MISMATCH, BOOLEAN_REQUIRED),
            new Rejection("CASE WHEN 1 THEN 2 END", TYPE_MISMATCH, BOOLEAN_REQUIRED),
            // branches with no common supertype
            new Rejection("coalesce(1, 'ab')", TYPE_MISMATCH, NO_COMMON_TYPE),
            new Rejection("CASE WHEN true THEN 1 ELSE 'x' END", TYPE_MISMATCH, NO_COMMON_TYPE),
            new Rejection("nullif(1, sequence(1, 2))", TYPE_MISMATCH, NO_COMMON_TYPE),
            new Rejection("1 BETWEEN 'a' AND 2", TYPE_MISMATCH, NO_COMMON_TYPE),
            new Rejection("'ab' IN (1, 2)", TYPE_MISMATCH, NO_COMMON_TYPE));

    private record Rejection(String sql, ErrorCodeSupplier analyzerError, SolverExpressionTypeChecker.TypeCheckFailure.Kind checkerKind) {}

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
    private final SolverExpressionTypeChecker checker = new SolverExpressionTypeChecker(
            CatalogLibrary.fromCatalog(inTransaction(session -> plannerContext.getMetadata().listGlobalFunctions(session))),
            plannerContext.getTypeManager());

    private <T> T inTransaction(Function<Session, T> callback)
    {
        return transaction(transactionManager, plannerContext.getMetadata(), new AllowAllAccessControl())
                .singleStatement()
                .execute(TEST_SESSION, callback::apply);
    }

    @Test
    void testSolverAgreesWithAnalyzer()
    {
        for (String sql : CORPUS) {
            Expression expression = parser.createExpression(sql);

            ExpressionAnalysis analyzer = inTransaction(session -> analyzeExpressions(
                    session,
                    plannerContext,
                    statementAnalyzerFactory,
                    new AllowAllAccessControl(),
                    List.of(expression),
                    Map.of(),
                    WarningCollector.NOOP,
                    OTHERS));
            SolverExpressionTypeChecker.Analysis solver = checker.analyze(expression);

            assertThat(solver.type())
                    .as("%s :: root type", sql)
                    .isEqualTo(analyzer.getExpressionTypes().get(NodeRef.of(expression)));

            // Every node the checker visited must carry the analyzer's type and exactly the
            // analyzer's coercion decision (or absence of one) — a missing or spurious coercion
            // on a shared node is a planner-visible divergence
            solver.types().forEach((node, type) -> {
                assertThat(type)
                        .as("%s :: type of [%s]", sql, node.getNode())
                        .isEqualTo(analyzer.getExpressionTypes().get(node));
                assertThat(solver.coercions().get(node))
                        .as("%s :: coercion of [%s]", sql, node.getNode())
                        .isEqualTo(analyzer.getCoercion(node.getNode()));
            });
        }
    }

    @Test
    void testRejectionsMatchAnalyzer()
    {
        for (Rejection rejection : REJECTIONS) {
            Expression expression = parser.createExpression(rejection.sql());

            assertThatThrownBy(() -> inTransaction(session -> analyzeExpressions(
                    session,
                    plannerContext,
                    statementAnalyzerFactory,
                    new AllowAllAccessControl(),
                    List.of(expression),
                    Map.of(),
                    WarningCollector.NOOP,
                    OTHERS)))
                    .as("%s :: analyzer rejection", rejection.sql())
                    .isInstanceOfSatisfying(TrinoException.class, e ->
                            assertThat(e.getErrorCode()).isEqualTo(rejection.analyzerError().toErrorCode()));

            assertThatThrownBy(() -> checker.analyze(expression))
                    .as("%s :: checker rejection", rejection.sql())
                    .isInstanceOfSatisfying(SolverExpressionTypeChecker.TypeCheckFailure.class, e ->
                            assertThat(e.kind()).isEqualTo(rejection.checkerKind()));
        }
    }
}
