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
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.type.TypeManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.SolverExpressionShadow;
import io.trino.sql.analyzer.SolverExpressionTypeChecker;
import io.trino.sql.analyzer.SolverShadow;
import io.trino.sql.analyzer.StatementAnalyzerFactory;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Expression;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.weakref.solver.TypeLibrary;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.sql.analyzer.ExpressionAnalyzer.analyzeExpressions;
import static io.trino.sql.analyzer.QueryType.OTHERS;
import static io.trino.sql.analyzer.StatementAnalyzerFactory.createTestingStatementAnalyzerFactory;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;

/**
 * Per-expression cost of whole-expression type analysis: Trino's {@link io.trino.sql.analyzer.ExpressionAnalyzer}
 * versus the solver-based {@link SolverExpressionTypeChecker}, over the same self-contained scalar
 * expressions. Both produce the same per-node type-and-coercion assignment; reported as average ns
 * per batch of {@link #EXPRESSIONS} analyses (divide by {@code EXPRESSIONS.size()} for per-expression).
 * <p>
 * The corpus is column-free, so the solver does the full inference — no analyzer leaf types are
 * borrowed. The engine arm runs inside one held-open transaction with its function-resolution caches
 * warm, which is the only state the engine analyzer has: those caches are non-evictable by design,
 * so there is no cold engine-analysis configuration to compare against. The cold comparison lives at
 * the resolution layer ({@link BenchmarkFunctionResolution}, where the engine exposes
 * {@code resolveBuiltinFunctionUncached}); here:
 * <ul>
 *   <li>{@link #analyzer} — the engine's production analysis path: a full tree walk every call,
 *       with its function-resolution caches warm. The engine never caches whole-expression
 *       analyses, so this is its true per-expression cost.</li>
 *   <li>{@link #solverCold} — the solver re-deriving every constraint per call (a cache miss). The
 *       solver, unlike the engine, genuinely pays this when a call shape is first seen.</li>
 *   <li>{@link #solverWarm} — the solver memoizing call outcomes and lambda parameter assignments
 *       across calls (the same things the engine's resolver caches), then re-walking the tree and
 *       re-typing every node — the faithful warm-versus-warm counterpart to {@link #analyzer},
 *       both re-deriving per-node types over a warm resolution cache.</li>
 *   <li>{@link #solverWholeAnalysisCache} — the floor if the integration memoized whole-expression
 *       analyses by expression. Neither system does this in the hot path (it reduces to a map
 *       lookup), so it is not comparable to {@link #analyzer}; it bounds the caching headroom. The
 *       faithful warm-vs-warm comparison is at the resolution layer, where the engine exposes an
 *       uncached path — see {@link BenchmarkFunctionResolution}.</li>
 * </ul>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class BenchmarkExpressionAnalysis
{
    // A spread from trivial to hard: literal arithmetic, type-precedence coercion, calculated
    // decimal/varchar signatures, generic instantiation, special forms with branch coercion,
    // lambda-taking functions, and row construction/comparison.
    private static final List<String> EXPRESSIONS = List.of(
            "1 + 2",
            "abs(-1) + length('abc')",
            "lower(upper('abc'))",
            "CASE WHEN 1 < 2 THEN CAST(1 AS bigint) ELSE 2 END",
            "COALESCE(NULL, 1, CAST(2 AS bigint))",
            "round(CAST(1.5 AS decimal(18,4)), 2)",
            "greatest(1, CAST(2 AS bigint))",
            "regexp_replace('aaa', 'a+', 'b')",
            "transform(sequence(1, 3), x -> x + 1)",
            "filter(sequence(1, 3), x -> x > 2)",
            "reduce(sequence(1, 3), CAST(0 AS bigint), (s, x) -> s + x, s -> s * 2.5e0)",
            "ROW(1, 'a', 2.5) = ROW(CAST(2 AS bigint), 'bc', 3.5)");

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

    private List<Expression> expressions;
    private SolverExpressionTypeChecker checker;
    private SolverExpressionTypeChecker warmChecker;
    private Map<Expression, Object> solverCache;

    private TransactionId transactionId;
    private Session session;

    @Setup
    public void setup()
    {
        // This branch's test JVMs run with both solver shadows enabled by default (the root pom
        // passes the flags to surefire); turn them off so the engine arm measures pure engine
        // analysis, not analysis plus a shadow re-resolution of every call
        SolverShadow.setEnabled(false);
        SolverExpressionShadow.setEnabled(false);

        expressions = EXPRESSIONS.stream()
                .map(sql -> (Expression) parser.createExpression(sql))
                .toList();

        // Hold one transaction open for the whole run so the engine arm pays transaction setup
        // once (as a query does), not per analysis; its function-resolution caches warm up across
        // the JMH warmup iterations and stay warm.
        transactionId = transactionManager.beginTransaction(false);
        session = TEST_SESSION.beginTransactionId(transactionId, transactionManager, new AllowAllAccessControl());

        TypeLibrary library = CatalogLibrary.fromCatalog(plannerContext.getMetadata().listGlobalFunctions(session));
        TypeManager typeManager = plannerContext.getTypeManager();
        checker = new SolverExpressionTypeChecker(library.typeSystem(), library.resolver(), library::functions, typeManager);
        // The warm checker memoizes call outcomes and lambda parameter assignments across calls,
        // the way the engine's resolver caches bound functions; its caches warm up over the JMH
        // warmup iterations and stay warm, exactly as the engine arm's do
        warmChecker = new SolverExpressionTypeChecker(
                library.typeSystem(),
                library.resolver(),
                library::functions,
                typeManager,
                _ -> Optional.empty(),
                true);
        solverCache = new HashMap<>();
    }

    @TearDown
    public void tearDown()
    {
        transactionManager.asyncAbort(transactionId);
    }

    @Benchmark
    public void analyzer(Blackhole blackhole)
    {
        for (Expression expression : expressions) {
            blackhole.consume(analyzeExpressions(
                    session,
                    plannerContext,
                    statementAnalyzerFactory,
                    new AllowAllAccessControl(),
                    List.of(expression),
                    Map.of(),
                    WarningCollector.NOOP,
                    OTHERS));
        }
    }

    @Benchmark
    public void solverCold(Blackhole blackhole)
    {
        for (Expression expression : expressions) {
            blackhole.consume(checker.analyze(expression));
        }
    }

    @Benchmark
    public void solverWarm(Blackhole blackhole)
    {
        for (Expression expression : expressions) {
            blackhole.consume(warmChecker.analyze(expression));
        }
    }

    @Benchmark
    public void solverWholeAnalysisCache(Blackhole blackhole)
    {
        for (Expression expression : expressions) {
            blackhole.consume(solverCache.computeIfAbsent(expression, checker::analyze));
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        new Runner(new OptionsBuilder()
                .include(BenchmarkExpressionAnalysis.class.getSimpleName())
                .forks(Integer.getInteger("benchmark.forks", 1))
                .build())
                .run();
    }

    /**
     * In-build entry point: {@code mvn test -Dtest=BenchmarkExpressionAnalysis -DrunBenchmarks=true}.
     * Uses {@code forks(0)} so JMH runs inside the surefire JVM (which has the incubator Vector module
     * and add-opens a bare {@code exec:java} JVM lacks); trades fork isolation for being runnable from
     * the build. Standalone rigorous runs go through {@link #main} with the default fork.
     */
    @Test
    @EnabledIfSystemProperty(named = "runBenchmarks", matches = "true")
    void runBenchmark()
            throws Exception
    {
        new Runner(new OptionsBuilder()
                .include(BenchmarkExpressionAnalysis.class.getSimpleName())
                .forks(0)
                .warmupIterations(3)
                .measurementIterations(5)
                .build())
                .run();
    }
}
