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
package io.trino.plugin.memory;

import com.google.inject.Scopes;
import io.trino.Session;
import io.trino.execution.FailureInjector;
import io.trino.execution.TestingFailureInjectionConfig;
import io.trino.execution.TestingFailureInjector;
import io.trino.spi.predicate.Domain;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.SystemSessionProperties.FILTERING_SEMI_JOIN_TO_INNER;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_FAILURE;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRuntimeConstraintOperatorSemantics
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        long memoryPoolSize = 2100L * 1024 * 1024;
        return MemoryQueryRunner.builder()
                .setWorkerCount(3)
                .addExtraProperty("legacy-dynamic-filtering", "false")
                .addExtraProperty("query.max-memory-per-node", "100MB")
                .addExtraProperty("memory.heap-headroom-per-node", (getRuntime().maxMemory() - memoryPoolSize) + "B")
                .addExtraProperty("fault-tolerant-execution-task-memory", "1GB")
                .addExtraProperty("retry-initial-delay", "10ms")
                .addExtraProperty("retry-max-delay", "10ms")
                .setAdditionalModule(binder -> {
                    configBinder(binder).bindConfig(TestingFailureInjectionConfig.class);
                    newOptionalBinder(binder, FailureInjector.class).setBinding().to(TestingFailureInjector.class).in(Scopes.SINGLETON);
                })
                .withExchange("filesystem")
                .build();
    }

    private Session session(boolean filtering, String retry)
    {
        return Session.builder(getSession())
                .setSystemProperty("enable_dynamic_filtering", Boolean.toString(filtering))
                .setSystemProperty("join_reordering_strategy", "NONE")
                .setSystemProperty("join_distribution_type", "PARTITIONED")
                .setSystemProperty("retry_policy", retry)
                .build();
    }

    @Test
    @Timeout(120)
    void testLegacyAndRuntimeFilteringModes()
    {
        assertUpdate("CREATE TABLE mode_probe AS SELECT * FROM (VALUES 11, 22, 33, NULL) t(k)", 4);
        assertUpdate("CREATE TABLE mode_build AS SELECT * FROM (VALUES 11, 33) t(k)", 2);
        for (String retry : List.of("NONE", "TASK")) {
            for (String distribution : List.of("PARTITIONED", "BROADCAST")) {
                for (boolean legacy : List.of(false, true)) {
                    Session session = Session.builder(session(true, retry))
                            .setSystemProperty("legacy_dynamic_filtering", Boolean.toString(legacy))
                            .setSystemProperty("join_distribution_type", distribution)
                            .setSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, "false")
                            .build();
                    for (String sql : List.of(
                            "SELECT p.k FROM mode_probe p JOIN mode_build b ON p.k = b.k",
                            "SELECT k FROM mode_probe WHERE k IN (SELECT k FROM mode_build)")) {
                        var result = getDistributedQueryRunner().executeWithPlan(session, sql);
                        assertThat(result.result().getOnlyColumnAsSet()).containsExactlyInAnyOrder(11, 33);
                        var statistics = getDistributedQueryRunner().getCoordinator().getQueryManager()
                                .getFullQueryInfo(result.queryId()).getQueryStats().getDynamicFiltersStats();
                        assertThat(statistics.getTotalDynamicFilters()).isPositive();
                        assertThat(statistics.getDynamicFiltersCompleted()).isEqualTo(statistics.getTotalDynamicFilters());
                        assertThat(statistics.getDynamicFilterDomainStats()).allSatisfy(domain ->
                                assertThat(domain.getDynamicFilterId().toString().startsWith("join_") || domain.getDynamicFilterId().toString().startsWith("semijoin_")).isEqualTo(!legacy));
                    }
                    Session disabled = Session.builder(session)
                            .setSystemProperty("enable_dynamic_filtering", "false")
                            .build();
                    var disabledResult = getDistributedQueryRunner().executeWithPlan(disabled, "SELECT p.k FROM mode_probe p JOIN mode_build b ON p.k = b.k");
                    assertThat(disabledResult.result().getOnlyColumnAsSet()).containsExactlyInAnyOrder(11, 33);
                    assertThat(getDistributedQueryRunner().getCoordinator().getQueryManager()
                            .getFullQueryInfo(disabledResult.queryId()).getQueryStats().getDynamicFiltersStats().getTotalDynamicFilters()).isZero();
                }
            }
        }
    }

    @Test
    @Timeout(60)
    void testUnionBuildWithTaskRetries()
    {
        assertUpdate("CREATE TABLE union_probe AS SELECT * FROM (VALUES 11, 22) t(k)", 2);
        assertUpdate("CREATE TABLE union_first AS SELECT 11 k", 1);
        assertUpdate("CREATE TABLE union_second AS SELECT 22 k", 1);
        String sql = "SELECT p.k FROM union_probe p JOIN (SELECT k FROM union_first UNION ALL SELECT k FROM union_second) b ON p.k = b.k";
        assertThat(getQueryRunner().execute(session(false, "TASK"), sql).getOnlyColumnAsSet()).containsExactlyInAnyOrder(11, 22);
        assertThat(getQueryRunner().execute(session(true, "TASK"), sql).getOnlyColumnAsSet()).containsExactlyInAnyOrder(11, 22);
    }

    @Test
    @Timeout(60)
    void testOuterJoinBuildWithMultipleOutputDrivers()
    {
        assertUpdate("CREATE TABLE outer_join_probe AS SELECT * FROM (VALUES 22, 23, 24) t(k)", 3);
        assertUpdate("CREATE TABLE outer_join_left AS SELECT * FROM (VALUES 21, 22, 24) t(k)", 3);
        assertUpdate("CREATE TABLE outer_join_right AS SELECT * FROM (VALUES 21, 22, 23) t(k)", 3);
        for (String joinType : List.of("RIGHT", "FULL")) {
            String sql =
                    """
                    SELECT p.k, b.k
                    FROM outer_join_probe p
                    RIGHT JOIN (
                        SELECT r.k FROM outer_join_left l %s JOIN outer_join_right r ON l.k = r.k
                    ) b ON p.k = b.k + 1
                    """.formatted(joinType);
            var expected = getQueryRunner().execute(session(false, "TASK"), sql);
            assertThat(expected.getRowCount()).isEqualTo(joinType.equals("RIGHT") ? 3 : 4);
            for (boolean adaptivePartitioning : List.of(false, true)) {
                Session session = Session.builder(session(true, "TASK"))
                        .setSystemProperty("fault_tolerant_execution_runtime_adaptive_partitioning_enabled", Boolean.toString(adaptivePartitioning))
                        .build();
                assertThat(getQueryRunner().execute(session, sql).getMaterializedRows())
                        .containsExactlyInAnyOrderElementsOf(expected.getMaterializedRows());
            }
        }
    }

    @Test
    @Timeout(60)
    void testComposedCoercionsAcrossAggregationAndExchange()
    {
        assertUpdate("CREATE TABLE coercion_probe AS SELECT CAST(k AS SMALLINT) k FROM (VALUES 10, 11, 20, 21, NULL) t(k)", 5);
        assertUpdate("CREATE TABLE coercion_build AS SELECT * FROM (VALUES DOUBLE '11.0', DOUBLE '21.0') t(k)", 2);
        String sql =
                """
                SELECT p.k
                FROM (SELECT CAST(k AS INTEGER) k FROM coercion_probe GROUP BY CAST(k AS INTEGER)) p
                JOIN coercion_build b ON CAST(p.k AS DOUBLE) = b.k
                """;
        for (String retry : List.of("NONE", "TASK")) {
            for (String distribution : List.of("PARTITIONED", "BROADCAST")) {
                Session enabled = Session.builder(session(true, retry))
                        .setSystemProperty("join_distribution_type", distribution)
                        .build();
                Session disabled = Session.builder(enabled)
                        .setSystemProperty("enable_dynamic_filtering", "false")
                        .build();
                assertThat(getQueryRunner().execute(disabled, sql).getOnlyColumnAsSet()).containsExactlyInAnyOrder(11, 21);
                assertThat(getQueryRunner().execute(enabled, sql).getOnlyColumnAsSet()).containsExactlyInAnyOrder(11, 21);
            }
        }
    }

    @Test
    @Timeout(60)
    void testComparisonDemandAcrossAggregation()
    {
        assertUpdate("CREATE TABLE aggregate_probe AS SELECT 1 v, 100 x", 1);
        assertUpdate("CREATE TABLE aggregate_build AS SELECT 2 k", 1);
        String sql = "SELECT b.k, sum(p.v), max(p.x) FROM aggregate_probe p CROSS JOIN aggregate_build b GROUP BY b.k HAVING b.k > sum(p.v)";
        var expected = getQueryRunner().execute(session(false, "NONE"), sql);
        assertThat(expected.getRowCount()).isEqualTo(1);
        assertThat(getQueryRunner().execute(session(true, "NONE"), sql).getMaterializedRows()).containsExactlyElementsOf(expected.getMaterializedRows());
    }

    @Test
    @Timeout(60)
    void testComparisonDemandAcrossWindow()
    {
        assertUpdate("CREATE TABLE window_probe AS SELECT 1 v, 100 x", 1);
        assertUpdate("CREATE TABLE window_build AS SELECT 2 k", 1);
        String sql = "SELECT k, s, m FROM (SELECT b.k, sum(p.v) OVER (PARTITION BY b.k) s, max(p.x) OVER (PARTITION BY b.k) m FROM window_probe p CROSS JOIN window_build b) WHERE k > s";
        var expected = getQueryRunner().execute(session(false, "NONE"), sql);
        assertThat(expected.getRowCount()).isEqualTo(1);
        assertThat(getQueryRunner().execute(session(true, "NONE"), sql).getMaterializedRows()).containsExactlyElementsOf(expected.getMaterializedRows());
    }

    @Test
    @Timeout(60)
    void testExplicitRoundingCastPreimage()
    {
        assertUpdate("CREATE TABLE cast_probe AS SELECT DOUBLE '1.1' x", 1);
        assertUpdate("CREATE TABLE cast_build AS SELECT BIGINT '1' k", 1);
        String sql = "SELECT p.x FROM cast_probe p JOIN cast_build b ON CAST(p.x AS BIGINT) = b.k";
        var expected = getQueryRunner().execute(session(false, "NONE"), sql);
        assertThat(expected.getRowCount()).isEqualTo(1);
        assertThat(getQueryRunner().execute(session(true, "NONE"), sql).getMaterializedRows()).containsExactlyElementsOf(expected.getMaterializedRows());
    }

    @Test
    @Timeout(60)
    void testTaskRetryValuesBuild()
    {
        assertUpdate("CREATE TABLE values_probe AS SELECT * FROM (VALUES 11, 22) t(k)", 2);
        String sql = "SELECT p.k FROM values_probe p JOIN (VALUES 11, 22) b(k) ON p.k = b.k";
        assertThat(getQueryRunner().execute(session(true, "TASK"), sql).getOnlyColumnAsSet()).containsExactlyInAnyOrder(11, 22);
    }

    @Test
    @Timeout(60)
    void testDecimalRoundingCastPreimage()
    {
        assertUpdate("CREATE TABLE decimal_probe AS SELECT DECIMAL '1.11' x", 1);
        assertUpdate("CREATE TABLE decimal_build AS SELECT DECIMAL '1.1' k", 1);
        String sql = "SELECT p.x FROM decimal_probe p JOIN decimal_build b ON CAST(p.x AS DECIMAL(2,1)) = b.k";
        var expected = getQueryRunner().execute(session(false, "NONE"), sql);
        assertThat(expected.getRowCount()).isEqualTo(1);
        assertThat(getQueryRunner().execute(session(true, "NONE"), sql).getMaterializedRows()).containsExactlyElementsOf(expected.getMaterializedRows());
    }

    @Test
    @Timeout(60)
    void testTaskRetryPublishesSelectiveDomain()
    {
        assertUpdate("CREATE TABLE selective_probe AS SELECT * FROM (VALUES 11, 22) t(k)", 2);
        assertUpdate("CREATE TABLE selective_build AS SELECT 11 k", 1);

        var result = getDistributedQueryRunner().executeWithPlan(
                session(true, "TASK"),
                "SELECT p.k FROM selective_probe p JOIN selective_build b ON p.k = b.k");

        assertThat(result.result().getOnlyColumnAsSet()).containsExactly(11);
        var statistics = getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.queryId())
                .getQueryStats()
                .getDynamicFiltersStats();
        assertThat(statistics.getTotalDynamicFilters()).isEqualTo(1);
        assertThat(statistics.getDynamicFilterDomainStats())
                .singleElement()
                .satisfies(statistic -> assertThat(statistic.getSimplifiedDomain()).isEqualTo(Domain.singleValue(INTEGER, 11L).toString(2)));
    }

    @Test
    @Timeout(60)
    void testTaskRetryAggregateBuildInstallsCollectionBeforeCompletion()
    {
        assertUpdate("CREATE TABLE aggregate_retry_probe AS SELECT 11 k", 1);
        assertUpdate("CREATE TABLE aggregate_retry_build AS SELECT 11 k", 1);
        String sql = "SELECT p.k FROM aggregate_retry_probe p JOIN (SELECT sum(k) k FROM aggregate_retry_build) b ON p.k = b.k";

        assertThat(getQueryRunner().execute(session(true, "TASK"), sql).getOnlyColumnAsSet()).containsExactly(11);
    }

    @Test
    @Timeout(60)
    void testConcurrentTaskRetryQueriesMakeProgressWithConstrainedAllocator()
            throws Exception
    {
        assertUpdate("CREATE TABLE concurrent_probe AS SELECT 11 k", 1);
        assertUpdate("CREATE TABLE concurrent_build AS SELECT 12 k", 1);
        String sql = "SELECT p.k FROM concurrent_probe p JOIN concurrent_build b ON p.k + 1 = b.k";
        int queryCount = 6;
        CountDownLatch ready = new CountDownLatch(queryCount);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = newFixedThreadPool(queryCount);
        try {
            List<CompletableFuture<MaterializedResult>> queries = IntStream.range(0, queryCount)
                    .mapToObj(_ -> CompletableFuture.supplyAsync(() -> {
                        ready.countDown();
                        awaitUninterruptibly(start);
                        return getQueryRunner().execute(session(true, "TASK"), sql);
                    }, executor))
                    .toList();
            assertThat(awaitUninterruptibly(ready, 10, SECONDS)).isTrue();
            start.countDown();

            CompletableFuture.allOf(queries.toArray(CompletableFuture[]::new)).get(30, SECONDS);
            queries.forEach(query -> assertThat(query.join().getOnlyColumnAsSet()).containsExactly(11));
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(60)
    void testTaskRetryRetriesFailedWiringInitialization()
    {
        assertUpdate("CREATE TABLE failed_wiring_probe AS SELECT 12 k", 1);
        assertUpdate("CREATE TABLE failed_wiring_build AS SELECT 11 k", 1);
        String sql = "SELECT p.k FROM failed_wiring_probe p JOIN failed_wiring_build b ON p.k = b.k + 1";
        String traceToken = UUID.randomUUID().toString();
        getQueryRunner().injectTaskFailure(traceToken, 1, 0, 0, TASK_FAILURE, Optional.of(INTERNAL_ERROR));
        Session failureSession = Session.builder(session(true, "TASK"))
                .setTraceToken(Optional.of(traceToken))
                .build();

        assertThat(getQueryRunner().execute(failureSession, sql).getOnlyColumnAsSet()).containsExactly(12);
    }

    @Test
    @Timeout(60)
    void testTaskRetryDiscoversScanAfterFailedEmptyWiringReport()
    {
        assertUpdate("CREATE TABLE failed_scan AS SELECT 11 k", 1);
        String traceToken = UUID.randomUUID().toString();
        getQueryRunner().injectTaskFailure(traceToken, 0, 0, 0, TASK_FAILURE, Optional.of(INTERNAL_ERROR));
        Session failureSession = Session.builder(session(true, "TASK"))
                .setTraceToken(Optional.of(traceToken))
                .build();

        assertThat(getQueryRunner().execute(failureSession, "SELECT k FROM failed_scan").getOnlyColumnAsSet()).containsExactly(11);
    }
}
