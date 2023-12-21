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
package io.trino.tests;

import com.google.common.collect.ImmutableMap;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.execution.warnings.WarningCollector;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tracing.TracingMetadata;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestGetTableStatisticsOperations
        extends AbstractTestQueryFramework
{
    @RegisterExtension
    static final OpenTelemetryExtension TELEMETRY = OpenTelemetryExtension.create();

    private LocalQueryRunner localQueryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        localQueryRunner = LocalQueryRunner.builder(testSessionBuilder().build())
                .withMetadataDecorator(metadata -> new TracingMetadata(TELEMETRY.getOpenTelemetry().getTracer("test"), metadata))
                .build();
        localQueryRunner.installPlugin(new TpchPlugin());
        localQueryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());
        return localQueryRunner;
    }

    @AfterAll
    public void tearDown()
    {
        localQueryRunner.close();
        localQueryRunner = null;
    }

    @Test
    public void testTwoWayJoin()
    {
        planDistributedQuery("SELECT * " +
                "FROM tpch.tiny.orders o, tpch.tiny.lineitem l " +
                "WHERE o.orderkey = l.orderkey");
        assertThat(getTableStatisticsMethodInvocations()).isEqualTo(2);
    }

    @Test
    public void testThreeWayJoin()
    {
        planDistributedQuery("SELECT * " +
                "FROM tpch.tiny.customer c, tpch.tiny.orders o, tpch.tiny.lineitem l " +
                "WHERE o.orderkey = l.orderkey AND c.custkey = o.custkey");
        assertThat(getTableStatisticsMethodInvocations()).isEqualTo(3);
    }

    private void planDistributedQuery(@Language("SQL") String sql)
    {
        transaction(localQueryRunner.getTransactionManager(), localQueryRunner.getMetadata(), localQueryRunner.getAccessControl())
                .execute(localQueryRunner.getDefaultSession(), transactionSession -> {
                    localQueryRunner.createPlan(transactionSession, sql, localQueryRunner.getPlanOptimizers(false), OPTIMIZED_AND_VALIDATED, WarningCollector.NOOP, createPlanOptimizersStatsCollector());
                });
    }

    private static long getTableStatisticsMethodInvocations()
    {
        return TELEMETRY.getSpans().stream()
                .map(SpanData::getName)
                .filter(name -> name.equals("Metadata.getTableStatistics"))
                .count();
    }
}
