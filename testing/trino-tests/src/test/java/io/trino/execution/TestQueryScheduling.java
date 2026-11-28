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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_FAILURE;
import static io.trino.execution.TestingBlockedSplitSource.BLOCKED_QUERY;
import static io.trino.execution.TestingBlockedSplitSource.installBlockedSplitSource;
import static io.trino.spi.ErrorType.EXTERNAL;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryScheduling
{
    @ParameterizedTest
    @ValueSource(strings = {"NONE", "QUERY", "TASK"})
    public void testBlockedQueryReleasesOnlyWorker(String retryPolicy, @TempDir Path exchangeDirectory)
            throws Exception
    {
        CompletableFuture<Void> requested = new CompletableFuture<>();
        CompletableFuture<List<ConnectorSplit>> batch = new CompletableFuture<>();
        try (ExecutorService clients = newFixedThreadPool(2);
                DistributedQueryRunner runner = TpchQueryRunner.builder()
                        .setWorkerCount(1)
                        .addCoordinatorProperty("query.executor-pool-size", "1")
                        .addExtraProperty("retry-policy", retryPolicy)
                        .withExchange("filesystem", ImmutableMap.of("exchange.base-directories", exchangeDirectory.toString()))
                        .build()) {
            installBlockedSplitSource(runner, () -> {
                requested.complete(null);
                return batch;
            });

            Future<Object> blocked = clients.submit(() -> runner.execute(BLOCKED_QUERY).getOnlyValue());
            try {
                requested.get(30, SECONDS);
                Future<Object> ready = clients.submit(() -> runner.execute("SELECT sum(nationkey) FROM tpch.tiny.nation").getOnlyValue());
                assertThat(ready.get(30, SECONDS)).isEqualTo(300L);
                assertThat(blocked.isDone()).isFalse();
            }
            finally {
                batch.complete(ImmutableList.of());
            }
            assertThat(blocked.get(30, SECONDS)).isEqualTo(0L);
        }
    }

    @Test
    public void testQueryRetryWithOneWorker()
            throws Exception
    {
        TestingFailureInjector injector = new TestingFailureInjector(new TestingFailureInjectionConfig());
        try (DistributedQueryRunner runner = TpchQueryRunner.builder()
                .setWorkerCount(1)
                .addCoordinatorProperty("query.executor-pool-size", "1")
                .addExtraProperty("retry-policy", "QUERY")
                .addExtraProperty("retry-initial-delay", "0s")
                .setAdditionalModule(binder -> newOptionalBinder(binder, FailureInjector.class).setBinding().toInstance(injector))
                .build()) {
            String token = UUID.randomUUID().toString();
            injector.injectTaskFailure(token, 1, 0, 0, TASK_FAILURE, Optional.of(EXTERNAL));
            Session session = Session.builder(runner.getDefaultSession()).setTraceToken(Optional.of(token)).build();
            MaterializedResultWithPlan result = runner.executeWithPlan(session, "SELECT sum(nationkey) FROM tpch.tiny.nation");
            assertThat(result.result().getOnlyValue()).isEqualTo(300L);
            QueryInfo queryInfo = runner.getCoordinator().getQueryManager().getFullQueryInfo(result.queryId());
            assertThat(queryInfo.getStages().orElseThrow().getStages())
                    .flatExtracting(StageInfo::tasks)
                    .anyMatch(task -> task.taskStatus().taskId().attemptId() == 1);
        }
    }
}
