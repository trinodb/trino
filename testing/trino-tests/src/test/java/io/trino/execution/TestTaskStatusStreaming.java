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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import io.airlift.units.Duration;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.TestingTableFunctions.EmptySourceFunction;
import io.trino.server.TaskResource;
import io.trino.server.remotetask.RemoteTaskStats;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTaskStatusStreaming
{
    @Language("SQL")
    private static final String BLOCKED_QUERY = "SELECT count(*) FROM TABLE(blocked.system.empty_source())";

    @Test
    public void testStatusStreamDisconnectWhileQueryIsRunning()
            throws Exception
    {
        CompletableFuture<List<ConnectorSplit>> batch = new CompletableFuture<>();
        try (ExecutorService clients = newFixedThreadPool(1);
                DistributedQueryRunner runner = TpchQueryRunner.builder()
                        .setWorkerCount(1)
                        .addCoordinatorProperty("experimental.task-status-streaming-enabled", "true")
                        .addCoordinatorProperty("query.execution-policy", "all-at-once")
                        .build()) {
            installBlockedSplitSource(runner, batch);
            RemoteTaskStats stats = runner.getCoordinator().getInstance(Key.get(RemoteTaskStats.class));
            runner.execute("SELECT sum(nationkey) FROM tpch.tiny.nation");
            Duration timeout = new Duration(10, SECONDS);
            assertEventually(timeout, () -> assertThat(stats.getStreamConnections()).isEqualTo(2));

            long polls = stats.getStatusRequests();
            long events = stats.getStreamEvents();
            Future<Object> blocked = clients.submit(() -> runner.execute(BLOCKED_QUERY).getOnlyValue());
            try {
                assertEventually(timeout, () -> assertThat(stats.getStreamEvents()).isGreaterThan(events));
                assertThat(stats.getStatusRequests()).isEqualTo(polls);
                runner.getServers().forEach(server -> server.getInstance(Key.get(TaskResource.class)).closeStatusStreams());
                assertEventually(timeout, () -> assertThat(stats.getStreamFallbackTasks()).isPositive());
                assertEventually(timeout, () -> assertThat(stats.getStatusRequests()).isGreaterThan(polls));
            }
            finally {
                batch.complete(ImmutableList.of());
            }
            assertThat(blocked.get(30, SECONDS)).isEqualTo(0L);
        }
    }

    private static void installBlockedSplitSource(DistributedQueryRunner runner, CompletableFuture<List<ConnectorSplit>> nextBatch)
    {
        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withTableFunctions(ImmutableSet.of(new EmptySourceFunction()))
                .withFunctionProvider(Optional.of(new FunctionProvider()
                {
                    @Override
                    public TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle handle)
                    {
                        return new EmptySourceFunction.EmptySourceFunctionProcessorProvider();
                    }
                }))
                .withTableFunctionSplitSources(_ -> new ConnectorSplitSource()
                {
                    @Override
                    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
                    {
                        return nextBatch;
                    }

                    @Override
                    public boolean isFinished()
                    {
                        return nextBatch.isDone();
                    }

                    @Override
                    public void close() {}
                })
                .build()));
        runner.createCatalog("blocked", "mock");
    }
}
