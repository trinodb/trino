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
package io.trino.server.remotetask;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.server.DynamicFilterService;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintContributionBatch;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintProtocol;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.server.InternalHeaders.TRINO_RUNTIME_CONSTRAINT_SEQUENCE_HEADER;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

class TestRuntimeConstraintFetcher
{
    @Test
    void testStatusUpdateWaitsForPendingResponseCallback()
    {
        Queue<Runnable> callbacks = new ArrayDeque<>();
        List<Long> acknowledgements = new ArrayList<>();
        List<Throwable> failures = new ArrayList<>();
        JsonCodec<RuntimeConstraintContributionBatch> codec = JsonCodec.jsonCodec(RuntimeConstraintContributionBatch.class);
        try (var executor = newSingleThreadScheduledExecutor();
                var httpExecutor = newDirectExecutorService();
                var client = new TestingHttpClient(request -> {
                    long acknowledgement = Long.parseLong(request.getHeader(TRINO_RUNTIME_CONSTRAINT_SEQUENCE_HEADER));
                    acknowledgements.add(acknowledgement);
                    RuntimeConstraintContributionBatch batch = new RuntimeConstraintContributionBatch(
                            RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION,
                            Math.min(acknowledgement + 1, 2),
                            0,
                            ImmutableList.of());
                    return new TestingResponse(OK, ImmutableListMultimap.of(CONTENT_TYPE, "application/json"), codec.toJsonBytes(batch));
                }, httpExecutor)) {
            RuntimeConstraintFetcher fetcher = new RuntimeConstraintFetcher(
                    failures::add,
                    new TaskId(new StageId("query", 0), 0, 0),
                    URI.create("http://localhost/task"),
                    new Duration(1, SECONDS),
                    codec,
                    callbacks::add,
                    client,
                    () -> noopTracer().spanBuilder("test"),
                    new Duration(10, SECONDS),
                    executor,
                    new RemoteTaskStats(),
                    new DynamicFilterService(PLANNER_CONTEXT.getMetadata(), PLANNER_CONTEXT.getFunctionManager(), new TypeOperators(), new DynamicFilterConfig()),
                    () -> {});
            fetcher.start();
            fetcher.updateSequenceAndFetchIfNecessary(1);
            assertThat(acknowledgements).containsExactly(0L);
            assertThat(callbacks).hasSize(1);

            // HTTP has completed, but its batch has not reached the coordinator yet.
            fetcher.updateSequenceAndFetchIfNecessary(2);
            assertThat(acknowledgements).containsExactly(0L);
            while (!callbacks.isEmpty()) {
                callbacks.remove().run();
            }
            assertThat(acknowledgements).containsExactly(0L, 1L, 2L);
            assertThat(fetcher.getSequence()).isEqualTo(2);
            assertThat(failures).isEmpty();
        }
    }
}
