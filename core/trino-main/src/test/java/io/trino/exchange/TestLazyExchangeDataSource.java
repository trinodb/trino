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
package io.trino.exchange;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.tracing.Tracing;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.operator.RetryPolicy;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.ExchangeId;
import org.junit.jupiter.api.Test;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLazyExchangeDataSource
{
    @Test
    public void testIsBlockedCancellationIsolationInInitializationPhase()
    {
        try (LazyExchangeDataSource source = new LazyExchangeDataSource(
                new QueryId("query"),
                new ExchangeId("exchange"),
                Span.getInvalid(),
                (queryId, exchangeId, span, memoryContext, taskFailureListener, retryPolicy) -> {
                    throw new UnsupportedOperationException();
                },
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), TestLazyExchangeDataSource.class.getSimpleName()),
                (taskId, failure) -> {
                    throw new UnsupportedOperationException();
                },
                RetryPolicy.NONE,
                new ExchangeManagerRegistry(OpenTelemetry.noop(), Tracing.noopTracer(), new SecretsResolver(ImmutableMap.of())))) {
            ListenableFuture<Void> first = source.isBlocked();
            ListenableFuture<Void> second = source.isBlocked();
            assertThat(first)
                    .isNotDone()
                    .isNotCancelled();
            assertThat(second)
                    .isNotDone()
                    .isNotCancelled();

            first.cancel(true);
            assertThat(first)
                    .isDone()
                    .isCancelled();
            assertThat(second)
                    .isNotDone()
                    .isNotCancelled();

            ListenableFuture<Void> third = source.isBlocked();
            assertThat(third)
                    .isNotDone()
                    .isNotCancelled();
        }
    }
}
