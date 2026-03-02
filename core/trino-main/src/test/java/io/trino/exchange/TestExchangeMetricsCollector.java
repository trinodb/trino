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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.execution.QueryIdGenerator;
import io.trino.execution.QueryState;
import io.trino.operator.RetryPolicy;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.server.BasicQueryInfo;
import io.trino.server.BasicQueryStats;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandleSource;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.resourcegroups.ResourceGroupId;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.BlockedReason.WAITING_FOR_MEMORY;
import static io.trino.spi.exchange.ExchangeId.createRandomExchangeId;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExchangeMetricsCollector
{
    private ExchangeMetricsCollector exchangeMetricsCollector;
    private final QueryIdGenerator idGenerator = new QueryIdGenerator();
    private final Duration cleanupDuration = Duration.ofMinutes(10);

    @Test
    public void testRegisterExchanges()
    {
        QueryId queryId1 = idGenerator.createNextQueryId();
        Metrics testCounter1 = new Metrics(ImmutableMap.of("testCounter", new LongCount(10L)));
        Metrics testCounter2 = new Metrics(ImmutableMap.of("testCounter2", new LongCount(100L)));

        QueryId queryId2 = idGenerator.createNextQueryId();
        Metrics testCounter3 = new Metrics(ImmutableMap.of("testCounter3", new LongCount(400L)));

        TestingMetricsExchange testExchange1 = TestingMetricsExchange.of(createRandomExchangeId(), testCounter1);
        TestingMetricsExchange testExchange2 = TestingMetricsExchange.of(createRandomExchangeId(), testCounter2);
        TestingMetricsExchange testExchange3 = TestingMetricsExchange.of(createRandomExchangeId(), testCounter3);

        exchangeMetricsCollector = exchangeMetricsCollector(
                () -> ImmutableList.of(queryInfo(queryId1), queryInfo(queryId2)));
        exchangeMetricsCollector.register(
                queryId1,
                testExchange1);
        exchangeMetricsCollector.register(
                queryId1,
                testExchange2);
        exchangeMetricsCollector.register(
                queryId2,
                testExchange3);

        assertThat(exchangeMetricsCollector.collectMetrics(queryId1))
                .isEqualTo(ImmutableMap.of(
                        testExchange1.getId(), testCounter1,
                        testExchange2.getId(), testCounter2));
        assertThat(exchangeMetricsCollector.collectMetrics(queryId2))
                .isEqualTo(ImmutableMap.of(testExchange3.getId(), testCounter3));
    }

    @Test
    public void testCollectMetricsForUnknownQueryReturnsEmpty()
    {
        exchangeMetricsCollector = exchangeMetricsCollector(ImmutableList::of);
        QueryId unknown = idGenerator.createNextQueryId();

        assertThat(exchangeMetricsCollector.collectMetrics(unknown)).isEmpty();
    }

    @Test
    public void testCollectMetricsWhenExchangeIsUpdated()
    {
        exchangeMetricsCollector = exchangeMetricsCollector(ImmutableList::of);
        QueryId queryId = idGenerator.createNextQueryId();
        TestingMetricsExchange exchangeWithMetricsToUpdate = new TestingMetricsExchange(createRandomExchangeId(), Metrics.EMPTY);

        exchangeMetricsCollector.register(queryId, exchangeWithMetricsToUpdate);

        Metrics first = new Metrics(ImmutableMap.of("metric", new LongCount(1L)));
        exchangeWithMetricsToUpdate.setMetrics(first); // set first value
        assertThat(exchangeMetricsCollector.collectMetrics(queryId).get(exchangeWithMetricsToUpdate.getId())).isEqualTo(first);

        Metrics second = new Metrics(ImmutableMap.of("metric", new LongCount(2L)));
        exchangeWithMetricsToUpdate.setMetrics(second); // update value
        assertThat(exchangeMetricsCollector.collectMetrics(queryId).get(exchangeWithMetricsToUpdate.getId())).isEqualTo(second);
    }

    @Test
    public void testCleanupOnShutdown()
    {
        exchangeMetricsCollector = exchangeMetricsCollector(ImmutableList::of);
        QueryId nextQueryId1 = idGenerator.createNextQueryId();
        QueryId nextQueryId2 = idGenerator.createNextQueryId();
        exchangeMetricsCollector.register(
                nextQueryId1,
                TestingMetricsExchange.of(createRandomExchangeId(), Metrics.EMPTY));
        exchangeMetricsCollector.register(
                nextQueryId2,
                TestingMetricsExchange.of(createRandomExchangeId(), Metrics.EMPTY));

        exchangeMetricsCollector.shutdown();

        assertThat(exchangeMetricsCollector.collectMetrics(nextQueryId1))
                .isEqualTo(ImmutableMap.of());
        assertThat(exchangeMetricsCollector.collectMetrics(nextQueryId2))
                .isEqualTo(ImmutableMap.of());
    }

    @Test
    public void testStaleEntryCleanup()
    {
        QueryId queryId1 = idGenerator.createNextQueryId();
        QueryId queryId2 = idGenerator.createNextQueryId();
        QueryId queryId3 = idGenerator.createNextQueryId();

        // Only queryId1 is active
        exchangeMetricsCollector = new ExchangeMetricsCollector(
                () -> ImmutableList.of(queryInfo(queryId1)), cleanupDuration);

        // Register exchanges for all three queries
        exchangeMetricsCollector.register(queryId1, TestingMetricsExchange.of(createRandomExchangeId(), Metrics.EMPTY));
        exchangeMetricsCollector.register(queryId2, TestingMetricsExchange.of(createRandomExchangeId(), Metrics.EMPTY));
        exchangeMetricsCollector.register(queryId3, TestingMetricsExchange.of(createRandomExchangeId(), Metrics.EMPTY));

        // Verify all are present
        assertThat(exchangeMetricsCollector.collectMetrics(queryId1)).isNotEmpty();
        assertThat(exchangeMetricsCollector.collectMetrics(queryId2)).isNotEmpty();
        assertThat(exchangeMetricsCollector.collectMetrics(queryId3)).isNotEmpty();

        // Trigger stale entries cleanup
        exchangeMetricsCollector.cleanupStaleEntries();

        // Only queryId1 should remain (it's the only active query)
        assertThat(exchangeMetricsCollector.collectMetrics(queryId1)).isNotEmpty();
        assertThat(exchangeMetricsCollector.collectMetrics(queryId2)).isEmpty();
        assertThat(exchangeMetricsCollector.collectMetrics(queryId3)).isEmpty();
    }

    @Test
    public void testConcurrentRegistration()
            throws Exception
    {
        exchangeMetricsCollector = exchangeMetricsCollector(ImmutableList::of);
        QueryId queryId = idGenerator.createNextQueryId();

        int threads = 5;
        ExecutorService executor = newFixedThreadPool(threads);
        try {
            range(0, threads)
                    .forEach(_ -> executor.submit(() -> exchangeMetricsCollector.register(
                            queryId,
                            TestingMetricsExchange.of(createRandomExchangeId(), Metrics.EMPTY))));

            assertEventually(() -> assertThat(exchangeMetricsCollector.collectMetrics(queryId)).hasSize(threads));
        }
        finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    private ExchangeMetricsCollector exchangeMetricsCollector(Supplier<List<BasicQueryInfo>> activeQueryProvider)
    {
        return new ExchangeMetricsCollector(activeQueryProvider, cleanupDuration);
    }

    private static BasicQueryInfo queryInfo(QueryId queryId)
    {
        return new BasicQueryInfo(
                queryId,
                TEST_SESSION.toSessionRepresentation(),
                Optional.of(new ResourceGroupId("global")),
                QueryState.RUNNING,
                true,
                URI.create("1"),
                "",
                Optional.empty(),
                Optional.empty(),
                new BasicQueryStats(
                        Instant.parse("2025-05-11T13:32:17.751968Z"),
                        Instant.parse("2025-05-11T13:32:17.751968Z"),
                        new io.airlift.units.Duration(8, MINUTES),
                        new io.airlift.units.Duration(7, MINUTES),
                        new io.airlift.units.Duration(9, MINUTES),
                        new io.airlift.units.Duration(34, MINUTES),
                        99,
                        13,
                        14,
                        15,
                        100,
                        0,
                        22,
                        DataSize.valueOf("23GB"),
                        DataSize.valueOf("23GB"),
                        DataSize.valueOf("23GB"),
                        DataSize.valueOf("23GB"),
                        24,
                        25,
                        DataSize.valueOf("26GB"),
                        DataSize.valueOf("27GB"),
                        DataSize.valueOf("28GB"),
                        DataSize.valueOf("29GB"),
                        new io.airlift.units.Duration(30, MINUTES),
                        new io.airlift.units.Duration(31, MINUTES),
                        new io.airlift.units.Duration(32, MINUTES),
                        new io.airlift.units.Duration(33, MINUTES),
                        new io.airlift.units.Duration(34, MINUTES),
                        new io.airlift.units.Duration(35, MINUTES),
                        new io.airlift.units.Duration(36, MINUTES),
                        new io.airlift.units.Duration(37, MINUTES),
                        true,
                        ImmutableSet.of(WAITING_FOR_MEMORY),
                        OptionalDouble.of(20),
                        OptionalDouble.of(0)),
                null,
                null,
                Optional.empty(),
                RetryPolicy.NONE);
    }

    private static class TestingMetricsExchange
            implements Exchange
    {
        private final ExchangeId exchangeId;
        private Metrics metrics;

        public static TestingMetricsExchange of(ExchangeId exchangeId, Metrics metrics)
        {
            return new TestingMetricsExchange(exchangeId, metrics);
        }

        private TestingMetricsExchange(ExchangeId exchangeId, Metrics metrics)
        {
            this.exchangeId = exchangeId;
            this.metrics = metrics;
        }

        @Override
        public ExchangeId getId()
        {
            return exchangeId;
        }

        @Override
        public ExchangeSinkHandle addSink(int taskPartitionId)
        {
            return null;
        }

        @Override
        public void noMoreSinks()
        {
        }

        @Override
        public CompletableFuture<ExchangeSinkInstanceHandle> instantiateSink(ExchangeSinkHandle sinkHandle, int taskAttemptId)
        {
            return null;
        }

        @Override
        public CompletableFuture<ExchangeSinkInstanceHandle> updateSinkInstanceHandle(ExchangeSinkHandle sinkHandle, int taskAttemptId)
        {
            return null;
        }

        @Override
        public void sinkFinished(ExchangeSinkHandle sinkHandle, int taskAttemptId)
        {
        }

        @Override
        public void allRequiredSinksFinished()
        {
        }

        @Override
        public ExchangeSourceHandleSource getSourceHandles()
        {
            return null;
        }

        @Override
        public void close()
        {
        }

        @Override
        public Metrics getMetrics()
        {
            return metrics;
        }

        public void setMetrics(Metrics metrics)
        {
            this.metrics = metrics;
        }
    }
}
