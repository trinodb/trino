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
package io.trino.eventlistener;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.secrets.SecretsResolver;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.session.ResourceEstimates;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

class TestEventListenerManager
{
    private static final QueryMetadata QUERY_METADATA = new QueryMetadata(
            "minimal_query",
            Optional.empty(),
            "query",
            Optional.empty(),
            Optional.empty(),
            "queryState",
            // not stored
            List.of(),
            // not stored
            List.of(),
            URI.create("http://localhost"),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    private static final QueryStatistics QUERY_STATISTICS = new QueryStatistics(
            ofMillis(101),
            ofMillis(102),
            ofMillis(103),
            ofMillis(104),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            115L,
            116L,
            117L,
            118L,
            119L,
            1191L,
            1192L,
            120L,
            121L,
            122L,
            123L,
            124L,
            125L,
            126L,
            127L,
            1271L,
            128.0,
            129.0,
            // not stored
            Collections.emptyList(),
            130,
            false,
            // not stored
            Collections.emptyList(),
            // not stored
            Collections.emptyList(),
            // not stored
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            // not stored
            Optional.empty());

    private static final QueryContext QUERY_CONTEXT = new QueryContext(
            "user",
            "originalUser",
            Optional.empty(),
            Set.of(),
            Set.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Set.of(),
            // not stored
            Set.of(),
            Optional.empty(),
            UTC_KEY.getId(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Map.of(),
            // not stored
            new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty()),
            "serverAddress",
            "serverVersion",
            "environment",
            Optional.empty(),
            "NONE");

    private static final QueryIOMetadata QUERY_IO_METADATA = new QueryIOMetadata(List.of(), Optional.empty());

    private static final QueryCompletedEvent QUERY_COMPLETED_EVENT = new QueryCompletedEvent(
            QUERY_METADATA,
            QUERY_STATISTICS,
            QUERY_CONTEXT,
            QUERY_IO_METADATA,
            Optional.empty(),
            List.of(),
            Instant.now(),
            Instant.now(),
            Instant.now());

    @Test
    public void testShutdownIsForwardedToListeners()
    {
        EventListenerManager eventListenerManager = new EventListenerManager(new EventListenerConfig(), new SecretsResolver(ImmutableMap.of()));
        AtomicBoolean wasCalled = new AtomicBoolean(false);
        EventListener listener = new EventListener()
        {
            @Override
            public void shutdown()
            {
                wasCalled.set(true);
            }
        };

        eventListenerManager.addEventListener(listener);
        eventListenerManager.loadEventListeners();
        eventListenerManager.shutdown();

        assertThat(wasCalled.get()).isTrue();
    }

    @Test
    public void testMaxConcurrentQueryCompletedEvents()
            throws InterruptedException
    {
        EventListenerManager eventListenerManager = new EventListenerManager(new EventListenerConfig().setMaxConcurrentQueryCompletedEvents(1), new SecretsResolver(ImmutableMap.of()));
        eventListenerManager.addEventListener(new BlockingEventListener());
        eventListenerManager.loadEventListeners();
        ExecutorService executor = newFixedThreadPool(2);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            Runnable queryCompletedEvent = () -> {
                eventListenerManager.queryCompleted(_ -> QUERY_COMPLETED_EVENT);
                countDownLatch.countDown();
            };
            executor.submit(queryCompletedEvent);
            executor.submit(queryCompletedEvent);

            countDownLatch.await();
            assertThat(eventListenerManager.getSkippedQueryCompletedEvents().getTotalCount()).isEqualTo(1);
            assertThat(eventListenerManager.getConcurrentQueryCompletedEvents()).isEqualTo(1);
        }
        finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static final class BlockingEventListener
            implements EventListener
    {
        @Override
        public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
        {
            try {
                // sleep forever
                Thread.sleep(100_000);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
