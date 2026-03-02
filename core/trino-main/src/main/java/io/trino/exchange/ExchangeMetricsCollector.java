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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.execution.QueryManager;
import io.trino.server.BasicQueryInfo;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.metrics.Metrics;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class ExchangeMetricsCollector
{
    private static final Logger log = Logger.get(ExchangeMetricsCollector.class);
    private final ConcurrentMap<QueryId, Set<Exchange>> registry = new ConcurrentHashMap<>();
    private final Supplier<List<BasicQueryInfo>> activeQueryProvider;
    private final ScheduledExecutorService cleanupExecutor;
    private final Duration staleEntryCleanupInterval;

    @Inject
    public ExchangeMetricsCollector(QueryManager queryManager)
    {
        this(queryManager::getQueries, Duration.ofMinutes(5));
    }

    public ExchangeMetricsCollector(Supplier<List<BasicQueryInfo>> activeQueryProvider, Duration staleEntryCleanupInterval)
    {
        this.activeQueryProvider = requireNonNull(activeQueryProvider, "activeQueryProvider is null");
        this.staleEntryCleanupInterval = requireNonNull(staleEntryCleanupInterval, "staleEntryCleanupInterval is null");
        this.cleanupExecutor = newSingleThreadScheduledExecutor(threadsNamed("exchange-metrics-cleanup-%s"));
    }

    @PostConstruct
    public void start()
    {
        cleanupExecutor.scheduleWithFixedDelay(() -> {
            try {
                cleanupStaleEntries();
            }
            catch (Throwable e) {
                log.warn(e, "Error cleaning up stale exchanges from the registry");
            }
        }, staleEntryCleanupInterval.toMillis(), staleEntryCleanupInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void register(QueryId queryId, Exchange exchange)
    {
        registry.computeIfAbsent(queryId, ignored -> Sets.newConcurrentHashSet())
                .add(exchange);
    }

    public Map<ExchangeId, Metrics> collectMetrics(QueryId queryId)
    {
        Set<Exchange> exchanges = registry.get(queryId);
        if (exchanges == null || exchanges.isEmpty()) {
            return ImmutableMap.of();
        }
        return exchanges.stream()
                .collect(toMap(
                        Exchange::getId,
                        Exchange::getMetrics));
    }

    @VisibleForTesting
    void cleanupStaleEntries()
    {
        Set<QueryId> activeQueryIds = activeQueryProvider.get().stream()
                .map(BasicQueryInfo::getQueryId)
                .collect(toSet());

        registry.keySet().removeIf(queryId -> !activeQueryIds.contains(queryId));
    }

    @PreDestroy
    public void shutdown()
    {
        cleanupExecutor.shutdownNow();
        registry.clear();
    }
}
