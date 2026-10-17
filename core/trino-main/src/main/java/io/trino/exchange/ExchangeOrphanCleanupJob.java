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

import com.google.inject.Inject;
import io.airlift.concurrent.Threads;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.dispatcher.DispatchManager;
import io.trino.server.BasicQueryInfo;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.exchange.ExtendedExchangeManager;
import jakarta.annotation.PreDestroy;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ExchangeOrphanCleanupJob
{
    private static final Logger log = Logger.get(ExchangeOrphanCleanupJob.class);
    private static final String TIMESTAMP_FORMAT = "yyyyMMdd_HHmmss";
    private static final DateTimeFormatter QUERY_ID_TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT).withZone(ZoneOffset.UTC);
    private static final int EXCHANGE_DIRECTORY_PARTS = 4;
    private static final long INITIAL_DELAY = 2 * 60 * 1000;
    private final DispatchManager dispatchManager;
    private final ExchangeManagerRegistry exchangeManagerRegistry;
    private final boolean enabled;
    private final Duration interval;
    private final Duration retention;
    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, Threads.daemonThreadsNamed("exchange-orphan-cleanup"));
    private final AtomicBoolean started = new AtomicBoolean();

    @Inject
    public ExchangeOrphanCleanupJob(
            DispatchManager dispatchManager,
            ExchangeManagerRegistry exchangeManagerRegistry,
            ExchangeManagerConfig config)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
        requireNonNull(config, "config is null");
        this.enabled = config.isOrphanCleanupEnabled();
        this.interval = config.getOrphanCleanupInterval();
        this.retention = config.getOrphanCleanupRetention();
    }

    public void start()
    {
        if (!enabled || started.getAndSet(true)) {
            return;
        }

        try {
            if (!(exchangeManagerRegistry.getExchangeManager() instanceof ExtendedExchangeManager exchangeManager)) {
                log.warn("Exchange orphan cleanup job skipped. ExtendedExchangeManager is not available.");
                return;
            }
            executor.scheduleWithFixedDelay(() -> {
                try {
                    cleanup(exchangeManager);
                }
                catch (Throwable e) {
                    log.warn(e, "Error cleaning orphan exchange files");
                }
            }, INITIAL_DELAY, interval.toMillis(), MILLISECONDS);
        }
        catch (TrinoException e) {
            log.warn(e, "Exchange orphan cleanup job skipped. Exchange manager is not configured.");
        }
    }

    @PreDestroy
    public void stop()
    {
        executor.shutdownNow();
    }

    void cleanup(ExtendedExchangeManager exchangeManager)
    {
        cleanup(dispatchManager.getQueries(), exchangeManager);
    }

    void cleanup(List<BasicQueryInfo> queries, ExtendedExchangeManager exchangeManager)
    {
        log.info("Exchange orphan cleanup job started.");
        Set<String> activeQueryIds = queries.stream()
                .filter(queryInfo -> !queryInfo.getState().isDone())
                .map(BasicQueryInfo::getQueryId)
                .map(QueryId::id)
                .collect(toImmutableSet());

        Instant retentionThreshold = Instant.now().minusMillis(retention.toMillis());
        for (URI baseDirectory : exchangeManager.getBaseDirectories()) {
            try {
                cleanupBaseDirectory(exchangeManager, baseDirectory, activeQueryIds, retentionThreshold);
            }
            catch (InterruptedException _) {
                Thread.currentThread().interrupt();
                return;
            }
            catch (ExecutionException | RuntimeException e) {
                log.warn(e, "Unable to clean orphan exchange directories in %s", baseDirectory);
            }
        }
    }

    private static void cleanupBaseDirectory(ExtendedExchangeManager exchangeManager, URI baseDirectory, Set<String> activeQueryIds, Instant retentionThreshold)
            throws ExecutionException, InterruptedException
    {
        List<URI> directoriesToDelete = exchangeManager.listDirectories(baseDirectory).stream()
                .filter(directory -> shouldDelete(directory, activeQueryIds, retentionThreshold))
                .toList();
        if (!directoriesToDelete.isEmpty()) {
            exchangeManager.deleteRecursively(directoriesToDelete);
            log.info("Deleted %s orphan exchange directories in %s", directoriesToDelete.size(), baseDirectory);
        }
    }

    private static boolean shouldDelete(URI exchangeDirectory, Set<String> activeQueryIds, Instant retentionThreshold)
    {
        return extractQueryId(exchangeDirectory)
                .filter(queryId -> !activeQueryIds.contains(queryId.id()))
                .flatMap(ExchangeOrphanCleanupJob::extractQueryInstant)
                .filter(queryInstant -> queryInstant.isBefore(retentionThreshold))
                .isPresent();
    }

    private static Optional<QueryId> extractQueryId(URI exchangeDirectory)
    {
        String path = exchangeDirectory.getPath();
        int pathEnd = path.endsWith("/") ? path.length() - 1 : path.length();
        int pathStart = path.lastIndexOf('/', pathEnd - 1);
        if (pathStart < 0) {
            return Optional.empty();
        }

        String directoryName = path.substring(pathStart + 1, pathEnd);
        String[] parts = directoryName.split("\\.", EXCHANGE_DIRECTORY_PARTS);
        if (parts.length != EXCHANGE_DIRECTORY_PARTS) {
            return Optional.empty();
        }
        try {
            return Optional.of(QueryId.valueOf(parts[1]));
        }
        catch (IllegalArgumentException _) {
            return Optional.empty();
        }
    }

    private static Optional<Instant> extractQueryInstant(QueryId queryId)
    {
        String id = queryId.id();
        if (id.length() < TIMESTAMP_FORMAT.length()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Instant.from(QUERY_ID_TIMESTAMP_FORMATTER.parse(id.substring(0, TIMESTAMP_FORMAT.length()))));
        }
        catch (DateTimeParseException _) {
            return Optional.empty();
        }
    }
}
