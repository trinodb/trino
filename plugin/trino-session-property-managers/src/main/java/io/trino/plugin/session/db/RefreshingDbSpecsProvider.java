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
package io.trino.plugin.session.db;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.Duration;
import io.trino.plugin.session.SessionMatchSpec;
import io.trino.spi.TrinoException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.Duration.succinctNanos;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_UNAVAILABLE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

/**
 * Periodically schedules the loading of specs from the database during initialization. Returns the most recent
 * successfully loaded specs on every get() invocation. If reloads keep failing for longer than
 * {@code max-refresh-interval}, the cached specs are considered stale and get() fails rather than serving them.
 */
public class RefreshingDbSpecsProvider
        implements DbSpecsProvider
{
    private static final Logger log = Logger.get(RefreshingDbSpecsProvider.class);

    private final AtomicReference<List<SessionMatchSpec>> sessionMatchSpecs = new AtomicReference<>(ImmutableList.of());
    private final AtomicLong lastRefresh = new AtomicLong();
    private final SessionPropertiesDao dao;

    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("RefreshingDbSpecsProvider"));
    private final AtomicBoolean started = new AtomicBoolean();
    private final long refreshIntervalMillis;
    private final Duration maxRefreshInterval;
    private final CounterStat dbLoadFailures = new CounterStat();

    @Inject
    public RefreshingDbSpecsProvider(DbSessionPropertyManagerConfig config, SessionPropertiesDao dao)
    {
        this.dao = requireNonNull(dao, "dao is null");
        this.refreshIntervalMillis = config.getRefreshInterval().toMillis();
        this.maxRefreshInterval = config.getMaxRefreshInterval();
    }

    @PostConstruct
    public void initialize()
    {
        if (!started.getAndSet(true)) {
            executor.scheduleWithFixedDelay(this::refresh, 0, refreshIntervalMillis, TimeUnit.MILLISECONDS);
        }
    }

    @VisibleForTesting
    void refresh()
    {
        try {
            sessionMatchSpecs.set(ImmutableList.copyOf(dao.getSessionMatchSpecs()));
            lastRefresh.set(System.nanoTime());
        }
        catch (Throwable e) {
            // Catch all exceptions here since throwing an exception from executor#scheduleWithFixedDelay method
            // suppresses all future scheduled invocations
            if (succinctNanos(System.nanoTime() - lastRefresh.get()).compareTo(maxRefreshInterval) > 0) {
                lastRefresh.set(0);
            }
            dbLoadFailures.update(1);
            log.error(e, "Error loading configuration from database");
        }
    }

    @PreDestroy
    public void destroy()
    {
        executor.shutdownNow();
    }

    @Override
    public List<SessionMatchSpec> get()
    {
        if (lastRefresh.get() == 0) {
            throw new TrinoException(CONFIGURATION_UNAVAILABLE, "Session property configuration cannot be fetched from database");
        }
        return sessionMatchSpecs.get();
    }

    @Managed
    @Nested
    public CounterStat getDbLoadFailures()
    {
        return dbLoadFailures;
    }
}
