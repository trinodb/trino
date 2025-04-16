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
package io.trino.connector;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CatalogRefreshTask
{
    private static final Logger log = Logger.get(CatalogRefreshTask.class);

    private final CoordinatorDynamicCatalogManager catalogManager;
    private final boolean enabled;
    private final Duration refreshInterval;

    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, daemonThreadsNamed("catalog-refresh"));

    private final AtomicBoolean started = new AtomicBoolean();

    @Inject
    public CatalogRefreshTask(CatalogRefreshTaskConfig config, CoordinatorDynamicCatalogManager catalogManager)
    {
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.enabled = config.isEnabled();
        this.refreshInterval = config.getRefreshInterval();
    }

    @PostConstruct
    public void start()
    {
        if (enabled && !started.getAndSet(true)) {
            executor.scheduleWithFixedDelay(() -> {
                try {
                    catalogManager.refreshCatalogs();
                }
                catch (Throwable e) {
                    // ignore to avoid getting unscheduled
                    log.warn(e, "Error refreshing catalogs");
                }
            }, refreshInterval.toMillis(), refreshInterval.toMillis(), MILLISECONDS);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }
}
