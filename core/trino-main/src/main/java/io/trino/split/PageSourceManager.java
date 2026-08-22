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
package io.trino.split;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.connector.CatalogHandle;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static java.util.Objects.requireNonNull;

public class PageSourceManager
        implements PageSourceProviderFactory
{
    private final CatalogServiceProvider<ConnectorPageSourceProviderFactory> pageSourceProviderFactory;

    @Inject
    public PageSourceManager(CatalogServiceProvider<ConnectorPageSourceProviderFactory> pageSourceProviderFactory)
    {
        this.pageSourceProviderFactory = requireNonNull(pageSourceProviderFactory, "pageSourceProviderFactory is null");
    }

    @Override
    public PageSourceProvider createPageSourceProvider(CatalogHandle catalogHandle)
    {
        ConnectorPageSourceProviderFactory provider = pageSourceProviderFactory.getService(catalogHandle);
        return new PageSourceProviderInstance(provider.createPageSourceProvider());
    }

    private static class PageSourceProviderInstance
            implements PageSourceProvider
    {
        private final ConnectorPageSourceProvider pageSourceProvider;
        // Shared provider state (e.g. loaded Iceberg equality delete filters) exists once per provider
        // instance. Each driver registers its own memory context, but only the active driver polls and
        // reports the usage, so that it is not counted once per driver.
        private final ConcurrentHashMap<MemoryContext, MemoryUsageReporter> memoryUsageReporters = new ConcurrentHashMap<>();
        private final AtomicReference<MemoryUsageReporter> activeMemoryUsageReporter = new AtomicReference<>();
        // Prevent an old registration's reset from racing with re-registration of the same context.
        private final Object memoryUsageReportersLock = new Object();

        private PageSourceProviderInstance(ConnectorPageSourceProvider pageSourceProvider)
        {
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        }

        @Override
        public ConnectorPageSource createPageSource(
                Session session,
                Split split,
                TableHandle table,
                Optional<ConnectorTableCredentials> tableCredentials,
                List<ColumnHandle> columns,
                DynamicFilter dynamicFilter,
                MemoryContext memoryContext)
        {
            requireNonNull(columns, "columns is null");
            checkArgument(split.getCatalogHandle().equals(table.catalogHandle()), "mismatched split and table");

            TupleDomain<ColumnHandle> constraint = dynamicFilter.getCurrentPredicate();
            if (constraint.isNone()) {
                return new EmptyPageSource();
            }
            if (!isAllowPushdownIntoConnectors(session)) {
                dynamicFilter = DynamicFilter.EMPTY;
            }
            return pageSourceProvider.createPageSource(
                    table.transaction(),
                    session.toConnectorSession(table.catalogHandle()),
                    split.getConnectorSplit(),
                    table.connectorHandle(),
                    tableCredentials,
                    columns,
                    dynamicFilter,
                    memoryContext);
        }

        @Override
        public void trackMemoryUsage(MemoryContext memoryContext)
        {
            synchronized (memoryUsageReportersLock) {
                memoryUsageReporters.putIfAbsent(memoryContext, new MemoryUsageReporter(memoryContext));
            }
        }

        @Override
        public void untrackMemoryUsage(MemoryContext memoryContext)
        {
            synchronized (memoryUsageReportersLock) {
                MemoryUsageReporter reporter = memoryUsageReporters.remove(memoryContext);
                if (reporter != null) {
                    try {
                        reporter.close();
                    }
                    finally {
                        activeMemoryUsageReporter.compareAndSet(reporter, null);
                    }
                }
            }
        }

        @Override
        public void updateMemoryUsage(MemoryContext memoryContext)
        {
            MemoryUsageReporter reporter = memoryUsageReporters.get(memoryContext);
            if (reporter == null) {
                return;
            }

            while (true) {
                MemoryUsageReporter activeReporter = activeMemoryUsageReporter.get();
                if (activeReporter == null) {
                    if (!activeMemoryUsageReporter.compareAndSet(null, reporter)) {
                        continue;
                    }
                    activeReporter = reporter;
                }

                if (activeReporter != reporter) {
                    return;
                }

                if (reporter.report(pageSourceProvider)) {
                    return;
                }
                // this context was untracked concurrently
                activeMemoryUsageReporter.compareAndSet(reporter, null);
                return;
            }
        }

        private static class MemoryUsageReporter
        {
            private final MemoryContext memoryContext;
            @GuardedBy("this")
            private boolean closed;

            private MemoryUsageReporter(MemoryContext memoryContext)
            {
                this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
            }

            private synchronized boolean report(ConnectorPageSourceProvider pageSourceProvider)
            {
                if (closed) {
                    return false;
                }
                memoryContext.setBytes(pageSourceProvider.getMemoryUsage());
                return true;
            }

            private synchronized void close()
            {
                if (closed) {
                    return;
                }
                closed = true;
                memoryContext.setBytes(0);
            }
        }
    }
}
