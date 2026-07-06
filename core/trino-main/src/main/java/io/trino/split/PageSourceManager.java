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
        // instance but is polled by every driver of the pipeline. At most one live tracker reports it,
        // so that it is not counted once per driver.
        private final AtomicReference<MemoryUsageTracker> memoryUsageReporter = new AtomicReference<>();

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
        public long getMemoryUsage()
        {
            return pageSourceProvider.getMemoryUsage();
        }

        @Override
        public MemoryUsageTracker createMemoryUsageTracker()
        {
            return new ClaimingMemoryUsageTracker();
        }

        private class ClaimingMemoryUsageTracker
                implements MemoryUsageTracker
        {
            private volatile boolean closed;

            @Override
            public long getMemoryUsage()
            {
                if (closed) {
                    return 0;
                }
                MemoryUsageTracker reporter = memoryUsageReporter.get();
                if (reporter == this || (reporter == null && memoryUsageReporter.compareAndSet(null, this))) {
                    return pageSourceProvider.getMemoryUsage();
                }
                return 0;
            }

            @Override
            public void close()
            {
                closed = true;
                memoryUsageReporter.compareAndSet(this, null);
            }
        }
    }
}
