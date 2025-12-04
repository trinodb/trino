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

import com.google.common.collect.ImmutableList;
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
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.SystemSessionProperties.isSourcePagesValidationEnabled;
import static io.trino.split.PageValidations.validateOutputPageTypes;
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

    private record PageSourceProviderInstance(ConnectorPageSourceProvider pageSourceProvider)
            implements PageSourceProvider
    {
        private PageSourceProviderInstance
        {
            requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        }

        @Override
        public ConnectorPageSource createPageSource(
                Session session,
                Split split,
                TableHandle table,
                List<ColumnHandle> columns,
                List<Type> types,
                DynamicFilter dynamicFilter)
        {
            boolean pageValidationEnabled = isSourcePagesValidationEnabled(session);

            requireNonNull(columns, "columns is null");
            checkArgument(split.getCatalogHandle().equals(table.catalogHandle()), "mismatched split and table");

            TupleDomain<ColumnHandle> constraint = dynamicFilter.getCurrentPredicate();
            if (constraint.isNone()) {
                return new EmptyPageSource();
            }
            if (!isAllowPushdownIntoConnectors(session)) {
                dynamicFilter = DynamicFilter.EMPTY;
            }

            ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                    table.transaction(),
                    session.toConnectorSession(table.catalogHandle()),
                    split.getConnectorSplit(),
                    table.connectorHandle(),
                    columns,
                    dynamicFilter);

            if (pageValidationEnabled) {
                return new ValidatingConnectorPageSource(pageSource, types, () -> "pageSource=%s; columnHandles=%s".formatted(pageSource, columns));
            }
            return pageSource;
        }
    }

    private static class ValidatingConnectorPageSource
            implements ConnectorPageSource
    {
        private final ConnectorPageSource delegate;
        private final List<Type> outputTypes;
        private final Supplier<String> debugContextSupplier;

        private ValidatingConnectorPageSource(ConnectorPageSource delegate, List<Type> outputTypes, Supplier<String> debugContextSupplier)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
            this.debugContextSupplier = requireNonNull(debugContextSupplier, "debugContextSupplier is null");
        }

        @Override
        public long getCompletedBytes()
        {
            return delegate.getCompletedBytes();
        }

        @Override
        public OptionalLong getCompletedPositions()
        {
            return delegate.getCompletedPositions();
        }

        @Override
        public long getReadTimeNanos()
        {
            return delegate.getReadTimeNanos();
        }

        @Override
        public boolean isFinished()
        {
            return delegate.isFinished();
        }

        @Override
        public SourcePage getNextSourcePage()
        {
            SourcePage page = delegate.getNextSourcePage();
            if (page != null) {
                validateOutputPageTypes(page, outputTypes, debugContextSupplier);
            }
            return page;
        }

        @Override
        public long getMemoryUsage()
        {
            return delegate.getMemoryUsage();
        }

        @Override
        public void close()
                throws IOException
        {
            delegate.close();
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return delegate.isBlocked();
        }

        @Override
        public Metrics getMetrics()
        {
            return delegate.getMetrics();
        }
    }
}
