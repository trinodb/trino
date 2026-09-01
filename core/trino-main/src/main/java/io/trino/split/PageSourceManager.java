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
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.ReferenceCount;
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
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
    public PageSourceProvider createPageSourceProvider(CatalogHandle catalogHandle, AggregatedMemoryContext memoryContext)
    {
        ConnectorPageSourceProviderFactory provider = pageSourceProviderFactory.getService(catalogHandle);
        return new PageSourceProviderInstance(provider, memoryContext.newLocalMemoryContext(PageSourceProvider.class.getSimpleName()));
    }

    /**
     * Holds the reservation for the connector state shared by all page sources of a single scan. The reservation
     * is given up once the operators of the scan have dropped their references.
     */
    private static final class PageSourceProviderInstance
            implements PageSourceProvider
    {
        private final ConnectorPageSourceProvider pageSourceProvider;
        private final ReferenceCount referenceCount = new ReferenceCount(1);

        private PageSourceProviderInstance(ConnectorPageSourceProviderFactory pageSourceProviderFactory, LocalMemoryContext sharedMemoryContext)
        {
            requireNonNull(sharedMemoryContext, "sharedMemoryContext is null");
            this.pageSourceProvider = pageSourceProviderFactory.createPageSourceProvider(sharedMemoryContext::setBytes);
            referenceCount.getFreeFuture().addListener(sharedMemoryContext::close, directExecutor());
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
        public void retain()
        {
            referenceCount.retain();
        }

        @Override
        public void release()
        {
            referenceCount.release();
        }
    }
}
