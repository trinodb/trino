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

import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static java.util.Objects.requireNonNull;

public class PageSourceManager
        implements PageSourceProvider
{
    private final CatalogServiceProvider<ConnectorPageSourceProvider> pageSourceProvider;

    @Inject
    public PageSourceManager(CatalogServiceProvider<ConnectorPageSourceProvider> pageSourceProvider)
    {
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
    }

    @Override
    public ConnectorPageSource createPageSource(Session session, Split split, TableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
    {
        requireNonNull(columns, "columns is null");
        checkArgument(split.getCatalogHandle().equals(table.getCatalogHandle()), "mismatched split and table");
        CatalogHandle catalogHandle = split.getCatalogHandle();

        ConnectorPageSourceProvider provider = pageSourceProvider.getService(catalogHandle);
        TupleDomain<ColumnHandle> constraint = dynamicFilter.getCurrentPredicate();
        if (constraint.isNone()) {
            return new EmptyPageSource();
        }
        if (!isAllowPushdownIntoConnectors(session)) {
            dynamicFilter = DynamicFilter.EMPTY;
        }
        return provider.createPageSource(
                table.getTransaction(),
                session.toConnectorSession(catalogHandle),
                split.getConnectorSplit(),
                table.getConnectorHandle(),
                columns,
                dynamicFilter);
    }
}
