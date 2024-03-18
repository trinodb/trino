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
package io.trino.plugin.varada.dispatcher;

import io.airlift.log.Logger;
import io.trino.plugin.varada.annotations.ForWarp;
import io.trino.plugin.varada.juffer.StorageEngineTxService;
import io.trino.plugin.varada.metrics.CustomStatsContext;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorAlternativePageSourceProvider;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.varada.tools.CatalogNameProvider;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class DispatcherAlternativePageSourceProvider
        implements ConnectorAlternativePageSourceProvider
{
    private static final Logger logger = Logger.get(DispatcherAlternativePageSourceProvider.class);

    private final ConnectorPageSourceProvider connectorPageSourceProvider;
    private final DispatcherPageSourceFactory pageSourceFactory;
    private final StorageEngineTxService txService;
    private final CustomStatsContext customStatsContext;
    private final String catalogName;
    private final ConnectorSplit split;
    private final ConnectorTableHandle table;
    private final DispatcherAlternativeChooser.ResourceCloser resourceCloser;

    public DispatcherAlternativePageSourceProvider(
            @ForWarp ConnectorPageSourceProvider connectorPageSourceProvider,
            DispatcherPageSourceFactory pageSourceFactory,
            StorageEngineTxService txService,
            CustomStatsContext customStatsContext,
            CatalogNameProvider catalogNameProvider,
            ConnectorSplit split,
            ConnectorTableHandle table,
            DispatcherAlternativeChooser.ResourceCloser resourceCloser)
    {
        this.connectorPageSourceProvider = requireNonNull(connectorPageSourceProvider);
        this.pageSourceFactory = requireNonNull(pageSourceFactory);
        this.txService = requireNonNull(txService);
        this.customStatsContext = requireNonNull(customStatsContext);
        this.catalogName = requireNonNull(catalogNameProvider).get();
        this.split = requireNonNull(split);
        this.table = requireNonNull(table);
        this.resourceCloser = requireNonNull(resourceCloser);
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            boolean splitAddressEnforced)
    {
        if (logger.isDebugEnabled()) {
            logger.debug("createPageSource: handle=%s, split=%s, table=%s, columns=%s, dynamicFilter=%s",
                    transactionHandle, split, table, columns, dynamicFilter.getCurrentPredicate().toString(session));
        }
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) table;
        DispatcherSplit dispatcherSplit = (DispatcherSplit) split;

        DispatcherWrapperPageSource dispatcherWrapperPageSource = new DispatcherWrapperPageSource(connectorPageSourceProvider,
                pageSourceFactory,
                txService,
                customStatsContext,
                transactionHandle,
                session,
                dispatcherSplit,
                dispatcherTableHandle,
                columns,
                dynamicFilter,
                catalogName);

        // so the resources will continue to be locked even if the page source provider will be closed
        // immediately after creating the page source (for instance, rowGroupData.getLock().getCount() will be 2 after this call)
        dispatcherWrapperPageSource.getConnectorPageSource();

        return dispatcherWrapperPageSource;
    }

    @Override
    public void close()
    {
        resourceCloser.close();
    }
}
