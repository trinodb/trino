/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.dynamicfiltering;

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordPageSource;

import java.util.List;

import static com.starburstdata.trino.plugins.dynamicfiltering.DynamicRowFilteringSessionProperties.getDynamicRowFilterSelectivityThreshold;
import static com.starburstdata.trino.plugins.dynamicfiltering.DynamicRowFilteringSessionProperties.getDynamicRowFilteringWaitTimeout;
import static com.starburstdata.trino.plugins.dynamicfiltering.DynamicRowFilteringSessionProperties.isDynamicRowFilteringEnabled;
import static java.util.Objects.requireNonNull;

public class DynamicRowFilteringPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final ConnectorPageSourceProvider delegatePageSourceProvider;
    private final DynamicPageFilterCache dynamicPageFilterCache;

    @Inject
    public DynamicRowFilteringPageSourceProvider(
            @ForDynamicRowFiltering ConnectorPageSourceProvider delegatePageSourceProvider,
            DynamicPageFilterCache dynamicPageFilterCache)
    {
        this.delegatePageSourceProvider = requireNonNull(delegatePageSourceProvider, "delegatePageSourceProvider is null");
        this.dynamicPageFilterCache = requireNonNull(dynamicPageFilterCache, "dynamicPageFilterCollector is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        ConnectorPageSource pageSource = delegatePageSourceProvider.createPageSource(transaction, session, split, tableHandle, columns, dynamicFilter);
        if (dynamicFilter == DynamicFilter.EMPTY
                || !isDynamicRowFilteringEnabled(session)
                || pageSource instanceof RecordPageSource) {
            return pageSource;
        }
        if (dynamicFilter.isComplete() && dynamicFilter.getCurrentPredicate().isAll()) {
            return pageSource;
        }

        return new DynamicRowFilteringPageSource(
                pageSource,
                getDynamicRowFilterSelectivityThreshold(session),
                getDynamicRowFilteringWaitTimeout(session),
                columns,
                dynamicPageFilterCache.getDynamicPageFilter(dynamicFilter, columns));
    }
}
