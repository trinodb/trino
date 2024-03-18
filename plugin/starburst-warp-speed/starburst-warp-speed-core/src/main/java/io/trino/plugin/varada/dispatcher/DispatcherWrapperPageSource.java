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

import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.varada.dictionary.DictionaryCacheService;
import io.trino.plugin.varada.juffer.StorageEngineTxService;
import io.trino.plugin.varada.metrics.CustomStatsContext;
import io.trino.plugin.warp.gen.stats.VaradaStatsDictionary;
import io.trino.plugin.warp.gen.stats.VaradaStatsDispatcherPageSource;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class DispatcherWrapperPageSource
        implements ConnectorPageSource
{
    private static final String CUSTOM_METRIC_TABLE_NAME = "TABLE_NAME:";
    private static final String CUSTOM_METRIC_SCHEMA_NAME = "SCHEMA_NAME:";
    private static final String CUSTOM_METRIC_CATALOG_NAME = "CATALOG_NAME:";

    private final ConnectorPageSourceProvider connectorPageSourceProvider;
    private final DispatcherPageSourceFactory pageSourceFactory;
    private final StorageEngineTxService txService;
    private final ConnectorTransactionHandle transactionHandle;
    private final ConnectorSession session;
    private final DispatcherSplit dispatcherSplit;
    private final DispatcherTableHandle dispatcherTableHandle;
    private final List<ColumnHandle> columns;
    private final CustomStatsContext customStatsContext;
    private final DynamicFilter dynamicFilter;
    private final String catalogName;

    private ConnectorPageSource connectorPageSource;

    private boolean closed;

    public DispatcherWrapperPageSource(ConnectorPageSourceProvider pageSourceProvider,
            DispatcherPageSourceFactory pageSourceFactory,
            StorageEngineTxService txService,
            CustomStatsContext customStatsContext,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            DispatcherSplit dispatcherSplit,
            DispatcherTableHandle dispatcherTableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            String catalogName)
    {
        this.connectorPageSourceProvider = pageSourceProvider;
        this.pageSourceFactory = pageSourceFactory;
        this.txService = txService;
        this.customStatsContext = customStatsContext;
        this.transactionHandle = transactionHandle;
        this.session = session;
        this.dispatcherSplit = dispatcherSplit;
        this.dispatcherTableHandle = dispatcherTableHandle;
        this.columns = columns;
        this.dynamicFilter = dynamicFilter;
        this.catalogName = catalogName;
    }

    private ConnectorPageSource buildDelegatePageSource()
    {
        customStatsContext.getOrRegister(new VaradaStatsDispatcherPageSource(DispatcherPageSourceFactory.STATS_DISPATCHER_KEY));
        customStatsContext.getOrRegister(new VaradaStatsDictionary(DictionaryCacheService.DICTIONARY_STAT_GROUP));
        return pageSourceFactory.createConnectorPageSource(connectorPageSourceProvider,
                transactionHandle,
                session,
                dispatcherSplit,
                dispatcherTableHandle,
                columns,
                dynamicFilter,
                customStatsContext);
    }

    @Override
    public long getCompletedBytes()
    {
        return (connectorPageSource != null) ? connectorPageSource.getCompletedBytes() : 0;
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return (connectorPageSource != null) ? connectorPageSource.getCompletedPositions() : OptionalLong.empty();
    }

    @Override
    public long getReadTimeNanos()
    {
        return (connectorPageSource != null) ? connectorPageSource.getReadTimeNanos() : 0;
    }

    @Override
    public boolean isFinished()
    {
        return (connectorPageSource != null) && connectorPageSource.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        return getConnectorPageSource().getNextPage();
    }

    @Override
    public long getMemoryUsage()
    {
        if (connectorPageSource != null) {
            return connectorPageSource.getMemoryUsage();
        }
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            if (connectorPageSource != null) {
                txService.updateRunningPageSourcesCount(false);
                connectorPageSource.close();
            }
        }
        finally {
            customStatsContext.copyStatsToGlobalMetricsManager();
            closed = true;
        }
    }

    @Override
    public Metrics getMetrics()
    {
        Metrics.Accumulator result = Metrics.accumulator();
        if (closed) {
            Map<String, Long> statsMap = new TreeMap<>();
            customStatsContext.getRegisteredStats().forEach((key, value) -> statsMap.putAll(value.statsCounterMapper()));
            statsMap.putAll(customStatsContext.getFixedStats());
            statsMap.put(CUSTOM_METRIC_SCHEMA_NAME + dispatcherTableHandle.getSchemaTableName().getSchemaName(), 0L);
            statsMap.put(CUSTOM_METRIC_TABLE_NAME + dispatcherTableHandle.getSchemaTableName().getTableName(), 0L);
            statsMap.put(CUSTOM_METRIC_CATALOG_NAME + catalogName, 0L);

            Map<String, Metric<?>> metricsMap = statsMap.entrySet().stream().collect(toImmutableMap(
                    Map.Entry::getKey,
                    entry -> new LongCount(entry.getValue())));
            result.add(new Metrics(metricsMap));
        }
        if (connectorPageSource != null) {
            result.add(getConnectorPageSource().getMetrics());
            return result.get();
        }
        else {
            return Metrics.EMPTY;
        }
    }

    public ConnectorPageSource getConnectorPageSource()
    {
        if (connectorPageSource == null) {
            connectorPageSource = buildDelegatePageSource();
            if (connectorPageSource != null) {
                txService.updateRunningPageSourcesCount(true);
            }
        }
        return connectorPageSource;
    }
}
