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
package io.trino.plugin.varada.dispatcher.warmup;

import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.annotations.ForWarp;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmingManager;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmupElementsCreator;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.varada.cloudvendors.configuration.CloudVendorConfiguration;

import java.util.List;

import static io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static java.util.Objects.requireNonNull;

@Singleton
public class WarmExecutionTaskFactory
{
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final EventBus eventBus;
    private final WarmingManager warmingManager;
    private final VaradaStatsWarmingService statsWarmingService;
    private final QueryClassifier queryClassifier;
    private final RowGroupDataService rowGroupDataService;
    private final GlobalConfiguration globalConfiguration;
    private final WarmupElementsCreator warmupElementsCreator;
    private final WorkerTaskExecutorService workerTaskExecutorService;
    private final StorageWarmerService storageWarmerService;
    private final CloudVendorConfiguration cloudVendorConfiguration;

    @Inject
    public WarmExecutionTaskFactory(DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            EventBus eventBus,
            WarmingManager warmingManager,
            MetricsManager metricsManager,
            QueryClassifier queryClassifier,
            RowGroupDataService rowGroupDataService,
            GlobalConfiguration globalConfiguration,
            WarmupElementsCreator warmupElementsCreator,
            WorkerTaskExecutorService workerTaskExecutorService,
            StorageWarmerService storageWarmerService,
            @ForWarp CloudVendorConfiguration cloudVendorConfiguration)
    {
        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
        this.eventBus = requireNonNull(eventBus);
        this.warmingManager = requireNonNull(warmingManager);
        this.queryClassifier = requireNonNull(queryClassifier);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.statsWarmingService = metricsManager.registerMetric(VaradaStatsWarmingService.create(WARMING_SERVICE_STAT_GROUP));
        this.globalConfiguration = requireNonNull(globalConfiguration);
        this.warmupElementsCreator = requireNonNull(warmupElementsCreator);
        this.workerTaskExecutorService = requireNonNull(workerTaskExecutorService);
        this.storageWarmerService = requireNonNull(storageWarmerService);
        this.cloudVendorConfiguration = requireNonNull(cloudVendorConfiguration);
    }

    public WorkerSubmittableTask createExecutionTask(ConnectorPageSourceProvider connectorPageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            DispatcherSplit dispatcherSplit,
            DispatcherTableHandle dispatcherTableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            RowGroupKey rowGroupKey,
            WorkerWarmingService workerWarmingService,
            int iterationCount,
            int executionTaskPriority,
            WorkerTaskExecutorService.TaskExecutionType taskExecutionType)
    {
        return switch (taskExecutionType) {
            case CLASSIFY -> new PrioritizeTask(this,
                    workerWarmingService,
                    connectorPageSourceProvider,
                    transactionHandle,
                    session,
                    dispatcherTableHandle,
                    rowGroupKey,
                    columns,
                    dispatcherSplit,
                    dynamicFilter,
                    rowGroupDataService,
                    queryClassifier,
                    workerTaskExecutorService,
                    iterationCount,
                    statsWarmingService,
                    warmingManager,
                    warmupElementsCreator,
                    globalConfiguration,
                    cloudVendorConfiguration);
            case PROXY -> new ProxyExecutionTask(this,
                        eventBus,
                        dispatcherProxiedConnectorTransformer,
                        warmingManager,
                    statsWarmingService,
                    workerWarmingService,
                    connectorPageSourceProvider,
                    transactionHandle,
                    session,
                    dispatcherTableHandle,
                    rowGroupKey,
                    columns,
                    dispatcherSplit,
                    dynamicFilter,
                    rowGroupDataService,
                    globalConfiguration,
                    queryClassifier,
                    warmupElementsCreator,
                    iterationCount,
                    executionTaskPriority,
                    workerTaskExecutorService,
                        storageWarmerService);
            case IMPORT -> new ImportExecutionTask(this,
                    statsWarmingService,
                    workerWarmingService,
                    connectorPageSourceProvider,
                    transactionHandle,
                    session,
                    dispatcherTableHandle,
                    rowGroupKey,
                    columns,
                    dispatcherSplit,
                    dynamicFilter,
                    rowGroupDataService,
                    queryClassifier,
                    workerTaskExecutorService,
                    warmingManager,
                    warmupElementsCreator,
                    iterationCount,
                    executionTaskPriority);
            default -> throw new RuntimeException("no task exists");
        };
    }
}
