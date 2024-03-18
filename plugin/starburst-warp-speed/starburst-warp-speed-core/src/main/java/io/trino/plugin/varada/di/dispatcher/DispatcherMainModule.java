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
package io.trino.plugin.varada.di.dispatcher;

import com.google.inject.Binder;
import io.trino.plugin.varada.di.ExtraModule;
import io.trino.plugin.varada.di.VaradaBaseModule;
import io.trino.plugin.varada.dictionary.AttachDictionaryService;
import io.trino.plugin.varada.dictionary.DictionaryCacheService;
import io.trino.plugin.varada.dispatcher.DispatcherAlternativeChooser;
import io.trino.plugin.varada.dispatcher.DispatcherPageSinkProvider;
import io.trino.plugin.varada.dispatcher.DispatcherPageSourceFactory;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandleBuilderProvider;
import io.trino.plugin.varada.dispatcher.ReadErrorHandler;
import io.trino.plugin.varada.dispatcher.cache.CoordinatorCacheManager;
import io.trino.plugin.varada.dispatcher.cache.WorkerCacheManager;
import io.trino.plugin.varada.dispatcher.connectors.CoordinatorDispatcherConnector;
import io.trino.plugin.varada.dispatcher.connectors.DispatcherConnectorBase;
import io.trino.plugin.varada.dispatcher.connectors.SingleDispatcherConnector;
import io.trino.plugin.varada.dispatcher.connectors.WorkerDispatcherConnector;
import io.trino.plugin.varada.dispatcher.dal.RowGroupDataDao;
import io.trino.plugin.varada.dispatcher.query.MatchCollectIdService;
import io.trino.plugin.varada.dispatcher.query.classifier.ClassifierFactory;
import io.trino.plugin.varada.dispatcher.query.classifier.PredicateContextFactory;
import io.trino.plugin.varada.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WarmExecutionTaskFactory;
import io.trino.plugin.varada.dispatcher.warmup.WorkerTaskExecutorService;
import io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService;
import io.trino.plugin.varada.dispatcher.warmup.WorkerWarmupRuleService;
import io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.varada.dispatcher.warmup.export.WarmupElementsCloudExporter;
import io.trino.plugin.varada.dispatcher.warmup.export.WarmupExportingService;
import io.trino.plugin.varada.dispatcher.warmup.transform.BlockTransformerFactory;
import io.trino.plugin.varada.dispatcher.warmup.warmers.CacheWarmer;
import io.trino.plugin.varada.dispatcher.warmup.warmers.EmptyRowGroupWarmer;
import io.trino.plugin.varada.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.varada.dispatcher.warmup.warmers.VaradaProxiedWarmer;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmingManager;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmupElementsCreator;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WeGroupWarmer;
import io.trino.plugin.varada.juffer.PredicatesCacheService;
import io.trino.plugin.varada.storage.read.ChunksQueueService;
import io.trino.plugin.varada.storage.read.CollectTxService;
import io.trino.plugin.varada.storage.read.StorageCollectorService;
import io.trino.plugin.varada.storage.read.fill.BlockFillersFactory;
import io.trino.plugin.varada.storage.write.StorageWriterService;
import io.trino.plugin.varada.storage.write.VaradaPageSinkFactory;
import io.trino.plugin.varada.storage.write.WarpCacheFilesMerger;
import io.trino.plugin.varada.storage.write.appenders.BlockAppenderFactory;
import io.trino.plugin.varada.storage.write.dictionary.DictionaryWriterFactory;
import io.trino.plugin.varada.util.FailureGeneratorInvocationHandler;
import io.trino.plugin.varada.warmup.dal.WarmupRuleDao;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorContext;

import java.util.Map;

public class DispatcherMainModule
        implements ExtraModule
{
    private final String catalogName;
    private ConnectorContext context;
    private Map<String, String> config;

    public DispatcherMainModule(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        this.catalogName = catalogName;
        this.config = config;
        this.context = context;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(FailureGeneratorInvocationHandler.class);
        if (VaradaBaseModule.isWorker(context, config)) {
            binder.bind(EmptyRowGroupWarmer.class);
            binder.bind(RowGroupDataService.class);
            binder.bind(BlockFillersFactory.class);
            binder.bind(DictionaryWriterFactory.class);
            binder.bind(DispatcherPageSourceFactory.class);
            binder.bind(DispatcherAlternativeChooser.class);
            binder.bind(DispatcherPageSinkProvider.class);
            binder.bind(StorageWarmerService.class);
            binder.bind(WarmupDemoterService.class);
            binder.bind(WorkerWarmingService.class);
            binder.bind(WorkerWarmupRuleService.class);
            binder.bind(PredicatesCacheService.class);
            binder.bind(ClassifierFactory.class);
            binder.bind(PredicateContextFactory.class);
            binder.bind(QueryClassifier.class);
            binder.bind(MatchCollectIdService.class);
            binder.bind(WarmupExportingService.class);
            binder.bind(WorkerTaskExecutorService.class);
            binder.bind(VaradaProxiedWarmer.class);
            binder.bind(WarmupElementsCreator.class);
            binder.bind(WarmExecutionTaskFactory.class);
            binder.bind(WarmingManager.class);
            binder.bind(WeGroupWarmer.class);
            binder.bind(WarmupElementsCloudExporter.class);
            binder.bind(DictionaryCacheService.class);
            binder.bind(AttachDictionaryService.class);
            binder.bind(VaradaPageSinkFactory.class);
            binder.bind(DispatcherPageSourceFactory.class);
            binder.bind(ReadErrorHandler.class);
            binder.bind(RowGroupDataDao.class);
            binder.bind(CollectTxService.class);
            binder.bind(ChunksQueueService.class);
            binder.bind(StorageCollectorService.class);
            binder.bind(StorageWriterService.class);
            binder.bind(BlockAppenderFactory.class);
            binder.bind(BlockTransformerFactory.class);
            binder.bind(WarpCacheFilesMerger.class);
        }
        if (VaradaBaseModule.isSingle(config)) {
            binder.bind(DispatcherConnectorBase.class).to(SingleDispatcherConnector.class);
            binder.bind(CoordinatorDispatcherConnector.class);
            binder.bind(WorkerDispatcherConnector.class);
            binder.bind(CacheManager.class).to(WorkerCacheManager.class);
            binder.bind(CacheWarmer.class);
        }
        else if (VaradaBaseModule.isCoordinator(context)) {
            binder.bind(CacheManager.class).to(CoordinatorCacheManager.class);
            binder.bind(DispatcherConnectorBase.class).to(CoordinatorDispatcherConnector.class);
        }
        else {
            binder.bind(CacheManager.class).to(WorkerCacheManager.class);
            binder.bind(DispatcherConnectorBase.class).to(WorkerDispatcherConnector.class);
            binder.bind(CacheWarmer.class);
        }
        binder.bind(WarmupRuleDao.class);
        binder.bind(DispatcherTableHandleBuilderProvider.class);
        binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
    }

    @Override
    public DispatcherMainModule withConfig(Map<String, String> config)
    {
        this.config = config;
        return this;
    }

    @Override
    public DispatcherMainModule withContext(ConnectorContext context)
    {
        this.context = context;
        return this;
    }
}
