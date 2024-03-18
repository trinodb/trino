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
package io.trino.plugin.varada.dispatcher.warmup.warmers;

import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.annotations.ForWarp;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dictionary.DictionaryCacheService;
import io.trino.plugin.varada.dictionary.DictionaryWarmInfo;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.model.DictionaryState;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.warp.gen.stats.VaradaStatsDictionary;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmupImportService;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.varada.cloudvendors.configuration.CloudVendorConfiguration;
import io.varada.tools.util.StopWatch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.varada.dictionary.DictionaryCacheService.DICTIONARY_STAT_GROUP;
import static io.trino.plugin.varada.dispatcher.warmup.WarmUtils.isImportExportEnabled;
import static io.trino.plugin.varada.dispatcher.warmup.warmers.WeGroupWarmer.WARMUP_IMPORTER_STAT_GROUP;
import static java.util.Objects.requireNonNull;

@Singleton
public class WarmingManager
{
    private static final Logger logger = Logger.get(WarmingManager.class);

    private final VaradaStatsDictionary varadaStatsDictionary;
    private final VaradaProxiedWarmer varadaProxiedWarmer;
    private final EmptyRowGroupWarmer emptyRowGroupWarmer;
    private final GlobalConfiguration globalConfiguration;
    private final CloudVendorConfiguration cloudVendorConfiguration;
    private final RowGroupDataService rowGroupDataService;
    private final VaradaStatsWarmupImportService varadaStatsWarmupImportService;
    private final DictionaryCacheService dictionaryCacheService;
    private final WeGroupWarmer weGroupWarmer;
    private final StorageWarmerService storageWarmerService;

    @Inject
    public WarmingManager(
            VaradaProxiedWarmer varadaProxiedWarmer,
            EmptyRowGroupWarmer emptyRowGroupWarmer,
            GlobalConfiguration globalConfiguration,
            @ForWarp CloudVendorConfiguration cloudVendorConfiguration,
            RowGroupDataService rowGroupDataService,
            MetricsManager metricsManager,
            DictionaryCacheService dictionaryCacheService,
            WeGroupWarmer weGroupWarmer,
            StorageWarmerService storageWarmerService)
    {
        this.varadaProxiedWarmer = requireNonNull(varadaProxiedWarmer);
        this.emptyRowGroupWarmer = requireNonNull(emptyRowGroupWarmer);
        this.globalConfiguration = requireNonNull(globalConfiguration);
        this.cloudVendorConfiguration = requireNonNull(cloudVendorConfiguration);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.varadaStatsWarmupImportService = requireNonNull(metricsManager).registerMetric(new VaradaStatsWarmupImportService(WARMUP_IMPORTER_STAT_GROUP));
        this.varadaStatsDictionary = requireNonNull(metricsManager).registerMetric(VaradaStatsDictionary.create(DICTIONARY_STAT_GROUP));
        this.dictionaryCacheService = requireNonNull(dictionaryCacheService);
        this.weGroupWarmer = requireNonNull(weGroupWarmer);
        this.storageWarmerService = requireNonNull(storageWarmerService);
    }

    public Optional<RowGroupData> importWeGroup(ConnectorSession session, RowGroupKey rowGroupKey, List<WarmUpElement> warmWarmUpElements)
    {
        if (isImportExportEnabled(globalConfiguration, cloudVendorConfiguration, session)) {
            if (warmWarmUpElements.isEmpty()) {
                return weGroupWarmer.importWeGroup(session, rowGroupKey);
            }
            else {
                return weGroupWarmer.importWarmUpElements(session, rowGroupKey, warmWarmUpElements);
            }
        }
        logger.debug("importWeGroup isImportExportEnabled = false");
        return Optional.empty();
    }

    public void warm(RowGroupKey rowGroupKey,
            ConnectorPageSourceProvider connectorPageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            DispatcherTableHandle dispatcherTableHandle,
            DispatcherSplit dispatcherSplit,
            List<ColumnHandle> columnsToWarm,
            SetMultimap<VaradaColumn, WarmupProperties> requiredWarmUpTypeMap,
            List<WarmUpElement> newWarmupElements,
            Map<VaradaColumn, String> partitionKeys,
            boolean skipWait)
            throws InterruptedException
    {
        List<DictionaryWarmInfo> outDictionariesWarmInfos = new ArrayList<>();
        RowGroupData rowGroupData = null;
        boolean locked = false;

        try {
            rowGroupData = rowGroupDataService.getOrCreateRowGroupData(rowGroupKey, partitionKeys);
            locked = storageWarmerService.lockRowGroup(rowGroupData);

            try {
                //TODO add logic of rowGroupDataService.verifyUpdatedRowGroupData
                if (!newWarmupElements.isEmpty()) {
                    StopWatch stopWatch = new StopWatch();
                    stopWatch.start();
                    rowGroupData = varadaProxiedWarmer.warm(connectorPageSourceProvider,
                            transactionHandle,
                            session,
                            dispatcherTableHandle,
                            rowGroupKey,
                            rowGroupData,
                            dispatcherSplit,
                            columnsToWarm,
                            newWarmupElements,
                            requiredWarmUpTypeMap,
                            skipWait,
                            (rowGroupData != null) ? rowGroupData.getNextOffset() : 0,
                            outDictionariesWarmInfos);
                    stopWatch.stop();
                    varadaStatsWarmupImportService.addhiveWarmTime(stopWatch.getNanoTime());
                }
            }
            catch (Exception e) {
                rowGroupDataService.markAsFailed(rowGroupKey, newWarmupElements, partitionKeys);
                throw e;
            }
            finally {
                releaseActiveDictionaries(outDictionariesWarmInfos);
            }
        }
        catch (InterruptedException e) {
            logger.warn(e, "failed to acquire write lock for row group %s", rowGroupKey);
            throw e;
        }
        finally {
            storageWarmerService.releaseRowGroup(rowGroupData, locked);
        }
    }

    public void warmEmptyRowGroup(RowGroupKey rowGroupKey, List<WarmUpElement> newWarmUpElements)
    {
        emptyRowGroupWarmer.warm(rowGroupKey, newWarmUpElements);
    }

    public void saveEmptyRowGroup(RowGroupKey rowGroupKey, List<WarmUpElement> newWarmUpElements, Map<VaradaColumn, String> partitionKeys)
    {
        emptyRowGroupWarmer.saveImportedEmptyRowGroup(newWarmUpElements, rowGroupKey, partitionKeys);
    }

    private void releaseActiveDictionaries(List<DictionaryWarmInfo> dictionariesWarmInfos)
    {
        for (DictionaryWarmInfo dictionaryWarmInfo : dictionariesWarmInfos) {
            if (dictionaryWarmInfo.dictionaryState() == DictionaryState.DICTIONARY_REJECTED) {
                varadaStatsDictionary.incdictionary_rejected_elements_count();
            }
            else if (dictionaryWarmInfo.dictionaryState() == DictionaryState.DICTIONARY_VALID) {
                dictionaryCacheService.releaseActiveDictionary(dictionaryWarmInfo.dictionaryKey());
                varadaStatsDictionary.incdictionary_success_elements_count();
            }
        }
    }
}
