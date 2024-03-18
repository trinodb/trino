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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.VaradaErrorCode;
import io.trino.plugin.varada.dictionary.DictionaryWarmInfo;
import io.trino.plugin.varada.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.flows.FlowIdGenerator;
import io.trino.plugin.varada.storage.write.PageSink;
import io.trino.plugin.varada.storage.write.StorageWriterService;
import io.trino.plugin.varada.storage.write.StorageWriterSplitConfiguration;
import io.trino.plugin.varada.storage.write.VaradaPageSinkFactory;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;
import io.trino.spi.TrinoException;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static io.trino.plugin.varada.dispatcher.warmup.warmers.StorageWarmerService.INVALID_TX_ID;
import static java.util.Objects.requireNonNull;

@Singleton
public class CacheWarmer
{
    private static final Logger logger = Logger.get(CacheWarmer.class);

    private final RowGroupDataService rowGroupDataService;
    private final WarmupElementsCreator warmupElementsCreator;
    private final VaradaStatsWarmingService statsWarmingService;
    private final VaradaPageSinkFactory varadaPageSinkFactory;
    private final StorageWarmerService storageWarmerService;
    private final StorageWriterService storageWriterService;
    private final UUID storeId = UUID.randomUUID(); //todo: change to inner function when support multi column warming
    private final AtomicInteger tmpUniqueKeyMarker;

    @Inject
    public CacheWarmer(RowGroupDataService rowGroupDataService,
            WarmupElementsCreator warmupElementsCreator,
            MetricsManager metricsManager,
            VaradaPageSinkFactory varadaPageSinkFactory,
            StorageWarmerService storageWarmerService,
            StorageWriterService storageWriterService)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.warmupElementsCreator = requireNonNull(warmupElementsCreator);
        this.statsWarmingService = metricsManager.registerMetric(VaradaStatsWarmingService.create(WARMING_SERVICE_STAT_GROUP));
        this.varadaPageSinkFactory = requireNonNull(varadaPageSinkFactory);
        this.storageWarmerService = requireNonNull(storageWarmerService);
        this.storageWriterService = requireNonNull(storageWriterService);
        this.tmpUniqueKeyMarker = new AtomicInteger(0);
    }

    public Optional<WarmupElementWriteMetadata> getWarmupElementWriteMetadata(List<CacheColumnId> columns,
            List<Type> columnsTypes,
            RowGroupKey rowGroupKey)
    {
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);

        Set<VaradaColumn> existingColumns = Collections.emptySet();
        if (rowGroupData != null) {
            existingColumns = rowGroupData.getWarmUpElements().stream()
                    .map(WarmUpElement::getVaradaColumn)
                    .collect(Collectors.toSet());
        }

        Optional<WarmupElementWriteMetadata> result = Optional.empty();
        for (int i = 0; i < columns.size(); i++) {
            RegularColumn currentColumn = new RegularColumn(columns.get(i).toString());
            if (!existingColumns.contains(currentColumn)) {
                result = createCacheWarmupElement(rowGroupKey, columns.get(i), columnsTypes.get(i), i, storeId);
                break;
            }
        }
        return result;
    }

    private Optional<WarmupElementWriteMetadata> createCacheWarmupElement(RowGroupKey rowGroupKey, CacheColumnId cacheColumnId, Type type, int connectorBlockIndex, UUID storeId)
    {
        Optional<WarmupElementWriteMetadata> res = Optional.empty();
        Optional<WarmUpElement> warmupElement = warmupElementsCreator.createWarmupElement(cacheColumnId, type, storeId);
        SchemaTableName schemaTableColumn = new SchemaTableName(rowGroupKey.schema(), rowGroupKey.table());
        if (warmupElement.isPresent()) {
            res = Optional.of(WarmupElementWriteMetadata.builder()
                    .warmUpElement(warmupElement.get())
                    .connectorBlockIndex(connectorBlockIndex)
                    .type(type)
                    .schemaTableColumn(new SchemaTableColumn(schemaTableColumn, warmupElement.get().getVaradaColumn()))
                    .build());
        }
        return res;
    }

    public WarmingCacheData initCacheWarming(RowGroupKey permanentRowGroupKey,
            WarmupElementWriteMetadata warmUpElementToWarm)
            throws IOException, InterruptedException, ExecutionException
    {
        long fileCookie;
        boolean locked;
        PageSink pageSink;
        long flowId;
        int fileOffset;
        int txId = INVALID_TX_ID;
        StorageWriterSplitConfiguration storageWriterSplitConfiguration = storageWriterService.startWarming("WarpCacheManager", permanentRowGroupKey.filePath(), false);
        RowGroupKey tempRowGroupKey = getTempKeyFile(warmUpElementToWarm, permanentRowGroupKey);
        try {
            flowId = FlowIdGenerator.generateFlowId();
            storageWarmerService.tryRunningWarmFlow(flowId, tempRowGroupKey);
            statsWarmingService.incwarm_started();
            pageSink = varadaPageSinkFactory.create(storageWriterSplitConfiguration);

            RowGroupData rowGroupData = rowGroupDataService.getOrCreateRowGroupData(tempRowGroupKey, Collections.emptyMap());
            locked = storageWarmerService.lockRowGroup(rowGroupData);
            storageWarmerService.createFile(tempRowGroupKey);
            fileCookie = storageWarmerService.fileOpen(tempRowGroupKey);
            txId = storageWarmerService.warmupOpen(txId);
            fileOffset = getFileOffset(tempRowGroupKey);
            List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();
            boolean success = pageSink.open(txId, fileCookie, fileOffset, warmUpElementToWarm, outDictionaryWarmInfos);
            if (!success) {
                throw new TrinoException(VaradaErrorCode.VARADA_WARMUP_OPEN_ERROR, "failed to open warmup element for write");
            }
        }
        catch (Exception e) {
            logger.error(e, "failed to init warm for key=%s. %s", permanentRowGroupKey, warmUpElementToWarm);
            throw e;
        }
        return new WarmingCacheData(fileCookie, pageSink, flowId, fileOffset, locked, txId, tempRowGroupKey, storageWriterSplitConfiguration);
    }

    public void finishCacheWarming(WarmingCacheData warmingCacheData)
    {
        storageWriterService.finishWarming(warmingCacheData.storageWriterSplitConfiguration());
    }

    private int getFileOffset(RowGroupKey rowGroupKey)
    {
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        return (rowGroupData != null) ? rowGroupData.getNextOffset() : 0;
    }

    public void warmEmptyPageSource(RowGroupData rowGroupData, WarmupElementWriteMetadata warmupElementWriteMetadata)
    {
        WarmUpElement warmUpElement = WarmUpElement.builder(warmupElementWriteMetadata.warmUpElement())
                .state(WarmUpElementState.VALID)
                .build();
        rowGroupDataService.updateRowGroupData(rowGroupData, warmUpElement, 0, 0);
    }

    private RowGroupKey getTempKeyFile(WarmupElementWriteMetadata warmupElementWriteMetadata, RowGroupKey permanentRowGroupKey)
    {
        String uniqueKey = permanentRowGroupKey.table();
        uniqueKey = uniqueKey + tmpUniqueKeyMarker.incrementAndGet();
        return new RowGroupKey("TMP_CACHE_MANAGER_" + warmupElementWriteMetadata.warmUpElement().getWarmUpType(),
                uniqueKey,
                "",
                0,
                0,
                0,
                "",
                permanentRowGroupKey.catalogName());
    }
}
