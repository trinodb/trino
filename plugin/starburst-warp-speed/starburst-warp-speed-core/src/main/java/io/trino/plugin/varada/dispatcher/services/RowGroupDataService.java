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
package io.trino.plugin.varada.dispatcher.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.dal.RowGroupDataDao;
import io.trino.plugin.varada.dispatcher.model.FastWarmingState;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmState;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.varada.tools.util.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.varada.VaradaErrorCode.VARADA_ROW_GROUP_ILLEGAL_STATE;
import static io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static java.util.Objects.requireNonNull;

@Singleton
public class RowGroupDataService
{
    private static final Logger logger = Logger.get(RowGroupDataService.class);

    private final StorageEngine storageEngine;
    private final GlobalConfiguration globalConfiguration;
    private final RowGroupDataDao rowGroupDataDao;
    private final VaradaStatsWarmingService varadaStatsWarmingService;
    private final ConnectorSync connectorSync;
    private final String nodeIdentifier;

    @Inject
    public RowGroupDataService(RowGroupDataDao rowGroupDataDao,
            StorageEngine storageEngine,
            GlobalConfiguration globalConfiguration,
            MetricsManager metricsManager,
            NodeManager nodeManager,
            ConnectorSync connectorSync)
    {
        this.rowGroupDataDao = requireNonNull(rowGroupDataDao);
        this.storageEngine = requireNonNull(storageEngine);
        this.globalConfiguration = requireNonNull(globalConfiguration);
        this.varadaStatsWarmingService = metricsManager.registerMetric(VaradaStatsWarmingService.create(WARMING_SERVICE_STAT_GROUP));
        this.nodeIdentifier = requireNonNull(nodeManager).getCurrentNode().getNodeIdentifier();
        this.connectorSync = requireNonNull(connectorSync);
    }

    public RowGroupKey createRowGroupKey(String schema,
            String table,
            String path,
            long start,
            long length,
            long fileModifiedTime,
            String deletedFilesHash)
    {
        return new RowGroupKey(schema,
                table,
                path,
                start,
                length,
                fileModifiedTime,
                deletedFilesHash,
                connectorSync.getCatalogName());
    }

    public void save(RowGroupData rowGroupData)
    {
        rowGroupDataDao.save(rowGroupData);
    }

    public void flush(RowGroupKey rowGroupKey)
    {
        rowGroupDataDao.flush(rowGroupKey);
    }

    public void updateEmptyRowGroup(RowGroupData rowGroupData,
            List<WarmUpElement> newWarmUpElements,
            List<WarmUpElement> warmUpElementsToDelete)
    {
        if (!rowGroupData.isEmpty()) {
            throw new TrinoException(VARADA_ROW_GROUP_ILLEGAL_STATE, "row group should be null " + rowGroupData);
        }
        List<WarmUpElement> updatedWarmupElements = Stream.concat(rowGroupData.getWarmUpElements().stream().filter(we -> !warmUpElementsToDelete.contains(we)),
                        newWarmUpElements.stream()
                                .map(emptyWeElement -> WarmUpElement.builder(emptyWeElement).totalRecords(0).build()))
                .collect(Collectors.toList());
        RowGroupData updatedRowGroupData = RowGroupData.builder(rowGroupData)
                .warmUpElements(updatedWarmupElements)
                .build();
        save(updatedRowGroupData);
        varadaStatsWarmingService.addwarmup_elements_count(newWarmUpElements.size() - warmUpElementsToDelete.size());
    }

    public RowGroupData get(RowGroupKey rowGroupKey)
    {
        return rowGroupDataDao.get(rowGroupKey);
    }

    public RowGroupData getIfPresent(RowGroupKey rowGroupKey)
    {
        return rowGroupDataDao.getIfPresent(rowGroupKey);
    }

    public RowGroupData reload(RowGroupKey rowGroupKey, RowGroupData origRowGroupData)
    {
        rowGroupDataDao.refresh(rowGroupKey);
        RowGroupData newRowGroupData = get(rowGroupKey);

        if (newRowGroupData == null) {
            // refresh does not propagate exceptions
            // RowGroupData file is corrupted, delete it and invalidate cache
            rowGroupDataDao.delete(rowGroupKey, true);
            logger.error("reload failed. delete corrupted file rowGroupKey %s", rowGroupKey);
            return null;
        }
        if (origRowGroupData != null) {
            return rowGroupDataDao.merge(origRowGroupData, newRowGroupData);
        }
        else {
            return newRowGroupData;
        }
    }

    public List<RowGroupData> getAll()
    {
        return new ArrayList<>(rowGroupDataDao.getAll());
    }

    public void deleteFile(RowGroupData rowGroupData)
    {
        if (rowGroupData == null) {
            return;
        }
        RowGroupKey rowGroupKey = rowGroupData.getRowGroupKey();
        rowGroupDataDao.delete(rowGroupKey, true);
    }

    public void deleteData(RowGroupData rowGroupData, boolean deleteFromCache)
    {
        if (rowGroupData == null) {
            return;
        }
        RowGroupKey rowGroupKey = rowGroupData.getRowGroupKey();
        storageEngine.fileIsAboutToBeDeleted(rowGroupKey.stringFileNameRepresentation(globalConfiguration.getLocalStorePath()), rowGroupData.getNextOffset());
        rowGroupDataDao.delete(rowGroupKey, deleteFromCache);
        logger.debug("deleted rowGroupKey %s offset %d next-export-offset %d",
                rowGroupKey, rowGroupData.getNextOffset(), rowGroupData.getNextExportOffset());
        varadaStatsWarmingService.incdeleted_row_group_count();
    }

    private void invalidateData(RowGroupData rowGroupData)
    {
        rowGroupDataDao.invalidate(rowGroupData.getRowGroupKey());
    }

    public synchronized RowGroupData getOrCreateRowGroupData(RowGroupKey rowGroupKey,
            Map<VaradaColumn, String> partitionKeys)
    {
        RowGroupData rowGroupData = get(rowGroupKey);

        if (rowGroupData == null) {
            rowGroupData = RowGroupData.builder()
                    .rowGroupKey(rowGroupKey)
                    .nodeIdentifier(nodeIdentifier)
                    .partitionKeys(partitionKeys)
                    .warmUpElements(Collections.emptyList())
                    .build();
            save(rowGroupData);
            varadaStatsWarmingService.incrow_group_count();
        }
        return rowGroupData;
    }

    public synchronized RowGroupData updateRowGroupData(RowGroupData rowGroupData,
            WarmUpElement warmUpElement,
            int nextOffset,
            int totalRecords)
    {
        Collection<WarmUpElement> existingWarmUpElements = rowGroupData.getWarmUpElements();
        WarmUpElement.Builder warmupElementBuilder = WarmUpElement.builder(warmUpElement).totalRecords(totalRecords);
        if (!warmUpElement.isValid()) {
            warmupElementBuilder
                    .state(addTemporaryFailure(warmUpElement.getState(), System.currentTimeMillis()))
                    .warmState(WarmState.COLD);
        }
        warmUpElement = warmupElementBuilder.build();

        Optional<WarmUpElement> weToOverride = existingWarmUpElements.stream().filter(warmUpElement::isSameColNameAndWarmUpType).findFirst();
        Collection<WarmUpElement> updatedWarmUpElements = new ArrayList<>(existingWarmUpElements);

        weToOverride.ifPresent(updatedWarmUpElements::remove);
        updatedWarmUpElements.add(warmUpElement);

        FastWarmingState fastWarmingState = warmUpElement.isValid() ?
                FastWarmingState.NOT_EXPORTED :
                FastWarmingState.EXPORTED; // nothing to export

        if (FastWarmingState.State.FAILED_PERMANENTLY.equals(rowGroupData.getFastWarmingState().state())) {
            fastWarmingState = rowGroupData.getFastWarmingState();
        }

        RowGroupData.Builder rowGroupDataBuilder = RowGroupData.builder(rowGroupData).warmUpElements(updatedWarmUpElements);

        if (warmUpElement.isValid()) {
            rowGroupDataBuilder.isEmpty(totalRecords == 0);
        }
        RowGroupData updatedRowGroupData = rowGroupDataBuilder
                .nextOffset(nextOffset)
                .fastWarmingState(fastWarmingState)
                .build();
        save(updatedRowGroupData);

        if (warmUpElement.isValid()) {
            varadaStatsWarmingService.incwarmup_elements_count();
        }
        else {
            logger.debug("updateRowGroupData failure, row groupKey = %s, warmupElement=%s", rowGroupData.getRowGroupKey().stringFileNameRepresentation(globalConfiguration.getLocalStorePath()), warmUpElement);
            varadaStatsWarmingService.incwarm_failed();
        }

        if (weToOverride.isPresent()) {
            varadaStatsWarmingService.addwarm_success_retry_warmup_element(warmUpElement.isValid() && !weToOverride.get().isValid() ? 1 : 0);
        }

        if (warmUpElement.isValid() && totalRecords == 0) {
            varadaStatsWarmingService.incempty_row_group();
        }
        return updatedRowGroupData;
    }

    public void markAsFailed(RowGroupKey rowGroupKey,
            Collection<WarmUpElement> proxiedWarmUpElements,
            Map<VaradaColumn, String> partitionKeys)
    {
        RowGroupData rowGroupData = get(rowGroupKey);

        long lastTemporaryFailure = System.currentTimeMillis();
        Map<Pair<VaradaColumn, WarmUpType>, WarmUpElement> failedElementByColNameAndWarmUpType = new HashMap<>();

        for (WarmUpElement we : proxiedWarmUpElements) {
            WarmUpElementState state = addTemporaryFailure(we.getState(), lastTemporaryFailure);
            failedElementByColNameAndWarmUpType.put(
                    Pair.of(we.getVaradaColumn(), we.getWarmUpType()),
                    WarmUpElement.builder(we)
                            .state(state)
                            .warmState(WarmState.COLD)
                            .build());
        }

        RowGroupData.Builder builder;

        if (Objects.isNull(rowGroupData)) {
            builder = RowGroupData.builder()
                    .rowGroupKey(rowGroupKey)
                    .nodeIdentifier(nodeIdentifier)
                    .warmUpElements(failedElementByColNameAndWarmUpType.values())
                    .partitionKeys(partitionKeys);
            varadaStatsWarmingService.incrow_group_count();
        }
        else {
            builder = RowGroupData.builder(rowGroupData);
            List<WarmUpElement> updatedWarmUpElements = new ArrayList<>(failedElementByColNameAndWarmUpType.values());

            for (WarmUpElement existingWarmUpElement : rowGroupData.getWarmUpElements()) {
                if (!failedElementByColNameAndWarmUpType.containsKey(Pair.of(existingWarmUpElement.getVaradaColumn(), existingWarmUpElement.getWarmUpType()))) {
                    updatedWarmUpElements.add(existingWarmUpElement);
                }
            }
            builder.warmUpElements(updatedWarmUpElements);
        }

        save(builder.build());
        varadaStatsWarmingService.addwarm_failed(failedElementByColNameAndWarmUpType.size());

        flush(rowGroupKey);
    }

    private WarmUpElementState addTemporaryFailure(WarmUpElementState warmUpElementState, long lastTemporaryFailure)
    {
        if (WarmUpElementState.State.FAILED_PERMANENTLY.equals(warmUpElementState.state()) ||
                warmUpElementState.temporaryFailureCount() >= globalConfiguration.getMaxWarmRetries()) {
            return WarmUpElementState.FAILED_PERMANENTLY;
        }

        int failureCount = warmUpElementState.temporaryFailureCount() + 1;
        return new WarmUpElementState(WarmUpElementState.State.FAILED_TEMPORARILY, failureCount, lastTemporaryFailure);
    }

    // for debug command
    public void deleteAll()
    {
        rowGroupDataDao.getAll().forEach(rowGroupData -> deleteData(rowGroupData, true));
    }

    // for debug command
    public void invalidateAll()
    {
        rowGroupDataDao.getAll().forEach(this::invalidateData);
    }

    public synchronized void removeElements(RowGroupData rowGroupData, Collection<WarmUpElement> deletedWarmUpElements)
    {
        if (deletedWarmUpElements.isEmpty()) {
            return;
        }

        Collection<WarmUpElement> existingWarmUpElements = rowGroupData.getWarmUpElements();

        if (existingWarmUpElements.size() == deletedWarmUpElements.size()) {
            removeElements(rowGroupData);
            return;
        }

        List<WarmUpElement> updatedWarmUpElements = new ArrayList<>();
        Collection<WarmUpElement> toDeleteWarmUpElements = new ArrayList<>();
        RowGroupKey rowGroupKey = rowGroupData.getRowGroupKey();
        String fileName = rowGroupKey.stringFileNameRepresentation(globalConfiguration.getLocalStorePath());

        for (WarmUpElement existingWarmUpElement : existingWarmUpElements) {
            if (deletedWarmUpElements.stream().noneMatch(we -> we.isSameColNameAndWarmUpType(existingWarmUpElement))) {
                updatedWarmUpElements.add(existingWarmUpElement);
            }
            else {
                // keep offsets and update native only for valid warmUpElements
                if (existingWarmUpElement.isValid()) {
                    // no need to keep offsets if file wasn't ever exported
                    if (rowGroupData.getNextExportOffset() != 0) {
                        WarmUpElement warmWarmUpElement = WarmUpElement.builder(existingWarmUpElement)
                                .warmState(WarmState.WARM)
                                .build();
                        updatedWarmUpElements.add(warmWarmUpElement);
                    }
                    toDeleteWarmUpElements.add(existingWarmUpElement);
                }
            }
        }

        RowGroupData newRowGroupData = RowGroupData.builder(rowGroupData)
                .warmUpElements(updatedWarmUpElements)
                .sparseFile(toDeleteWarmUpElements.size() > 0)
                .build();
        save(newRowGroupData);
        flush(newRowGroupData.getRowGroupKey());

        toDeleteWarmUpElements.forEach(toDeleteWarmUpElement -> storageEngine.filePunchHole(fileName, toDeleteWarmUpElement.getStartOffset(), toDeleteWarmUpElement.getEndOffset()));

        varadaStatsWarmingService.adddeleted_warmup_elements_count(deletedWarmUpElements.size());
    }

    public synchronized void removeElements(RowGroupData rowGroupData)
    {
        // no need to keep offsets if file wasn't ever exported
        if (rowGroupData.getNextExportOffset() == 0) {
            deleteData(rowGroupData, true);
            return;
        }

        Collection<WarmUpElement> existingWarmUpElements = rowGroupData.getWarmUpElements();
        List<WarmUpElement> updatedWarmUpElements = new ArrayList<>();
        RowGroupKey rowGroupKey = rowGroupData.getRowGroupKey();
        String fileName = rowGroupKey.stringFileNameRepresentation(globalConfiguration.getLocalStorePath());

        for (WarmUpElement existingWarmUpElement : existingWarmUpElements) {
            // keep offsets only for valid warmUpElements
            if (existingWarmUpElement.isValid()) {
                WarmUpElement warmWarmUpElement = existingWarmUpElement;

                if (existingWarmUpElement.isHot()) {
                    warmWarmUpElement = WarmUpElement.builder(existingWarmUpElement)
                            .warmState(WarmState.WARM)
                            .build();
                }
                updatedWarmUpElements.add(warmWarmUpElement);
            }
        }

        // no offsets to keep
        if (updatedWarmUpElements.size() == 0) {
            deleteData(rowGroupData, true);
            return;
        }

        RowGroupData newRowGroupData = RowGroupData.builder(rowGroupData)
                .warmUpElements(updatedWarmUpElements)
                .sparseFile(true)
                .build();
        save(newRowGroupData);
        flush(newRowGroupData.getRowGroupKey());

        storageEngine.filePunchHole(fileName, 0, rowGroupData.getNextOffset());

        varadaStatsWarmingService.adddeleted_warmup_elements_count(updatedWarmUpElements.size());
    }
}
