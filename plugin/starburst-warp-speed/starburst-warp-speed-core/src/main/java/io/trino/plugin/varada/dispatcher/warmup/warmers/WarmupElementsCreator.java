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
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.model.ExportState;
import io.trino.plugin.varada.dispatcher.model.RecordData;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmState;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.varada.expression.TransformFunction;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.DateType;
import io.trino.spi.type.Type;
import io.varada.log.ShapingLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static java.util.Objects.requireNonNull;

@Singleton
public class WarmupElementsCreator
{
    private static final Logger logger = Logger.get(WarmupElementsCreator.class);
    private final ShapingLogger shapingLogger;

    private final RowGroupDataService rowGroupDataService;
    private final StorageEngineConstants storageEngineConstants;
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final VaradaStatsWarmingService statsWarmingService;

    @Inject
    public WarmupElementsCreator(
            RowGroupDataService rowGroupDataService,
            MetricsManager metricsManager,
            StorageEngineConstants storageEngineConstants,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            GlobalConfiguration globalConfiguration)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
        this.statsWarmingService = (VaradaStatsWarmingService) metricsManager.get(VaradaStatsWarmingService.createKey(WARMING_SERVICE_STAT_GROUP));
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfiguration.getShapingLoggerThreshold(),
                globalConfiguration.getShapingLoggerDuration(),
                globalConfiguration.getShapingLoggerNumberOfSamples());
    }

    public List<WarmUpElement> createWarmupElements(
            RowGroupKey rowGroupKey,
            SetMultimap<VaradaColumn, WarmupProperties> requiredWarmUpTypeMap,
            SchemaTableName schemaTableName,
            List<ColumnHandle> columnsToWarm)
    {
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);

        List<RecordData> recordDataList = getRecordData(
                schemaTableName,
                columnsToWarm,
                requiredWarmUpTypeMap.keySet());
        List<WarmUpElement> warmUpElements = new ArrayList<>();

        for (RecordData recordData : recordDataList) {
            if (recordData.recTypeCode() == RecTypeCode.REC_TYPE_INVALID) {
                continue;
            }
            VaradaColumn varadaColumn = recordData.schemaTableColumn().varadaColumn();

            for (WarmupProperties warmUpProperty : requiredWarmUpTypeMap.get(varadaColumn)) {
                // There might be a failed or demoted warmup element which we need to try to warm again
                Optional<WarmUpElement> existingWarmUpElement = rowGroupData == null ?
                        Optional.empty() :
                        rowGroupData.getWarmUpElements().stream()
                                .filter(we -> we.getWarmUpType().equals(warmUpProperty.warmUpType()) &&
                                        we.getVaradaColumn().equals(varadaColumn))
                                .findFirst();

                if (existingWarmUpElement.isPresent()) {
                    if (WarmState.WARM.equals(existingWarmUpElement.get().getWarmState())) {
                        warmUpElements.add(WarmUpElement.builder()
                                .warmUpType(existingWarmUpElement.get().getWarmUpType())
                                .recTypeCode(existingWarmUpElement.get().getRecTypeCode())
                                .recTypeLength(existingWarmUpElement.get().getRecTypeLength())
                                .varadaColumn(varadaColumn)
                                .warmupElementStats(new WarmupElementStats(0, null, null))
                                .build());
                    }
                    else {
                        statsWarmingService.incwarm_begin_retry_warmup_element();
                        warmUpElements.add(existingWarmUpElement.get());
                    }
                }
                else {
                    WarmUpType warmUpType = warmUpProperty.warmUpType();
                    TransformFunction transformFunction = warmUpProperty.transformFunction();
                    int recTypeLength;
                    RecTypeCode recTypeCode;

                    if (warmUpType == WarmUpType.WARM_UP_TYPE_BASIC && Objects.equals(transformFunction, TransformFunction.DATE)) {
                        recTypeCode = RecTypeCode.REC_TYPE_DATE;
                        recTypeLength = TypeUtils.getTypeLength(DateType.DATE, storageEngineConstants.getFixedLengthStringLimit());
                    }
                    else {
                        recTypeLength = recordData.recTypeLength();
                        recTypeCode = recordData.recTypeCode();
                    }

                    if (warmUpType != WarmUpType.WARM_UP_TYPE_DATA) {
                        recTypeLength = TypeUtils.getIndexTypeLength(recTypeCode, recTypeLength, storageEngineConstants.getFixedLengthStringLimit());
                    }

                    warmUpElements.add(WarmUpElement.builder()
                            .warmUpType(warmUpType)
                            .recTypeCode(recTypeCode)
                            .recTypeLength(recTypeLength)
                            .varadaColumn(varadaColumn)
                            .warmupElementStats(new WarmupElementStats(0, null, null))
                            .build());
                }
            }
        }
        return warmUpElements;
    }

    List<RecordData> getRecordData(SchemaTableName schemaTableName,
            List<ColumnHandle> columnHandles,
            Set<VaradaColumn> actualColumnsToWarm)
    {
        Map<String, Type> columnNameToType = columnHandles.stream()
                .collect(Collectors.toMap(
                        columnHandle -> dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(columnHandle).getName(),
                        dispatcherProxiedConnectorTransformer::getColumnType));
        return actualColumnsToWarm.stream()
                .map(varadaColumn -> createColumn(schemaTableName, varadaColumn, columnNameToType))
                .toList();
    }

    private RecordData createColumn(SchemaTableName schemaTableName, VaradaColumn varadaColumn, Map<String, Type> columnNameToType)
    {
        Type type = null;
        int recTypeLength = -1;
        RecTypeCode recTypeCode = RecTypeCode.REC_TYPE_INVALID;
        try {
            if (varadaColumn instanceof RegularColumn) {
                type = columnNameToType.get(varadaColumn.getName());
                recTypeLength = TypeUtils.getTypeLength(type, storageEngineConstants.getVarcharMaxLen());
                recTypeCode = TypeUtils.convertToRecTypeCode(type, recTypeLength, storageEngineConstants.getFixedLengthStringLimit());
            }
        }
        catch (Exception e) {
            recTypeLength = -1;
        }

        return new RecordData(
                new SchemaTableColumn(schemaTableName, varadaColumn),
                type,
                recTypeCode,
                recTypeLength);
    }

    public Optional<WarmUpElement> createWarmupElement(CacheColumnId cacheColumnId, Type columnType, UUID storeId)
    {
        Optional<WarmUpElement> res = Optional.empty();
        try {
            VaradaColumn cachedColumn = new RegularColumn(cacheColumnId.toString());
            int recTypeLength = TypeUtils.getTypeLength(columnType, storageEngineConstants.getVarcharMaxLen());
            RecTypeCode recTypeCode = TypeUtils.convertToRecTypeCode(columnType, recTypeLength, storageEngineConstants.getFixedLengthStringLimit());
            res = Optional.of(WarmUpElement
                    .builder()
                    .varadaColumn(cachedColumn)
                    .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                    .recTypeCode(recTypeCode)
                    .recTypeLength(recTypeLength)
                    .exportState(ExportState.NOT_EXPORTED)
                    .storeId(storeId)
                    .state(WarmUpElementState.VALID)
                    .warmupElementStats(new WarmupElementStats(0, null, null))
                    .build());
        }
        catch (UnsupportedOperationException e) {
            statsWarmingService.incwarm_warp_cache_invalid_type();
            shapingLogger.error(e, "failed to create warmup element. columnType=%s", columnType);
        }
        catch (Exception e) {
            shapingLogger.error(e, "failed to create warmup element. columnType=%s", columnType);
        }
        return res;
    }
}
