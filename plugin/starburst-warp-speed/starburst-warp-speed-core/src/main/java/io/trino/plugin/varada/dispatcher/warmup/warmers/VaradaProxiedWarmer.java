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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.VaradaSessionProperties;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dictionary.DictionaryWarmInfo;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.varada.dispatcher.model.TransformedColumn;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.storage.engine.ExceptionThrower;
import io.trino.plugin.varada.storage.write.PageSink;
import io.trino.plugin.varada.storage.write.StorageWriterService;
import io.trino.plugin.varada.storage.write.StorageWriterSplitConfiguration;
import io.trino.plugin.varada.storage.write.VaradaPageSinkFactory;
import io.trino.spi.NodeManager;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.varada.tools.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.trino.plugin.varada.dispatcher.warmup.warmers.StorageWarmerService.INVALID_FILE_COOKIE;
import static io.trino.plugin.varada.dispatcher.warmup.warmers.StorageWarmerService.INVALID_TX_ID;
import static java.util.Objects.requireNonNull;

@Singleton
public class VaradaProxiedWarmer
{
    private static final Logger logger = Logger.get(VaradaProxiedWarmer.class);

    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final String nodeIdentifier;
    private final VaradaPageSinkFactory varadaPageSinkFactory;
    private final ConnectorSync connectorSync;
    private final GlobalConfiguration globalConfiguration;

    private final RowGroupDataService rowGroupDataService;
    private final StorageWarmerService storageWarmerService;
    private final StorageWriterService storageWriterService;

    @Inject
    public VaradaProxiedWarmer(VaradaPageSinkFactory varadaPageSinkFactory,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            NodeManager nodeManager,
            ConnectorSync connectorSync,
            GlobalConfiguration globalConfiguration,
            RowGroupDataService rowGroupDataService,
            StorageWarmerService storageWarmerService,
            StorageWriterService storageWriterService)
    {
        this.varadaPageSinkFactory = varadaPageSinkFactory;
        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
        this.nodeIdentifier = requireNonNull(nodeManager).getCurrentNode().getNodeIdentifier();
        this.connectorSync = requireNonNull(connectorSync);
        this.globalConfiguration = requireNonNull(globalConfiguration);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.storageWarmerService = requireNonNull(storageWarmerService);
        this.storageWriterService = requireNonNull(storageWriterService);
    }

    RowGroupData warm(ConnectorPageSourceProvider connectorPageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            DispatcherTableHandle dispatcherTableHandle,
            RowGroupKey rowGroupKey,
            RowGroupData rowGroupData,
            DispatcherSplit dispatcherSplit,
            List<ColumnHandle> columnsToWarm,
            List<WarmUpElement> proxiedWarmupElements,
            SetMultimap<VaradaColumn, WarmupProperties> requiredWarmUpTypeMap,
            boolean skipWait,
            int firstOffset,
            List<DictionaryWarmInfo> outDictionariesWarmInfos)
    {
        SetMultimap<VaradaColumn, WarmUpElement> proxiedWarmupElementsMultimap = proxiedWarmupElements.stream()
                .collect(Multimaps.toMultimap(WarmUpElement::getVaradaColumn, Function.identity(), HashMultimap::create));
        SchemaTableName schemaTableName = new SchemaTableName(rowGroupKey.schema(), rowGroupKey.table());
        List<Pair<WarmupElementWriteMetadata, ColumnHandle>> warmupElementsWriteMetadata = createWarmupElementWriteMetadata(proxiedWarmupElementsMultimap,
                schemaTableName,
                columnsToWarm,
                requiredWarmUpTypeMap);
        if (warmupElementsWriteMetadata.isEmpty()) {
            return rowGroupData;
        }

        WarmupElementWriteMetadata currWarmUpElementWriteMetadata;
        String rowGroupFilePath = rowGroupKey.stringFileNameRepresentation(globalConfiguration.getLocalStorePath());
        long fileCookie = INVALID_FILE_COOKIE;
        int txId = INVALID_TX_ID;
        StorageWriterSplitConfiguration storageWriterSplitConfiguration = null;
        try {
            storageWarmerService.createFile(rowGroupKey);
            fileCookie = storageWarmerService.fileOpen(rowGroupKey);
            storageWriterSplitConfiguration = storageWriterService.startWarming(nodeIdentifier,
                    rowGroupFilePath,
                    VaradaSessionProperties.getEnableDictionary(session));
            try {
                int fileOffset = firstOffset;
                ConnectorSplit nonFilterSplit = dispatcherProxiedConnectorTransformer.createProxiedConnectorNonFilteredSplit(dispatcherSplit.getProxyConnectorSplit());
                ConnectorTableHandle nonFilterTableHandle = dispatcherProxiedConnectorTransformer.createProxyTableHandleForWarming(dispatcherTableHandle);

                txId = storageWarmerService.warmupOpen(txId);
                for (Pair<WarmupElementWriteMetadata, ColumnHandle> pair : warmupElementsWriteMetadata) {
                    ConnectorPageSource connectorPageSource = connectorPageSourceProvider.createPageSource(transactionHandle,
                            session,
                            nonFilterSplit,
                            nonFilterTableHandle,
                            List.of(pair.getValue()),
                            DynamicFilter.EMPTY);
                    logger.debug("create connectorPageSource for element %s offset %d connector %s",
                            pair.getValue(), fileOffset, connectorSync.getCatalogName());

                    PageSink pageSink = null;
                    try {
                        currWarmUpElementWriteMetadata = WarmupElementWriteMetadata.builder(pair.getKey()).connectorBlockIndex(0).build();
                        pageSink = varadaPageSinkFactory.create(storageWriterSplitConfiguration);
                        boolean isValidWE = true;
                        int rowCount = 0;
                        while (isValidWE && !connectorPageSource.isFinished()) {
                            Page nextPage = connectorPageSource.getNextPage();
                            int pagePositionCount = (nextPage != null) ? nextPage.getPositionCount() : 0;
                            if (pagePositionCount > 0) {
                                if (rowCount == 0) { //first time
                                    boolean openSuccess = pageSink.open(txId, fileCookie, fileOffset, currWarmUpElementWriteMetadata, outDictionariesWarmInfos);
                                    if (!openSuccess) {
                                        txId = storageWarmerService.warmupOpen(txId);
                                        openSuccess = pageSink.open(txId, fileCookie, fileOffset, currWarmUpElementWriteMetadata, outDictionariesWarmInfos);
                                        if (!openSuccess) {
                                            throw new RuntimeException(String.format("failed twice to open warm up element for write txId %d fileCookie %d fileOffset %d",
                                                    txId, fileCookie, fileOffset));
                                        }
                                    }
                                }
                                isValidWE = pageSink.appendPage(nextPage, rowCount);
                                rowCount += pagePositionCount;
                            }
                            if (!skipWait) {
                                storageWarmerService.waitForLoaders();
                            }
                        }

                        WarmSinkResult warmSinkResult = storageWarmerService.sinkClose(pageSink, currWarmUpElementWriteMetadata, rowCount, isValidWE, fileOffset, fileCookie);
                        pageSink = null;
                        if (fileOffset == warmSinkResult.offset()) { // if we failed, we re-open the tx since we might had a native exception
                            txId = storageWarmerService.warmupOpen(txId);
                        }
                        fileOffset = warmSinkResult.offset();
                        rowGroupData = rowGroupDataService.updateRowGroupData(rowGroupData, warmSinkResult.warmUpElement(), fileOffset, rowCount);
                    }
                    catch (Exception e) {
                        if (pageSink != null) {
                            boolean nativeThrowed = e instanceof TrinoException tx && ExceptionThrower.isNativeException(tx);
                            pageSink.abort(nativeThrowed);
                        }
                        throw e;
                    }
                    finally {
                        closeConnector(connectorPageSource, rowGroupKey);
                    }
                }
            }
            catch (Exception e) {
                logger.error(e, "unexpected error in warm up file %s", rowGroupFilePath);
                throw e;
            }
            finally {
                storageWarmerService.warmupClose(txId);
                storageWarmerService.flushRecords(fileCookie, rowGroupData);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (storageWriterSplitConfiguration != null) {
                try {
                    storageWriterService.finishWarming(storageWriterSplitConfiguration);
                }
                catch (Exception e) {
                    logger.error(e, "failed to release warming resources file %s", rowGroupFilePath);
                }
            }
            storageWarmerService.fileClose(fileCookie, rowGroupData);
        }
        return rowGroupData;
    }

    private void closeConnector(ConnectorPageSource connectorPageSource,
            RowGroupKey rowGroupKey)
    {
        try {
            connectorPageSource.close();
        }
        catch (IOException e) {
            logger.error(e, "failed to close external connector %s", rowGroupKey);
        }
    }

    List<Pair<WarmupElementWriteMetadata, ColumnHandle>> createWarmupElementWriteMetadata(Multimap<VaradaColumn, WarmUpElement> proxiedWarmupElements,
            SchemaTableName schemaTableName,
            List<ColumnHandle> columnsToWarm,
            SetMultimap<VaradaColumn, WarmupProperties> requiredWarmUpTypeMap)
    {
        logger.debug("createWarmupElementWriteMetadata proxiedWarmupElements = %s, schemaTableName = %s, columnsToWarm= %s, requiredWarmUpTypeMap = %s",
                proxiedWarmupElements, schemaTableName, columnsToWarm, requiredWarmUpTypeMap);

        Multimap<VaradaColumn, WarmupElementWriteMetadata> columnMap = ArrayListMultimap.create();
        Map<WarmupElementWriteMetadata, ColumnHandle> warmupElementWriteMetadataColumnHandleMap = new HashMap<>();

        for (int connectorBlockIdx = 0; connectorBlockIdx < columnsToWarm.size(); connectorBlockIdx++) {
            ColumnHandle columnHandle = columnsToWarm.get(connectorBlockIdx);
            Type type = dispatcherProxiedConnectorTransformer.getColumnType(columnHandle);
            RegularColumn varadaColumn = dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(columnHandle);
            Collection<WarmUpElement> warmUpElements = new ArrayList<>();

            List<TransformedColumn> transformedColumns = requiredWarmUpTypeMap
                    .keySet()
                    .stream()
                    .filter(x -> x instanceof TransformedColumn transformedColumn && transformedColumn.getName().equalsIgnoreCase(varadaColumn.getName()))
                    .map(x -> (TransformedColumn) x)
                    .toList();
            if (transformedColumns.isEmpty()) {
                warmUpElements = proxiedWarmupElements.get(varadaColumn);
            }
            else {
                for (TransformedColumn transformedColumn : transformedColumns) {
                    Collection<WarmUpElement> elements = proxiedWarmupElements.get(transformedColumn);
                    warmUpElements.addAll(elements);
                }
            }

            for (WarmUpElement warmUpElement : warmUpElements) {
                WarmupElementWriteMetadata metadata = WarmupElementWriteMetadata.builder()
                        .warmUpElement(warmUpElement)
                        .connectorBlockIndex(connectorBlockIdx)
                        .type(type)
                        .schemaTableColumn(new SchemaTableColumn(schemaTableName, warmUpElement.getVaradaColumn()))
                        .build();
                columnMap.put(warmUpElement.getVaradaColumn(), metadata);
                warmupElementWriteMetadataColumnHandleMap.put(metadata, columnHandle);
            }
        }

        Map<VaradaColumn, Double> priorityMap = new HashMap<>();
        for (VaradaColumn varadaColumn : requiredWarmUpTypeMap.keySet()) {
            if (proxiedWarmupElements.containsKey(varadaColumn) && varadaColumn instanceof RegularColumn) { //if we warmed/skipped all elements of this column we don't need to add it to priority map.
                for (WarmupProperties warmupProperties : requiredWarmUpTypeMap.get(varadaColumn)) {
                    priorityMap.compute(varadaColumn, (k, v) -> Math.max(warmupProperties.priority(), (v == null) ? 0 : v));
                }
            }
        }

        Comparator<Map.Entry<VaradaColumn, Double>> comp = Comparator.comparingDouble(Map.Entry::getValue);
        Set<WarmupElementWriteMetadata> elementsToWarmSet = new HashSet<>();
        List<WarmupElementWriteMetadata> elementsToWarmOrdered = new ArrayList<>();
        priorityMap.entrySet().stream()
                .sorted(comp.reversed())
                .forEach(entry -> elementsToWarmOrdered.addAll(columnMap.get(entry.getKey()).stream().filter(elementsToWarmSet::add).toList()));
        return elementsToWarmOrdered.stream().map(metadata -> Pair.of(metadata, warmupElementWriteMetadataColumnHandleMap.get(metadata))).toList();
    }
}
