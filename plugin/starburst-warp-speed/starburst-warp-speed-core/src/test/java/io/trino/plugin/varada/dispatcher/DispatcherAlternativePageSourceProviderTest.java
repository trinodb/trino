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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.MetricsConfiguration;
import io.trino.plugin.varada.connector.TestingConnectorPageSource;
import io.trino.plugin.varada.connector.TestingConnectorPageSourceProvider;
import io.trino.plugin.varada.dictionary.DictionaryCacheService;
import io.trino.plugin.varada.dispatcher.dal.RowGroupDataDao;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.classifier.PredicateContextData;
import io.trino.plugin.varada.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService;
import io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.varada.expression.VaradaPrimitiveConstant;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.juffer.PredicatesCacheService;
import io.trino.plugin.varada.juffer.StorageEngineTxService;
import io.trino.plugin.varada.metrics.CustomStatsContext;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.metrics.MetricsRegistry;
import io.trino.plugin.varada.metrics.PrintMetricsTimerTask;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.engine.StubsStorageEngine;
import io.trino.plugin.varada.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.varada.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.varada.storage.read.ChunksQueueService;
import io.trino.plugin.varada.storage.read.CollectTxService;
import io.trino.plugin.varada.storage.read.PrefilledPageSource;
import io.trino.plugin.varada.storage.read.RangeFillerService;
import io.trino.plugin.varada.storage.read.StorageCollectorService;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.testing.TestingMetadata;
import io.varada.tools.CatalogNameProvider;
import io.varada.tools.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.generateRowGroupData;
import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.mockColumns;
import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.mockDispatcherTableHandle;
import static io.trino.plugin.varada.util.NodeUtils.mockNodeManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DispatcherAlternativePageSourceProviderTest
{
    private GlobalConfiguration globalConfiguration;
    private ConnectorTransactionHandle connectorTransactionHandle;
    private MetricsManager metricsManager;
    private CustomStatsContext customStatsContext;
    private StorageEngineConstants storageEngineConstants;
    private WorkerWarmingService workerWarmingService;
    private ConnectorSession connectorSession;
    private DispatcherTableHandle dispatcherTableHandle;
    private List<ColumnHandle> columnHandles;
    private DispatcherSplit dispatcherSplit;
    private RowGroupDataService rowGroupDataService;
    private RowGroupKey rowGroupKey;

    private DispatcherAlternativeChooser.ResourceCloser resourceCloser;
    private DispatcherAlternativePageSourceProvider dispatcherAlternativePageSourceProvider;
    private TestingConnectorPageSourceProvider proxiedPageSourceProvider;
    private DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private QueryClassifier queryClassifier;
    private final SchemaTableName schemaTableName = new SchemaTableName("s", "t");

    DynamicFilter dynamicFilter;

    @BeforeEach
    public void before()
    {
        connectorTransactionHandle = mock(ConnectorTransactionHandle.class);

        globalConfiguration = new GlobalConfiguration();
        globalConfiguration.setEnableDefaultWarming(false);

        MetricsConfiguration metricsConfiguration = new MetricsConfiguration();
        metricsConfiguration.setEnabled(false);

        metricsManager = new MetricsManager(new MetricsRegistry(new CatalogNameProvider("catalog-name"), metricsConfiguration));
        workerWarmingService = mock(WorkerWarmingService.class);
        StorageEngine storageEngine = new StubsStorageEngine();
        storageEngineConstants = spy(new StubsStorageEngineConstants());
        connectorSession = mock(ConnectorSession.class);
        dispatcherProxiedConnectorTransformer = mock(DispatcherProxiedConnectorTransformer.class);
        when(dispatcherProxiedConnectorTransformer.isValidForAcceleration(any())).thenReturn(true);

        columnHandles = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of("C1", VarcharType.VARCHAR)));
        Pair<DispatcherSplit, RowGroupKey> dispatcherSplitRowGroupKeyPair = WarmupTestDataUtil.mockConnectorSplit();
        dispatcherSplit = dispatcherSplitRowGroupKeyPair.getLeft();
        rowGroupKey = dispatcherSplitRowGroupKeyPair.getRight();
        ConnectorSync connectorSync = mock(ConnectorSync.class);
        when(connectorSync.getCatalogName()).thenReturn("");
        when(connectorSync.getCatalogSequence()).thenReturn(0);
        rowGroupDataService = spy(new RowGroupDataService(mock(RowGroupDataDao.class),
                storageEngine,
                globalConfiguration,
                metricsManager,
                mockNodeManager(),
                connectorSync));
        rowGroupKey = rowGroupDataService.createRowGroupKey(dispatcherSplit.getSchemaName(),
                dispatcherSplit.getTableName(),
                dispatcherSplit.getPath(),
                dispatcherSplit.getStart(),
                dispatcherSplit.getLength(),
                dispatcherSplit.getFileModifiedTime(),
                dispatcherSplit.getDeletedFilesHash());

        proxiedPageSourceProvider = mock(TestingConnectorPageSourceProvider.class);

        queryClassifier = mock(QueryClassifier.class);

        dispatcherTableHandle = mockDispatcherTableHandle(schemaTableName.getSchemaName(), schemaTableName.getTableName(), TupleDomain.all());

        customStatsContext = new CustomStatsContext(metricsManager, dispatcherTableHandle.getCustomStats());

        resourceCloser = mock(DispatcherAlternativeChooser.ResourceCloser.class);
        doAnswer(invocation -> {
                    rowGroupDataService.get(rowGroupKey).getLock().readUnLock();
                    return null;
                }
        ).when(resourceCloser).close();

        dispatcherAlternativePageSourceProvider = createDispatcherPageSourceProvider(dispatcherSplit, dispatcherTableHandle, resourceCloser);

        dynamicFilter = DynamicFilter.EMPTY;
    }

    @SuppressWarnings("CheckReturnValue")
    @Test
    public void testReadFlow_ColumnNotWarm_ReturnProxiedPageSourceProvider()
            throws IOException
    {
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(null);

        QueryContext queryContext = mock(QueryContext.class);
        when(queryContext.getPredicateContextData()).thenReturn(new PredicateContextData(ImmutableMap.of(), VaradaPrimitiveConstant.TRUE));
        when(queryClassifier.getBasicQueryContext(anyList(), eq(dispatcherTableHandle), any(DynamicFilter.class), any(ConnectorSession.class)))
                .thenReturn(queryContext);

        when(proxiedPageSourceProvider.createPageSource(eq(connectorTransactionHandle),
                eq(connectorSession),
                any(ConnectorSplit.class),
                eq(dispatcherTableHandle.getProxyConnectorTableHandle()),
                anyList(),
                any(DynamicFilter.class))).thenReturn(mock(TestingConnectorPageSource.class));

        when(dispatcherProxiedConnectorTransformer.createProxiedConnectorTableHandleForMixedQuery(eq(dispatcherTableHandle)))
                .thenReturn(dispatcherTableHandle.getProxyConnectorTableHandle());

        try (ConnectorPageSource wrapperPageSource = dispatcherAlternativePageSourceProvider.createPageSource(connectorTransactionHandle,
                connectorSession,
                columnHandles,
                dynamicFilter,
                true)) {
            assertThat(((DispatcherWrapperPageSource) wrapperPageSource).getConnectorPageSource())
                    .isInstanceOf(TestingConnectorPageSource.class);
        }
    }

    @Test
    public void testReadFlow_FileNotExist_ReturnProxiedPageSourceProvider()
    {
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(null);

        mockQueryClassifier(true, false, false);

        when(proxiedPageSourceProvider.createPageSource(eq(connectorTransactionHandle),
                eq(connectorSession),
                any(ConnectorSplit.class),
                eq(dispatcherTableHandle.getProxyConnectorTableHandle()),
                anyList(),
                any(DynamicFilter.class))).thenReturn(mock(TestingConnectorPageSource.class));

        when(dispatcherProxiedConnectorTransformer.createProxiedConnectorTableHandleForMixedQuery(eq(dispatcherTableHandle)))
                .thenReturn(dispatcherTableHandle.getProxyConnectorTableHandle());

        ConnectorPageSource pageSource = dispatcherAlternativePageSourceProvider.createPageSource(
                connectorTransactionHandle,
                connectorSession,
                columnHandles,
                dynamicFilter,
                true);
        pageSource = ((DispatcherWrapperPageSource) pageSource).getConnectorPageSource();
        assertThat(pageSource).isInstanceOf(TestingConnectorPageSource.class);
    }

    @Test
    public void testReadFlow_FileNotExistWithRuleReturnSendWarmRequest()
            throws IOException
    {
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(null);

        QueryContext queryContext = mock(QueryContext.class);
        when(queryContext.getPredicateContextData()).thenReturn(new PredicateContextData(ImmutableMap.of(), VaradaPrimitiveConstant.TRUE));
        when(queryClassifier.getBasicQueryContext(anyList(), eq(dispatcherTableHandle), any(DynamicFilter.class), any(ConnectorSession.class)))
                .thenReturn(queryContext);

        when(proxiedPageSourceProvider.createPageSource(eq(connectorTransactionHandle),
                eq(connectorSession),
                any(ConnectorSplit.class),
                eq(dispatcherTableHandle.getProxyConnectorTableHandle()),
                anyList(),
                any(DynamicFilter.class))).thenReturn(mock(TestingConnectorPageSource.class));

        when(dispatcherProxiedConnectorTransformer.createProxiedConnectorTableHandleForMixedQuery(eq(dispatcherTableHandle)))
                .thenReturn(dispatcherTableHandle.getProxyConnectorTableHandle());

        try (ConnectorPageSource pageSource = dispatcherAlternativePageSourceProvider.createPageSource(
                connectorTransactionHandle,
                connectorSession,
                columnHandles,
                dynamicFilter,
                true)) {
            assertThat(((DispatcherWrapperPageSource) pageSource).getConnectorPageSource())
                    .isInstanceOf(TestingConnectorPageSource.class);
            verify(workerWarmingService, times(1))
                    .warm(any(), any(), any(), any(), any(), anyList(), any(), anyInt());
        }
    }

    @Test
    public void testReadFlow_NewWarmedColumnDefinitionSendWarmRequest()
            throws IOException
    {
        List<String> columnsStr = List.of("C0");
        List<Pair<String, Type>> columnsMetadata = columnsStr.stream().map((colName) -> Pair.of(colName, (Type) IntegerType.INTEGER)).collect(Collectors.toList());

        List<ColumnHandle> allColumns = mockColumns(dispatcherProxiedConnectorTransformer, columnsMetadata);

        mockQueryClassifier(false, false, false);
        RowGroupData rowGroupDataWarmedUpElements = generateRowGroupData(rowGroupKey, columnHandles);
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupDataWarmedUpElements);
        when(rowGroupDataService.getIfPresent(rowGroupKey)).thenReturn(rowGroupDataWarmedUpElements);

        try (ConnectorPageSource pageSource = dispatcherAlternativePageSourceProvider.createPageSource(
                connectorTransactionHandle,
                connectorSession,
                allColumns,
                dynamicFilter,
                true)) {
            assertThat(((DispatcherWrapperPageSource) pageSource).getConnectorPageSource())
                    .isInstanceOf(DispatcherPageSource.class);

            verify(workerWarmingService, times(1))
                    .warm(any(), any(), any(), any(), any(), anyList(), any(), anyInt());
        }
    }

    @Test
    public void testReadFlow_FileExist_EmptyColumnQuery_WarmColumnExist()
    {
        mockQueryClassifier(false, false, true);

        RowGroupData rowGroupData = generateRowGroupData(rowGroupKey, columnHandles);

        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);
        when(rowGroupDataService.getIfPresent(rowGroupKey)).thenReturn(rowGroupData);

        ConnectorPageSource pageSource = dispatcherAlternativePageSourceProvider.createPageSource(connectorTransactionHandle,
                connectorSession,
                Collections.emptyList(),
                DynamicFilter.EMPTY,
                true);

        pageSource = ((DispatcherWrapperPageSource) pageSource).getConnectorPageSource();
        assertThat(pageSource).isInstanceOf(PrefilledPageSource.class);
    }

    @Test
    public void testReadFlow_FileExist_AllColumnsAreWarmed()
    {
        Type columnType = IntegerType.INTEGER;
        ImmutableList<Pair<String, Type>> columnsMetadata = ImmutableList.of(Pair.of("c1", columnType));
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer, columnsMetadata);
        ColumnHandle columnHandle = columns.get(0);
        TupleDomain<ColumnHandle> proxiedPredicate = TupleDomain.withColumnDomains(Map.of(columnHandle, Domain.singleValue(columnType, 3L)));
        TupleDomain<ColumnHandle> predicate = proxiedPredicate.transformKeys(ColumnHandle.class::cast);

        dispatcherTableHandle = mockDispatcherTableHandle(schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                predicate);
        dispatcherAlternativePageSourceProvider = createDispatcherPageSourceProvider(dispatcherSplit, dispatcherTableHandle, resourceCloser);

        mockQueryClassifier(false, true, false);

        RowGroupData rowGroupData = generateRowGroupData(rowGroupKey, columnHandles);

        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);
        when(rowGroupDataService.getIfPresent(rowGroupKey)).thenReturn(rowGroupData);

        ConnectorPageSource pageSource = dispatcherAlternativePageSourceProvider.createPageSource(connectorTransactionHandle,
                connectorSession,
                columns,
                new CompletedDynamicFilter(predicate),
                true);
        pageSource = ((DispatcherWrapperPageSource) pageSource).getConnectorPageSource();
        assertThat(pageSource).isInstanceOf(DispatcherPageSource.class);
        assertThat(((DispatcherPageSource) pageSource).getPageSourceDecision())
                .isEqualTo(PageSourceDecision.VARADA);
    }

    @Test
    public void testReadFlow_FileExist_mixedWithoutPredicate()
    {
        List<Pair<String, Type>> columnsMetadata = List.of(Pair.of("c1", VarcharType.VARCHAR),
                Pair.of("c2", IntegerType.INTEGER));
        Type columnType = columnsMetadata.get(0).getRight();
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer, columnsMetadata);

        ColumnHandle connectorColumnHandle = columns.get(0);
        TupleDomain<ColumnHandle> proxiedPredicate = TupleDomain.withColumnDomains(
                Map.of(connectorColumnHandle,
                        Domain.singleValue(columnType, Slices.wrappedBuffer("value".getBytes(Charset.defaultCharset())))));
        TupleDomain<ColumnHandle> predicate = proxiedPredicate.transformKeys(ColumnHandle.class::cast);

        dispatcherTableHandle = mockDispatcherTableHandle(schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                predicate);
        dispatcherAlternativePageSourceProvider = createDispatcherPageSourceProvider(dispatcherSplit, dispatcherTableHandle, resourceCloser);

        mockQueryClassifier(false, false, false);

        RowGroupData rowGroupData = generateRowGroupData(rowGroupKey, columns.subList(0, 1));
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);
        when(rowGroupDataService.getIfPresent(rowGroupKey)).thenReturn(rowGroupData);

        when(proxiedPageSourceProvider.createPageSource(eq(connectorTransactionHandle),
                eq(connectorSession),
                any(DispatcherSplit.class),
                isA(TestingMetadata.TestingTableHandle.class),
                anyList(),
                any(DynamicFilter.class)))
                .thenReturn(mock(TestingConnectorPageSource.class));

        ConnectorPageSource pageSource = dispatcherAlternativePageSourceProvider.createPageSource(connectorTransactionHandle,
                connectorSession,
                columns,
                new CompletedDynamicFilter(predicate),
                true);
        pageSource = ((DispatcherWrapperPageSource) pageSource).getConnectorPageSource();
        assertThat(pageSource).isInstanceOf(DispatcherPageSource.class);
        assertThat(((DispatcherPageSource) pageSource).getPageSourceDecision())
                .isEqualTo(PageSourceDecision.MIXED);
    }

    @Test
    public void testReadFlow_FileExist_mixedWithOnlyPredicateOnProxied()
    {
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of("C1", VarcharType.VARCHAR),
                        Pair.of("C2", IntegerType.INTEGER)));

        RowGroupData rowGroupData = generateRowGroupData(rowGroupKey, columns.subList(0, 1));
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);
        when(rowGroupDataService.getIfPresent(rowGroupKey)).thenReturn(rowGroupData);

        mockQueryClassifier(false, false, false);

        when(proxiedPageSourceProvider.createPageSource(eq(connectorTransactionHandle),
                eq(connectorSession),
                eq(dispatcherSplit.getProxyConnectorSplit()),
                eq(dispatcherTableHandle.getProxyConnectorTableHandle()),
                anyList(),
                any(DynamicFilter.class)))
                .thenReturn(mock(TestingConnectorPageSource.class));

        ConnectorPageSource pageSource = dispatcherAlternativePageSourceProvider.createPageSource(connectorTransactionHandle,
                connectorSession,
                columns,
                dynamicFilter,
                true);

        pageSource = ((DispatcherWrapperPageSource) pageSource).getConnectorPageSource();
        assertThat(pageSource).isInstanceOf(DispatcherPageSource.class);
        assertThat(((DispatcherPageSource) pageSource).getPageSourceDecision())
                .isEqualTo(PageSourceDecision.MIXED);
    }

    @Test
    public void testReadFlow_FileExist_mixedWithPredicateOnVarada()
    {
        Type columnType = IntegerType.INTEGER;
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of("c1", columnType),
                        Pair.of("c2", VarcharType.VARCHAR)));

        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(
                Map.of(columns.get(0), Domain.singleValue(columnType, 3L)));

        RowGroupData rowGroupData = generateRowGroupData(rowGroupKey, columns.subList(0, 1));
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);
        when(rowGroupDataService.getIfPresent(rowGroupKey)).thenReturn(rowGroupData);

        mockQueryClassifier(false, false, false);

        when(proxiedPageSourceProvider.createPageSource(eq(connectorTransactionHandle),
                eq(connectorSession),
                eq(dispatcherSplit.getProxyConnectorSplit()),
                eq(dispatcherTableHandle.getProxyConnectorTableHandle()),
                anyList(),
                any(DynamicFilter.class)))
                .thenReturn(mock(TestingConnectorPageSource.class));

        ConnectorPageSource pageSource = dispatcherAlternativePageSourceProvider.createPageSource(connectorTransactionHandle,
                connectorSession,
                columns,
                new CompletedDynamicFilter(predicate),
                true);

        pageSource = ((DispatcherWrapperPageSource) pageSource).getConnectorPageSource();
        assertThat(pageSource).isInstanceOf(DispatcherPageSource.class);
        assertThat(((DispatcherPageSource) pageSource).getPageSourceDecision())
                .isEqualTo(PageSourceDecision.MIXED);
    }

    @Test
    public void testLockRowGroup()
            throws IOException
    {
        RowGroupData rowGroupData = generateRowGroupData(rowGroupKey, columnHandles);
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);
        when(rowGroupDataService.getIfPresent(rowGroupKey)).thenReturn(rowGroupData);
        mockQueryClassifier(false, false, false);

        // rowGroup is locked before creating PageSourceProvider
        rowGroupData.getLock().readLock();

        dispatcherAlternativePageSourceProvider = createDispatcherPageSourceProvider(dispatcherSplit, dispatcherTableHandle, resourceCloser);
        assertThat(rowGroupData.getLock().getCount()).isEqualTo(1);

        ConnectorPageSource pageSource = dispatcherAlternativePageSourceProvider.createPageSource(
                connectorTransactionHandle,
                connectorSession,
                columnHandles,
                dynamicFilter,
                true);
        assertThat(rowGroupData.getLock().getCount()).isEqualTo(2);

        dispatcherAlternativePageSourceProvider.close();
        assertThat(rowGroupData.getLock().getCount()).isEqualTo(1);

        pageSource.close();
        assertThat(rowGroupData.getLock().getCount()).isEqualTo(0);
    }

    private DispatcherAlternativePageSourceProvider createDispatcherPageSourceProvider(ConnectorSplit split, ConnectorTableHandle table, DispatcherAlternativeChooser.ResourceCloser resourceCloser)
    {
        StorageEngineTxService txService = mock(StorageEngineTxService.class);

        NativeStorageStateHandler nativeStorageStateHandler = mock(NativeStorageStateHandler.class);
        when(nativeStorageStateHandler.isStorageAvailable()).thenReturn(true);

        DispatcherPageSourceFactory pageSourceFactory = new DispatcherPageSourceFactory(
                mock(StorageEngine.class),
                storageEngineConstants,
                mock(BufferAllocator.class),
                rowGroupDataService,
                workerWarmingService,
                metricsManager,
                dispatcherProxiedConnectorTransformer,
                mock(PredicatesCacheService.class),
                queryClassifier,
                mock(DictionaryCacheService.class),
                globalConfiguration,
                nativeStorageStateHandler,
                new ReadErrorHandler(mock(WarmupDemoterService.class), rowGroupDataService, mock(PrintMetricsTimerTask.class)),
                mock(CollectTxService.class),
                mock(ChunksQueueService.class),
                mock(StorageCollectorService.class),
                mock(RangeFillerService.class));

        return new DispatcherAlternativePageSourceProvider(proxiedPageSourceProvider,
                pageSourceFactory,
                txService,
                customStatsContext,
                new CatalogNameProvider("varada"),
                split,
                table,
                resourceCloser);
    }

    protected void mockQueryClassifier(boolean isProxyOnly, boolean isVaradaOnly, boolean isPrefilledOnly)
    {
        QueryContext basicQueryContext = mock(QueryContext.class);
        when(basicQueryContext.getPredicateContextData()).thenReturn(new PredicateContextData(ImmutableMap.of(), VaradaPrimitiveConstant.TRUE));
        when(queryClassifier.getBasicQueryContext(anyList(),
                eq(dispatcherTableHandle),
                any(DynamicFilter.class),
                any(ConnectorSession.class)))
                .thenReturn(basicQueryContext);
        QueryContext queryContext = mock(QueryContext.class);
        when(queryContext.getNativeQueryCollectDataList()).thenReturn(ImmutableList.of());
        when(queryContext.getPrefilledQueryCollectDataByBlockIndex()).thenReturn(ImmutableMap.of());
        when(queryContext.getMatchData()).thenReturn(Optional.empty());
        when(queryContext.getRemainingCollectColumns()).thenReturn(ImmutableList.of());
        when(queryContext.getRemainingCollectColumnByBlockIndex()).thenReturn(ImmutableMap.of());
        when(queryContext.getPredicateContextData()).thenReturn(new PredicateContextData(ImmutableMap.of(), VaradaPrimitiveConstant.TRUE));
        when(queryContext.isProxyOnly()).thenReturn(isProxyOnly);
        when(queryContext.isVaradaOnly()).thenReturn(isVaradaOnly);
        when(queryContext.isPrefilledOnly()).thenReturn(isPrefilledOnly);
        when(queryClassifier.classify(eq(basicQueryContext),
                any(),
                eq(dispatcherTableHandle),
                eq(Optional.of(connectorSession))))
                .thenReturn(queryContext);
    }
}
