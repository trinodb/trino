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
import io.trino.plugin.varada.TestingTxService;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.connector.TestingConnectorProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.dal.RowGroupDataDao;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.query.MatchCollectIdService;
import io.trino.plugin.varada.dispatcher.query.PredicateData;
import io.trino.plugin.varada.dispatcher.query.classifier.ClassifierFactory;
import io.trino.plugin.varada.dispatcher.query.classifier.PredicateContextFactory;
import io.trino.plugin.varada.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.juffer.PredicateBufferInfo;
import io.trino.plugin.varada.juffer.PredicateBufferPoolType;
import io.trino.plugin.varada.juffer.PredicateCacheData;
import io.trino.plugin.varada.juffer.PredicatesCacheService;
import io.trino.plugin.varada.juffer.StorageEngineTxService;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.storage.engine.StubsStorageEngine;
import io.trino.plugin.varada.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.varada.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorAlternativeChooser;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.testing.TestingMetadata.TestingTableHandle;
import io.trino.testing.TestingSplit;
import io.varada.tools.CatalogNameProvider;
import io.varada.tools.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.LongStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.varada.VaradaSessionProperties.ENABLE_MATCH_COLLECT;
import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.generateRowGroupData;
import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.mockColumnHandle;
import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.mockColumns;
import static io.trino.plugin.varada.util.NodeUtils.mockNodeManager;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingSplit.createRemoteSplit;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DispatcherAlternativeChooserTest
{
    private static final String SCHEMA = "schema";
    private static final String TABLE = "table";
    private static final ColumnHandle COLUMN1 = new TestingColumnHandle("COLUMN1");
    private static final ColumnHandle COLUMN2 = new TestingColumnHandle("COLUMN2");
    private static final ColumnHandle COLUMN3 = new TestingColumnHandle("COLUMN3");

    private ConnectorSession session;
    private DispatcherSplit dispatcherSplit;
    private RowGroupData rowGroupData;
    private TupleDomain<ColumnHandle> pushedDownPredicate;
    private TupleDomain<ColumnHandle> notPushedDownPredicate;
    private RowGroupDataService rowGroupDataService;
    private PredicatesCacheService predicatesCacheService;
    private RowGroupKey rowGroupKey;
    private TestingConnectorPageSourceProvider connectorPageSourceProvider;
    private DispatcherAlternativeChooser dispatcherAlternativeChooser;

    @BeforeEach
    public void before()
    {
        session = mock(ConnectorSession.class);
        when(session.getProperty(eq(ENABLE_MATCH_COLLECT), eq(Boolean.class))).thenReturn(true);
        dispatcherSplit = new DispatcherSplit(SCHEMA, TABLE, "path", 1, 2, 3, emptyList(), emptyList(), "a", createRemoteSplit());

        rowGroupKey = new RowGroupKey(
                dispatcherSplit.getSchemaName(),
                dispatcherSplit.getTableName(),
                dispatcherSplit.getPath(),
                dispatcherSplit.getStart(),
                dispatcherSplit.getLength(),
                dispatcherSplit.getFileModifiedTime(),
                dispatcherSplit.getDeletedFilesHash(),
                "");
        Type columnType = INTEGER;
        List<ColumnHandle> columnHandleList = mockColumns(List.of(
                Pair.of("warmedColumn", columnType),
                Pair.of("notWarmedColumn", columnType)));
        Domain domain = singleValue(columnType, 1L);
        rowGroupData = generateRowGroupData(rowGroupKey, List.of(columnHandleList.get(0)));
        pushedDownPredicate = TupleDomain.withColumnDomains(Map.of(columnHandleList.get(0), domain));
        notPushedDownPredicate = TupleDomain.withColumnDomains(Map.of(columnHandleList.get(1), domain));

        RowGroupDataDao rowGroupDataDao = mock(RowGroupDataDao.class);
        when(rowGroupDataDao.get(rowGroupKey)).thenReturn(rowGroupData);
        when(rowGroupDataDao.getIfPresent(rowGroupKey)).thenReturn(rowGroupData);
        StubsStorageEngine storageEngine = new StubsStorageEngine();
        GlobalConfiguration globalConfiguration = new GlobalConfiguration();
        MetricsManager metricsManager = TestingTxService.createMetricsManager();
        NodeManager nodeManager = mockNodeManager();
        ConnectorSync connectorSync = mock(ConnectorSync.class);
        when(connectorSync.getCatalogName()).thenReturn("");
        when(connectorSync.getCatalogSequence()).thenReturn(0);
        rowGroupDataService = new RowGroupDataService(rowGroupDataDao,
                storageEngine,
                globalConfiguration,
                metricsManager,
                nodeManager,
                connectorSync);

        StubsStorageEngineConstants storageEngineConstants = new StubsStorageEngineConstants();
        predicatesCacheService = mock(PredicatesCacheService.class);
        PredicateBufferInfo predicateBufferInfo = new PredicateBufferInfo(null, PredicateBufferPoolType.INVALID);
        when(predicatesCacheService.predicateDataToBuffer(isA(PredicateData.class), any()))
                .thenReturn(Optional.of(new PredicateCacheData(predicateBufferInfo, Optional.empty())));
        when(predicatesCacheService.getOrCreatePredicateBufferId(isA(PredicateData.class), any()))
                .thenReturn(Optional.of(new PredicateCacheData(predicateBufferInfo, Optional.empty())));
        BufferAllocator bufferAllocator = mock(BufferAllocator.class);
        NativeConfiguration nativeConfiguration = mock(NativeConfiguration.class);
        DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer = new TestingConnectorProxiedConnectorTransformer();
        MatchCollectIdService matchCollectIdService = mock(MatchCollectIdService.class);
        PredicateContextFactory predicateContextFactory = new PredicateContextFactory(globalConfiguration, new TestingConnectorProxiedConnectorTransformer());
        QueryClassifier queryClassifier = new QueryClassifier(
                new ClassifierFactory(
                        storageEngineConstants,
                        predicatesCacheService,
                        bufferAllocator,
                        nativeConfiguration,
                        dispatcherProxiedConnectorTransformer,
                        matchCollectIdService,
                        globalConfiguration),
                connectorSync,
                matchCollectIdService,
                predicateContextFactory);
        connectorPageSourceProvider = new TestingConnectorPageSourceProvider();
        dispatcherAlternativeChooser = createAlternativeChooser(queryClassifier);
    }

    @Test
    public void testSubsumedPredicates()
    {
        assertChoice(pushedDownPredicate, true);
    }

    @Test
    public void testAllPredicate()
    {
        assertChoice(TupleDomain.all(), false);
    }

    @Test
    public void testNotPushedDownPredicate()
    {
        assertChoice(notPushedDownPredicate, false);
    }

    @Test
    public void testPartiallyPushedDownPredicate()
    {
        assertChoice(notPushedDownPredicate.intersect(pushedDownPredicate), false);
    }

    @Test
    public void testImpossibleAlternatives()
    {
        List<ConnectorTableHandle> alternatives = List.of(
                createDispatcherTableHandle(notPushedDownPredicate, true),
                createDispatcherTableHandle(notPushedDownPredicate, true));
        assertThatThrownBy(() -> dispatcherAlternativeChooser.chooseAlternative(session, dispatcherSplit, alternatives))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Trivial alternative doesn't exists");
        assertNoResourcesAreOpened();
    }

    @Test
    public void testNoSubsumedPredicatesAlternative()
    {
        List<ConnectorTableHandle> alternatives = List.of(
                createDispatcherTableHandle(pushedDownPredicate, false),
                createDispatcherTableHandle(pushedDownPredicate, false));
        ConnectorAlternativeChooser.Choice choice = dispatcherAlternativeChooser.chooseAlternative(session, dispatcherSplit, alternatives);
        assertThat(choice.chosenTableHandleIndex()).isIn(0, 1);
        // DispatcherAlternativeChooser should not keep resources when choosing the trivial alternative
        assertNoResourcesAreOpened();
    }

    @Test
    public void testThrowOnClassify()
    {
        List<ConnectorTableHandle> alternatives = List.of(
                createDispatcherTableHandle(pushedDownPredicate, false),
                createDispatcherTableHandle(pushedDownPredicate, true));

        QueryClassifier queryClassifier = mock(QueryClassifier.class);
        Exception exception = new IllegalArgumentException("TEST");
        when(queryClassifier.classify(any(), any(), any(), any(), any(), any())).thenThrow(exception);
        DispatcherAlternativeChooser dispatcherAlternativeChooser = createAlternativeChooser(queryClassifier);

        assertThrows(exception.getClass(), () -> dispatcherAlternativeChooser.chooseAlternative(session, dispatcherSplit, alternatives));
        assertNoResourcesAreOpened();
    }

    @Test
    public void testGetUnenforcedPredicateForFullPredicate()
    {
        // full predicate should only be pruned, but not simplified
        connectorPageSourceProvider.setUnenforcedPredicate(TupleDomain.withColumnDomains(ImmutableMap.of(
                COLUMN1, singleValue(BIGINT, 10L),
                COLUMN2, multipleValues(BIGINT, ImmutableList.of(20L, 22L, 23L)))));
        connectorPageSourceProvider.setPrunedPredicate(TupleDomain.withColumnDomains(ImmutableMap.of(
                COLUMN2, multipleValues(BIGINT, ImmutableList.of(20L, 21L)))));
        assertThat(dispatcherAlternativeChooser.getUnenforcedPredicate(
                session,
                dispatcherSplit,
                createDispatcherTableHandle(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                COLUMN2, multipleValues(BIGINT, ImmutableList.of(20L, 21L, 23L)),
                                COLUMN3, singleValue(BIGINT, 30L))),
                        false),
                TupleDomain.all()))
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                        COLUMN1, singleValue(BIGINT, 10L),
                        COLUMN2, singleValue(BIGINT, 20L))));

        // delegate provider returns TupleDomain.none()
        connectorPageSourceProvider.setUnenforcedPredicate(TupleDomain.none());
        connectorPageSourceProvider.setPrunedPredicate(TupleDomain.none());
        assertThat(dispatcherAlternativeChooser.getUnenforcedPredicate(
                session,
                dispatcherSplit,
                createDispatcherTableHandle(
                        TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN3, singleValue(BIGINT, 1L))),
                        false),
                TupleDomain.all()))
                .isEqualTo(TupleDomain.none());

        // delegate provider returns TupleDomain.all()
        connectorPageSourceProvider.setUnenforcedPredicate(TupleDomain.all());
        connectorPageSourceProvider.setPrunedPredicate(TupleDomain.all());
        assertThat(dispatcherAlternativeChooser.getUnenforcedPredicate(
                session,
                dispatcherSplit,
                createDispatcherTableHandle(
                        TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN3, singleValue(BIGINT, 1L))),
                        false),
                TupleDomain.all()))
                .isEqualTo(TupleDomain.all());
    }

    @Test
    public void testGetUnenforcedPredicateForDynamicFilter()
    {
        // dynamic filter should only be pruned, but not simplified
        connectorPageSourceProvider.setUnenforcedPredicate(TupleDomain.withColumnDomains(ImmutableMap.of(
                COLUMN1, singleValue(BIGINT, 10L),
                COLUMN2, multipleValues(BIGINT, ImmutableList.of(20L, 22L, 23L)))));
        connectorPageSourceProvider.setPrunedPredicate(TupleDomain.withColumnDomains(ImmutableMap.of(
                COLUMN2, multipleValues(BIGINT, ImmutableList.of(20L, 21L)))));
        assertThat(dispatcherAlternativeChooser.getUnenforcedPredicate(
                session,
                dispatcherSplit,
                createDispatcherTableHandle(
                        TupleDomain.all(),
                        false),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        COLUMN2, multipleValues(BIGINT, ImmutableList.of(20L, 21L, 23L)),
                        COLUMN3, singleValue(BIGINT, 30L)))))
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                        COLUMN1, singleValue(BIGINT, 10L),
                        COLUMN2, singleValue(BIGINT, 20L))));

        // delegate provider returns TupleDomain.none()
        connectorPageSourceProvider.setUnenforcedPredicate(TupleDomain.none());
        connectorPageSourceProvider.setPrunedPredicate(TupleDomain.none());
        assertThat(dispatcherAlternativeChooser.getUnenforcedPredicate(
                session,
                dispatcherSplit,
                createDispatcherTableHandle(
                        TupleDomain.all(),
                        false),
                TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN3, singleValue(BIGINT, 1L)))))
                .isEqualTo(TupleDomain.none());

        // delegate provider returns TupleDomain.all()
        connectorPageSourceProvider.setUnenforcedPredicate(TupleDomain.all());
        connectorPageSourceProvider.setPrunedPredicate(TupleDomain.all());
        assertThat(dispatcherAlternativeChooser.getUnenforcedPredicate(
                session,
                dispatcherSplit,
                createDispatcherTableHandle(
                        TupleDomain.all(),
                        false),
                TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN3, singleValue(BIGINT, 1L)))))
                .isEqualTo(TupleDomain.all());
    }

    @Test
    public void testNoPredicateBufferIsAvailable()
    {
        // no buffer is available
        when(predicatesCacheService.getOrCreatePredicateBufferId(isA(PredicateData.class), any())).thenReturn(Optional.empty());

        // create a column that potentially can be fully pushed down
        DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer = mock(DispatcherProxiedConnectorTransformer.class);
        ColumnHandle columnHandle = mockColumnHandle("column", BIGINT, dispatcherProxiedConnectorTransformer);
        rowGroupData = generateRowGroupData(rowGroupKey, List.of(columnHandle));
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);

        // create a predicate that fits to the small cache (big enough to not go to PREALLOC)
        List<Range> ranges = LongStream.range(0, (BufferAllocator.PREDICATE_SMALL_BUF_SIZE / BIGINT.getFixedSize()) + 1)
                .mapToObj(i -> Range.equal(BIGINT, i))
                .toList();
        Domain domain = Domain.create(ValueSet.ofRanges(ranges), true);
        TupleDomain<ColumnHandle> predicateFitToSmallCache = TupleDomain.withColumnDomains(Map.of(columnHandle, domain));

        List<ConnectorTableHandle> alternatives = List.of(
                createDispatcherTableHandle(predicateFitToSmallCache, false),
                createDispatcherTableHandle(predicateFitToSmallCache, true));

        ConnectorAlternativeChooser.Choice choice = dispatcherAlternativeChooser.chooseAlternative(session, dispatcherSplit, alternatives);

        assertThat(choice.chosenTableHandleIndex()).isEqualTo(0);
        // DispatcherAlternativeChooser should not keep resources when choosing the trivial alternative
        assertNoResourcesAreOpened();
    }

    private DispatcherAlternativeChooser createAlternativeChooser(QueryClassifier queryClassifier)
    {
        NativeStorageStateHandler nativeStorageStateHandler = mock(NativeStorageStateHandler.class);
        when(nativeStorageStateHandler.isStorageAvailable()).thenReturn(true);
        return new DispatcherAlternativeChooser(
                connectorPageSourceProvider,
                mock(DispatcherPageSourceFactory.class),
                mock(StorageEngineTxService.class),
                mock(MetricsManager.class),
                new CatalogNameProvider("connector"),
                queryClassifier,
                rowGroupDataService,
                predicatesCacheService,
                nativeStorageStateHandler);
    }

    private void assertChoice(TupleDomain<ColumnHandle> predicate, boolean expectSubsumedPredicates)
    {
        List<ConnectorTableHandle> alternatives = List.of(
                createDispatcherTableHandle(predicate, false),
                createDispatcherTableHandle(predicate, true));
        ConnectorAlternativeChooser.Choice choice = dispatcherAlternativeChooser.chooseAlternative(session, dispatcherSplit, alternatives);
        if (expectSubsumedPredicates) {
            assertThat(choice.chosenTableHandleIndex()).isEqualTo(1);
            // DispatcherAlternativeChooser should keep resources when choosing the non-trivial alternative until provider is closed
            assertResourcesAreOpened();
            choice.pageSourceProvider().close();
            assertNoResourcesAreOpened();
        }
        else {
            assertThat(choice.chosenTableHandleIndex()).isEqualTo(0);
            // DispatcherAlternativeChooser should not keep resources when choosing the trivial alternative
            assertNoResourcesAreOpened();
        }
    }

    private DispatcherTableHandle createDispatcherTableHandle(TupleDomain<ColumnHandle> fullPredicate, boolean subsumedPredicates)
    {
        return new DispatcherTableHandle(
                SCHEMA,
                TABLE,
                OptionalLong.of(1L),
                fullPredicate,
                new SimplifiedColumns(emptySet()),
                new TestingTableHandle(),
                Optional.empty(),
                Collections.emptyList(),
                subsumedPredicates);
    }

    private void assertResourcesAreOpened()
    {
        assertThat(rowGroupData.getLock().getCount()).isEqualTo(1);

        int openedBuffers = (int) Mockito.mockingDetails(predicatesCacheService).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("getOrCreatePredicateBufferId"))
                .count();
        int closedBuffers = Mockito.mockingDetails(predicatesCacheService).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("markFinished"))
                .mapToInt(invocation -> ((List<PredicateCacheData>) invocation.getArguments()[0]).size())
                .sum();
        if (openedBuffers > 0) {
            assertThat(openedBuffers).isGreaterThan(closedBuffers);
        }
    }

    private void assertNoResourcesAreOpened()
    {
        assertThat(rowGroupData.getLock().getCount()).isEqualTo(0);

        int closedBuffers = Mockito.mockingDetails(predicatesCacheService).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("markFinished"))
                .mapToInt(invocation -> ((List<PredicateCacheData>) invocation.getArguments()[0]).size())
                .sum();

        verify(predicatesCacheService, times(closedBuffers)).getOrCreatePredicateBufferId(isA(PredicateData.class), any());
    }

    private static class TestingConnectorPageSourceProvider
            implements ConnectorPageSourceProvider
    {
        private TupleDomain<ColumnHandle> unenforcedPredicate;
        private TupleDomain<ColumnHandle> prunedPredicate;

        public void setUnenforcedPredicate(TupleDomain<ColumnHandle> unenforcedPredicate)
        {
            this.unenforcedPredicate = requireNonNull(unenforcedPredicate, "unenforcedPredicate is null");
        }

        public void setPrunedPredicate(TupleDomain<ColumnHandle> prunedPredicate)
        {
            this.prunedPredicate = requireNonNull(prunedPredicate, "prunedPredicate is null");
        }

        @Override
        public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TupleDomain<ColumnHandle> getUnenforcedPredicate(ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, TupleDomain<ColumnHandle> dynamicFilter)
        {
            checkArgument(split instanceof TestingSplit);
            checkArgument(table instanceof TestingTableHandle);
            return requireNonNull(unenforcedPredicate, "unenforcedPredicate is null");
        }

        @Override
        public TupleDomain<ColumnHandle> prunePredicate(ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, TupleDomain<ColumnHandle> predicate)
        {
            checkArgument(split instanceof TestingSplit);
            checkArgument(table instanceof TestingTableHandle);
            return requireNonNull(prunedPredicate, "prunedPredicate is null");
        }
    }
}
