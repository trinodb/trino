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
package io.trino.plugin.varada.storage.read;

import io.trino.plugin.varada.TestingTxService;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dictionary.DictionaryCacheService;
import io.trino.plugin.varada.dispatcher.DispatcherPageSourceFactory;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.data.collect.NativeQueryCollectData;
import io.trino.plugin.varada.dispatcher.query.data.match.BasicBloomQueryMatchData;
import io.trino.plugin.varada.expression.NativeExpression;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.juffer.PredicateBufferInfo;
import io.trino.plugin.varada.juffer.PredicateBufferPoolType;
import io.trino.plugin.varada.juffer.PredicateCacheData;
import io.trino.plugin.varada.juffer.PredicatesCacheService;
import io.trino.plugin.varada.metrics.CustomStatsContext;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.JbufType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.RecordIndexListHeader;
import io.trino.plugin.warp.gen.constants.RecordIndexListType;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.VaradaStatsDictionary;
import io.trino.plugin.warp.gen.stats.VaradaStatsDispatcherPageSource;
import io.trino.spi.Page;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VaradaPageSourceTest
{
    private final boolean isMatchGetNumRanges = false;
    private StorageEngine storageEngine;
    private StorageEngineConstants storageEngineConstants;
    private BufferAllocator bufferAllocator;
    private DictionaryCacheService dictionaryCacheService;
    private CustomStatsContext customStatsContext;
    private GlobalConfiguration globalConfiguration;

    @BeforeEach
    public void before()
    {
        this.storageEngine = mock(StorageEngine.class);
        this.storageEngineConstants = mock(StorageEngineConstants.class);
        when(storageEngineConstants.getChunkSizeShift()).thenReturn(16);
        when(storageEngineConstants.getMaxChunksInRange()).thenReturn(8);
        this.bufferAllocator = mock(BufferAllocator.class);
        MetricsManager metricsManager = TestingTxService.createMetricsManager();
        this.customStatsContext = new CustomStatsContext(metricsManager, Collections.emptyList());
        customStatsContext.getOrRegister(new VaradaStatsDispatcherPageSource(DispatcherPageSourceFactory.STATS_DISPATCHER_KEY));
        customStatsContext.getOrRegister(new VaradaStatsDictionary(DictionaryCacheService.DICTIONARY_STAT_GROUP));
        globalConfiguration = new GlobalConfiguration();
        dictionaryCacheService = mock(DictionaryCacheService.class);
    }

    @Disabled
    @Test
    public void testValidPage()
    {
        ByteBuffer rowsBuff = setMocks();
        PredicatesCacheService predicatesCacheService = mock(PredicatesCacheService.class);
        VaradaPageSource varadaPageSource = new VaradaPageSource(
                storageEngine,
                storageEngineConstants,
                Integer.MAX_VALUE,
                bufferAllocator,
                mock(QueryParams.class),
                isMatchGetNumRanges,
                predicatesCacheService,
                dictionaryCacheService,
                customStatsContext,
                globalConfiguration,
                mock(CollectTxService.class),
                mock(ChunksQueueService.class),
                mock(StorageCollectorService.class),
                new StubsRangeFillerService());
        when(storageEngine.match(anyInt(), anyInt(), any(), any())).thenReturn(mockMatch(rowsBuff)).thenReturn(0L);
        Page nextPage = varadaPageSource.getNextPage();
        assertThat(nextPage).isNotNull();
        assertThat(varadaPageSource.isFinished()).isFalse();
    }

    @Disabled
    @Test
    public void testMatchCollect()
    {
        ByteBuffer rowsBuff = setMocks();
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        SchemaTableColumn schemaTableColumn = new SchemaTableColumn(schemaTableName, new RegularColumn("column"));
        WarmUpElement warmUpElement = WarmUpElement.builder()
                .varadaColumn(schemaTableColumn.varadaColumn())
                .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();

        PredicatesCacheService predicatesCacheService = mock(PredicatesCacheService.class);
        BasicBloomQueryMatchData.Builder queryMatchDataBuilder = new BasicBloomQueryMatchData.Builder();
        queryMatchDataBuilder
                .varadaColumn(warmUpElement.getVaradaColumn())
                .type(IntegerType.INTEGER)
                .warmUpElement(warmUpElement)
                .predicateCacheData(new PredicateCacheData(new PredicateBufferInfo(null, PredicateBufferPoolType.SMALL), Optional.empty()))
                .nativeExpression(NativeExpression.builder()
                        .predicateType(PredicateType.PREDICATE_TYPE_NONE)
                        .functionType(FunctionType.FUNCTION_TYPE_NONE)
                        .domain(Domain.all(IntegerType.INTEGER))
                        .build())
                .build();

/*        NativeQueryCollectData.Builder nativeCollectBuilder = new NativeQueryCollectData.Builder()
                .blockIndex(0)
                .type(IntegerType.INTEGER)
                .warmUpElement(warmUpElement)
                .varadaColumn(warmUpElement.getVaradaColumn())
                .matchCollectType(MatchCollectType.ORDINARY);*/

        VaradaPageSource varadaPageSource = new VaradaPageSource(
                storageEngine,
                storageEngineConstants,
                Integer.MAX_VALUE,
                bufferAllocator,
                mock(QueryParams.class),
                isMatchGetNumRanges,
                predicatesCacheService,
                dictionaryCacheService,
                customStatsContext,
                globalConfiguration,
                mock(CollectTxService.class),
                mock(ChunksQueueService.class),
                mock(StorageCollectorService.class),
                new StubsRangeFillerService());
        when(storageEngine.match(anyInt(), anyInt(), any(), any())).thenReturn(mockMatch(rowsBuff)).thenReturn(0L);
        assertThat(varadaPageSource.getNextPage()).isNotNull();
        assertThat(varadaPageSource.isFinished()).isFalse();
    }

    @Disabled
    @Test
    public void testMatchOnly()
    {
        ByteBuffer rowsBuff = setMocks();
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        SchemaTableColumn schemaTableColumn = new SchemaTableColumn(schemaTableName, new RegularColumn("column"));
        WarmUpElement warmUpElement = WarmUpElement.builder()
                .varadaColumn(schemaTableColumn.varadaColumn())
                .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();

        PredicatesCacheService predicatesCacheService = mock(PredicatesCacheService.class);
        BasicBloomQueryMatchData.Builder queryMatchDataBuilder = new BasicBloomQueryMatchData.Builder();
        queryMatchDataBuilder
                .varadaColumn(warmUpElement.getVaradaColumn())
                .type(IntegerType.INTEGER)
                .warmUpElement(warmUpElement)
                .predicateCacheData(new PredicateCacheData(new PredicateBufferInfo(null, PredicateBufferPoolType.SMALL), Optional.empty()))
                .nativeExpression(NativeExpression.builder()
                        .predicateType(PredicateType.PREDICATE_TYPE_NONE)
                        .functionType(FunctionType.FUNCTION_TYPE_NONE)
                        .domain(Domain.all(IntegerType.INTEGER))
                        .build())
                .build();

        VaradaPageSource varadaPageSource = new VaradaPageSource(
                storageEngine,
                storageEngineConstants,
                Integer.MAX_VALUE,
                bufferAllocator,
                mock(QueryParams.class),
                isMatchGetNumRanges,
                predicatesCacheService,
                dictionaryCacheService,
                customStatsContext,
                globalConfiguration,
                mock(CollectTxService.class),
                mock(ChunksQueueService.class),
                mock(StorageCollectorService.class),
                new StubsRangeFillerService());
        when(storageEngine.match(anyInt(), anyInt(), any(), any())).thenReturn(mockMatch(rowsBuff)).thenReturn(0L);
        assertThat(varadaPageSource.getNextPage()).isNotNull();
        assertThat(varadaPageSource.isFinished()).isFalse();
    }

    @Disabled
    @Test
    public void testCollect()
    {
        ByteBuffer rowsBuff = setMocks();
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        SchemaTableColumn schemaTableColumn = new SchemaTableColumn(schemaTableName, new RegularColumn("column"));
        WarmUpElement warmUpElement = WarmUpElement.builder()
                .varadaColumn(schemaTableColumn.varadaColumn())
                .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();

        PredicatesCacheService predicatesCacheService = mock(PredicatesCacheService.class);

        NativeQueryCollectData.Builder nativeCollectBuilder = new NativeQueryCollectData.Builder();
        nativeCollectBuilder.blockIndex(0).type(IntegerType.INTEGER).warmUpElement(warmUpElement).varadaColumn(warmUpElement.getVaradaColumn());

        VaradaPageSource varadaPageSource = new VaradaPageSource(
                storageEngine,
                storageEngineConstants,
                Integer.MAX_VALUE,
                bufferAllocator,
                mock(QueryParams.class),
                isMatchGetNumRanges,
                predicatesCacheService,
                dictionaryCacheService,
                customStatsContext,
                globalConfiguration,
                mock(CollectTxService.class),
                mock(ChunksQueueService.class),
                mock(StorageCollectorService.class),
                new StubsRangeFillerService());
        when(storageEngine.match(anyInt(), anyInt(), any(), any())).thenReturn(mockMatch(rowsBuff)).thenReturn(0L);
        Page nextPage = varadaPageSource.getNextPage();
        assertThat(nextPage).isNotNull();
        assertThat(nextPage.getPositionCount()).isEqualTo(10);
        assertThat(varadaPageSource.isFinished()).isFalse();
    }

    @Disabled
    @Test
    public void testCollectWithMatchOnDifferentColumn()
    {
        ByteBuffer rowsBuff = setMocks();
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        SchemaTableColumn schemaTableColumn = new SchemaTableColumn(schemaTableName, new RegularColumn("column"));
        WarmUpElement warmUpElement = WarmUpElement.builder()
                .varadaColumn(schemaTableColumn.varadaColumn())
                .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();
        WarmUpElement warmUpElementMatch = WarmUpElement.builder()
                .varadaColumn(new RegularColumn("m_" + schemaTableColumn.varadaColumn().getName()))
                .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();

        PredicatesCacheService predicatesCacheService = mock(PredicatesCacheService.class);

        BasicBloomQueryMatchData.Builder queryMatchDataBuilder = new BasicBloomQueryMatchData.Builder();
        queryMatchDataBuilder.varadaColumn(warmUpElementMatch.getVaradaColumn())
                .type(IntegerType.INTEGER)
                .warmUpElement(warmUpElementMatch)
                .predicateCacheData(new PredicateCacheData(new PredicateBufferInfo(null, PredicateBufferPoolType.SMALL), Optional.empty()))
                .nativeExpression(NativeExpression.builder()
                        .predicateType(PredicateType.PREDICATE_TYPE_NONE)
                        .functionType(FunctionType.FUNCTION_TYPE_NONE)
                        .domain(Domain.all(IntegerType.INTEGER))
                        .build())
                .build();

        NativeQueryCollectData.Builder nativeCollectBuilder = new NativeQueryCollectData.Builder();
        nativeCollectBuilder.blockIndex(0).type(IntegerType.INTEGER).warmUpElement(warmUpElement).varadaColumn(warmUpElement.getVaradaColumn());
        NativeQueryCollectData.Builder nativeCollectBuilderMatch = new NativeQueryCollectData.Builder();
        nativeCollectBuilderMatch.blockIndex(1).type(IntegerType.INTEGER).warmUpElement(warmUpElementMatch).varadaColumn(warmUpElementMatch.getVaradaColumn());

        VaradaPageSource varadaPageSource = new VaradaPageSource(
                storageEngine,
                storageEngineConstants,
                Integer.MAX_VALUE,
                bufferAllocator,
                mock(QueryParams.class),
                isMatchGetNumRanges,
                predicatesCacheService,
                dictionaryCacheService,
                customStatsContext,
                globalConfiguration,
                mock(CollectTxService.class),
                mock(ChunksQueueService.class),
                mock(StorageCollectorService.class),
                new StubsRangeFillerService());
        when(storageEngine.match(anyInt(), anyInt(), any(), any())).thenReturn(mockMatch(rowsBuff)).thenReturn(0L);
        assertThat(varadaPageSource.getNextPage()).isNotNull();
        assertThat(varadaPageSource.isFinished()).isFalse();
    }

    private ByteBuffer setMocks()
    {
        ByteBuffer rowsBuff = ByteBuffer.allocate(100);
        when(bufferAllocator.getQueryIdsArray(anyBoolean())).thenReturn(new long[JbufType.JBUF_TYPE_QUERY_NUM_OF.ordinal()]);
        when(bufferAllocator.ids2RecBuff(any())).thenReturn(ByteBuffer.allocate(100));
        when(bufferAllocator.ids2NullBuff(any())).thenReturn(ByteBuffer.allocate(100));
        when(bufferAllocator.ids2RowsBuff(anyLong())).thenReturn(rowsBuff.asShortBuffer());
        when(bufferAllocator.id2ByteBuff(anyLong())).thenReturn(ByteBuffer.allocate(100));
        when(bufferAllocator.ids2RecordBufferStateBuff(anyLong())).thenReturn(ByteBuffer.allocate(100).asIntBuffer());
        return rowsBuff;
    }

    private long mockMatch(ByteBuffer rowsBuff)
    {
        rowsBuff.asShortBuffer().put(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal(), (short) RecordIndexListType.RECORD_INDEX_LIST_TYPE_VALUES.ordinal());
        rowsBuff.asShortBuffer().put(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TOTAL_SIZE.ordinal(), (short) 10);
        return 0x100000001L;
    }
}
