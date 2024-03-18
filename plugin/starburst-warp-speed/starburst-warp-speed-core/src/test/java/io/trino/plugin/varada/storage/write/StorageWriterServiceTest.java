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
package io.trino.plugin.varada.storage.write;

import io.trino.plugin.varada.TestingTxService;
import io.trino.plugin.varada.WarmColumnDataTestUtil;
import io.trino.plugin.varada.configuration.DictionaryConfiguration;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.dictionary.DictionaryCacheService;
import io.trino.plugin.varada.dictionary.DictionaryWarmInfo;
import io.trino.plugin.varada.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.varada.dispatcher.model.DictionaryState;
import io.trino.plugin.varada.dispatcher.warmup.transform.BlockTransformerFactory;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.metrics.PrintMetricsTimerTask;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.BaseJuffer;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.varada.storage.lucene.LuceneFileType;
import io.trino.plugin.varada.storage.lucene.LuceneIndexer;
import io.trino.plugin.varada.storage.write.appenders.BlockAppenderFactory;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.IntArrayBlockBuilder;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.VarcharType;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.mockBufferAllocator;
import static io.trino.spi.block.ColumnarTestUtils.createTestDictionaryBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("deprecation")
public class StorageWriterServiceTest
{
    private static final int SHOULD_BE_NULL = -1;
    private StorageEngine storageEngine;
    private DictionaryCacheService dictionaryCacheService;
    private StorageWriterService storageWriterService;

    public static Page buildIntPage(int... values)
    {
        IntArrayBlockBuilder block = new IntArrayBlockBuilder(null, values.length);
        IntStream.range(0, values.length).forEach((i) -> block.writeInt(values[i]));
        return new Page(block.build());
    }

    public static Page buildLongPage(long... values)
    {
        LongArrayBlockBuilder block = new LongArrayBlockBuilder(null, values.length);
        IntStream.range(0, values.length).forEach((i) -> block.writeLong(values[i]));
        LazyBlock lazyBlock = new LazyBlock(values.length, block::build);
        return new Page(lazyBlock);
    }

    public static Page buildVarcharPage(String... values)
    {
        VarcharType unboundedVarcharType = createUnboundedVarcharType();
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, values.length, values.length);
        for (String value : values) {
            unboundedVarcharType.writeString(blockBuilder, value);
        }
        Block block = blockBuilder.build();

        return new Page(block);
    }

    @BeforeEach
    public void before()
    {
        storageEngine = mock(StorageEngine.class);
        when(storageEngine.warmupElementOpen(anyInt(), anyLong(), anyInt(), anyInt(), anyInt(), anyInt(), anyLong(), any())).thenReturn(1L);
        StubsStorageEngineConstants storageEngineConstants = new StubsStorageEngineConstants();
        MetricsManager metricsManager = TestingTxService.createMetricsManager();

        DictionaryConfiguration dictionaryConfiguration = new DictionaryConfiguration();
        dictionaryConfiguration.setDictionaryCacheConcurrencyLevel(1);

        NativeConfiguration nativeConfiguration = new NativeConfiguration();
        nativeConfiguration.setBundleSize(1 << 27);
        nativeConfiguration.setTaskMaxWorkerThreads(4);
        nativeConfiguration.setPredicateBundleSizeInMegaBytes(20);

        BufferAllocator bufferAllocator = mockBufferAllocator(storageEngineConstants, nativeConfiguration, metricsManager);
        dictionaryCacheService = mock(DictionaryCacheService.class);
        BlockTransformerFactory blockTransformerFactory = new BlockTransformerFactory();
        BlockAppenderFactory blockAppenderFactory = new BlockAppenderFactory(storageEngineConstants, bufferAllocator, new GlobalConfiguration(), blockTransformerFactory);
        storageWriterService = new StorageWriterService(storageEngine,
                storageEngineConstants,
                bufferAllocator,
                dictionaryCacheService,
                metricsManager,
                mock(PrintMetricsTimerTask.class),
                blockAppenderFactory);
    }

    @Test
    public void writeInt()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1", IntegerType.INTEGER), WarmUpType.WARM_UP_TYPE_DATA);
        int[] values = new int[] {1, 2, 3};
        Page page = buildIntPage(values);
        List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();
        StorageWriterContext storageWriterContext = runTest(page, warmupElementWriteMetadata, outDictionaryWarmInfos);
        WriteJuffersWarmUpElement dataRecordJuffer = storageWriterContext.getWriteJuffersWarmUpElement();
        IntBuffer actualRecBuffer = (IntBuffer) dataRecordJuffer.getRecordBuffer();

        assertPositionResults(dataRecordJuffer, values.length, SHOULD_BE_NULL);
        assertThat(actualRecBuffer.position()).isEqualTo(values.length);
        actualRecBuffer.position(0);
        int[] writtenValues = new int[values.length];
        actualRecBuffer.get(writtenValues, 0, values.length);
        assertThat(values).isEqualTo(writtenValues);
        assertThat(outDictionaryWarmInfos).anyMatch(x -> x.dictionaryState() == DictionaryState.DICTIONARY_NOT_EXIST);
    }

    @Test
    public void writeReal()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1", RealType.REAL), WarmUpType.WARM_UP_TYPE_DATA);
        int[] values = new int[] {1, 2, 3};
        Page page = buildIntPage(values);
        List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();
        StorageWriterContext storageWriterContext = runTest(page, warmupElementWriteMetadata, outDictionaryWarmInfos);
        WriteJuffersWarmUpElement dataRecordJuffer = storageWriterContext.getWriteJuffersWarmUpElement();

        assertPositionResults(dataRecordJuffer, values.length, SHOULD_BE_NULL);

        IntBuffer actualRecBuffer = (IntBuffer) dataRecordJuffer.getRecordBuffer();
        assertThat(actualRecBuffer.position()).isEqualTo(values.length);
        actualRecBuffer.position(0);
        int[] writtenValues = new int[values.length];
        actualRecBuffer.get(writtenValues, 0, values.length);
        assertThat(values).isEqualTo(writtenValues);
    }

    @Test
    public void writeLong()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1", BIGINT), WarmUpType.WARM_UP_TYPE_DATA);

        long[] values = new long[] {1, 2, 3};
        Page page = buildLongPage(values);
        List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();
        StorageWriterContext storageWriterContext = runTest(page, warmupElementWriteMetadata, outDictionaryWarmInfos);
        WriteJuffersWarmUpElement dataRecordJuffer = storageWriterContext.getWriteJuffersWarmUpElement();

        assertPositionResults(dataRecordJuffer, values.length, SHOULD_BE_NULL);

        LongBuffer actualRecBuffer = (LongBuffer) dataRecordJuffer.getRecordBuffer();
        assertThat(actualRecBuffer.position()).isEqualTo(values.length);
        actualRecBuffer.position(0);
        long[] writtenValues = new long[values.length];
        actualRecBuffer.get(writtenValues, 0, values.length);
        assertThat(values).isEqualTo(writtenValues);
    }

    @Test
    public void writeArrayTypeArrayOfInteger()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        ArrayType arrayIntType = new ArrayType(IntegerType.INTEGER);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1", arrayIntType), WarmUpType.WARM_UP_TYPE_DATA);
        int[] values = new int[] {1, 2, 3};
        Page page = buildArrayType_IntPage(values);
        List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();
        StorageWriterContext storageWriterContext = runTest(page, warmupElementWriteMetadata, outDictionaryWarmInfos);
        WriteJuffersWarmUpElement dataRecordJuffer = storageWriterContext.getWriteJuffersWarmUpElement();

        assertPositionResults(dataRecordJuffer, 1, 1);
    }

    @Test
    public void writeArrayTypeArrayOfBigInt()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        ArrayType arrayBigIntType = new ArrayType(BIGINT);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1", arrayBigIntType), WarmUpType.WARM_UP_TYPE_DATA);

        long[][] values = new long[][] {
                new long[] {Long.MAX_VALUE, 7, Long.MIN_VALUE},
                new long[] {1, 2, 3}
        };

        Page page = buildArrayType_BigIntPage(values);
        List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();
        StorageWriterContext storageWriterContext = runTest(page, warmupElementWriteMetadata, outDictionaryWarmInfos);
        WriteJuffersWarmUpElement dataRecordJuffer = storageWriterContext.getWriteJuffersWarmUpElement();
        assertPositionResults(dataRecordJuffer, 2, 1);
    }

    @Test
    public void writeVarcharArray_EmptyArray()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        ArrayType varcharArrayType = new ArrayType(VARCHAR);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1",
                        varcharArrayType),
                WarmUpType.WARM_UP_TYPE_DATA);
        String[][] values = new String[][] {
        };

        Page page = buildArrayType_VarcharPage(values);
        List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();
        StorageWriterContext storageWriterContext = runTest(page, warmupElementWriteMetadata, outDictionaryWarmInfos);
        WriteJuffersWarmUpElement dataRecordJuffer = storageWriterContext.getWriteJuffersWarmUpElement();
        assertPositionResults(dataRecordJuffer, values.length, 0);
    }

    @Test
    public void writeVarcharArray()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        ArrayType varcharArrayType = new ArrayType(VARCHAR);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1", varcharArrayType), WarmUpType.WARM_UP_TYPE_DATA);

        String[][] values = new String[][] {
                new String[] {"1", "22", "33"},
                new String[] {"333", "4444"},
                new String[] {"5"},
        };

        Page page = buildArrayType_VarcharPage(values);
        List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();
        StorageWriterContext storageWriterContext = runTest(page, warmupElementWriteMetadata, outDictionaryWarmInfos);
        WriteJuffersWarmUpElement dataRecordJuffer = storageWriterContext.getWriteJuffersWarmUpElement();
        assertPositionResults(dataRecordJuffer, values.length, 1);

        ByteBuffer recordBuffer = (ByteBuffer) dataRecordJuffer.getRecordBuffer();
        assertThat(recordBuffer.getInt(1)).isEqualTo(values[0].length);
    }

    @Test
    public void writeVarcharArrayTest_TestNullBuffer()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        ArrayType varcharArrayType = new ArrayType(VARCHAR);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1", varcharArrayType), WarmUpType.WARM_UP_TYPE_DATA);

        String[][] values = new String[][] {
                new String[] {null}
        };

        Page page = buildArrayType_VarcharPage(values);
        List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();
        StorageWriterContext storageWriterContext = runTest(page, warmupElementWriteMetadata, outDictionaryWarmInfos);
        WriteJuffersWarmUpElement dataRecordJuffer = storageWriterContext.getWriteJuffersWarmUpElement();
        assertPositionResults(dataRecordJuffer, 1, 1);
        ByteBuffer recordBuffer = (ByteBuffer) dataRecordJuffer.getRecordBuffer();
        assertThat(recordBuffer.getInt(1)).isEqualTo(values[0].length);
    }

    @Test
    public void writeVarcharArrayTest_TestNullAtEndOfRow()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        ArrayType varcharArrayType = new ArrayType(VarcharType.VARCHAR);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1", varcharArrayType), WarmUpType.WARM_UP_TYPE_DATA);

        String[][] values = new String[][] {
                new String[] {"a", null}
        };

        Page page = buildArrayType_VarcharPage(values);
        List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();
        StorageWriterContext storageWriterContext = runTest(page, warmupElementWriteMetadata, outDictionaryWarmInfos);
        WriteJuffersWarmUpElement dataRecordJuffer = storageWriterContext.getWriteJuffersWarmUpElement();
        assertPositionResults(dataRecordJuffer, 1, 1);
        ByteBuffer recordBuffer = (ByteBuffer) dataRecordJuffer.getRecordBuffer();
        assertThat(recordBuffer.getInt(1)).isEqualTo(values[0].length);
    }

    @Disabled
    @Test
    public void testBigArrayVarcharWithRecycleBuffers()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        Page page = createBigVarcharArrayPage(false);
        Page dictPage = createBigVarcharArrayPage(true);

        ArrayType varcharArrayType = new ArrayType(VarcharType.VARCHAR);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1", varcharArrayType), WarmUpType.WARM_UP_TYPE_DATA);

        List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();
        StorageWriterSplitConfiguration storageWriterSplitConfiguration = storageWriterService.startWarming("nodeIdentifier", "rowGroupFilePath", true);
        StorageWriterContext storageWriterContext = txCreate(storageWriterSplitConfiguration, warmupElementWriteMetadata, outDictionaryWarmInfos);
        storageWriterService.appendPage(page, storageWriterContext);
        verify(storageEngine, times(2)).commitRecordBufferPrepare(0);

        storageWriterService.appendPage(dictPage, storageWriterContext);

        // Dict page has X2 number of rows
        verify(storageEngine, times(4)).commitRecordBufferPrepare(0);
    }

    @Disabled
    @Test
    public void testBigVarcharWithRecycleBuffers()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        Page page = createBigPageVarchar();

        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1", VARCHAR), WarmUpType.WARM_UP_TYPE_DATA);

        List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();
        StorageWriterSplitConfiguration storageWriterSplitConfiguration = storageWriterService.startWarming("nodeIdentifier", "rowGroupFilePath", true);
        StorageWriterContext storageWriterContext = txCreate(storageWriterSplitConfiguration, warmupElementWriteMetadata, outDictionaryWarmInfos);
        storageWriterService.appendPage(page, storageWriterContext);

        verify(storageEngine, times(2)).commitRecordBufferPrepare(0);
    }

    @Test
    public void writeVarchar_varcharIsSmallerThanVarcharAsCharLimit()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        final int typeLength = 5;
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1",
                        VarcharType.createVarcharType(typeLength)),
                WarmUpType.WARM_UP_TYPE_DATA);

        String[] values = new String[] {"a", "A"};
        Page page = buildVarcharPage(values);
        List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();
        StorageWriterContext storageWriterContext = runTest(page, warmupElementWriteMetadata, outDictionaryWarmInfos);
        WriteJuffersWarmUpElement dataRecordJuffer = storageWriterContext.getWriteJuffersWarmUpElement();

        ByteBuffer actualRecBuffer = (ByteBuffer) dataRecordJuffer.getRecordBuffer();

        assertPositionResults(dataRecordJuffer, values.length, SHOULD_BE_NULL);
        assertThat(actualRecBuffer.position()).isEqualTo(typeLength * values.length);
    }

    @Test
    public void writeVarchar()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1",
                        VarcharType.createVarcharType(9)),
                WarmUpType.WARM_UP_TYPE_DATA);
        String[] values = new String[] {"a", "A"};
        Page page = buildVarcharPage(values);
        List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();
        StorageWriterContext storageWriterContext = runTest(page, warmupElementWriteMetadata, outDictionaryWarmInfos);
        WriteJuffersWarmUpElement dataRecordJuffer = storageWriterContext.getWriteJuffersWarmUpElement();

        ByteBuffer actualRecBuffer = (ByteBuffer) dataRecordJuffer.getRecordBuffer();

        assertPositionResults(dataRecordJuffer, values.length, 1);
        assertThat(actualRecBuffer.position()).isEqualTo(2 * values.length);
    }

    @Test
    public void writeVarcharIndex()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1",
                        VarcharType.createVarcharType(9)),
                WarmUpType.WARM_UP_TYPE_BASIC);
        String[] values = new String[] {"a", "A"};
        Page page = buildVarcharPage(values);
        List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();
        StorageWriterContext storageWriterContext = runTest(page, warmupElementWriteMetadata, outDictionaryWarmInfos);
        WriteJuffersWarmUpElement dataRecordJuffer = storageWriterContext.getWriteJuffersWarmUpElement();

        ByteBuffer actualRecBuffer = dataRecordJuffer.getCrcBuffer();
        int expectedPosition = values.length * (8 + 2);

        assertBuffer(dataRecordJuffer.getCrcJuffer(), expectedPosition);

        assertPositionResults(dataRecordJuffer, 2, SHOULD_BE_NULL);
        assertThat(actualRecBuffer.position()).isEqualTo(expectedPosition);
    }

    @Test
    public void writeVarcharWithLucene()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1",
                        VarcharType.createVarcharType(9)),
                WarmUpType.WARM_UP_TYPE_LUCENE);

        String[] values = new String[] {"a", "b"};
        Page page = buildVarcharPage(values);
        List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();
        StorageWriterSplitConfiguration storageWriterSplitConfiguration = storageWriterService.startWarming("nodeIdentifier", "rowGroupFilePath", true);
        StorageWriterContext storageWriterContext = txCreate(storageWriterSplitConfiguration, warmupElementWriteMetadata, outDictionaryWarmInfos);
        storageWriterService.appendPage(page, storageWriterContext);
        WriteJuffersWarmUpElement dataRecordJuffer = storageWriterContext.getWriteJuffersWarmUpElement();

        LuceneIndexer luceneIndexer = storageWriterContext.getLuceneIndexer().get();
        ByteBuffersDirectory directory = (ByteBuffersDirectory) luceneIndexer.getLuceneDirectory();
        assertThat(directory).isNotNull();
        storageWriterService.close(page.getPositionCount(), storageWriterSplitConfiguration, storageWriterContext);

        assertThatThrownBy(directory::listAll).isInstanceOf(AlreadyClosedException.class); //since it finish will throw AlreadyClosedException.class

        assertThat(dataRecordJuffer.getLuceneFileBuffer(LuceneFileType.SI).position()).isGreaterThan(0);
        assertThat(dataRecordJuffer.getLuceneFileBuffer(LuceneFileType.CFE).position()).isGreaterThan(0);
        assertThat(dataRecordJuffer.getLuceneFileBuffer(LuceneFileType.CFS).position()).isGreaterThan(0);
        assertThat(dataRecordJuffer.getLuceneFileBuffer(LuceneFileType.SEGMENTS).position()).isGreaterThan(0);
        //todo: check why failed
        //verify(storageEngine, times(values.length * 4)).luceneWriteBuffer(anyLong(), anyInt(), anyInt(), anyInt());
        //verify(storageEngine, times(values.length)).luceneCommitBuffers(anyLong(), anyBoolean(), any(int[].class));
    }

    @Test
    public void abortVarcharWithLucene()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1",
                        VarcharType.createVarcharType(9)),
                WarmUpType.WARM_UP_TYPE_LUCENE);

        String[] values = new String[] {"a"};
        Page page = buildVarcharPage(values);
        List<DictionaryWarmInfo> outDictionaryWarmInfos = new ArrayList<>();
        StorageWriterSplitConfiguration storageWriterSplitConfiguration = storageWriterService.startWarming("nodeIdentifier", "rowGroupFilePath", true);
        StorageWriterContext storageWriterContext = txCreate(storageWriterSplitConfiguration, warmupElementWriteMetadata, outDictionaryWarmInfos);
        storageWriterService.appendPage(page, storageWriterContext);

        storageWriterService.abort(false, storageWriterContext, storageWriterSplitConfiguration);
        WriteJuffersWarmUpElement juffersWE = storageWriterContext.getWriteJuffersWarmUpElement();

        LuceneIndexer luceneIndexer = storageWriterContext.getLuceneIndexer().get();
        ByteBuffersDirectory directory = (ByteBuffersDirectory) luceneIndexer.getLuceneDirectory();
        assertThat(directory).isNotNull();
        storageWriterService.abort(false, storageWriterContext, storageWriterSplitConfiguration);

        assertThatThrownBy(directory::listAll).isInstanceOf(AlreadyClosedException.class); //since it finish will throw AlreadyClosedException.class

        assertThat(juffersWE.getLuceneFileBuffer(LuceneFileType.SI).position()).isEqualTo(0);
        assertThat(juffersWE.getLuceneFileBuffer(LuceneFileType.CFE).position()).isEqualTo(0);
        assertThat(juffersWE.getLuceneFileBuffer(LuceneFileType.CFS).position()).isEqualTo(0);
        assertThat(juffersWE.getLuceneFileBuffer(LuceneFileType.SEGMENTS).position()).isEqualTo(0);
        verify(storageEngine, never()).luceneWriteBuffer(anyLong(), anyInt(), anyInt(), anyInt());
        verify(storageEngine, never()).luceneCommitBuffers(anyLong(), anyBoolean(), any(int[].class));
    }

    private StorageWriterContext txCreate(StorageWriterSplitConfiguration storageWriterSplitConfiguration, WarmupElementWriteMetadata warmupElementWriteMetadata, List<DictionaryWarmInfo> outDictionaryWarmInfos)
    {
        return storageWriterService.open(0, 0, 0, storageWriterSplitConfiguration, warmupElementWriteMetadata, outDictionaryWarmInfos);
    }

    private StorageWriterContext runTest(Page page, WarmupElementWriteMetadata warmupElementWriteMetadata, List<DictionaryWarmInfo> outDictionaryWarmInfos)
    {
        StorageWriterSplitConfiguration storageWriterSplitConfiguration = storageWriterService.startWarming("nodeIdentifier", "rowGroupFilePath", true);
        StorageWriterContext storageWriterContext = txCreate(storageWriterSplitConfiguration, warmupElementWriteMetadata, outDictionaryWarmInfos);
        storageWriterService.appendPage(page, storageWriterContext);
        return storageWriterContext;
    }

    private Page createBigPageVarchar()
    {
        VarcharType unboundedVarcharType = createUnboundedVarcharType();
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 100, 100);
        for (int i = 0; i < 100000; i++) {
            unboundedVarcharType.writeString(blockBuilder, "values " + i);
        }
        Block block = blockBuilder.build();

        return new Page(block);
    }

    private Page createBigVarcharArrayPage(Boolean toDictBlock)
    {
        int totalRows = 65537;
        int elementsInRow = 1;
        ArrayBlockBuilder blockBuilder = new ArrayBlockBuilder(VarcharType.VARCHAR, null, totalRows);
        for (int i = 0; i < totalRows; i++) {
            BlockBuilder elementBlockBuilder = VarcharType.VARCHAR.createBlockBuilder(null, elementsInRow);
            for (int j = 0; j < elementsInRow; j++) {
                VarcharType.VARCHAR.writeString(elementBlockBuilder, " " + j);
            }
            VarcharType.VARCHAR.writeObject(blockBuilder, elementBlockBuilder);
        }
        Block block = toDictBlock ? createTestDictionaryBlock(blockBuilder.build()) : blockBuilder.build();

        return new Page(block);
    }

    private Page buildArrayType_VarcharPage(String[][] values)
    {
        ArrayBlockBuilder blockBuilder = new ArrayBlockBuilder(VARCHAR, null, 100);
        for (String[] stringArray : values) {
            BlockBuilder elementBlockBuilder = VARCHAR.createBlockBuilder(null, stringArray.length);
            for (String v : stringArray) {
                if (v == null) {
                    elementBlockBuilder.appendNull();
                }
                else {
                    VARCHAR.writeString(elementBlockBuilder, v);
                }
            }
            ArrayType arrayType = new ArrayType(VARCHAR);
            arrayType.writeObject(blockBuilder, elementBlockBuilder.build());
        }
        Block block = blockBuilder.build();

        return new Page(block);
    }

    public void assertPositionResults(WriteJuffersWarmUpElement dataRecordJuffer, int expectedNullPosition, int expectedMdPosition)
    {
        assertBuffer(dataRecordJuffer.getNullJuffer(), expectedNullPosition);
        assertBuffer(dataRecordJuffer.getVarlenMdJuffer(), expectedMdPosition);
    }

    private void assertBuffer(BaseJuffer juffer, int expectedPosition)
    {
        if (expectedPosition == SHOULD_BE_NULL) {
            assertNull(juffer);
        }
        else {
            Buffer buffer = juffer.getWrappedBuffer();
            assertThat(buffer.position()).isEqualTo(expectedPosition);
        }
    }

    private Page buildArrayType_BigIntPage(long[][] values)
    {
        BlockBuilder blockBuilder = new ArrayBlockBuilder(BIGINT, null, values.length, values.length);
        writeBigIntValues(blockBuilder, values);
        Block block = blockBuilder.build();
        return new Page(block);
    }

    private void writeBigIntValues(BlockBuilder blockBuilder, long[][] values)
    {
        for (long[] longArray : values) {
            BlockBuilder elementBlockBuilder = BIGINT.createBlockBuilder(null, longArray.length);
            for (long v : longArray) {
                BIGINT.writeLong(elementBlockBuilder, v);
            }
            ArrayType arrayType = new ArrayType(BIGINT);
            arrayType.writeObject(blockBuilder, elementBlockBuilder.build());
        }
    }

    private Page buildArrayType_IntPage(int... values)
    {
        BlockBuilder blockBuilder = new ArrayBlockBuilder(IntegerType.INTEGER, null, values.length, values.length);
        writeIntValues(blockBuilder, values);
        Block block = blockBuilder.build();
        return new Page(block);
    }

    private void writeIntValues(BlockBuilder blockBuilder, int... values)
    {
        BlockBuilder elementBlockBuilder = IntegerType.INTEGER.createBlockBuilder(null, values.length);
        for (int v : values) {
            IntegerType.INTEGER.writeLong(elementBlockBuilder, v);
        }
        ArrayType arrayType = new ArrayType(IntegerType.INTEGER);
        arrayType.writeObject(blockBuilder, elementBlockBuilder.build());
    }
}
