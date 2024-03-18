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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.dictionary.DictionaryCacheService;
import io.trino.plugin.varada.dictionary.DictionaryMaxException;
import io.trino.plugin.varada.dictionary.DictionaryWarmInfo;
import io.trino.plugin.varada.dictionary.WriteDictionary;
import io.trino.plugin.varada.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.varada.dispatcher.model.DictionaryInfo;
import io.trino.plugin.varada.dispatcher.model.DictionaryKey;
import io.trino.plugin.varada.dispatcher.model.DictionaryState;
import io.trino.plugin.varada.dispatcher.model.WarmState;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmSinkResult;
import io.trino.plugin.varada.juffer.BlockPosHolder;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.juffer.WarmUpElementAllocationParams;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.metrics.PrintMetricsTimerTask;
import io.trino.plugin.varada.storage.engine.ExceptionThrower;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.varada.storage.lucene.LuceneIndexer;
import io.trino.plugin.varada.storage.write.appenders.AppendResult;
import io.trino.plugin.varada.storage.write.appenders.BlockAppender;
import io.trino.plugin.varada.storage.write.appenders.BlockAppenderFactory;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.varada.warmup.exceptions.WarmupException;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.VaradaStatsLuceneIndexer;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.varada.tools.util.Pair;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.varada.dictionary.DictionaryCacheService.DICTIONARY_REC_TYPE_CODE_NUM;
import static io.trino.plugin.varada.dictionary.DictionaryCacheService.DICTIONARY_REC_TYPE_LENGTH;
import static java.util.Objects.requireNonNull;

@Singleton
public class StorageWriterService
{
    private static final Logger logger = Logger.get(StorageWriterService.class);
    private static final String LUCENE_STATS_GROUP_NAME = "lucene-index";
    private static final int STAT_MAX_SLICE_LENGTH = 8;

    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final BufferAllocator bufferAllocator;
    private final DictionaryCacheService dictionaryCacheService;
    private final BlockAppenderFactory blockAppenderFactory;
    private final PrintMetricsTimerTask metricsTimerTask;
    private final VaradaStatsLuceneIndexer statsLuceneIndexer;

    enum WeProperties
    {
        WE_PROPERTIES_QUERY_OFFSET,
        WE_PROPERTIES_QUERY_READ_SIZE,
        WE_PROPERTIES_WARM_EVENTS,
        WE_PROPERTIES_END_OFFSET
    }

    @Inject
    public StorageWriterService(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator,
            DictionaryCacheService dictionaryCacheService,
            MetricsManager metricsManager,
            PrintMetricsTimerTask metricsTimerTask,
            BlockAppenderFactory blockAppenderFactory)
    {
        this.storageEngine = requireNonNull(storageEngine);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.dictionaryCacheService = requireNonNull(dictionaryCacheService);
        this.blockAppenderFactory = requireNonNull(blockAppenderFactory);
        VaradaStatsLuceneIndexer varadaStatsLuceneIndexer = new VaradaStatsLuceneIndexer(LUCENE_STATS_GROUP_NAME, "0");
        this.statsLuceneIndexer = metricsManager.registerMetric(varadaStatsLuceneIndexer);
        this.metricsTimerTask = requireNonNull(metricsTimerTask);
    }

    public StorageWriterSplitConfiguration startWarming(String nodeIdentifier,
            String rowGroupFilePath,
            Boolean dictionaryEnabled)
    {
        return new StorageWriterSplitConfiguration(nodeIdentifier,
                rowGroupFilePath,
                bufferAllocator.allocateLoadSegment(),
                bufferAllocator.allocateLoadWriteBuffer(),
                dictionaryEnabled);
    }

    public void finishWarming(StorageWriterSplitConfiguration storageWriterSplitConfiguration)
    {
        bufferAllocator.freeLoadWriteBuffer(storageWriterSplitConfiguration.writeBuff());
        bufferAllocator.freeLoadSegment(storageWriterSplitConfiguration.buff());
    }

    StorageWriterContext open(int txId,
            long fileCookie,
            int fileOffset,
            StorageWriterSplitConfiguration storageWriterSplitConfiguration,
            WarmupElementWriteMetadata warmupElementWriteMetadata,
            List<DictionaryWarmInfo> outDictionaryWarmInfos)
    {
        Optional<LuceneIndexer> luceneIndexerOpt = Optional.empty();
        Optional<WriteDictionary> writeDictionaryOpt = Optional.empty();
        Pair<DictionaryKey, DictionaryState> dictionaryKeyAndState = dictionaryOpen(warmupElementWriteMetadata, storageWriterSplitConfiguration.nodeIdentifier(), storageWriterSplitConfiguration.dictionaryEnabled());
        DictionaryState dictionaryState = dictionaryKeyAndState.getValue();
        DictionaryKey dictionaryKey = dictionaryKeyAndState.getKey();
        WarmUpElementAllocationParams allocParams = bufferAllocator.calculateAllocationParams(warmupElementWriteMetadata, storageWriterSplitConfiguration.buff());

        // initialize file
        WarmUpElement warmUpElement = warmupElementWriteMetadata.warmUpElement();
        WarmUpElement.Builder warmupElementBuilder = WarmUpElement.builder(warmUpElement);
        warmupElementBuilder.startOffset(fileOffset);
        // open storage engine WE

        boolean hasDictionary = dictionaryState == DictionaryState.DICTIONARY_VALID;
        StorageOpenResult storageOpenResult = storageWeOpen(warmUpElement,
                hasDictionary,
                txId,
                fileCookie,
                fileOffset,
                storageWriterSplitConfiguration.writeBuff().address(),
                allocParams);
        if (storageOpenResult.weCookie() == 0) {
            return null;
        }

        // set up buffers
        WriteJuffersWarmUpElement writeJuffersWarmUpElement = getWriteJuffersWarmUpElement(storageOpenResult, hasDictionary, allocParams);
        if (hasDictionary) {
            WriteDictionary writeDictionary = dictionaryCacheService.computeWriteIfAbsent(dictionaryKey, warmUpElement.getRecTypeCode());
            dictionaryKey = writeDictionary.getDictionaryKey(); //in order to be aligned with createdTimestamp
            writeDictionaryOpt = Optional.of(writeDictionary);
        }
        //set up dictionary
        DictionaryWarmInfo dictionaryWarmInfo = new DictionaryWarmInfo(dictionaryState, dictionaryKey);
        outDictionaryWarmInfos.add(dictionaryWarmInfo);

        if (warmUpElement.getWarmUpType() == WarmUpType.WARM_UP_TYPE_LUCENE) {
            // initialize lucene
            LuceneIndexer luceneIndexer = new LuceneIndexer(storageEngine,
                    storageEngineConstants,
                    writeJuffersWarmUpElement,
                    statsLuceneIndexer);
            luceneIndexer.resetLuceneIndex();
            luceneIndexerOpt = Optional.of(luceneIndexer);
        }

        BlockAppender blockAppender = blockAppenderFactory.createBlockAppender(warmUpElement,
                warmupElementWriteMetadata.type(),
                writeJuffersWarmUpElement,
                luceneIndexerOpt);
        return new StorageWriterContext(warmupElementWriteMetadata,
                warmupElementBuilder,
                writeJuffersWarmUpElement,
                dictionaryWarmInfo,
                storageOpenResult.weCookie(),
                blockAppender,
                true,
                writeDictionaryOpt,
                luceneIndexerOpt);
    }

    WriteJuffersWarmUpElement getWriteJuffersWarmUpElement(StorageOpenResult storageOpenResult, boolean dictionaryValid, WarmUpElementAllocationParams allocParams)
    {
        WriteJuffersWarmUpElement juffersWE = new WriteJuffersWarmUpElement(storageEngine, storageEngineConstants, bufferAllocator, storageOpenResult.buffs(), storageOpenResult.weCookie(), allocParams);
        juffersWE.createBuffers(dictionaryValid);
        return juffersWE;
    }

    private Pair<DictionaryKey, DictionaryState> dictionaryOpen(
            WarmupElementWriteMetadata warmupElementWriteMetadata,
            String nodeIdentifier,
            Boolean dictionaryEnabled)
    {
        DictionaryKey dictionaryKey;
        WarmUpElement warmUpElement = warmupElementWriteMetadata.warmUpElement();
        if (warmUpElement.getDictionaryInfo() != null) {
            dictionaryKey = warmUpElement.getDictionaryInfo().dictionaryKey();
        }
        else {
            long createdTimestamp = dictionaryCacheService.getLastCreatedTimestamp(warmupElementWriteMetadata.schemaTableColumn(), nodeIdentifier);
            dictionaryKey = new DictionaryKey(warmupElementWriteMetadata.schemaTableColumn(), nodeIdentifier, createdTimestamp);
        }
        DictionaryState dictionaryState = dictionaryCacheService.calculateDictionaryStateForWrite(warmUpElement, dictionaryEnabled);
        return Pair.of(dictionaryKey, dictionaryState);
    }

    private StorageOpenResult storageWeOpen(WarmUpElement warmUpElement,
            boolean hasDictionary,
            int txId,
            long fileCookie,
            int fileOffset,
            long writeBuffAddress,
            WarmUpElementAllocationParams allocParams)
    {
        // initialize warm up element attributes
        int recTypeCode = hasDictionary ? DICTIONARY_REC_TYPE_CODE_NUM : TypeUtils.nativeRecTypeCode(warmUpElement.getRecTypeCode());
        int recTypeLength = hasDictionary ? DICTIONARY_REC_TYPE_LENGTH : warmUpElement.getRecTypeLength();
        int warmUpType = warmUpElement.getWarmUpType().ordinal();

        MemorySegment[] buffs = bufferAllocator.getWarmBuffers(allocParams);
        long[] buffAddresses = new long[buffs.length];
        for (int i = 0; i < buffs.length; i++) {
            MemorySegment buff = buffs[i];
            if (buff != null) {
                buffAddresses[i] = buffs[i].address();
            }
        }

        // open storage engine WE
        long weCookie = storageEngine.warmupElementOpen(txId,
                fileCookie,
                fileOffset,
                recTypeCode,
                recTypeLength,
                warmUpType,
                writeBuffAddress,
                buffAddresses);

        return new StorageOpenResult(buffs, weCookie);
    }

    WarmSinkResult close(int totalRecords, StorageWriterSplitConfiguration storageWriterSplitConfiguration, StorageWriterContext storageWriterContext)
    {
        int[] outFileParams = cleanup(false, false, storageWriterContext);

        WarmUpElement.Builder warmupElementBuilder = storageWriterContext.getWarmupElementBuilder();
        if (!storageWriterContext.weSuccess() || outFileParams == null) {
            return new WarmSinkResult(warmupElementBuilder.build(), 0);
        }

        WarmupElementStats closedStats;
        WarmupElementWriteMetadata warmupElementWriteMetadata = storageWriterContext.getWarmupElementWriteMetadata();
        try {
            closedStats = getFinalStats(storageWriterContext.getWarmupElementStats(), warmupElementWriteMetadata);
        }
        catch (Exception e) {
            logger.warn(e, "failed to get range on write");
            throw new IllegalArgumentException();
        }

        warmupElementBuilder.state(WarmUpElementState.VALID)
                .warmState(WarmState.HOT)
                .queryOffset(outFileParams[WeProperties.WE_PROPERTIES_QUERY_OFFSET.ordinal()])
                .queryReadSize(outFileParams[WeProperties.WE_PROPERTIES_QUERY_READ_SIZE.ordinal()])
                .warmEvents(outFileParams[WeProperties.WE_PROPERTIES_WARM_EVENTS.ordinal()])
                .totalRecords(totalRecords)
                .warmupElementStats(closedStats);

        // update record type length if needed
        final int actualRecTypeLength = storageWriterContext.getWriteJuffersWarmUpElement().getActualRecTypeLength();
        if (actualRecTypeLength > 0) {
            final int recTypeLength = warmupElementWriteMetadata.warmUpElement().getRecTypeLength();
            if (actualRecTypeLength < recTypeLength) {
                warmupElementBuilder.recTypeLength(actualRecTypeLength);
            }
        }

        int offset = outFileParams[WeProperties.WE_PROPERTIES_END_OFFSET.ordinal()];
        if (offset == 0) {
            logger.error("offset 0 warmupElementWriteMetadata=%s, storageWriterSplitConfiguration=%s", warmupElementWriteMetadata, storageWriterSplitConfiguration);
            // Native failed to write
            updateToFailedState(warmupElementBuilder, storageWriterContext.getWarmupElementWriteMetadata());
            storageWriterContext.setFailed();
        }
        // attach dictionary if needed
        if (storageWriterContext.weSuccess() && storageWriterContext.getWriteDictionary().isPresent()) {
            int dictionarySize = 0;

            WriteDictionary writeDictionary = storageWriterContext.getWriteDictionary().get();
            try {
                dictionarySize = dictionaryCacheService.writeDictionary(
                        writeDictionary.getDictionaryKey(),
                        warmupElementWriteMetadata.warmUpElement().getRecTypeCode(),
                        offset,
                        storageWriterSplitConfiguration.rowGroupFilePath());
            }
            catch (Exception e) {
                storageWriterContext.setFailed();
                updateToFailedState(warmupElementBuilder, storageWriterContext.getWarmupElementWriteMetadata());
            }
            logger.debug("close fileOffsetsEnd (= dictionaryOffset) %d dictionarySize %d",
                    offset, dictionarySize);

            if (dictionarySize != 0) {
                DictionaryInfo dictionaryInfo = new DictionaryInfo(writeDictionary.getDictionaryKey(),
                        storageWriterContext.getDictionaryWarmInfo().dictionaryState(),
                        writeDictionary.getRecTypeLength(),
                        offset);
                warmupElementBuilder.dictionaryInfo(dictionaryInfo)
                        .usedDictionarySize(writeDictionary.getWriteSize());
                offset += dictionarySize;
            }
        }
        if (storageWriterContext.weSuccess()) {
            warmupElementBuilder.endOffset(offset);
        }
        return new WarmSinkResult(warmupElementBuilder.build(), offset);
    }

    // bad path cleanup of resources and release the storage engine tx
    WarmUpElement abort(boolean nativeThrowed, StorageWriterContext storageWriterContext, StorageWriterSplitConfiguration storageWriterSplitConfiguration)
    {
        storageWriterContext.getLuceneIndexer().ifPresent(LuceneIndexer::abort);
        cleanup(true, nativeThrowed, storageWriterContext);
        if (storageWriterContext.weSuccess()) {
            updateToFailedState(storageWriterContext.getWarmupElementBuilder(), storageWriterContext.getWarmupElementWriteMetadata());
            storageWriterContext.setFailed();
        }

        WarmUpElement abortedWarmupElement = storageWriterContext.getWarmupElementBuilder().build();
        if (nativeThrowed) {
            logger.error("warm failed path %s native throwed on element %s", storageWriterSplitConfiguration.rowGroupFilePath(), abortedWarmupElement);
            metricsTimerTask.print(false);
        }
        return abortedWarmupElement;
    }

    boolean appendPage(Page page, StorageWriterContext storageWriterContext)
    {
        WarmupElementWriteMetadata warmupElementWriteMetadata = storageWriterContext.getWarmupElementWriteMetadata();
        Block block = page.getBlock(warmupElementWriteMetadata.connectorBlockIndex());
        int totalRecords = block.getPositionCount();

        int currentRecordNumber = 0;
        while (storageWriterContext.weSuccess() && currentRecordNumber < totalRecords) {
            recycleBuffers(storageWriterContext);

            int maxRecordsToAdd = Math.min(storageWriterContext.getRemainingBufferSize(), totalRecords - currentRecordNumber);
            BlockPosHolder blockPosHolder = new BlockPosHolder(block, warmupElementWriteMetadata.type(), currentRecordNumber, maxRecordsToAdd);

            appendToBuffer(storageWriterContext, blockPosHolder);
            storageWriterContext.incRecordBufferPos(blockPosHolder.getPos());
            currentRecordNumber += blockPosHolder.getPos();
        }
        return storageWriterContext.weSuccess();
    }

    private int[] cleanup(boolean aborted, boolean nativeThrowed, StorageWriterContext storageWriterContext)
    {
        int[] outFileParams = null;
        if (storageWriterContext.getIsCleanupDone().compareAndSet(null, aborted)) {
            try {
                if (!aborted) {
                    try {
                        if (storageWriterContext.getRecordBufferPos() > 0) {
                            flushRecordBuffer(storageWriterContext);
                        }
                    }
                    catch (TrinoException te) {
                        nativeThrowed |= ExceptionThrower.isNativeException(te);
                        aborted = true;
                    }
                }
                if (!nativeThrowed) {
                    if (aborted) {
                        storageWriterContext.setFailed();
                        updateToFailedState(storageWriterContext.getWarmupElementBuilder(), storageWriterContext.getWarmupElementWriteMetadata());
                    }
                    outFileParams = weClose(storageWriterContext);
                }
                else if (aborted) {
                    storageWriterContext.setFailed();
                    updateToFailedState(storageWriterContext.getWarmupElementBuilder(), storageWriterContext.getWarmupElementWriteMetadata());
                }
            }
            catch (Exception e) {
                logger.error(e, "abort tx wes %s", storageWriterContext.getWarmupElementWriteMetadata());
                throw e;
            }
            finally {
                storageWriterContext.getIsCleanupDone().getAndSet(true);
            }
        }
        return outFileParams;
    }

    void updateToFailedState(WarmUpElement.Builder warmupElementBuilder, WarmupElementWriteMetadata warmupElementWriteMetadata)
    {
        warmupElementBuilder.state(new WarmUpElementState(WarmUpElementState.State.FAILED_TEMPORARILY,
                        warmupElementWriteMetadata.warmUpElement().getState().temporaryFailureCount(),
                        System.currentTimeMillis()))
                .startOffset(-1)
                .queryOffset(-1)
                .queryReadSize(-1)
                .endOffset(-1)
                .warmState(WarmState.COLD);
    }

    /**
     * Flushes the record buffer to storage.
     */
    private void flushRecordBuffer(StorageWriterContext storageWriterContext)
    {
        if (storageWriterContext.getLuceneIndexer().isPresent()) {
            storageWriterContext.getLuceneIndexer().get().closeLuceneIndex(storageWriterContext.getWeCookie());
        }

        if (storageWriterContext.weSuccess()) {
            storageWriterContext.getWriteJuffersWarmUpElement().commitWE(storageWriterContext.getRecordBufferPos());
        }
        storageWriterContext.resetRecords();
    }

    private int[] weClose(StorageWriterContext storageWriterContext)
    {
        int[] outFileParams = new int[WeProperties.values().length];
        if (!storageWriterContext.isWeClosed()) {
            storageWriterContext.getBlockAppender().writeChunkMapValuesIntoChunkMapJuffer(storageWriterContext.getWriteJuffersWarmUpElement().getChunkMapList());
            outFileParams[WeProperties.WE_PROPERTIES_END_OFFSET.ordinal()] = storageEngine.warmupElementClose(storageWriterContext.getWeCookie(), outFileParams);
            storageWriterContext.setWeClosed();
        }
        return outFileParams;
    }

    private WarmupElementStats getFinalStats(WarmupElementStats warmupElementStats, WarmupElementWriteMetadata warmupElementWriteMetadata)
    {
        WarmUpElement warmUpElement = warmupElementWriteMetadata.warmUpElement();
        if (warmUpElement.getRecTypeCode().isSupportedFiltering() &&
                warmUpElement.getWarmUpType() != WarmUpType.WARM_UP_TYPE_LUCENE &&
                warmupElementStats.isValidRange()) {
            if (warmUpElement.getRecTypeCode() == RecTypeCode.REC_TYPE_VARCHAR ||
                    warmUpElement.getRecTypeCode() == RecTypeCode.REC_TYPE_CHAR) {
                Slice maxSlice = (Slice) warmupElementStats.getMaxValue();
                String maxValue;
                String minValue;
                Slice minSlice = (Slice) warmupElementStats.getMinValue();
                //if type is Slice we want to save the first 8 bytes for min/max values, for max value we add 1 to last position
                //need to convert them to byte array in order to preserve the original values
                byte[] maxSliceValue;
                if (maxSlice.length() > STAT_MAX_SLICE_LENGTH) {
                    maxSliceValue = maxSlice.getBytes(0, STAT_MAX_SLICE_LENGTH);
                    if (maxSliceValue[STAT_MAX_SLICE_LENGTH - 1] == Byte.MAX_VALUE) {
                        //protect from overflow
                        maxValue = null;
                    }
                    else {
                        //need to increase value by 1 in order to make sure ranges will overlaps (see @RangeMatcher.java)
                        maxSliceValue[STAT_MAX_SLICE_LENGTH - 1]++;
                        maxValue = Slices.wrappedBuffer(maxSliceValue).toStringUtf8();
                    }
                }
                else {
                    maxValue = maxSlice.toStringUtf8();
                }

                if (minSlice.length() > STAT_MAX_SLICE_LENGTH) {
                    byte[] minSliceValue = minSlice.getBytes(0, STAT_MAX_SLICE_LENGTH);
                    minValue = Slices.wrappedBuffer(minSliceValue).toStringUtf8();
                }
                else {
                    minValue = minSlice.toStringUtf8();
                }
                warmupElementStats = new WarmupElementStats(warmupElementStats.getNullsCount(), minValue, maxValue);
            }
        }
        return warmupElementStats;
    }

    /**
     * Initializes or recycles the buffers if full. If already initialized and not full, won't do anything.
     */
    private void recycleBuffers(StorageWriterContext storageWriterContext)
    {
        if (storageWriterContext.isRecordBufferFull()) {
            flushRecordBuffer(storageWriterContext);
        }
        else if (storageWriterContext.getRecordBufferSize() > 0) {
            return;
        }

        storageEngine.commitRecordBufferPrepare(storageWriterContext.getWeCookie());
        storageWriterContext.resetRecordBufferPos();
        storageWriterContext.setRecordBufferSize(1 << storageEngineConstants.getChunkSizeShift());

        storageWriterContext.getWriteJuffersWarmUpElement().resetAllBuffers();
        if (storageWriterContext.getLuceneIndexer().isPresent()) {
            resetLucene(storageWriterContext.getLuceneIndexer().get());
        }
    }

    /**
     * Should be called sequentially with each col and its block
     */
    private void appendToBuffer(StorageWriterContext storageWriterContext, BlockPosHolder blockPos)
    {
        WarmupElementWriteMetadata warmupElementWriteMetadata = storageWriterContext.getWarmupElementWriteMetadata();
        WarmUpElement warmUpElement = warmupElementWriteMetadata.warmUpElement();
        try {
            AppendResult appendResult = storageWriterContext.getBlockAppender().append(
                    storageWriterContext.getRecordBufferPos(),
                    blockPos,
                    false,
                    storageWriterContext.getWriteDictionary(),
                    warmUpElement,
                    storageWriterContext.getWarmupElementStats());
            storageWriterContext.getWriteJuffersWarmUpElement().increaseNullsCount(appendResult.nullsCount());
        }
        catch (WarmupException e) {
            storageWriterContext.setFailed();
            logger.debug("failed appending block to WE %s exception %s", warmupElementWriteMetadata, e);

            WarmUpElement.Builder warmupElementBuilder = WarmUpElement.builder(warmUpElement);
            if (e instanceof DictionaryMaxException dictionaryMaxException) {
                DictionaryInfo dictionaryInfo = new DictionaryInfo(dictionaryMaxException.getDictionaryKey(),
                        DictionaryState.DICTIONARY_MAX_EXCEPTION,
                        0, // dataValuesRecTypeLength
                        DictionaryInfo.NO_OFFSET);
                warmupElementBuilder.dictionaryInfo(dictionaryInfo);
                dictionaryCacheService.updateOnFailedWrite(dictionaryMaxException.getDictionaryKey());
            }
            warmupElementBuilder.state(new WarmUpElementState(e.getState(), 0, System.currentTimeMillis()));
            storageWriterContext.setWarmupElementBuilder(warmupElementBuilder);
        }
        catch (Exception e) {
            storageWriterContext.setFailed();
            logger.debug("failed appending block to WE %s exception %s", warmupElementWriteMetadata, e);
            updateToFailedState(storageWriterContext.getWarmupElementBuilder(), warmupElementWriteMetadata);
            throw e;
        }
    }

    void resetLucene(LuceneIndexer luceneIndexer)
    {
        luceneIndexer.resetLuceneIndex();
        luceneIndexer.resetLuceneBufferPosition();
    }
}
