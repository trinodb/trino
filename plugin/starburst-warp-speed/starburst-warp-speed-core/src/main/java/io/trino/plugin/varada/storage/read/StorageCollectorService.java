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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.varada.storage.read.fill.BlockFiller;
import io.trino.plugin.varada.storage.read.fill.BlockFillersFactory;
import io.trino.plugin.warp.gen.constants.QueryResultType;
import io.trino.plugin.warp.gen.constants.RecordBufferState;
import io.trino.plugin.warp.gen.constants.RecordIndexListHeader;
import io.trino.plugin.warp.gen.stats.VaradaStatsDictionary;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;

import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.trino.plugin.varada.dictionary.DictionaryCacheService.DICTIONARY_STAT_GROUP;
import static java.util.Objects.requireNonNull;

public class StorageCollectorService
{
    private static final Logger logger = Logger.get(StorageCollectorService.class);
    // services
    private final StorageEngine storageEngine;
    private final BufferAllocator bufferAllocator;
    private final VaradaStatsDictionary varadaStatsDictionary;
    private final RangeFillerService rangeFillerService;
    private final ChunksQueueService chunksQueueService;
    private final StorageEngineConstants storageEngineConstants;
    private final BlockFillersFactory blockFillersFactory;

    @Inject
    StorageCollectorService(
            StorageEngine storageEngine,
            BufferAllocator bufferAllocator,
            MetricsManager metricsManager,
            ChunksQueueService chunksQueueService,
            RangeFillerService rangeFillerService,
            StorageEngineConstants storageEngineConstants,
            BlockFillersFactory blockFillersFactory)
    {
        this.storageEngine = requireNonNull(storageEngine);
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.varadaStatsDictionary = requireNonNull(metricsManager).registerMetric(VaradaStatsDictionary.create(DICTIONARY_STAT_GROUP));
        this.rangeFillerService = requireNonNull(rangeFillerService);
        this.chunksQueueService = requireNonNull(chunksQueueService);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.blockFillersFactory = requireNonNull(blockFillersFactory);
    }

    // returns indication if anything is collected in the buffer and if the buffer is full
    CollectFromStorageResult collectFromStorage(CollectOpenResult collectOpenResult,
            boolean isMatchGetNumRanges,
            boolean chunkPrepared,
            int numCollectedRows,
            StorageCollectorArgs storageCollectorArgs)
    {
        if (chunksQueueService.isCompletelyFinished(storageCollectorArgs.chunksQueue(), storageCollectorArgs.numChunks())) {
            return new CollectFromStorageResult(CollectBufferState.COLLECT_BUFFER_STATE_EMPTY, chunkPrepared, numCollectedRows);
        }

        int numToCollect = 1; // Not a real value, just making sure to enter the loop in the first iteration
        while (!chunksQueueService.isChunkRangeCompleted(storageCollectorArgs.chunksQueue()) && numToCollect > 0) {
            // get next chunk to collect and check if its already done on buffer
            int chunkIndex = storageCollectorArgs.chunksQueue().getCurrent();
            boolean chunkWasPrepared = chunkPrepared;
            boolean bufferIsFull = chunksQueueService.prepareNextChunk(storageCollectorArgs.chunksQueue(),
                    chunkPrepared,
                    collectOpenResult.collectTxId(),
                    collectOpenResult.rowsLimit(),
                    numCollectedRows,
                    collectOpenResult.outResultType());
            chunkPrepared = true;
            if (bufferIsFull) { // if returns true we need to stop for query result optimization
                numToCollect = 0;
                break;
            }

            QueryParams queryParams = storageCollectorArgs.queryParams();
            if (queryParams.getNumCollectElements() > 0) {
                int numCollectedFromCurrentChunk = rangeFillerService.getNumCollectedFromCurrentChunk(chunkIndex, collectOpenResult.rangeData());
                numToCollect = getNumToCollect(storageCollectorArgs, numCollectedFromCurrentChunk, collectOpenResult, numCollectedRows);
                if (numToCollect > 0) {
                    try {
                        storageEngine.collect(collectOpenResult.collectTxId(), 0, queryParams.getNumCollectElements(), chunkIndex, numToCollect, collectOpenResult.outResultType());
                    }
                    catch (Exception e) {
                        logger.error(e, "collect failed chunkIndex %d chunkWasPrepared %b rowsLimit %d numCollectedRows %d numToCollect %d numCollectedFromCurrentChunk %d restoredChunkIndex %d",
                                 chunkIndex, chunkWasPrepared, collectOpenResult.rowsLimit(), numCollectedRows, numToCollect, numCollectedFromCurrentChunk, collectOpenResult.restoredChunkIndex());
                        throw e;
                    }
                }
                numCollectedRows += rangeFillerService.add(chunkIndex, numToCollect, storageCollectorArgs, isMatchGetNumRanges, collectOpenResult.rangeData());
            }
            else {
                numCollectedRows += rangeFillerService.add(chunkIndex, 0, storageCollectorArgs, isMatchGetNumRanges, collectOpenResult.rangeData());
            }
            logger.debug("collectFromStorage after native collect chunkIndex %d numToCollect %d numCollectedRows %d", chunkIndex, numToCollect, numCollectedRows);

            boolean currentChunkCompleted = rangeFillerService.isCurrentChunkCompleted(collectOpenResult.rangeData(), storageCollectorArgs.chunkSize());
            if (currentChunkCompleted || numCollectedRows == 0) {
                storageCollectorArgs.chunksQueue().currentCompleted();
                logger.debug("collectFromStorage advance numCollectedRows %d", numCollectedRows);
                chunkPrepared = false;
            }
            else {
                numToCollect = 0; // We do not collect from one chunk twice in one round
            }
            // In case we are in full scan we are stopping after one chunk
            if (queryParams.getNumMatchElements() == 0) {
                numToCollect = 0;
            }
        }

        logger.debug("collectFromStorage end numCollectedRows %d numToCollect %d", numCollectedRows, numToCollect);
        CollectBufferState collectBufferState;
        if (numToCollect == 0) {
            collectBufferState = CollectBufferState.COLLECT_BUFFER_STATE_FULL;
        }
        else {
            collectBufferState = (numCollectedRows > 0) ? CollectBufferState.COLLECT_BUFFER_STATE_PARTIAL : CollectBufferState.COLLECT_BUFFER_STATE_EMPTY;
        }
        return new CollectFromStorageResult(collectBufferState, chunkPrepared, numCollectedRows);
    }

    int fillBlocks(Block[] blocks, StorageCollectorArgs storageCollectorArgs, CollectOpenResult collectOpenResult, int numCollectedRows)
    {
        int rowsToFill = Math.min(numCollectedRows, collectOpenResult.rowsLimit());
        int collectIx = 0;
        for (WarmupElementCollectParams collectParams : storageCollectorArgs.queryParams().getCollectElementsParamsList()) {
            QueryResultType queryResultType = QueryResultType.values()[collectOpenResult.outResultType()[collectIx]];

            List<BlockFiller<?>> blockFillers = storageCollectorArgs.blockFillers();
            try {
                Block block;
                List<ReadJuffersWarmUpElement> readJuffersWarmUpElements = storageCollectorArgs.collectJuffersWE();
                if (collectParams.hasDictionary()) {
                    block = blockFillers.get(collectIx).fillBlockWithDictionary(readJuffersWarmUpElements.get(collectIx),
                            queryResultType,
                            rowsToFill,
                            collectParams.getBlockRecTypeCode(),
                            collectParams.getBlockRecTypeLength(),
                            collectParams.isCollectNulls(),
                            collectParams.getDictionary());
                    if (logger.isDebugEnabled() && block instanceof DictionaryBlock) {
                        varadaStatsDictionary.adddictionary_block_saved_bytes(block.getLogicalSizeInBytes() - block.getSizeInBytes());
                    }
                }
                else if (collectParams.mappedMatchCollect()) {
                    // The presence of collectParams.getValuesDictBlock() doesn't guarantee a map-match-collect,
                    // as the map is pre-created (and cached) based on the predicate data without knowing if it will meet the match-collect conditions.                    Block valuesDict = collectParams.getValuesDictBlock().get();
                    Block valuesDict = collectParams.getValuesDictBlock().get();
                    block = blockFillers.get(collectIx).fillBlockWithMapping(readJuffersWarmUpElements.get(collectIx),
                            queryResultType,
                            rowsToFill,
                            collectParams.isCollectNulls(),
                            valuesDict);
                }
                else {
                    block = blockFillers.get(collectIx).fillBlock(readJuffersWarmUpElements.get(collectIx),
                            queryResultType,
                            rowsToFill,
                            collectParams.getBlockRecTypeCode(),
                            collectParams.getBlockRecTypeLength(),
                            collectParams.isCollectNulls());
                }
                blocks[collectParams.getBlockIndex()] = block;
            }
            catch (Exception e) {
                logger.error(e, "fill block failed queryResultType=%s, collectIx=%d, rowsToFill=%d, blockFiller=%s, collectParams=%s",
                        queryResultType, collectIx, rowsToFill, blockFillers.get(collectIx), collectParams);
                throw new RuntimeException(e);
            }

            collectIx++;
        }
        return rowsToFill;
    }

    int getNumToCollect(StorageCollectorArgs storageCollectorArgs,
            int numCollectedFromCurrentChunk,
            CollectOpenResult collectOpenResult,
            int numCollectedRows)
    {
        List<WarmupElementRecordBufferState> warmupElementRecordBufferStates = collectOpenResult.warmupElementRecordBufferStates();
        if (warmupElementRecordBufferStates.isEmpty()) {
            logger.debug("getNumToCollect no wes %d", storageCollectorArgs.chunkSize() - numCollectedRows);
            return storageCollectorArgs.chunkSize() - numCollectedRows;
        }

        int recLimit = Integer.MAX_VALUE;
        for (WarmupElementRecordBufferState warmupElementRecordBufferState : warmupElementRecordBufferStates) {
            int limit = getNumToCollect(storageCollectorArgs,
                    numCollectedFromCurrentChunk,
                    warmupElementRecordBufferState,
                    collectOpenResult.rangeData(),
                    numCollectedRows);
            if (limit < recLimit) {
                recLimit = limit;
            }
        }
        logger.debug("getNumToCollect numCollectedFromCurrentChunk %d recLimit %d", numCollectedFromCurrentChunk, recLimit);
        return recLimit;
    }

    // @TODO we should consider in the future to move the small calculation to native (back to native) and return the result instead of passing the parameters to java and calculating here
    // currently we are getting 3 parameters here and calculating one result. in the future this might be reduced to 2 parameters only (chunk max reclen and used bytes) and
    // the point of calculation might require extra jni call which is not performance oriented. this is why we currently leave it like it is now.
    int getNumToCollect(StorageCollectorArgs storageCollectorArgs,
            int numCollectedFromCurrentChunk,
            WarmupElementRecordBufferState warmupElementRecordBufferState,
            RangeData rangeData,
            int numCollectedRows)
    {
        IntBuffer recordBufferStateBuff = bufferAllocator.ids2RecordBufferStateBuff(warmupElementRecordBufferState.getRecordBufferStateBuffId());
        ShortBuffer rowsBuff = bufferAllocator.ids2RowsBuff(rangeData.getRowsBuffId());
        int maxToCollect = storageCollectorArgs.chunkSize() - numCollectedRows; // according to buffer capacity
        int numToCollect = getTotalNumToCollect(storageCollectorArgs, rowsBuff) - numCollectedFromCurrentChunk; // according to current chunk
        if (numToCollect > maxToCollect) {
            logger.debug("getNumToCollect zero basePos %d numToCollect %d maxToCollect %d", warmupElementRecordBufferState.getBasePos(), numToCollect, maxToCollect);
            return 0; // we want to avoid decompressing twice the same chunk
        }

        int maxRecordLength = recordBufferStateBuff.get(warmupElementRecordBufferState.getBasePos() + RecordBufferState.RECORD_BUFFER_STATE_MAX_RECORD_LENGTH.ordinal());
        if (maxRecordLength <= storageCollectorArgs.fixedLengthStringLimit()) {
            logger.debug("getNumToCollect fixed size basePos %d numToCollect %d maxToCollect %d", warmupElementRecordBufferState.getBasePos(), numToCollect, maxToCollect);
            return numToCollect;
        }
        int freeBytes = recordBufferStateBuff.get(warmupElementRecordBufferState.getBasePos() + RecordBufferState.RECORD_BUFFER_STATE_TOTAL_BYTES.ordinal()) - recordBufferStateBuff.get(warmupElementRecordBufferState.getBasePos() + RecordBufferState.RECORD_BUFFER_STATE_USED_BYTES.ordinal());
        int actualNumToCollect = Math.min(freeBytes / maxRecordLength, numToCollect);
        logger.debug("getNumToCollect var size basePos %d actualNumToCollect %d numToCollect %d maxToCollect %d", warmupElementRecordBufferState.getBasePos(), actualNumToCollect, numToCollect, maxToCollect);
        return actualNumToCollect;
    }

    // since the size is a short, zero means a full chunk, we translate to integer here
    private int getTotalNumToCollect(StorageCollectorArgs storageCollectorArgs, ShortBuffer rowsBuff)
    {
        int total = Short.toUnsignedInt(rowsBuff.get(RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TOTAL_SIZE.ordinal()));
        return (total > 0) ? total : storageCollectorArgs.chunkSize();
    }

    StorageCollectorArgs getStorageCollectorArgs(QueryParams queryParams)
    {
        ArrayList<BlockFiller<?>> blockFillers = new ArrayList<>(queryParams.getNumCollectElements());
        for (WarmupElementCollectParams collectParams : queryParams.getCollectElementsParamsList()) {
            blockFillers.add(blockFillersFactory.getBlockFiller(collectParams.getBlockRecTypeCode().ordinal()));
        }
        int numChunksInRange = storageEngineConstants.getMaxChunksInRange();
        int fixedLengthStringLimit = storageEngineConstants.getFixedLengthStringLimit();
        int[] weCollectParams = queryParams.dumpCollectParams();
        long[][] collectBuffIds = new long[queryParams.getNumCollectElements()][];
        for (int collectIx = 0; collectIx < queryParams.getNumCollectElements(); collectIx++) {
            collectBuffIds[collectIx] = bufferAllocator.getQueryIdsArray(false);
        }
        List<ReadJuffersWarmUpElement> collectJuffersWE = queryParams.getCollectElementsParamsList()
                .stream()
                .map(we -> new ReadJuffersWarmUpElement(bufferAllocator, true, false))
                .collect(Collectors.toList());
        int chunkSize = 1 << storageEngineConstants.getChunkSizeShift();
        byte[] collectStoreBuff = new byte[storageEngine.queryGetCollectStateSize(queryParams.getNumMatchCollect())];
        byte[] storeRowListBuff = new byte[(chunkSize + RecordIndexListHeader.RECORD_INDEX_LIST_HEADER_TYPE.ordinal()) * Short.BYTES];
        byte[] collect2MatchParams = new byte[storageEngine.queryGetCollect2MatchSize()];
        // number of chunks is number of records divided by the chunk size which is fixed. we round it up in case the last chunk is not full.
        int numChunks = (int) Math.ceil((double) queryParams.getTotalNumRecords() / (double) chunkSize);
        if (numChunks == 0) {
            throw new RuntimeException("no chunks");
        }
        //  file
        long fileCookie = storageEngine.fileOpen(queryParams.getFilePath());
        ChunksQueue chunksQueue = new ChunksQueue(numChunksInRange, storageEngineConstants.getPageSize());
        return new StorageCollectorArgs(
                blockFillers,
                numChunksInRange,
                fixedLengthStringLimit,
                weCollectParams,
                collectBuffIds,
                collectJuffersWE,
                collectStoreBuff,
                storeRowListBuff,
                collect2MatchParams,
                queryParams,
                chunkSize,
                numChunks,
                fileCookie,
                chunksQueue);
    }
}
