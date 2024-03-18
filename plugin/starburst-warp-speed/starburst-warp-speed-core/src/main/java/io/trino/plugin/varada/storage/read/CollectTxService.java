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
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.storage.engine.ExceptionThrower;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.RecordBufferState;
import io.trino.plugin.warp.gen.constants.RecordIndexListType;
import io.trino.spi.TrinoException;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.varada.VaradaErrorCode.VARADA_TX_ALLOCATION_FAILED;

public class CollectTxService
{
    private static final Logger logger = Logger.get(CollectTxService.class);
    private static final int INVALID_TX_ID = -1;

    private final StorageEngine storageEngine;
    private final ChunksQueueService chunksQueueService;
    private final RangeFillerService rangeFillerService;
    private ArrayBlockingQueue<MemorySegment> matchBitmapsQueue;

    @Inject
    public CollectTxService(StorageEngine storageEngine,
            ChunksQueueService chunksQueueService,
            RangeFillerService rangeFillerService,
            StorageEngineConstants storageEngineConstants,
            NativeConfiguration nativeConfiguration)
    {
        this.storageEngine = storageEngine;
        this.chunksQueueService = chunksQueueService;
        this.rangeFillerService = rangeFillerService;

        final int numSegments = nativeConfiguration.getTaskMaxWorkerThreads();
        checkArgument(numSegments > 0, "no segments configured for match bitmaps");
        final long alignment = 32; // this is the alignment required for intel optimized bitmap operations
        final long maxChunks = storageEngineConstants.getMaxChunksInRange();
        final long segmentSize = storageEngineConstants.getPageSize() * maxChunks;
        final long allocSize = segmentSize * numSegments + alignment;

        SegmentAllocator nativeAllocator = SegmentAllocator.slicingAllocator(Arena.global().allocate(allocSize, alignment));
        ArrayList<MemorySegment> segmentList = new ArrayList<>(numSegments);
        for (int i = 0; i < numSegments; i++) {
            segmentList.add(nativeAllocator.allocate(segmentSize, alignment));
        }

        matchBitmapsQueue = new ArrayBlockingQueue<>(segmentList.size(), true, segmentList);
    }

    public void freeCollectOpenResources(CollectOpenResult collectOpenResult)
    {
        if (collectOpenResult == null) {
            return;
        }
        if (collectOpenResult.matchResultBitmaps() != null) {
            matchBitmapsQueue.add(collectOpenResult.matchResultBitmaps());
        }
    }

    /**
     * prepare buffers for filling
     */
    CollectOpenResult collectOpen(int rowsLimit,
            StorageCollectorArgs storageCollectorArgs,
            int storeRowListSize,
            RecordIndexListType storeRowListType,
            StorageCollectorCallBack storageCollectorCallBack)
    {
        long[] metadataBuffIds = new long[2];
        metadataBuffIds[0] = -1;
        metadataBuffIds[1] = -1;
        QueryParams queryParams = storageCollectorArgs.queryParams();
        int[] outResultType = new int[storageCollectorArgs.queryParams().getNumCollectElements()];
        long matchBmAddr = 0;
        MemorySegment bmSeg = null;
        boolean isFullScan = (queryParams.getNumMatchElements() == 0);
        if (!isFullScan) {
            bmSeg = matchBitmapsQueue.remove();
            matchBmAddr = bmSeg.address();
        }
        int collectTxId = storageEngine.collectOpen(queryParams.getTotalNumRecords(),
                storageCollectorArgs.fileCookie(),
                storageCollectorArgs.collectStoreBuff(),
                storageCollectorArgs.collect2MatchParams(),
                queryParams.getNumCollectElements(),
                storageCollectorArgs.weCollectParams(),
                queryParams.getCatalogSequence(),
                matchBmAddr,
                queryParams.getMinCollectOffset(),
                storageCollectorArgs.collectBuffIds(),
                metadataBuffIds,
                outResultType);
        if (collectTxId < 0) {
            throw new TrinoException(VARADA_TX_ALLOCATION_FAILED, "failed to allocate tx for collect");
        }

        int collectIx = 0;
        for (WarmupElementCollectParams collectParams : queryParams.getCollectElementsParamsList()) {
            storageCollectorArgs.collectJuffersWE().get(collectIx).createBuffers(
                    collectParams.mappedMatchCollect() ? RecTypeCode.REC_TYPE_TINYINT : collectParams.getRecTypeCode(),
                    collectParams.mappedMatchCollect() ? 1 : collectParams.getRecTypeLength(),
                    collectParams.hasDictionary(),
                    storageCollectorArgs.collectBuffIds()[collectIx]);
            collectIx++;
        }

        RangeData rangeData = new RangeData(metadataBuffIds[0]);
        List<WarmupElementRecordBufferState> warmupElementRecordBufferStates = Collections.emptyList();

        int numCollectElements = queryParams.getNumCollectElements();
        if (numCollectElements > 0) {
            warmupElementRecordBufferStates = IntStream.range(0, numCollectElements)
                    .mapToObj(weIx -> new WarmupElementRecordBufferState(weIx * RecordBufferState.RECORD_BUFFER_STATE_NUM_OF.ordinal(), metadataBuffIds))
                    .toList();
        }

        int restoredChunkIndex = -1;
        if (chunksQueueService.storeRestoreRequired(storageCollectorArgs.chunksQueue())) {
            rangeFillerService.restoreRowList(rangeData, storeRowListSize, storeRowListType, storageCollectorArgs.storeRowListBuff());
            restoredChunkIndex = storageCollectorArgs.chunksQueue().getCurrent();
            storageEngine.collectRestoreState(collectTxId, restoredChunkIndex, storageCollectorCallBack);
        }

        logger.debug("collectOpen collectTxId %d rowsLimit %d numChunks %d numCollectElements %d restoredChunkIndex %d",
                collectTxId, rowsLimit, storageCollectorArgs.numChunks(), queryParams.getNumCollectElements(), restoredChunkIndex);
        return new CollectOpenResult(collectTxId, outResultType, rowsLimit, rangeData, warmupElementRecordBufferStates, bmSeg, restoredChunkIndex);
    }

    CollectCloseResult collectClose(CollectOpenResult collectOpenResult,
            StorageCollectorArgs storageCollectorArgs,
            boolean chunkPrepared,
            int numCollectedRows,
            int storeRowListSize,
            RecordIndexListType storeRowListType,
            StorageCollectorCallBack storageCollectorCallBack)
    {
        // idiom potent case
        if (collectOpenResult == null || collectOpenResult.collectTxId() == INVALID_TX_ID) {
            return new CollectCloseResult(new StoreRowListResult(storeRowListType, storeRowListSize), 0, false);
        }

        StoreRowListResult storeRowListResult = new StoreRowListResult(storeRowListType, storeRowListSize);
        boolean bufferIsFull = false;
        Optional<int[]> chunksWithBitmapsToStoreOpt = Optional.empty();
        if (chunksQueueService.storeRestoreRequired(storageCollectorArgs.chunksQueue())) {
            bufferIsFull = chunksQueueService.prepareNextChunk(storageCollectorArgs.chunksQueue(),
                    chunkPrepared,
                    collectOpenResult.collectTxId(),
                    collectOpenResult.rowsLimit(),
                    numCollectedRows,
                    collectOpenResult.outResultType()); // will be done only if needed
            chunksWithBitmapsToStoreOpt = storageCollectorArgs.chunksQueue().getChunkIndexesWithBitmap();
            storeRowListResult = rangeFillerService.storeRowList(storageCollectorArgs, collectOpenResult.rangeData());
        }
        int[] chunksWithBitmaps = chunksWithBitmapsToStoreOpt.orElse(null);
        int numChunksWithBitmap = (chunksWithBitmaps != null) ? chunksWithBitmaps.length : 0;
        long readPages = storageEngine.collectClose(collectOpenResult.collectTxId(), chunksWithBitmaps, numChunksWithBitmap, storageCollectorCallBack);
        freeCollectOpenResources(collectOpenResult);
        logger.debug("collectClose collectTxId %d readPages %d", collectOpenResult.collectTxId(), readPages);
        return new CollectCloseResult(storeRowListResult, readPages, bufferIsFull);
    }

    void collectAbort(Exception e, CollectOpenResult collectOpenResult, int collectTxId)
    {
        boolean nativeThrowed = false;
        if (e instanceof TrinoException) {
            nativeThrowed = ExceptionThrower.isNativeException((TrinoException) e);
        }
        storageEngine.queryAbort(collectTxId, nativeThrowed);
        freeCollectOpenResources(collectOpenResult);
    }
}
