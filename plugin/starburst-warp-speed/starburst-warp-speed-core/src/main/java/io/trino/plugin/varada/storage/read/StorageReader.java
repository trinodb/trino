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

import io.airlift.log.Logger;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dictionary.DictionaryCacheService;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.storage.engine.ExceptionThrower;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.varada.storage.lucene.LuceneMatcher;
import io.trino.plugin.warp.gen.constants.RecordIndexListType;
import io.trino.plugin.warp.gen.stats.VaradaStatsDictionary;
import io.trino.plugin.warp.gen.stats.VaradaStatsDispatcherPageSource;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.varada.log.ShapingLogger;

import java.util.List;
import java.util.stream.Collectors;

import static io.trino.plugin.varada.VaradaErrorCode.VARADA_MATCH_FAILED;
import static io.trino.plugin.varada.VaradaErrorCode.VARADA_NATIVE_UNRECOVERABLE_ERROR;
import static io.trino.plugin.varada.VaradaErrorCode.VARADA_TX_ALLOCATION_FAILED;
import static io.trino.plugin.varada.VaradaErrorCode.VARADA_UNRECOVERABLE_COLLECT_FAILED;
import static io.trino.plugin.varada.VaradaErrorCode.VARADA_UNRECOVERABLE_MATCH_FAILED;
import static java.util.Objects.requireNonNull;

public class StorageReader
{
    private static final Logger logger = Logger.get(StorageReader.class);
    private static final int INVALID_TX_ID = -1;
    private static final long MATCH_RESULT_MASK = 0x00000000ffffffffL;
    private final ShapingLogger shapingLogger;

    // services
    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final VaradaStatsDictionary varadaStatsDictionary;
    private final VaradaStatsDispatcherPageSource statsDispatcherPageSource;
    private final DictionaryCacheService dictionaryCacheService;

    // parameters
    private final QueryParams queryParams;

    // match
    private final int[] weMatchTree;
    private final long[][] matchBuffIds;
    private final List<ReadJuffersWarmUpElement> matchJuffersWE;
    private final LuceneMatcher[] luceneMatchers;
    private boolean matchExhausted;
    private int matchTxId;
    short[] matchedChunksIndexes;
    int[] matchBitmapResetPoints;

    // collect
    private final StorageCollectorArgs storageCollectorArgs;
    private final CollectTxService collectTxService;
    private final ChunksQueueService chunksQueueService;
    private final StorageCollectorService storageCollectorService;
    private int collectTxId;
    private boolean chunkPrepared;
    private int numCollectedRows;
    private boolean dictionariesLoaded;
    private RecordIndexListType storeRowListType;
    private int storeRowListSize;

    StorageReader(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator,
            DictionaryCacheService dictionaryCacheService,
            QueryParams queryParams,
            VaradaStatsDictionary varadaStatsDictionary,
            VaradaStatsDispatcherPageSource statsDispatcherPageSource,
            StorageCollectorArgs storageCollectorArgs,
            CollectTxService collectTxService,
            ChunksQueueService chunksQueueService,
            StorageCollectorService storageCollectorService,
            GlobalConfiguration globalConfiguration)
    {
        this.storageEngine = requireNonNull(storageEngine);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.varadaStatsDictionary = varadaStatsDictionary;
        this.statsDispatcherPageSource = statsDispatcherPageSource;
        this.storageCollectorService = requireNonNull(storageCollectorService);
        this.dictionaryCacheService = requireNonNull(dictionaryCacheService);

        this.queryParams = queryParams;
        this.matchBuffIds = new long[queryParams.getNumMatchElements()][];
        this.luceneMatchers = new LuceneMatcher[queryParams.getNumLucene()];
        this.storageCollectorArgs = storageCollectorArgs;
        this.collectTxService = collectTxService;
        this.chunksQueueService = chunksQueueService;
        this.chunkPrepared = false;
        this.storeRowListSize = 0;
        this.storeRowListType = RecordIndexListType.RECORD_INDEX_LIST_TYPE_ALL;
        //  match
        this.matchTxId = INVALID_TX_ID;
        this.collectTxId = INVALID_TX_ID;
        this.matchExhausted = true; // we initialize as true, and at the first call it will be set by calling match

        this.weMatchTree = queryParams.dumpMatchParams();
        int matchIx = 0;
        for (WarmupElementMatchParams matchParams : queryParams.getMatchElementsParamsList()) {
            matchBuffIds[matchIx] = bufferAllocator.getQueryIdsArray(matchParams.hasLuceneParams());
            matchIx++;
        }
        this.matchJuffersWE = queryParams.getMatchElementsParamsList()
                .stream()
                .map(we -> new ReadJuffersWarmUpElement(bufferAllocator, false, we.hasLuceneParams()))
                .collect(Collectors.toList());

        createLuceneMatchers(globalConfiguration); // this call must be after creating the matchJuffersWE

        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfiguration.getShapingLoggerThreshold(),
                globalConfiguration.getShapingLoggerDuration(),
                globalConfiguration.getShapingLoggerNumberOfSamples());
    }

    void close()
    {
        storageEngine.fileClose(storageCollectorArgs.fileCookie());
    }

    private void createLuceneMatchers(GlobalConfiguration globalConfiguration)
    {
        if (luceneMatchers.length == 0) {
            return;
        }

        int matchIx = 0;
        for (WarmupElementMatchParams matchParams : queryParams.getMatchElementsParamsList()) {
            if (matchParams.hasLuceneParams()) {
                luceneMatchers[matchParams.getLuceneIx()] = new LuceneMatcher(storageEngine,
                        storageEngineConstants,
                        matchJuffersWE.get(matchIx),
                        matchParams.getLuceneQueryMatchData(),
                        statsDispatcherPageSource,
                        globalConfiguration);
            }
            matchIx++;
        }
    }

    private void loadDictionaries()
    {
        if (queryParams.getNumLoadDataValues() == 0) {
            return;
        }

        try {
            for (WarmupElementCollectParams collectParams : queryParams.getCollectElementsParamsList()) {
                // load dictionaries if needed according to existence of dictionary key prepared earlier
                if (collectParams.hasDictionaryParams()) {
                    collectParams.setDictionary(dictionaryCacheService.computeReadIfAbsent(
                            collectParams.getDictionaryKey(),
                            collectParams.getUsedDictionarySize(),
                            collectParams.getDataValuesRecTypeCode(),
                            collectParams.getRecTypeLength(),
                            collectParams.getDictionaryOffset(),
                            queryParams.getFilePath()));
                    varadaStatsDictionary.incdictionary_read_elements_count();
                }
            }
        }
        catch (Exception e) {
            shapingLogger.error(e, "loadDictionaries failed");
            throw e;
        }
    }

    /**
     * prepare buffers for filling
     */
    CollectOpenResult queryOpen(int rowsLimit, StorageCollectorCallBack storageCollectorCallBack)
    {
        if (!dictionariesLoaded) {
            loadDictionaries();
            dictionariesLoaded = true;
        }

        CollectOpenResult collectOpenResult = collectTxService.collectOpen(rowsLimit, storageCollectorArgs, storeRowListSize, storeRowListType, storageCollectorCallBack);
        collectTxId = collectOpenResult.collectTxId();

        if (queryParams.getNumMatchElements() > 0) {
            try {
                matchTxId = storageEngine.matchOpen(queryParams.getTotalNumRecords(),
                        storageCollectorArgs.fileCookie(),
                        collectOpenResult.collectTxId(),
                        storageCollectorArgs.collectStoreBuff(),
                        storageCollectorArgs.collect2MatchParams(),
                        queryParams.getNumMatchElements(),
                        weMatchTree,
                        luceneMatchers.length,
                        luceneMatchers,
                        collectOpenResult.matchResultBitmaps().address(),
                        queryParams.getMinMatchOffset(),
                        matchBuffIds);
            }
            catch (Exception e) {
                shapingLogger.warn(e, "matchOpen failed"); // we dont re-throw, we will throw in the next if since matchTxId was not set
            }
            if (matchTxId < 0) {
                collectTxService.freeCollectOpenResources(collectOpenResult);
                TrinoException te = new TrinoException(VARADA_TX_ALLOCATION_FAILED, "failed to allocate tx for match");
                throw te;
            }
            matchedChunksIndexes = new short[storageEngineConstants.getMaxChunksInRange()];
            matchBitmapResetPoints = new int[storageEngineConstants.getMaxChunksInRange()];
        }

        int matchIx = 0;
        for (WarmupElementMatchParams matchParams : queryParams.getMatchElementsParamsList()) {
            // only for lucene
            matchJuffersWE.get(matchIx).createBuffers(
                    matchParams.getRecTypeCode(),
                    matchParams.getRecTypeLength(),
                    false,
                    matchBuffIds[matchIx]);
            matchIx++;
        }
        numCollectedRows = 0;
        return collectOpenResult;
    }

    private void matchIfNeeded()
    {
        if (matchExhausted) {
            ChunksQueue chunksQueue = storageCollectorArgs.chunksQueue();
            if (matchTxId == INVALID_TX_ID) {
                matchExhausted = chunksQueueService.updateChunkRangeFullScan(chunksQueue, storageCollectorArgs.numChunks(), storageCollectorArgs.numChunksInRange());
                logger.debug("matchIfNeeded matchExhausted %b after full scan update", matchExhausted);
            }
            else {
                long matcheResult = 0;
                try {
                    matcheResult = storageEngine.match(matchTxId, chunksQueueService.getChunkIndexForMatch(chunksQueue), matchedChunksIndexes, matchBitmapResetPoints);
                }
                catch (Exception e) {
                    abortMatch(e); // will close only the match tx here. the caller will close the collect tx
                    // We can't throw the original exception cause it will skip closing the collect TX.
                    // But we do need to preserve the recverable notion from native.
                    if (e instanceof TrinoException trinoException &&
                            trinoException.getErrorCode().equals(VARADA_NATIVE_UNRECOVERABLE_ERROR.toErrorCode())) {
                        throw new TrinoException(VARADA_UNRECOVERABLE_MATCH_FAILED, "failed to match: " + e.getMessage());
                    }
                    else {
                        throw new TrinoException(VARADA_MATCH_FAILED, "failed to match: " + e.getMessage());
                    }
                }

                matchExhausted = matcheResult == 0;
                if (!matchExhausted) {
                    int matchEndChunkIndex = (int) (matcheResult & MATCH_RESULT_MASK);
                    int numMatchedChunks = (int) (matcheResult >> 32);
                    logger.debug("matchIfNeeded matchStartChunkIndex %d matchEndChunkIndex %d numMatchedChunks %d",
                            chunksQueueService.getChunkIndexForMatch(chunksQueue), matchEndChunkIndex, numMatchedChunks);
                    chunksQueueService.updateChunkRangeAfterMatch(chunksQueue, matchEndChunkIndex, numMatchedChunks, matchedChunksIndexes, matchBitmapResetPoints);
                }
            }
        }
    }

    /**
     * collect rows from native, return true if something was collected, false otherwise
     */
    boolean matchAndCollect(StorageCollectorArgs storageCollectorArgs, CollectOpenResult collectOpenResult, boolean isMatchGetNumRanges)
    {
        if (collectOpenResult.collectTxId() == INVALID_TX_ID) {
            throw new TrinoException(VARADA_UNRECOVERABLE_COLLECT_FAILED, "no collect tx available, probably a secondary error");
        }
        CollectBufferState collectBufferState = CollectBufferState.COLLECT_BUFFER_STATE_EMPTY;
        matchIfNeeded();
        // we continue as long as buffer is not full and we have more chunks to match and collect
        while (!matchExhausted && (collectBufferState != CollectBufferState.COLLECT_BUFFER_STATE_FULL)) {
            CollectFromStorageResult collectFromStorageResult = storageCollectorService.collectFromStorage(collectOpenResult, isMatchGetNumRanges, chunkPrepared, numCollectedRows, storageCollectorArgs);
            collectBufferState = collectFromStorageResult.collectBufferState();
            chunkPrepared = collectFromStorageResult.chunkPrepared();
            numCollectedRows = collectFromStorageResult.numCollectedRows();
            matchExhausted = chunksQueueService.isChunkRangeCompleted(storageCollectorArgs.chunksQueue());
            matchIfNeeded();
        }
        return collectBufferState != CollectBufferState.COLLECT_BUFFER_STATE_EMPTY;
    }

    long queryClose(CollectOpenResult collectOpenResult, StorageCollectorArgs storageCollectorArgs, StorageCollectorCallBack storageCollectorCallBack)
    {
        // match
        if (matchTxId != INVALID_TX_ID) {
            storageEngine.matchClose(matchTxId);
            matchTxId = INVALID_TX_ID;
        }

        if (collectTxId == INVALID_TX_ID) {
            return 0;
        }

        CollectCloseResult collectCloseResult = collectTxService.collectClose(collectOpenResult,
                storageCollectorArgs,
                chunkPrepared,
                numCollectedRows,
                storeRowListSize,
                storeRowListType,
                storageCollectorCallBack);
        storeRowListSize = collectCloseResult.storeRowListResult().storeRowListSize();
        storeRowListType = collectCloseResult.storeRowListResult().storeRowListType();
        collectTxId = INVALID_TX_ID;
        return collectCloseResult.readPages();
    }

    void abortMatch(Exception e)
    {
        if (matchTxId != INVALID_TX_ID) {
            boolean nativeThrowed = false;
            if (e instanceof TrinoException) {
                nativeThrowed = ExceptionThrower.isNativeException((TrinoException) e);
            }
            storageEngine.queryAbort(matchTxId, nativeThrowed);
            matchTxId = INVALID_TX_ID;
        }
    }

    void abortCollect(Exception e, CollectOpenResult collectOpenResult)
    {
        if (collectTxId != INVALID_TX_ID) {
            collectTxService.collectAbort(e, collectOpenResult, collectTxId);
        }
        collectTxId = INVALID_TX_ID;
    }

    int fillBlocks(Block[] blocks, CollectOpenResult collectOpenResult)
    {
        return storageCollectorService.fillBlocks(blocks, storageCollectorArgs, collectOpenResult, numCollectedRows);
    }
}
