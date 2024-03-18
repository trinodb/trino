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
package io.trino.plugin.varada.storage.engine;

import io.trino.plugin.varada.storage.lucene.LuceneMatcher;
import io.trino.plugin.varada.storage.read.StorageCollectorCallBack;

import java.nio.ByteBuffer;

/**
 * API with the storage engine implementation
 * <p>
 */
public interface StorageEngine
{
    //----------------------- initialization ----------------------------------------
    default void initRecordBufferSizes(int[] fixedRecordBufferSizes, int[] varlenRecordBufferSizes)
    {
        throw new UnsupportedOperationException();
    }

    default void initCollectTxSizes(int[] fixedCollectTxSizes, int[] varlenCollectTxSizes)
    {
        throw new UnsupportedOperationException();
    }

    default ByteBuffer getBundleFromPool(int bufIx)
    {
        throw new UnsupportedOperationException();
    }

    //----------------------- file ----------------------------------------
    default long fileOpen(String fileName)
    {
        throw new UnsupportedOperationException();
    }

    default void fileClose(long fileCookie)
    {
        throw new UnsupportedOperationException();
    }

    default void fileTruncate(long fileCookie, int offset)
    {
        throw new UnsupportedOperationException();
    }

    default void filePunchHole(String fileName, int startOffset, int endOffset)
    {
        throw new UnsupportedOperationException();
    }

    default void fileIsAboutToBeDeleted(String fileName, int fileSizeInPages)
    {
        throw new UnsupportedOperationException();
    }

    //----------------------- warmup ----------------------------------------

    default int warmupOpen(int connecterSequence)
    {
        throw new UnsupportedOperationException();
    }

    default long warmupElementOpen(int txId, long fileCookie, int offetInPages,
            int recTypeCode, int recTypeLength, int warmUpType, long writeBuffAddress, long[] buffAddresses)
    {
        throw new UnsupportedOperationException();
    }

    default int warmupElementClose(long weCookie, int[] outQueryFileParams)
    {
        throw new UnsupportedOperationException();
    }

    // returns offset in pages
    default void warmupClose(int txId)
    {
        throw new UnsupportedOperationException();
    }

    default void commitRecordBufferPrepare(long weCookie)
    {
        throw new UnsupportedOperationException();
    }

    default void commitRecordBuffer(long weCookie, int addedNumRows, int addedNV, int addedBytes, long valueMin, long valueMax,
            int singleValOffset, boolean close, byte[] outChunkCookies)
    {
        throw new UnsupportedOperationException();
    }

    default void commitExtRecordBuffer(long weCookie, int extRecordFirstOffset, int addedExtBytes)
    {
        throw new UnsupportedOperationException();
    }

    default void luceneWriteBuffer(long weCookie, int fileId, int offset, int len)
    {
    }

    default int luceneCommitBuffers(long weCookie, boolean singleVal, int[] fileLengths)
    {
        return 0;
    }

    //----------------------- query ----------------------------------------
    default int queryGetCollect2MatchSize()
    {
        throw new UnsupportedOperationException();
    }

    default int queryGetCollectStateSize(int numMatchCollect)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * open a collect transaction
     *
     * @param totalNumRecords - total number of records in the warm up element
     * @param fileCookie - hot file to read from
     * @param parsingBuff - buffer for native to parse the collect parameters
     * @param collect2MatchParams - parameters to pass from collect to match
     * @param numCollectWes - number of collect warm up elements
     * @param weCollectParams - parameters for collect warmup elements dumped into an array
     * @param connectorId - connector id
     * @param outCollectColBuffIds - buffer ids for data and nulls per warm up element
     * @param outMetadataBuffIds - buffer id for row numbers. buffer id for record buffer state. valid id is zero of positive, -1 for invalid.
     * @param outResultType - buffer for result type optimization
     *
     * @return transaction id
     */
    default int collectOpen(int totalNumRecords, long fileCookie, byte[] parsingBuff, byte[] collect2MatchParams, int numCollectWes,
            int[] weCollectParams, int connectorId, long matchBitmapAddress, int minOffset,
            long[][] outCollectColBuffIds, long[] outMetadataBuffIds, int[] outResultType)
    {
        throw new UnsupportedOperationException();
    }

    default int matchOpen(int totalNumRecords, long fileCookie, int collectTxId, byte[] parsingBuff, byte[] collect2MatchParams,
            int numMatchWes, int[] weMatchTree, int numLucenes, LuceneMatcher[] luceneMatchers, long matchBitmapAddress, int minOffset, long[][] outMatchColBuffIds)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * restore collect state from java state array
     *
     * @param txId - transaction id
     * @param chunkIndex - chunk index to collect from
     * @param collectStateObj - collect object to retreive store/restore state mehtod ids
     */
    default void collectRestoreState(int txId, int chunkIndex, StorageCollectorCallBack collectStateObj)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Perform the match part
     *
     * @param txId - identifies tx, passed from native to java during import_create
     * @param startChunkIndex - chunk to start match from
     * @param outMatchedChunksIndexes - indexes of chunks that have at least one match
     *
     * @return MSB 32 bits number of matched chuks as filled in the output array
     *    LSB 32 bits end chunk index of the range matched
     *    0 if we complmeted all chunks
     */
    default long match(int txId, int startChunkIndex, short[] outMatchedChunksIndexes, int[] outMatchBitmapResetPoints)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * process match result on a chunk before collect
     *
     * @param txId - identifies tx, passed from native to java during import_create
     * @param chunkIndex - chunk to collect from
     * @param bitmapResetPoint - reset point of the match bitmap
     * @param rowsLimit - optional limit on the number of rows to collect from this chunk
     * @param resultTypes - array of result types for each collected WE for deciding if we should stop collect
     *
     * @return > 0 if buffer is full and we need to close collect, 0 if not, -1 for error
     */
    default int processMatchResult(int txId, int chunkIndex, int bitmapResetPoint, int rowsLimit, int[] resultTypes)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Collect from the provided chunk according to the processMatchResult result
     *
     * @param txId - identifies tx, passed from native to java during import_create
     * @param startWeIx - we to start collect from - inclusive
     * @param endWeIx - we to end collect - exclusive
     * @param chunkIndex - chunk to collect from
     * @param numToCollect - how many rows to collect
     * @param outResultTypes - array to hold updated result type for each collected WE for java to process the collect buffers
     */
    default void collect(int txId, int startWeIx, int endWeIx, int chunkIndex, int numToCollect, int[] outResultTypes)
    {
        throw new UnsupportedOperationException();
    }

    default long collectClose(int txId, int[] chunksWithBitmapsToStore, int numChunksWithBitmaps, StorageCollectorCallBack obj)
    {
        throw new UnsupportedOperationException();
    }

    default void matchClose(int txId)
    {
        throw new UnsupportedOperationException();
    }

    default void queryAbort(int txId, boolean nativeThrowed)
    {
        throw new UnsupportedOperationException();
    }

    default int luceneReadBuffer(long nativeCookie, int fileId, int offset, int length)
    {
        return -1;
    }

    //----------------------- statistics and debug ----------------------------------------
    default void setDebugThrowPolicy(int numElements, int[] panicID, int[] repetitionMode, int[] ratio)
    {
        throw new UnsupportedOperationException();
    }

    default String executeDebugCommand(String commandName, int numParams, String[] paramNames, String[] paramValues)
    {
        throw new UnsupportedOperationException();
    }
}
