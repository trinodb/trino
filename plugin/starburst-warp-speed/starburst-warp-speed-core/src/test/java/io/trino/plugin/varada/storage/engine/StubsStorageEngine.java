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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class StubsStorageEngine
        implements StorageEngine
{
    private final List<RuntimeException> throwOnColletRuntimeExceptionList = new ArrayList<>();
    private final List<Integer> matchResults = new ArrayList<>();

    private final AtomicInteger luceneColumns = new AtomicInteger(0);
    ByteBuffer firstBundle;

    public StubsStorageEngine()
    {
    }

    @Override
    public void initRecordBufferSizes(int[] fixedRecordBufferSizes, int[] varlenRecordBufferSizes)
    {
        for (int i = 0; i < fixedRecordBufferSizes.length; i++) {
            fixedRecordBufferSizes[i] = 64 * 1024 * i;
        }
        for (int i = 0; i < varlenRecordBufferSizes.length; i++) {
            varlenRecordBufferSizes[i] = 64 * 1024 * i;
        }
    }

    @Override
    public void initCollectTxSizes(int[] fixedCollectTxSizes, int[] varlenCollectTxSizes)
    {
        for (int i = 0; i < fixedCollectTxSizes.length; i++) {
            fixedCollectTxSizes[i] = 1024 * i;
        }
        for (int i = 0; i < varlenCollectTxSizes.length; i++) {
            varlenCollectTxSizes[i] = 1024 * i;
        }
    }

    @Override
    public long fileOpen(String fileName)
    {
        return 0;
    }

    @Override
    public void fileClose(long fileCookie)
    {
    }

    @Override
    public void fileTruncate(long fileCookie, int offset)
    {
    }

    @Override
    public void filePunchHole(String fileName, int startOffset, int endOffset)
    {
    }

    @Override
    public void fileIsAboutToBeDeleted(String fileName, int fileSizeInPages)
    {
    }

    @Override
    public int warmupOpen(int connecterSequence)
    {
        return 0;
    }

    @Override
    public long warmupElementOpen(int txId, long fileCookie, int offetInPages,
            int recTypeCode, int recTypeLength, int warmUpType, long writeBuffAddress, long[] buffAddresses)
    {
        return 1;
    }

    @Override
    public int warmupElementClose(long weCookie, int[] outQueryFileParams)
    {
        outQueryFileParams[0] = 0;
        outQueryFileParams[1] = 1;
        return 1;
    }

    @Override
    public void warmupClose(int txId)
    {
    }

    @Override
    public void commitRecordBufferPrepare(long weCookie)
    {
    }

    @Override
    public void commitRecordBuffer(long weCookie, int addedNumRows, int addedNV, int addedBytes, long valueMin, long valueMax,
            int singleValOffset, boolean close, byte[] outChunkCookies)
    {
    }

    @Override
    public void commitExtRecordBuffer(long weCookie, int extRecordFirstOffset, int addedExtBytes)
    {
    }

    @Override
    public int queryGetCollect2MatchSize()
    {
        return 0;
    }

    @Override
    public int queryGetCollectStateSize(int numMatchCollect)
    {
        return 0;
    }

    @Override
    public int collectOpen(int totalNumRecords, long fileCookie, byte[] parsingBuff, byte[] collect2MatchParams, int numCollectWes,
            int[] weCollectParams, int connectorId, long matchBitmapAddress, int minOffset,
            long[][] outCollectColBuffIds, long[] outMetadataBuffIds, int[] outResultType)
    {
        Arrays.fill(outMetadataBuffIds, 0);
        Arrays.fill(firstBundle.array(), (byte) 0);
        firstBundle.position(0);
        return 0;
    }

    @Override
    public int matchOpen(int totalNumRecords, long fileCookie, int collectTxId, byte[] parsingBuff, byte[] collect2MatchParams,
            int numMatchWes, int[] weMatchTree, int numLucenes, LuceneMatcher[] luceneMatchers, long matchBitmapAddress, int minOffset, long[][] outMatchColBuffIds)
    {
        luceneColumns.addAndGet(luceneMatchers.length);
        return 0;
    }

    @Override
    public void collectRestoreState(int txId, int chunkIndex, StorageCollectorCallBack collectStateObj)
    {
    }

    @Override
    public long match(int txId, int nextState, short[] outMatchedChunksIndexes, int[] outMatchBitmapResetPoints)
    {
        if (!matchResults.isEmpty()) {
            return matchResults.remove(0);
        }
        return 0L;
    }

    @Override
    public int processMatchResult(int txId, int chunkIndex, int bitmapResetPoint, int rowsLimit, int[] resultTypes)
    {
        return 0;
    }

    @Override
    public void collect(int txId, int startWeIx, int endWeIx, int chunkIndex, int numToCollect, int[] outResultTypes)
    {
        if (!throwOnColletRuntimeExceptionList.isEmpty()) {
            throw throwOnColletRuntimeExceptionList.remove(0);
        }
    }

    @Override
    public long collectClose(int txId, int[] chunksWithBitmapsToStore, int numChunksWithBitmaps, StorageCollectorCallBack obj)
    {
        return 0;
    }

    @Override
    public void matchClose(int txId)
    {
    }

    @Override
    public void queryAbort(int txId, boolean nativeThrowed)
    {
    }

    @Override
    public ByteBuffer getBundleFromPool(int bufIx)
    {
        if (bufIx == 0) {
            firstBundle = ByteBuffer.allocate(1 << 20);
            return firstBundle;
        }
        return ByteBuffer.allocate(1 << 20);
    }

    @Override
    public void setDebugThrowPolicy(int numElements, int[] panicID, int[] repetitionMode, int[] ratio)
    {
    }

    @Override
    public String executeDebugCommand(String commandName, int numParams, String[] paramNames, String[] paramValues)
    {
        return "";
    }

    public synchronized void clear()
    {
        throwOnColletRuntimeExceptionList.clear();
        matchResults.clear();
    }

    public void setThrowOnCollect(RuntimeException... e)
    {
        throwOnColletRuntimeExceptionList.addAll(Arrays.asList(e));
    }

    public void setMatchResults(Integer... matchResult)
    {
        matchResults.addAll(Arrays.asList(matchResult));
    }

    public int getLuceneReadColumns()
    {
        return luceneColumns.get();
    }
}
