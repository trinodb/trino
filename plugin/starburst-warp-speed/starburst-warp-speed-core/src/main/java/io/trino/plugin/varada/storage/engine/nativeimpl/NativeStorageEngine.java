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
package io.trino.plugin.varada.storage.engine.nativeimpl;

import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.di.VaradaNativeStorageEngineModule;
import io.trino.plugin.varada.dispatcher.query.classifier.PredicateUtil;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.ExceptionThrower;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.lucene.LuceneMatcher;
import io.trino.plugin.varada.storage.read.StorageCollectorCallBack;
import io.trino.plugin.warp.gen.stats.VaradaStatsMgr;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

@Singleton
public class NativeStorageEngine
        implements StorageEngine
{
    private static final Logger logger = Logger.get(NativeStorageEngine.class);
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final ExceptionThrower exceptionThrower; // we keep a reference to hold this object for native layer ref

    public NativeStorageEngine(
            NativeConfiguration nativeConfiguration,
            MetricsManager metricsManager,
            ExceptionThrower exceptionThrower)
    {
        this.exceptionThrower = requireNonNull(exceptionThrower);

        final int taskMaxWorkerThreads = nativeConfiguration.getTaskMaxWorkerThreads();
        final int panicHaltPolicy = nativeConfiguration.getDebugPanicHaltPolicy();
        logger.info("load storage engine taskMaxWorkerThreads %d panicHaltPolicy %d bundleSize %d",
                taskMaxWorkerThreads,
                panicHaltPolicy,
                nativeConfiguration.getBundleSize());
        try {
            nativeInit(taskMaxWorkerThreads,
                    Runtime.getRuntime().maxMemory(),
                    nativeConfiguration.getGeneralReservedMemory(),
                    nativeConfiguration.getBundleSize(),
                    nativeConfiguration.getMaxRecJufferSize(),
                    nativeConfiguration.getCompressionLevel(),
                    panicHaltPolicy,
                    nativeConfiguration.getCollectTxSize(),
                    nativeConfiguration.getStorageCacheSizeInPages(),
                    PredicateUtil.PREDICATE_HEADER_SIZE,
                    nativeConfiguration.getSkipIndexPercent(),
                    VaradaNativeStorageEngineModule.getNativeLibrariesDirectory().toString(),
                    nativeConfiguration.getEnableSingleChunk(),
                    nativeConfiguration.getEnableQueryResultType(),
                    nativeConfiguration.getEnablePackedChunk(),
                    nativeConfiguration.getEnableCompression(),
                    nativeConfiguration.getExceptionalListCompression());
        }
        catch (Throwable t) {
            logger.error(t, "failed loading storage engine");
            throw t;
        }
        new VaradaStatsMgr(metricsManager);
        logger.debug("finish initializing storage engine");
    }

    private native void nativeInit(int maxWorkerThreads,
            long jvmMemory,
            long generalReservedMemory,
            int bundleSizeInBytes,
            int maxRecJufferSize,
            int lz4HcPercent,
            int panicHaltPolicy,
            int collectTxSize,
            int storageCacheSizeInPages,
            int predicateHeaderSize,
            int skipIndexPercentage,
            String libraryPath,
            boolean enableSingleChunk,
            boolean enableQueryResultType,
            boolean enablePackedChunk,
            boolean enableCompression,
            int exceptionalListCompression);

    @Override
    public native void initRecordBufferSizes(int[] fixedRecordBufferSizes, int[] varlenRecordBufferSizes);

    @Override
    public native void initCollectTxSizes(int[] fixedCollectTxSizes, int[] varlenCollectTxSizes);

    @Override
    public native long fileOpen(String fileName);

    @Override
    public native void fileClose(long fileCookie);

    @Override
    public native void fileTruncate(long fileCookie, int offset);

    @Override
    public native void filePunchHole(String fileName, int startOffset, int endOffset);

    @Override
    public native void fileIsAboutToBeDeleted(String fileName, int fileSizeInPages);

    @Override
    public native int warmupOpen(int connecterSequence);

    @Override
    public native long warmupElementOpen(int txId, long fileCookie, int offetInPages,
            int recTypeCode, int recTypeLength, int warmUpType, long writeBuffAddress, long[] buffAddresses);

    @Override
    public native int warmupElementClose(long weCookie, int[] outQueryFileParams);

    @Override
    public native void warmupClose(int txId);

    @Override
    public native void commitRecordBufferPrepare(long weCookie);

    @Override
    public native void commitRecordBuffer(long weCookie, int addedNumRows, int addedNV, int addedBytes, long valueMin, long valueMax,
            int singleValOffset, boolean close, byte[] outChunkCookies);

    @Override
    public native void commitExtRecordBuffer(long weCookie, int extRecordFirstOffset, int addedExtBytes);

    @Override
    public native void luceneWriteBuffer(long weCookie, int fileId, int offset, int len);

    @Override
    public native int luceneCommitBuffers(long weCookie, boolean singleVal, int[] fileLengths);

    @Override
    public native int queryGetCollect2MatchSize();

    @Override
    public native int queryGetCollectStateSize(int numMatchCollect);

    @Override
    public native int collectOpen(int totalNumRecords, long fileCookie, byte[] parsingBuff, byte[] collect2MatchParams, int numCollectWes,
            int[] weCollectParams, int connectorId, long matchBitmapAddress, int minOffset,
            long[][] outCollectColBuffIds, long[] outMetadataBuffIds, int[] outResultType);

    @Override
    public native int matchOpen(int totalNumRecords, long fileCookie, int collectTxId, byte[] parsingBuff, byte[] collect2MatchParams,
            int numMatchWes, int[] weMatchTree, int numLucenes, LuceneMatcher[] luceneMatchers, long matchBitmapAddress, int minOffset, long[][] outMatchColBuffIds);

    @Override
    public native void collectRestoreState(int txId, int chunkIndex, StorageCollectorCallBack collectStateObj);

    @Override
    public native long match(int txId, int nextState, short[] outMatchedChunksIndexes, int[] outMatchBitmapResetPoints);

    @Override
    public native int processMatchResult(int txId, int chunkIndex, int bitmapResetPoint, int rowsLimit, int[] resultTypes);

    @Override
    public native void collect(int txId, int startWeIx, int endWeIx, int chunkIndex, int numToCollect, int[] outResultTypes);

    @Override
    public native long collectClose(int txId, int[] chunksWithBitmapsToStore, int numChunksWithBitmaps, StorageCollectorCallBack obj);

    @Override
    public native void matchClose(int txId);

    @Override
    public native void queryAbort(int txId, boolean nativeThrowed);

    @Override
    public native ByteBuffer getBundleFromPool(int bufIx);

    @Override
    public native void setDebugThrowPolicy(int numElements, int[] panicID, int[] repetitionMode, int[] ratio);

    @Override
    public native String executeDebugCommand(String commandName, int numParams, String[] paramNames, String[] paramValues);

    @Override
    public native int luceneReadBuffer(long nativeCookie, int fileId, int offset, int length);
}
