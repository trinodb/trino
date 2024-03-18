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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;

@Singleton
public class NativeStorageEngineConstants
        implements StorageEngineConstants
{
    private static final Logger logger = Logger.get(NativeStorageEngineConstants.class);

    // page size
    private final int pageSizeShift;                 // native layer page size shift
    private final int pageSize;                      // native layer page size
    private final int pageSizeMask;                  // mask for page alignment
    private final int pageOffsetMask;                // mask for in page offset
    // sector
    private final int sectorSizeShift;
    private final int sectorSize;
    private final int sectorSizeMask;
    private final int sectorOffsetMask;
    // buffer
    private final int recordBufferMaxSize;
    private final int indexChunkMaxSize;
    private final int queryStringNullValueSize;
    private final int chunksMapSize;
    private final int chunkHeaderMaxSize;
    private final int warmupDataTempBufferSize;
    private final int warmupIndexTempBufferSize;
    // varchar
    private final int maxRecLen;
    private final int fixedLengthStringLimit;
    private final int varcharMaxLen;
    private final int varlenExtMark;
    private final int varlenMarkEnd;
    private final int varlenSkiplistGranularity;
    private final int varlenExtLimit;
    private final int varlenExtRecordHeaderSize;
    // tx and memory resources
    private final int numBundles;
    private final int bundleNonCollectSize;
    // chunk
    private final int chunkSizeShift;
    private final int matchCollectBufferSize;
    private final int maxChunksInRange;
    private final int matchCollectNumIds;
    private final int maxMatchColumns;
    private final int matchTxSize;
    // lucene
    private final int luceneSmallJufferSize;
    private final int luceneBigJufferSize;

    @Inject
    public NativeStorageEngineConstants(StorageEngine storageEngine)
    {
        logger.debug("start loading storage engine constants");
        // page size
        pageSizeShift = getPageSizeShiftImpl();
        pageSize = 1 << pageSizeShift;
        pageOffsetMask = pageSize - 1;
        pageSizeMask = ~pageOffsetMask;

        // sector
        sectorSizeShift = 9;
        sectorSize = 1 << sectorSizeShift;
        sectorOffsetMask = sectorSize - 1;
        sectorSizeMask = ~sectorOffsetMask;

        // buffers
        recordBufferMaxSize = getRecordBufferMaxSizeImpl();
        indexChunkMaxSize = getIndexChunkMaxSizeImpl();
        queryStringNullValueSize = getQueryStringNullValueSizeImpl();
        chunksMapSize = getChunksMapSizeImpl();
        chunkHeaderMaxSize = getChunkHeaderMaxSizeImpl();
        warmupDataTempBufferSize = getWarmupDataTempBufferSizeImpl();
        warmupIndexTempBufferSize = getWarmupIndexTempBufferSizeImpl();

        // varchar
        maxRecLen = getMaxRecLenImpl();
        fixedLengthStringLimit = getFixedLengthStringLimitImpl();
        varcharMaxLen = getVarlenMaxLenImpl();
        varlenExtMark = getVarlenMarkExtImpl();
        varlenMarkEnd = getVarlenMarkEndImpl();
        varlenSkiplistGranularity = getVarlenMdGranularityImpl();
        varlenExtLimit = getVarlenExtLimitImpl();
        varlenExtRecordHeaderSize = getVarlenExtRecordHeaderSizeImpl();

        // tx and memory
        numBundles = getNumBundlesImpl();
        bundleNonCollectSize = getBundleNonCollectSizeImpl();

        // chunk
        chunkSizeShift = getChunkSizeShiftImpl();
        matchCollectBufferSize = getMatchCollectBufferSizeImpl();
        maxChunksInRange = getMaxChunksInRangeImpl();
        matchCollectNumIds = getMatchCollectNumIdsImpl();
        maxMatchColumns = getMaxMatchColumnsImpl();
        matchTxSize = getMatchTxSizeImpl();

        // lucene
        luceneBigJufferSize = getLuceneBigJufferSizeImpl();
        luceneSmallJufferSize = getLuceneSmallJufferSizeImpl();

        logger.debug("finished loading storage engine constants, extLimit=%d", varlenExtLimit);
    }

    private native int getPageSizeShiftImpl();

    private native int getRecordBufferMaxSizeImpl();

    private native int getIndexChunkMaxSizeImpl();

    private native int getMaxRecLenImpl();

    private native int getVarlenMarkEndImpl();

    private native int getVarlenMdGranularityImpl();

    private native int getVarlenMaxLenImpl();

    private native int getVarlenMarkExtImpl();

    private native int getFixedLengthStringLimitImpl();

    private native int getVarlenExtLimitImpl();

    private native int getVarlenExtRecordHeaderSizeImpl();

    private native int getNumBundlesImpl();

    private native int getChunkSizeShiftImpl();

    private native int getMatchCollectBufferSizeImpl();

    private native int getMaxChunksInRangeImpl();

    private native int getMatchCollectNumIdsImpl();

    private native int getMaxMatchColumnsImpl();

    private native int getMatchTxSizeImpl();

    private native int getBundleNonCollectSizeImpl();

    private native int getLuceneSmallJufferSizeImpl();

    private native int getLuceneBigJufferSizeImpl();

    private native int getQueryStringNullValueSizeImpl();

    private native int getChunksMapSizeImpl();

    private native int getChunkHeaderMaxSizeImpl();

    private native int getWarmupDataTempBufferSizeImpl();

    private native int getWarmupIndexTempBufferSizeImpl();

    @Override
    public int getMaxRecLen()
    {
        return maxRecLen;
    }

    @Override
    public int getPageOffsetMask()
    {
        return pageOffsetMask;
    }

    @Override
    public int getPageSize()
    {
        return pageSize;
    }

    @Override
    public int getPageSizeMask()
    {
        return pageSizeMask;
    }

    @Override
    public int getPageSizeShift()
    {
        return pageSizeShift;
    }

    @Override
    public int getRecordBufferMaxSize()
    {
        return recordBufferMaxSize;
    }

    @Override
    public int getIndexChunkMaxSize()
    {
        return indexChunkMaxSize;
    }

    @Override
    public int getQueryStringNullValueSize()
    {
        return queryStringNullValueSize;
    }

    @Override
    public int getChunksMapSize()
    {
        return chunksMapSize;
    }

    @Override
    public int getChunkHeaderMaxSize()
    {
        return chunkHeaderMaxSize;
    }

    @Override
    public int getWarmupDataTempBufferSize()
    {
        return warmupDataTempBufferSize;
    }

    @Override
    public int getWarmupIndexTempBufferSize()
    {
        return warmupIndexTempBufferSize;
    }

    @Override
    public int getSectorOffsetMask()
    {
        return sectorOffsetMask;
    }

    @Override
    public int getSectorSize()
    {
        return sectorSize;
    }

    @Override
    public int getSectorSizeMask()
    {
        return sectorSizeMask;
    }

    @Override
    public int getSectorSizeShift()
    {
        return sectorSizeShift;
    }

    @Override
    public int getFixedLengthStringLimit()
    {
        return fixedLengthStringLimit;
    }

    @Override
    public int getVarcharMaxLen()
    {
        return varcharMaxLen;
    }

    @Override
    public int getVarlenExtLimit()
    {
        return varlenExtLimit;
    }

    @Override
    public int getVarlenExtMark()
    {
        return varlenExtMark;
    }

    @Override
    public int getVarlenExtRecordHeaderSize()
    {
        return varlenExtRecordHeaderSize;
    }

    @Override
    public int getVarlenMarkEnd()
    {
        return varlenMarkEnd;
    }

    @Override
    public int getVarlenMdGranularity()
    {
        return varlenSkiplistGranularity;
    }

    @Override
    public int getNumBundles()
    {
        return numBundles;
    }

    @Override
    public int getBundleNonCollectSize()
    {
        return bundleNonCollectSize;
    }

    @Override
    public int getChunkSizeShift()
    {
        return chunkSizeShift;
    }

    @Override
    public int getMatchCollectBufferSize()
    {
        return matchCollectBufferSize;
    }

    @Override
    public int getMaxChunksInRange()
    {
        return maxChunksInRange;
    }

    @Override
    public int getMatchCollectNumIds()
    {
        return matchCollectNumIds;
    }

    @Override
    public int getMaxMatchColumns()
    {
        return maxMatchColumns;
    }

    @Override
    public int getMatchTxSize()
    {
        return matchTxSize;
    }

    @Override
    public int getLuceneSmallJufferSize()
    {
        return luceneSmallJufferSize;
    }

    @Override
    public int getLuceneBigJufferSize()
    {
        return luceneBigJufferSize;
    }
}
