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

/**
 * Central point of access for StorageEngineConstants.
 */
public interface StorageEngineConstants
{
    int getMaxRecLen();

    int getPageOffsetMask();

    int getPageSize();

    int getPageSizeMask();

    int getPageSizeShift();

    int getRecordBufferMaxSize();

    int getIndexChunkMaxSize();

    int getQueryStringNullValueSize();

    int getChunksMapSize();

    int getChunkHeaderMaxSize();

    int getWarmupDataTempBufferSize();

    int getWarmupIndexTempBufferSize();

    int getSectorOffsetMask();

    int getSectorSize();

    int getSectorSizeMask();

    int getSectorSizeShift();

    int getFixedLengthStringLimit();

    int getVarcharMaxLen();

    int getVarlenExtLimit();

    int getVarlenExtMark();

    int getVarlenExtRecordHeaderSize();

    int getVarlenMarkEnd();

    int getVarlenMdGranularity();

    int getNumBundles();

    int getBundleNonCollectSize();

    int getChunkSizeShift();

    int getMatchCollectBufferSize();

    int getMaxChunksInRange();

    int getMatchCollectNumIds();

    int getMaxMatchColumns();

    int getMatchTxSize();

    int getLuceneSmallJufferSize();

    int getLuceneBigJufferSize();
}
