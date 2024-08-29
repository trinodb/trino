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
package io.trino.parquet;

import io.airlift.units.DataSize;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class ParquetReaderOptions
{
    private static final DataSize DEFAULT_MAX_READ_BLOCK_SIZE = DataSize.of(16, MEGABYTE);
    private static final int DEFAULT_MAX_READ_BLOCK_ROW_COUNT = 8 * 1024;
    private static final DataSize DEFAULT_MAX_MERGE_DISTANCE = DataSize.of(1, MEGABYTE);
    private static final DataSize DEFAULT_MAX_BUFFER_SIZE = DataSize.of(8, MEGABYTE);
    private static final DataSize DEFAULT_SMALL_FILE_THRESHOLD = DataSize.of(3, MEGABYTE);

    private final boolean ignoreStatistics;
    private final DataSize maxReadBlockSize;
    private final int maxReadBlockRowCount;
    private final DataSize maxMergeDistance;
    private final DataSize maxBufferSize;
    private final boolean useColumnIndex;
    private final boolean useBloomFilter;
    private final DataSize smallFileThreshold;
    private final boolean vectorizedDecodingEnabled;
    private final ParquetReaderEncryptionOptions parquetReaderEncryptionOptions;

    public ParquetReaderOptions()
    {
        ignoreStatistics = false;
        maxReadBlockSize = DEFAULT_MAX_READ_BLOCK_SIZE;
        maxReadBlockRowCount = DEFAULT_MAX_READ_BLOCK_ROW_COUNT;
        maxMergeDistance = DEFAULT_MAX_MERGE_DISTANCE;
        maxBufferSize = DEFAULT_MAX_BUFFER_SIZE;
        useColumnIndex = true;
        useBloomFilter = true;
        smallFileThreshold = DEFAULT_SMALL_FILE_THRESHOLD;
        vectorizedDecodingEnabled = true;
        parquetReaderEncryptionOptions = new ParquetReaderEncryptionOptions();
    }

    private ParquetReaderOptions(
            boolean ignoreStatistics,
            DataSize maxReadBlockSize,
            int maxReadBlockRowCount,
            DataSize maxMergeDistance,
            DataSize maxBufferSize,
            boolean useColumnIndex,
            boolean useBloomFilter,
            DataSize smallFileThreshold,
            boolean vectorizedDecodingEnabled,
            ParquetReaderEncryptionOptions parquetReaderEncryptionOptions)
    {
        this.ignoreStatistics = ignoreStatistics;
        this.maxReadBlockSize = requireNonNull(maxReadBlockSize, "maxReadBlockSize is null");
        checkArgument(maxReadBlockRowCount > 0, "maxReadBlockRowCount must be greater than 0");
        this.maxReadBlockRowCount = maxReadBlockRowCount;
        this.maxMergeDistance = requireNonNull(maxMergeDistance, "maxMergeDistance is null");
        this.maxBufferSize = requireNonNull(maxBufferSize, "maxBufferSize is null");
        this.useColumnIndex = useColumnIndex;
        this.useBloomFilter = useBloomFilter;
        this.smallFileThreshold = requireNonNull(smallFileThreshold, "smallFileThreshold is null");
        this.vectorizedDecodingEnabled = vectorizedDecodingEnabled;
        this.parquetReaderEncryptionOptions = parquetReaderEncryptionOptions;
    }

    public boolean isIgnoreStatistics()
    {
        return ignoreStatistics;
    }

    public DataSize getMaxReadBlockSize()
    {
        return maxReadBlockSize;
    }

    public DataSize getMaxMergeDistance()
    {
        return maxMergeDistance;
    }

    public boolean isUseColumnIndex()
    {
        return useColumnIndex;
    }

    public boolean useBloomFilter()
    {
        return useBloomFilter;
    }

    public boolean isVectorizedDecodingEnabled()
    {
        return vectorizedDecodingEnabled;
    }

    public DataSize getMaxBufferSize()
    {
        return maxBufferSize;
    }

    public int getMaxReadBlockRowCount()
    {
        return maxReadBlockRowCount;
    }

    public DataSize getSmallFileThreshold()
    {
        return smallFileThreshold;
    }

    public String getCryptoFactoryClass()
    {
        return parquetReaderEncryptionOptions.cryptoFactoryClass;
    }

    public long getEncryptionCacheLifetimeSeconds()
    {
        return this.parquetReaderEncryptionOptions.encryptionCacheLifetimeSeconds;
    }

    public String getEncryptionKeyAccessToken()
    {
        return this.parquetReaderEncryptionOptions.encryptionKeyAccessToken;
    }

    public String getEncryptionKmsInstanceId()
    {
        return this.parquetReaderEncryptionOptions.encryptionKmsInstanceId;
    }

    public String getEncryptionKmsInstanceUrl()
    {
        return this.parquetReaderEncryptionOptions.encryptionKmsInstanceUrl;
    }

    public String getEncryptionKmsClientClass()
    {
        return this.parquetReaderEncryptionOptions.encryptionKmsClientClass;
    }

    public boolean isUniformEncryption()
    {
        return parquetReaderEncryptionOptions.uniformEncryption;
    }

    public boolean isEncryptionParameterChecked()
    {
        return parquetReaderEncryptionOptions.encryptionParameterChecked;
    }

    public String getFailsafeEncryptionKeyId()
    {
        return parquetReaderEncryptionOptions.failsafeEncryptionKeyId;
    }

    public String getEncryptionColumnKeys()
    {
        return parquetReaderEncryptionOptions.columnKeys;
    }

    public String getEncryptionFooterKeyId()
    {
        return parquetReaderEncryptionOptions.footerKeyId;
    }

    public String[] getEncryptionVersionedKeyList()
    {
        return parquetReaderEncryptionOptions.versionedKeyList;
    }

    public String[] getEncryptionKeyList()
    {
        return parquetReaderEncryptionOptions.keyList;
    }

    public String getEncryptionKeyFile()
    {
        return parquetReaderEncryptionOptions.keyFile;
    }

    public boolean isEncryptionEnvironmentKeys()
    {
        return parquetReaderEncryptionOptions.isEncryptionEnvironmentKeys;
    }

    public void setEncryptionParameterChecked(boolean encryptionParameterChecked)
    {
        parquetReaderEncryptionOptions.encryptionParameterChecked = encryptionParameterChecked;
    }

    public ParquetReaderEncryptionOptions encryptionOptions()
    {
        return this.parquetReaderEncryptionOptions;
    }

    public ParquetReaderOptions withIgnoreStatistics(boolean ignoreStatistics)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBloomFilter,
                smallFileThreshold,
                vectorizedDecodingEnabled,
                parquetReaderEncryptionOptions);
    }

    public ParquetReaderOptions withMaxReadBlockSize(DataSize maxReadBlockSize)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBloomFilter,
                smallFileThreshold,
                vectorizedDecodingEnabled,
                parquetReaderEncryptionOptions);
    }

    public ParquetReaderOptions withMaxReadBlockRowCount(int maxReadBlockRowCount)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBloomFilter,
                smallFileThreshold,
                vectorizedDecodingEnabled,
                parquetReaderEncryptionOptions);
    }

    public ParquetReaderOptions withMaxMergeDistance(DataSize maxMergeDistance)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBloomFilter,
                smallFileThreshold,
                vectorizedDecodingEnabled,
                parquetReaderEncryptionOptions);
    }

    public ParquetReaderOptions withMaxBufferSize(DataSize maxBufferSize)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBloomFilter,
                smallFileThreshold,
                vectorizedDecodingEnabled,
                parquetReaderEncryptionOptions);
    }

    public ParquetReaderOptions withUseColumnIndex(boolean useColumnIndex)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBloomFilter,
                smallFileThreshold,
                vectorizedDecodingEnabled,
                parquetReaderEncryptionOptions);
    }

    public ParquetReaderOptions withBloomFilter(boolean useBloomFilter)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBloomFilter,
                smallFileThreshold,
                vectorizedDecodingEnabled,
                parquetReaderEncryptionOptions);
    }

    public ParquetReaderOptions withSmallFileThreshold(DataSize smallFileThreshold)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBloomFilter,
                smallFileThreshold,
                vectorizedDecodingEnabled,
                parquetReaderEncryptionOptions);
    }

    public ParquetReaderOptions withVectorizedDecodingEnabled(boolean vectorizedDecodingEnabled)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBloomFilter,
                smallFileThreshold,
                vectorizedDecodingEnabled,
                parquetReaderEncryptionOptions);
    }

    public ParquetReaderOptions withEncryptionOption(ParquetReaderEncryptionOptions encryptionOptions)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxReadBlockRowCount,
                maxMergeDistance,
                maxBufferSize,
                useColumnIndex,
                useBloomFilter,
                smallFileThreshold,
                vectorizedDecodingEnabled,
                encryptionOptions);
    }
}
