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
import org.apache.parquet.crypto.keytools.KmsClient;
import org.apache.parquet.crypto.keytools.TrinoKeyToolkit;

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
    private final String cryptoFactoryClass;
    private final String encryptionKmsClientClass;
    private final String encryptionKmsInstanceId;
    private final String encryptionKmsInstanceUrl;
    private final String encryptionKeyAccessToken;
    private final long encryptionCacheLifetimeSeconds;

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
        this.cryptoFactoryClass = null;
        this.encryptionKmsClientClass = null;
        this.encryptionKmsInstanceId = null;
        this.encryptionKmsInstanceUrl = null;
        this.encryptionKeyAccessToken = KmsClient.KEY_ACCESS_TOKEN_DEFAULT;
        this.encryptionCacheLifetimeSeconds = TrinoKeyToolkit.CACHE_LIFETIME_DEFAULT_SECONDS;
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
            String cryptoFactoryClass,
            String encryptionKmsClientClass,
            String encryptionKmsInstanceId,
            String encryptionKmsInstanceUrl,
            String encryptionKeyAccessToken,
            long encryptionCacheLifetimeSeconds)
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
        this.cryptoFactoryClass = cryptoFactoryClass;
        this.encryptionKmsClientClass = encryptionKmsClientClass;
        this.encryptionKmsInstanceId = encryptionKmsInstanceId;
        this.encryptionKmsInstanceUrl = encryptionKmsInstanceUrl;
        this.encryptionKeyAccessToken = requireNonNull(encryptionKeyAccessToken, "encryptionKeyAccessToken is null");
        this.encryptionCacheLifetimeSeconds = encryptionCacheLifetimeSeconds;
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
        return cryptoFactoryClass;
    }

    public long getEncryptionCacheLifetimeSeconds()
    {
        return this.encryptionCacheLifetimeSeconds;
    }

    public String getEncryptionKeyAccessToken()
    {
        return this.encryptionKeyAccessToken;
    }

    public String getEncryptionKmsInstanceId()
    {
        return this.encryptionKmsInstanceId;
    }

    public String getEncryptionKmsInstanceUrl()
    {
        return this.encryptionKmsInstanceUrl;
    }

    public String getEncryptionKmsClientClass()
    {
        return this.encryptionKmsClientClass;
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
                cryptoFactoryClass,
                encryptionKmsClientClass,
                encryptionKmsInstanceId,
                encryptionKmsInstanceUrl,
                encryptionKeyAccessToken,
                encryptionCacheLifetimeSeconds);
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
                cryptoFactoryClass,
                encryptionKmsClientClass,
                encryptionKmsInstanceId,
                encryptionKmsInstanceUrl,
                encryptionKeyAccessToken,
                encryptionCacheLifetimeSeconds);
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
                cryptoFactoryClass,
                encryptionKmsClientClass,
                encryptionKmsInstanceId,
                encryptionKmsInstanceUrl,
                encryptionKeyAccessToken,
                encryptionCacheLifetimeSeconds);
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
                cryptoFactoryClass,
                encryptionKmsClientClass,
                encryptionKmsInstanceId,
                encryptionKmsInstanceUrl,
                encryptionKeyAccessToken,
                encryptionCacheLifetimeSeconds);
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
                cryptoFactoryClass,
                encryptionKmsClientClass,
                encryptionKmsInstanceId,
                encryptionKmsInstanceUrl,
                encryptionKeyAccessToken,
                encryptionCacheLifetimeSeconds);
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
                cryptoFactoryClass,
                encryptionKmsClientClass,
                encryptionKmsInstanceId,
                encryptionKmsInstanceUrl,
                encryptionKeyAccessToken,
                encryptionCacheLifetimeSeconds);
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
                cryptoFactoryClass,
                encryptionKmsClientClass,
                encryptionKmsInstanceId,
                encryptionKmsInstanceUrl,
                encryptionKeyAccessToken,
                encryptionCacheLifetimeSeconds);
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
                cryptoFactoryClass,
                encryptionKmsClientClass,
                encryptionKmsInstanceId,
                encryptionKmsInstanceUrl,
                encryptionKeyAccessToken,
                encryptionCacheLifetimeSeconds);
    }

    public ParquetReaderOptions withCryptoFactoryClass(String cryptoFactoryClass)
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
                cryptoFactoryClass,
                encryptionKmsClientClass,
                encryptionKmsInstanceId,
                encryptionKmsInstanceUrl,
                encryptionKeyAccessToken,
                encryptionCacheLifetimeSeconds);
    }

    public ParquetReaderOptions withEncryptionKmsClientClass(String encryptionKmsClientClass)
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
                cryptoFactoryClass,
                encryptionKmsClientClass,
                encryptionKmsInstanceId,
                encryptionKmsInstanceUrl,
                encryptionKeyAccessToken,
                encryptionCacheLifetimeSeconds);
    }

    public ParquetReaderOptions withEncryptionKmsInstanceId(String encryptionKmsInstanceId)
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
                cryptoFactoryClass,
                encryptionKmsClientClass,
                encryptionKmsInstanceId,
                encryptionKmsInstanceUrl,
                encryptionKeyAccessToken,
                encryptionCacheLifetimeSeconds);
    }

    public ParquetReaderOptions withEncryptionKmsInstanceUrl(String encryptionKmsInstanceUrl)
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
                cryptoFactoryClass,
                encryptionKmsClientClass,
                encryptionKmsInstanceId,
                encryptionKmsInstanceUrl,
                encryptionKeyAccessToken,
                encryptionCacheLifetimeSeconds);
    }

    public ParquetReaderOptions withEncryptionKeyAccessToken(String encryptionKeyAccessToken)
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
                cryptoFactoryClass,
                encryptionKmsClientClass,
                encryptionKmsInstanceId,
                encryptionKmsInstanceUrl,
                encryptionKeyAccessToken,
                encryptionCacheLifetimeSeconds);
    }

    public ParquetReaderOptions withEncryptionCacheLifetimeSeconds(long encryptionCacheLifetimeSeconds)
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
                cryptoFactoryClass,
                encryptionKmsClientClass,
                encryptionKmsInstanceId,
                encryptionKmsInstanceUrl,
                encryptionKeyAccessToken,
                encryptionCacheLifetimeSeconds);
    }
}
