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
package io.prestosql.orc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;

import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class OrcWriterOptions
{
    @VisibleForTesting
    static final DataSize DEFAULT_MAX_STRING_STATISTICS_LIMIT = DataSize.ofBytes(64);
    @VisibleForTesting
    static final DataSize DEFAULT_MAX_COMPRESSION_BUFFER_SIZE = DataSize.of(256, KILOBYTE);
    static final double DEFAULT_BLOOM_FILTER_FPP = 0.05;
    private static final DataSize DEFAULT_STRIPE_MIN_SIZE = DataSize.of(32, MEGABYTE);
    private static final DataSize DEFAULT_STRIPE_MAX_SIZE = DataSize.of(64, MEGABYTE);
    private static final int DEFAULT_STRIPE_MAX_ROW_COUNT = 10_000_000;
    private static final int DEFAULT_ROW_GROUP_MAX_ROW_COUNT = 10_000;
    private static final DataSize DEFAULT_DICTIONARY_MAX_MEMORY = DataSize.of(16, MEGABYTE);
    private final DataSize stripeMinSize;
    private final DataSize stripeMaxSize;
    private final int stripeMaxRowCount;
    private final int rowGroupMaxRowCount;
    private final DataSize dictionaryMaxMemory;
    private final DataSize maxStringStatisticsLimit;
    private final DataSize maxCompressionBufferSize;
    private final Set<String> bloomFilterColumns;
    private final double bloomFilterFpp;

    public OrcWriterOptions()
    {
        this(
                DEFAULT_STRIPE_MIN_SIZE,
                DEFAULT_STRIPE_MAX_SIZE,
                DEFAULT_STRIPE_MAX_ROW_COUNT,
                DEFAULT_ROW_GROUP_MAX_ROW_COUNT,
                DEFAULT_DICTIONARY_MAX_MEMORY,
                DEFAULT_MAX_STRING_STATISTICS_LIMIT,
                DEFAULT_MAX_COMPRESSION_BUFFER_SIZE,
                ImmutableSet.of(),
                DEFAULT_BLOOM_FILTER_FPP);
    }

    private OrcWriterOptions(
            DataSize stripeMinSize,
            DataSize stripeMaxSize,
            int stripeMaxRowCount,
            int rowGroupMaxRowCount,
            DataSize dictionaryMaxMemory,
            DataSize maxStringStatisticsLimit,
            DataSize maxCompressionBufferSize,
            Set<String> bloomFilterColumns,
            double bloomFilterFpp)
    {
        requireNonNull(stripeMinSize, "stripeMinSize is null");
        requireNonNull(stripeMaxSize, "stripeMaxSize is null");
        checkArgument(stripeMaxRowCount >= 1, "stripeMaxRowCount must be at least 1");
        checkArgument(rowGroupMaxRowCount >= 1, "rowGroupMaxRowCount must be at least 1");
        requireNonNull(dictionaryMaxMemory, "dictionaryMaxMemory is null");
        requireNonNull(maxStringStatisticsLimit, "maxStringStatisticsLimit is null");
        requireNonNull(maxCompressionBufferSize, "maxCompressionBufferSize is null");
        requireNonNull(bloomFilterColumns, "bloomFiltersColumns is null");
        checkArgument(bloomFilterFpp > 0.0 && bloomFilterFpp < 1.0, "bloomFilterFpp should be > 0.0 & < 1.0");

        this.stripeMinSize = stripeMinSize;
        this.stripeMaxSize = stripeMaxSize;
        this.stripeMaxRowCount = stripeMaxRowCount;
        this.rowGroupMaxRowCount = rowGroupMaxRowCount;
        this.dictionaryMaxMemory = dictionaryMaxMemory;
        this.maxStringStatisticsLimit = maxStringStatisticsLimit;
        this.maxCompressionBufferSize = maxCompressionBufferSize;
        this.bloomFilterColumns = ImmutableSet.copyOf(bloomFilterColumns);
        this.bloomFilterFpp = bloomFilterFpp;
    }

    public DataSize getStripeMinSize()
    {
        return stripeMinSize;
    }

    public DataSize getStripeMaxSize()
    {
        return stripeMaxSize;
    }

    public int getStripeMaxRowCount()
    {
        return stripeMaxRowCount;
    }

    public int getRowGroupMaxRowCount()
    {
        return rowGroupMaxRowCount;
    }

    public DataSize getDictionaryMaxMemory()
    {
        return dictionaryMaxMemory;
    }

    public DataSize getMaxStringStatisticsLimit()
    {
        return maxStringStatisticsLimit;
    }

    public DataSize getMaxCompressionBufferSize()
    {
        return maxCompressionBufferSize;
    }

    public double getBloomFilterFpp()
    {
        return bloomFilterFpp;
    }

    public boolean isBloomFilterColumn(String columnName)
    {
        return bloomFilterColumns.contains(columnName);
    }

    public OrcWriterOptions withStripeMinSize(DataSize stripeMinSize)
    {
        return new OrcWriterOptions(stripeMinSize, stripeMaxSize, stripeMaxRowCount, rowGroupMaxRowCount, dictionaryMaxMemory, maxStringStatisticsLimit, maxCompressionBufferSize, bloomFilterColumns, bloomFilterFpp);
    }

    public OrcWriterOptions withStripeMaxSize(DataSize stripeMaxSize)
    {
        return new OrcWriterOptions(stripeMinSize, stripeMaxSize, stripeMaxRowCount, rowGroupMaxRowCount, dictionaryMaxMemory, maxStringStatisticsLimit, maxCompressionBufferSize, bloomFilterColumns, bloomFilterFpp);
    }

    public OrcWriterOptions withStripeMaxRowCount(int stripeMaxRowCount)
    {
        return new OrcWriterOptions(stripeMinSize, stripeMaxSize, stripeMaxRowCount, rowGroupMaxRowCount, dictionaryMaxMemory, maxStringStatisticsLimit, maxCompressionBufferSize, bloomFilterColumns, bloomFilterFpp);
    }

    public OrcWriterOptions withRowGroupMaxRowCount(int rowGroupMaxRowCount)
    {
        return new OrcWriterOptions(stripeMinSize, stripeMaxSize, stripeMaxRowCount, rowGroupMaxRowCount, dictionaryMaxMemory, maxStringStatisticsLimit, maxCompressionBufferSize, bloomFilterColumns, bloomFilterFpp);
    }

    public OrcWriterOptions withDictionaryMaxMemory(DataSize dictionaryMaxMemory)
    {
        return new OrcWriterOptions(stripeMinSize, stripeMaxSize, stripeMaxRowCount, rowGroupMaxRowCount, dictionaryMaxMemory, maxStringStatisticsLimit, maxCompressionBufferSize, bloomFilterColumns, bloomFilterFpp);
    }

    public OrcWriterOptions withMaxStringStatisticsLimit(DataSize maxStringStatisticsLimit)
    {
        return new OrcWriterOptions(stripeMinSize, stripeMaxSize, stripeMaxRowCount, rowGroupMaxRowCount, dictionaryMaxMemory, maxStringStatisticsLimit, maxCompressionBufferSize, bloomFilterColumns, bloomFilterFpp);
    }

    public OrcWriterOptions withMaxCompressionBufferSize(DataSize maxCompressionBufferSize)
    {
        return new OrcWriterOptions(stripeMinSize, stripeMaxSize, stripeMaxRowCount, rowGroupMaxRowCount, dictionaryMaxMemory, maxStringStatisticsLimit, maxCompressionBufferSize, bloomFilterColumns, bloomFilterFpp);
    }

    public OrcWriterOptions withBloomFilterColumns(Set<String> bloomFilterColumns)
    {
        return new OrcWriterOptions(stripeMinSize, stripeMaxSize, stripeMaxRowCount, rowGroupMaxRowCount, dictionaryMaxMemory, maxStringStatisticsLimit, maxCompressionBufferSize, bloomFilterColumns, bloomFilterFpp);
    }

    public OrcWriterOptions withBloomFilterFpp(double bloomFilterFpp)
    {
        return new OrcWriterOptions(stripeMinSize, stripeMaxSize, stripeMaxRowCount, rowGroupMaxRowCount, dictionaryMaxMemory, maxStringStatisticsLimit, maxCompressionBufferSize, bloomFilterColumns, bloomFilterFpp);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("stripeMinSize", stripeMinSize)
                .add("stripeMaxSize", stripeMaxSize)
                .add("stripeMaxRowCount", stripeMaxRowCount)
                .add("rowGroupMaxRowCount", rowGroupMaxRowCount)
                .add("dictionaryMaxMemory", dictionaryMaxMemory)
                .add("maxStringStatisticsLimit", maxStringStatisticsLimit)
                .add("maxCompressionBufferSize", maxCompressionBufferSize)
                .add("bloomFilterColumns", bloomFilterColumns)
                .add("bloomFilterFpp", bloomFilterFpp)
                .toString();
    }
}
