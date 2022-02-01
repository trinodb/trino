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
package io.trino.orc;

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
    public enum WriterIdentification
    {
        /**
         * Write ORC files with a writer identification and version number that is readable by Hive 2.0.0 to 2.2.0
         */
        LEGACY_HIVE_COMPATIBLE,

        /**
         * Write ORC files with Trino writer identification.
         */
        TRINO,
    }

    @VisibleForTesting
    static final DataSize DEFAULT_MAX_STRING_STATISTICS_LIMIT = DataSize.ofBytes(64);
    @VisibleForTesting
    static final DataSize DEFAULT_MAX_COMPRESSION_BUFFER_SIZE = DataSize.of(256, KILOBYTE);
    private static final double DEFAULT_BLOOM_FILTER_FPP = 0.05;
    private static final DataSize DEFAULT_STRIPE_MIN_SIZE = DataSize.of(32, MEGABYTE);
    private static final DataSize DEFAULT_STRIPE_MAX_SIZE = DataSize.of(64, MEGABYTE);
    private static final int DEFAULT_STRIPE_MAX_ROW_COUNT = 10_000_000;
    private static final int DEFAULT_ROW_GROUP_MAX_ROW_COUNT = 10_000;
    private static final DataSize DEFAULT_DICTIONARY_MAX_MEMORY = DataSize.of(16, MEGABYTE);

    private final WriterIdentification writerIdentification;
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
                WriterIdentification.TRINO,
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
            WriterIdentification writerIdentification,
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
        requireNonNull(bloomFilterColumns, "bloomFilterColumns is null");
        checkArgument(bloomFilterFpp > 0.0 && bloomFilterFpp < 1.0, "bloomFilterFpp should be > 0.0 & < 1.0");

        this.writerIdentification = requireNonNull(writerIdentification, "writerIdentification is null");
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

    public WriterIdentification getWriterIdentification()
    {
        return writerIdentification;
    }

    public OrcWriterOptions withWriterIdentification(WriterIdentification writerIdentification)
    {
        return builderFrom(this)
                .setWriterIdentification(writerIdentification)
                .build();
    }

    public DataSize getStripeMinSize()
    {
        return stripeMinSize;
    }

    public OrcWriterOptions withStripeMinSize(DataSize stripeMinSize)
    {
        return builderFrom(this)
                .setStripeMinSize(stripeMinSize)
                .build();
    }

    public DataSize getStripeMaxSize()
    {
        return stripeMaxSize;
    }

    public OrcWriterOptions withStripeMaxSize(DataSize stripeMaxSize)
    {
        return builderFrom(this)
                .setStripeMaxSize(stripeMaxSize)
                .build();
    }

    public int getStripeMaxRowCount()
    {
        return stripeMaxRowCount;
    }

    public OrcWriterOptions withStripeMaxRowCount(int stripeMaxRowCount)
    {
        return builderFrom(this)
                .setStripeMaxRowCount(stripeMaxRowCount)
                .build();
    }

    public int getRowGroupMaxRowCount()
    {
        return rowGroupMaxRowCount;
    }

    public OrcWriterOptions withRowGroupMaxRowCount(int rowGroupMaxRowCount)
    {
        return builderFrom(this)
                .setRowGroupMaxRowCount(rowGroupMaxRowCount)
                .build();
    }

    public DataSize getDictionaryMaxMemory()
    {
        return dictionaryMaxMemory;
    }

    public OrcWriterOptions withDictionaryMaxMemory(DataSize dictionaryMaxMemory)
    {
        return builderFrom(this)
                .setDictionaryMaxMemory(dictionaryMaxMemory)
                .build();
    }

    public DataSize getMaxStringStatisticsLimit()
    {
        return maxStringStatisticsLimit;
    }

    public OrcWriterOptions withMaxStringStatisticsLimit(DataSize maxStringStatisticsLimit)
    {
        return builderFrom(this)
                .setMaxStringStatisticsLimit(maxStringStatisticsLimit)
                .build();
    }

    public DataSize getMaxCompressionBufferSize()
    {
        return maxCompressionBufferSize;
    }

    public OrcWriterOptions withMaxCompressionBufferSize(DataSize maxCompressionBufferSize)
    {
        return builderFrom(this)
                .setMaxCompressionBufferSize(maxCompressionBufferSize)
                .build();
    }

    public boolean isBloomFilterColumn(String columnName)
    {
        return bloomFilterColumns.contains(columnName);
    }

    public OrcWriterOptions withBloomFilterColumns(Set<String> bloomFilterColumns)
    {
        return builderFrom(this)
                .setBloomFilterColumns(bloomFilterColumns)
                .build();
    }

    public double getBloomFilterFpp()
    {
        return bloomFilterFpp;
    }

    public OrcWriterOptions withBloomFilterFpp(double bloomFilterFpp)
    {
        return builderFrom(this)
                .setBloomFilterFpp(bloomFilterFpp)
                .build();
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

    public static Builder builder()
    {
        return builderFrom(new OrcWriterOptions());
    }

    public static Builder builderFrom(OrcWriterOptions options)
    {
        return new Builder(options);
    }

    public static final class Builder
    {
        private WriterIdentification writerIdentification;
        private DataSize stripeMinSize;
        private DataSize stripeMaxSize;
        private int stripeMaxRowCount;
        private int rowGroupMaxRowCount;
        private DataSize dictionaryMaxMemory;
        private DataSize maxStringStatisticsLimit;
        private DataSize maxCompressionBufferSize;
        private Set<String> bloomFilterColumns;
        private double bloomFilterFpp;

        private Builder(OrcWriterOptions options)
        {
            requireNonNull(options, "options is null");

            this.writerIdentification = options.writerIdentification;
            this.stripeMinSize = options.stripeMinSize;
            this.stripeMaxSize = options.stripeMaxSize;
            this.stripeMaxRowCount = options.stripeMaxRowCount;
            this.rowGroupMaxRowCount = options.rowGroupMaxRowCount;
            this.dictionaryMaxMemory = options.dictionaryMaxMemory;
            this.maxStringStatisticsLimit = options.maxStringStatisticsLimit;
            this.maxCompressionBufferSize = options.maxCompressionBufferSize;
            this.bloomFilterColumns = ImmutableSet.copyOf(options.bloomFilterColumns);
            this.bloomFilterFpp = options.bloomFilterFpp;
        }

        public Builder setWriterIdentification(WriterIdentification writerIdentification)
        {
            this.writerIdentification = writerIdentification;
            return this;
        }

        public Builder setStripeMinSize(DataSize stripeMinSize)
        {
            this.stripeMinSize = stripeMinSize;
            return this;
        }

        public Builder setStripeMaxSize(DataSize stripeMaxSize)
        {
            this.stripeMaxSize = stripeMaxSize;
            return this;
        }

        public Builder setStripeMaxRowCount(int stripeMaxRowCount)
        {
            this.stripeMaxRowCount = stripeMaxRowCount;
            return this;
        }

        public Builder setRowGroupMaxRowCount(int rowGroupMaxRowCount)
        {
            this.rowGroupMaxRowCount = rowGroupMaxRowCount;
            return this;
        }

        public Builder setDictionaryMaxMemory(DataSize dictionaryMaxMemory)
        {
            this.dictionaryMaxMemory = dictionaryMaxMemory;
            return this;
        }

        public Builder setMaxStringStatisticsLimit(DataSize maxStringStatisticsLimit)
        {
            this.maxStringStatisticsLimit = maxStringStatisticsLimit;
            return this;
        }

        public Builder setMaxCompressionBufferSize(DataSize maxCompressionBufferSize)
        {
            this.maxCompressionBufferSize = maxCompressionBufferSize;
            return this;
        }

        public Builder setBloomFilterColumns(Set<String> bloomFilterColumns)
        {
            this.bloomFilterColumns = bloomFilterColumns;
            return this;
        }

        public Builder setBloomFilterFpp(double bloomFilterFpp)
        {
            this.bloomFilterFpp = bloomFilterFpp;
            return this;
        }

        public OrcWriterOptions build()
        {
            return new OrcWriterOptions(
                    writerIdentification,
                    stripeMinSize,
                    stripeMaxSize,
                    stripeMaxRowCount,
                    rowGroupMaxRowCount,
                    dictionaryMaxMemory,
                    maxStringStatisticsLimit,
                    maxCompressionBufferSize,
                    bloomFilterColumns,
                    bloomFilterFpp);
        }
    }
}
