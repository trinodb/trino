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

import io.airlift.units.DataSize;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class OrcReaderOptions
{
    private static final boolean DEFAULT_BLOOM_FILTERS_ENABLED = false;
    private static final DataSize DEFAULT_MAX_MERGE_DISTANCE = DataSize.of(1, MEGABYTE);
    private static final DataSize DEFAULT_MAX_BUFFER_SIZE = DataSize.of(8, MEGABYTE);
    private static final DataSize DEFAULT_TINY_STRIPE_THRESHOLD = DataSize.of(8, MEGABYTE);
    private static final DataSize DEFAULT_STREAM_BUFFER_SIZE = DataSize.of(8, MEGABYTE);
    private static final DataSize DEFAULT_MAX_BLOCK_SIZE = DataSize.of(16, MEGABYTE);
    private static final boolean DEFAULT_LAZY_READ_SMALL_RANGES = true;
    private static final boolean DEFAULT_NESTED_LAZY = true;
    private static final boolean DEFAULT_READ_LEGACY_SHORT_ZONE_ID = false;

    private final boolean bloomFiltersEnabled;

    private final DataSize maxMergeDistance;
    private final DataSize maxBufferSize;
    private final DataSize tinyStripeThreshold;
    private final DataSize streamBufferSize;
    private final DataSize maxBlockSize;
    private final boolean lazyReadSmallRanges;
    private final boolean nestedLazy;
    private final boolean readLegacyShortZoneId;

    public OrcReaderOptions()
    {
        this(
                DEFAULT_BLOOM_FILTERS_ENABLED,
                DEFAULT_MAX_MERGE_DISTANCE,
                DEFAULT_MAX_BUFFER_SIZE,
                DEFAULT_TINY_STRIPE_THRESHOLD,
                DEFAULT_STREAM_BUFFER_SIZE,
                DEFAULT_MAX_BLOCK_SIZE,
                DEFAULT_LAZY_READ_SMALL_RANGES,
                DEFAULT_NESTED_LAZY,
                DEFAULT_READ_LEGACY_SHORT_ZONE_ID);
    }

    private OrcReaderOptions(
            boolean bloomFiltersEnabled,
            DataSize maxMergeDistance,
            DataSize maxBufferSize,
            DataSize tinyStripeThreshold,
            DataSize streamBufferSize,
            DataSize maxBlockSize,
            boolean lazyReadSmallRanges,
            boolean nestedLazy,
            boolean readLegacyShortZoneId)
    {
        this.maxMergeDistance = requireNonNull(maxMergeDistance, "maxMergeDistance is null");
        this.maxBufferSize = requireNonNull(maxBufferSize, "maxBufferSize is null");
        this.tinyStripeThreshold = requireNonNull(tinyStripeThreshold, "tinyStripeThreshold is null");
        this.streamBufferSize = requireNonNull(streamBufferSize, "streamBufferSize is null");
        this.maxBlockSize = requireNonNull(maxBlockSize, "maxBlockSize is null");
        this.lazyReadSmallRanges = lazyReadSmallRanges;
        this.bloomFiltersEnabled = bloomFiltersEnabled;
        this.nestedLazy = nestedLazy;
        this.readLegacyShortZoneId = readLegacyShortZoneId;
    }

    public boolean isBloomFiltersEnabled()
    {
        return bloomFiltersEnabled;
    }

    public DataSize getMaxMergeDistance()
    {
        return maxMergeDistance;
    }

    public DataSize getMaxBufferSize()
    {
        return maxBufferSize;
    }

    public DataSize getTinyStripeThreshold()
    {
        return tinyStripeThreshold;
    }

    public DataSize getStreamBufferSize()
    {
        return streamBufferSize;
    }

    public DataSize getMaxBlockSize()
    {
        return maxBlockSize;
    }

    public boolean isLazyReadSmallRanges()
    {
        return lazyReadSmallRanges;
    }

    public boolean isNestedLazy()
    {
        return nestedLazy;
    }

    public boolean isReadLegacyShortZoneId()
    {
        return readLegacyShortZoneId;
    }

    public OrcReaderOptions withBloomFiltersEnabled(boolean bloomFiltersEnabled)
    {
        return new Builder(this)
                .withBloomFiltersEnabled(bloomFiltersEnabled)
                .build();
    }

    public OrcReaderOptions withMaxMergeDistance(DataSize maxMergeDistance)
    {
        return new Builder(this)
                .withMaxMergeDistance(maxMergeDistance)
                .build();
    }

    public OrcReaderOptions withMaxBufferSize(DataSize maxBufferSize)
    {
        return new Builder(this)
                .withMaxBufferSize(maxBufferSize)
                .build();
    }

    public OrcReaderOptions withTinyStripeThreshold(DataSize tinyStripeThreshold)
    {
        return new Builder(this)
                .withTinyStripeThreshold(tinyStripeThreshold)
                .build();
    }

    public OrcReaderOptions withStreamBufferSize(DataSize streamBufferSize)
    {
        return new Builder(this)
                .withStreamBufferSize(streamBufferSize)
                .build();
    }

    public OrcReaderOptions withMaxReadBlockSize(DataSize maxBlockSize)
    {
        return new Builder(this)
                .withMaxBlockSize(maxBlockSize)
                .build();
    }

    // TODO remove config option once efficacy is proven
    @Deprecated
    public OrcReaderOptions withLazyReadSmallRanges(boolean lazyReadSmallRanges)
    {
        return new Builder(this)
                .withLazyReadSmallRanges(lazyReadSmallRanges)
                .build();
    }

    // TODO remove config option once efficacy is proven
    @Deprecated
    public OrcReaderOptions withNestedLazy(boolean nestedLazy)
    {
        return new Builder(this)
                .withNestedLazy(nestedLazy)
                .build();
    }

    @Deprecated
    public OrcReaderOptions withReadLegacyShortZoneId(boolean readLegacyShortZoneId)
    {
        return new Builder(this)
                .withReadLegacyShortZoneId(readLegacyShortZoneId)
                .build();
    }

    private static class Builder
    {
        private boolean bloomFiltersEnabled;
        private DataSize maxMergeDistance;
        private DataSize maxBufferSize;
        private DataSize tinyStripeThreshold;
        private DataSize streamBufferSize;
        private DataSize maxBlockSize;
        private boolean lazyReadSmallRanges;
        private boolean nestedLazy;
        private boolean readLegacyShortZoneId;

        private Builder(OrcReaderOptions orcReaderOptions)
        {
            requireNonNull(orcReaderOptions, "orcReaderOptions is null");
            this.bloomFiltersEnabled = orcReaderOptions.bloomFiltersEnabled;
            this.maxMergeDistance = orcReaderOptions.maxMergeDistance;
            this.maxBufferSize = orcReaderOptions.maxBufferSize;
            this.tinyStripeThreshold = orcReaderOptions.tinyStripeThreshold;
            this.streamBufferSize = orcReaderOptions.streamBufferSize;
            this.maxBlockSize = orcReaderOptions.maxBlockSize;
            this.lazyReadSmallRanges = orcReaderOptions.lazyReadSmallRanges;
            this.nestedLazy = orcReaderOptions.nestedLazy;
            this.readLegacyShortZoneId = orcReaderOptions.readLegacyShortZoneId;
        }

        public Builder withBloomFiltersEnabled(boolean bloomFiltersEnabled)
        {
            this.bloomFiltersEnabled = bloomFiltersEnabled;
            return this;
        }

        public Builder withMaxMergeDistance(DataSize maxMergeDistance)
        {
            this.maxMergeDistance = requireNonNull(maxMergeDistance, "maxMergeDistance is null");
            return this;
        }

        public Builder withMaxBufferSize(DataSize maxBufferSize)
        {
            this.maxBufferSize = requireNonNull(maxBufferSize, "maxBufferSize is null");
            return this;
        }

        public Builder withTinyStripeThreshold(DataSize tinyStripeThreshold)
        {
            this.tinyStripeThreshold = requireNonNull(tinyStripeThreshold, "tinyStripeThreshold is null");
            return this;
        }

        public Builder withStreamBufferSize(DataSize streamBufferSize)
        {
            this.streamBufferSize = requireNonNull(streamBufferSize, "streamBufferSize is null");
            return this;
        }

        public Builder withMaxBlockSize(DataSize maxBlockSize)
        {
            this.maxBlockSize = requireNonNull(maxBlockSize, "maxBlockSize is null");
            return this;
        }

        public Builder withLazyReadSmallRanges(boolean lazyReadSmallRanges)
        {
            this.lazyReadSmallRanges = lazyReadSmallRanges;
            return this;
        }

        public Builder withNestedLazy(boolean nestedLazy)
        {
            this.nestedLazy = nestedLazy;
            return this;
        }

        public Builder withReadLegacyShortZoneId(boolean shortZoneIdEnabled)
        {
            this.readLegacyShortZoneId = shortZoneIdEnabled;
            return this;
        }

        private OrcReaderOptions build()
        {
            return new OrcReaderOptions(
                    bloomFiltersEnabled,
                    maxMergeDistance,
                    maxBufferSize,
                    tinyStripeThreshold,
                    streamBufferSize,
                    maxBlockSize,
                    lazyReadSmallRanges,
                    nestedLazy,
                    readLegacyShortZoneId);
        }
    }
}
