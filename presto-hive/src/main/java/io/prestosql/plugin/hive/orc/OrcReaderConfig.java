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
package io.prestosql.plugin.hive.orc;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class OrcReaderConfig
{
    private boolean useColumnNames;
    private boolean bloomFiltersEnabled;

    private DataSize maxMergeDistance = new DataSize(1, MEGABYTE);
    private DataSize maxBufferSize = new DataSize(8, MEGABYTE);
    private DataSize tinyStripeThreshold = new DataSize(8, MEGABYTE);
    private DataSize streamBufferSize = new DataSize(8, MEGABYTE);
    private DataSize maxBlockSize = new DataSize(16, MEGABYTE);
    private boolean lazyReadSmallRanges = true;

    public boolean isUseColumnNames()
    {
        return useColumnNames;
    }

    @Config("hive.orc.use-column-names")
    @ConfigDescription("Access ORC columns using names from the file")
    public OrcReaderConfig setUseColumnNames(boolean useColumnNames)
    {
        this.useColumnNames = useColumnNames;
        return this;
    }

    public boolean isBloomFiltersEnabled()
    {
        return bloomFiltersEnabled;
    }

    @Config("hive.orc.bloom-filters.enabled")
    public OrcReaderConfig setBloomFiltersEnabled(boolean bloomFiltersEnabled)
    {
        this.bloomFiltersEnabled = bloomFiltersEnabled;
        return this;
    }

    @NotNull
    public DataSize getMaxMergeDistance()
    {
        return maxMergeDistance;
    }

    @Config("hive.orc.max-merge-distance")
    public OrcReaderConfig setMaxMergeDistance(DataSize maxMergeDistance)
    {
        this.maxMergeDistance = maxMergeDistance;
        return this;
    }

    @NotNull
    public DataSize getMaxBufferSize()
    {
        return maxBufferSize;
    }

    @Config("hive.orc.max-buffer-size")
    public OrcReaderConfig setMaxBufferSize(DataSize maxBufferSize)
    {
        this.maxBufferSize = maxBufferSize;
        return this;
    }

    @NotNull
    public DataSize getTinyStripeThreshold()
    {
        return tinyStripeThreshold;
    }

    @Config("hive.orc.tiny-stripe-threshold")
    public OrcReaderConfig setTinyStripeThreshold(DataSize tinyStripeThreshold)
    {
        this.tinyStripeThreshold = tinyStripeThreshold;
        return this;
    }

    @NotNull
    public DataSize getStreamBufferSize()
    {
        return streamBufferSize;
    }

    @Config("hive.orc.stream-buffer-size")
    public OrcReaderConfig setStreamBufferSize(DataSize streamBufferSize)
    {
        this.streamBufferSize = streamBufferSize;
        return this;
    }

    @NotNull
    public DataSize getMaxBlockSize()
    {
        return maxBlockSize;
    }

    @Config("hive.orc.max-read-block-size")
    public OrcReaderConfig setMaxBlockSize(DataSize maxBlockSize)
    {
        this.maxBlockSize = maxBlockSize;
        return this;
    }

    @Deprecated
    public boolean isLazyReadSmallRanges()
    {
        return lazyReadSmallRanges;
    }

    // TODO remove config option once efficacy is proven
    @Deprecated
    @Config("hive.orc.lazy-read-small-ranges")
    @ConfigDescription("ORC read small disk ranges lazily")
    public OrcReaderConfig setLazyReadSmallRanges(boolean lazyReadSmallRanges)
    {
        this.lazyReadSmallRanges = lazyReadSmallRanges;
        return this;
    }
}
