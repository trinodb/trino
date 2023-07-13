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
package io.trino.plugin.hive.orc;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.trino.orc.OrcReaderOptions;

import javax.validation.constraints.NotNull;

public class OrcReaderConfig
{
    private boolean useColumnNames;

    private OrcReaderOptions options = new OrcReaderOptions();

    public OrcReaderOptions toOrcReaderOptions()
    {
        return options;
    }

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
        return options.isBloomFiltersEnabled();
    }

    @Config("hive.orc.bloom-filters.enabled")
    public OrcReaderConfig setBloomFiltersEnabled(boolean bloomFiltersEnabled)
    {
        options = options.withBloomFiltersEnabled(bloomFiltersEnabled);
        return this;
    }

    @NotNull
    public DataSize getMaxMergeDistance()
    {
        return options.getMaxMergeDistance();
    }

    @Config("hive.orc.max-merge-distance")
    public OrcReaderConfig setMaxMergeDistance(DataSize maxMergeDistance)
    {
        options = options.withMaxMergeDistance(maxMergeDistance);
        return this;
    }

    @NotNull
    public DataSize getMaxBufferSize()
    {
        return options.getMaxBufferSize();
    }

    @Config("hive.orc.max-buffer-size")
    public OrcReaderConfig setMaxBufferSize(DataSize maxBufferSize)
    {
        options = options.withMaxBufferSize(maxBufferSize);
        return this;
    }

    @NotNull
    public DataSize getTinyStripeThreshold()
    {
        return options.getTinyStripeThreshold();
    }

    @Config("hive.orc.tiny-stripe-threshold")
    public OrcReaderConfig setTinyStripeThreshold(DataSize tinyStripeThreshold)
    {
        options = options.withTinyStripeThreshold(tinyStripeThreshold);
        return this;
    }

    @NotNull
    public DataSize getStreamBufferSize()
    {
        return options.getStreamBufferSize();
    }

    @Config("hive.orc.stream-buffer-size")
    public OrcReaderConfig setStreamBufferSize(DataSize streamBufferSize)
    {
        options = options.withStreamBufferSize(streamBufferSize);
        return this;
    }

    @NotNull
    public DataSize getMaxBlockSize()
    {
        return options.getMaxBlockSize();
    }

    @Config("hive.orc.max-read-block-size")
    public OrcReaderConfig setMaxBlockSize(DataSize maxBlockSize)
    {
        options = options.withMaxReadBlockSize(maxBlockSize);
        return this;
    }

    @Deprecated
    public boolean isLazyReadSmallRanges()
    {
        return options.isLazyReadSmallRanges();
    }

    // TODO remove config option once efficacy is proven
    @Deprecated
    @Config("hive.orc.lazy-read-small-ranges")
    @ConfigDescription("ORC read small disk ranges lazily")
    public OrcReaderConfig setLazyReadSmallRanges(boolean lazyReadSmallRanges)
    {
        options = options.withLazyReadSmallRanges(lazyReadSmallRanges);
        return this;
    }

    @Deprecated
    public boolean isNestedLazy()
    {
        return options.isNestedLazy();
    }

    // TODO remove config option once efficacy is proven
    @Deprecated
    @Config("hive.orc.nested-lazy")
    @ConfigDescription("ORC lazily read nested data")
    public OrcReaderConfig setNestedLazy(boolean nestedLazy)
    {
        options = options.withNestedLazy(nestedLazy);
        return this;
    }

    public boolean isReadLegacyShortZoneId()
    {
        return options.isReadLegacyShortZoneId();
    }

    @Config("hive.orc.read-legacy-short-zone-id")
    @ConfigDescription("Allow reads on ORC files with short zone ID in the stripe footer")
    public OrcReaderConfig setReadLegacyShortZoneId(boolean readLegacyShortZoneId)
    {
        options = options.withReadLegacyShortZoneId(readLegacyShortZoneId);
        return this;
    }
}
