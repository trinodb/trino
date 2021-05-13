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
package io.trino.parquet.reader.cache;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;

import static java.util.concurrent.TimeUnit.HOURS;

public class MetadataCacheConfig
{
    private int metadataCacheMaxEntries;
    private Duration metadataCacheTtl = new Duration(1, HOURS);

    @Min(0)
    public int getMetadataCacheMaxEntries()
    {
        return metadataCacheMaxEntries;
    }

    @Config("parquet.metadata-cache.max-entries")
    @ConfigDescription("Maximum number of entries in Parquet metadata cache")
    public MetadataCacheConfig setMetadataCacheMaxEntries(int metadataCacheMaxEntries)
    {
        this.metadataCacheMaxEntries = metadataCacheMaxEntries;
        return this;
    }

    @MinDuration("0s")
    public Duration getMetadataCacheTtl()
    {
        return metadataCacheTtl;
    }

    @Config("parquet.metadata-cache.ttl")
    @ConfigDescription("Time-to-live for parquet metadata cache entry after last access")
    public MetadataCacheConfig setMetadataCacheTtl(Duration metadataCacheTtl)
    {
        this.metadataCacheTtl = metadataCacheTtl;
        return this;
    }
}
