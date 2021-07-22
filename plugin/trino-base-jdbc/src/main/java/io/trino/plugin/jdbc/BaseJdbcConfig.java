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
package io.trino.plugin.jdbc;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.annotation.PostConstruct;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import java.util.Set;

import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.validation.constraints.Pattern.Flag.CASE_INSENSITIVE;

public class BaseJdbcConfig
{
    private String connectionUrl;
    private Set<String> jdbcTypesMappedToVarchar = ImmutableSet.of();
    public static final Duration CACHING_DISABLED = new Duration(0, MILLISECONDS);
    private Duration metadataCacheTtl = CACHING_DISABLED;
    private boolean cacheMissing;
    public static final long DEFAULT_METADATA_CACHE_SIZE = 10000;
    private long cacheMaximumSize = DEFAULT_METADATA_CACHE_SIZE;

    @NotNull
    // Some drivers match case insensitive in Driver.acceptURL
    @Pattern(regexp = "^jdbc:[a-z0-9]+:(?s:.*)$", flags = CASE_INSENSITIVE)
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("connection-url")
    public BaseJdbcConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    public Set<String> getJdbcTypesMappedToVarchar()
    {
        return jdbcTypesMappedToVarchar;
    }

    @Config("jdbc-types-mapped-to-varchar")
    public BaseJdbcConfig setJdbcTypesMappedToVarchar(String jdbcTypesMappedToVarchar)
    {
        this.jdbcTypesMappedToVarchar = ImmutableSet.copyOf(Splitter.on(",").omitEmptyStrings().trimResults().split(nullToEmpty(jdbcTypesMappedToVarchar)));
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getMetadataCacheTtl()
    {
        return metadataCacheTtl;
    }

    @Config("metadata.cache-ttl")
    @ConfigDescription("Determines how long meta information will be cached")
    public BaseJdbcConfig setMetadataCacheTtl(Duration metadataCacheTtl)
    {
        this.metadataCacheTtl = metadataCacheTtl;
        return this;
    }

    public boolean isCacheMissing()
    {
        return cacheMissing;
    }

    @Config("metadata.cache-missing")
    @ConfigDescription("Determines if missing information will be cached")
    public BaseJdbcConfig setCacheMissing(boolean cacheMissing)
    {
        this.cacheMissing = cacheMissing;
        return this;
    }

    @Min(1)
    public long getCacheMaximumSize()
    {
        return cacheMaximumSize;
    }

    @Config("metadata.cache-maximum-size")
    @ConfigDescription("Maximum number of objects stored in the metadata cache")
    public BaseJdbcConfig setCacheMaximumSize(long cacheMaximumSize)
    {
        this.cacheMaximumSize = cacheMaximumSize;
        return this;
    }

    @PostConstruct
    public void validate()
    {
        if (metadataCacheTtl.equals(CACHING_DISABLED) && cacheMaximumSize != BaseJdbcConfig.DEFAULT_METADATA_CACHE_SIZE) {
            throw new IllegalArgumentException(
                    format("metadata.cache-ttl must be set to a non-zero value when metadata.cache-maximum-size is set"));
        }
    }
}
