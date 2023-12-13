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
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Strings.nullToEmpty;
import static jakarta.validation.constraints.Pattern.Flag.CASE_INSENSITIVE;
import static java.util.concurrent.TimeUnit.SECONDS;

public class BaseJdbcConfig
{
    private static final String METADATA_CACHE_TTL = "metadata.cache-ttl";
    private static final String METADATA_SCHEMAS_CACHE_TTL = "metadata.schemas.cache-ttl";
    private static final String METADATA_TABLES_CACHE_TTL = "metadata.tables.cache-ttl";
    private static final String METADATA_STATISTICS_CACHE_TTL = "metadata.statistics.cache-ttl";
    private static final String METADATA_CACHE_MAXIMUM_SIZE = "metadata.cache-maximum-size";
    private static final long DEFAULT_METADATA_CACHE_SIZE = 10000;

    private String connectionUrl;
    private Set<String> jdbcTypesMappedToVarchar = ImmutableSet.of();
    private Duration metadataCacheTtl = new Duration(0, SECONDS);
    private Optional<Duration> schemaNamesCacheTtl = Optional.empty();
    private Optional<Duration> tableNamesCacheTtl = Optional.empty();
    private Optional<Duration> statisticsCacheTtl = Optional.empty();
    private boolean cacheMissing;
    private Optional<Long> cacheMaximumSize = Optional.empty();

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

    @Config(METADATA_CACHE_TTL)
    @ConfigDescription("Determines how long meta information will be cached")
    public BaseJdbcConfig setMetadataCacheTtl(Duration metadataCacheTtl)
    {
        this.metadataCacheTtl = metadataCacheTtl;
        return this;
    }

    @NotNull
    public Duration getSchemaNamesCacheTtl()
    {
        return schemaNamesCacheTtl.orElse(metadataCacheTtl);
    }

    @Config(METADATA_SCHEMAS_CACHE_TTL)
    @ConfigDescription("Determines how long schema names list information will be cached")
    public BaseJdbcConfig setSchemaNamesCacheTtl(Duration schemaNamesCacheTtl)
    {
        this.schemaNamesCacheTtl = Optional.ofNullable(schemaNamesCacheTtl);
        return this;
    }

    @NotNull
    public Duration getTableNamesCacheTtl()
    {
        return tableNamesCacheTtl.orElse(metadataCacheTtl);
    }

    @Config(METADATA_TABLES_CACHE_TTL)
    @ConfigDescription("Determines how long table names list information will be cached")
    public BaseJdbcConfig setTableNamesCacheTtl(Duration tableNamesCacheTtl)
    {
        this.tableNamesCacheTtl = Optional.ofNullable(tableNamesCacheTtl);
        return this;
    }

    @NotNull
    public Duration getStatisticsCacheTtl()
    {
        return statisticsCacheTtl.orElse(metadataCacheTtl);
    }

    @Config(METADATA_STATISTICS_CACHE_TTL)
    @ConfigDescription("Determines how long table statistics information will be cached")
    public BaseJdbcConfig setStatisticsCacheTtl(Duration statisticsCacheTtl)
    {
        this.statisticsCacheTtl = Optional.ofNullable(statisticsCacheTtl);
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
        return cacheMaximumSize.orElse(DEFAULT_METADATA_CACHE_SIZE);
    }

    @Config(METADATA_CACHE_MAXIMUM_SIZE)
    @ConfigDescription("Maximum number of objects stored in the metadata cache")
    public BaseJdbcConfig setCacheMaximumSize(long cacheMaximumSize)
    {
        this.cacheMaximumSize = Optional.of(cacheMaximumSize);
        return this;
    }

    @AssertTrue(message = METADATA_CACHE_TTL + " or " + METADATA_STATISTICS_CACHE_TTL + " must be set to a non-zero value when " + METADATA_CACHE_MAXIMUM_SIZE + " is set")
    public boolean isCacheMaximumSizeConsistent()
    {
        return !metadataCacheTtl.isZero() ||
                (statisticsCacheTtl.isPresent() && !statisticsCacheTtl.get().isZero()) ||
                cacheMaximumSize.isEmpty();
    }

    @AssertTrue(message = METADATA_SCHEMAS_CACHE_TTL + " must not be set when " + METADATA_CACHE_TTL + " is not set")
    public boolean isSchemaNamesCacheTtlConsistent()
    {
        return !metadataCacheTtl.isZero() || schemaNamesCacheTtl.isEmpty();
    }

    @AssertTrue(message = METADATA_TABLES_CACHE_TTL + " must not be set when " + METADATA_CACHE_TTL + " is not set")
    public boolean isTableNamesCacheTtlConsistent()
    {
        return !metadataCacheTtl.isZero() || tableNamesCacheTtl.isEmpty();
    }
}
