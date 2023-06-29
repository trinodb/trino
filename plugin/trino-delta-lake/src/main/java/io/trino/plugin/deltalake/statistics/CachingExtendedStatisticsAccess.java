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
package io.trino.plugin.deltalake.statistics;

import com.google.common.cache.Cache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.time.Duration;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.cache.CacheUtils.invalidateAllIf;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.util.Objects.requireNonNull;

public class CachingExtendedStatisticsAccess
        implements ExtendedStatisticsAccess
{
    private static final Duration CACHE_EXPIRATION = Duration.of(1, HOURS);
    private static final long CACHE_MAX_SIZE = 1000;

    private final ExtendedStatisticsAccess delegate;
    private final Cache<CacheKey, Optional<ExtendedStatistics>> cache = EvictableCacheBuilder.newBuilder()
            .expireAfterWrite(CACHE_EXPIRATION)
            .maximumSize(CACHE_MAX_SIZE)
            .build();

    @Inject
    public CachingExtendedStatisticsAccess(@ForCachingExtendedStatisticsAccess ExtendedStatisticsAccess delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Optional<ExtendedStatistics> readExtendedStatistics(ConnectorSession session, SchemaTableName schemaTableName, String tableLocation)
    {
        try {
            return uncheckedCacheGet(cache, new CacheKey(schemaTableName, tableLocation), () -> delegate.readExtendedStatistics(session, schemaTableName, tableLocation));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error reading statistics from cache", e.getCause());
        }
    }

    @Override
    public void updateExtendedStatistics(ConnectorSession session, SchemaTableName schemaTableName, String tableLocation, ExtendedStatistics statistics)
    {
        delegate.updateExtendedStatistics(session, schemaTableName, tableLocation, statistics);
        cache.invalidate(new CacheKey(schemaTableName, tableLocation));
    }

    @Override
    public void deleteExtendedStatistics(ConnectorSession session, SchemaTableName schemaTableName, String tableLocation)
    {
        delegate.deleteExtendedStatistics(session, schemaTableName, tableLocation);
        cache.invalidate(new CacheKey(schemaTableName, tableLocation));
    }

    public void invalidateCache()
    {
        cache.invalidateAll();
    }

    // for explicit cache invalidation
    public void invalidateCache(SchemaTableName schemaTableName, Optional<String> tableLocation)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        // Invalidate by location in case one table (location) unregistered and re-register under different name
        tableLocation.ifPresent(location -> invalidateAllIf(cache, cacheKey -> cacheKey.location().equals(location)));
        invalidateAllIf(cache, cacheKey -> cacheKey.tableName().equals(schemaTableName));
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForCachingExtendedStatisticsAccess {}

    private record CacheKey(SchemaTableName tableName, String location)
    {
        CacheKey
        {
            requireNonNull(tableName, "tableName is null");
            requireNonNull(location, "location is null");
        }
    }
}
