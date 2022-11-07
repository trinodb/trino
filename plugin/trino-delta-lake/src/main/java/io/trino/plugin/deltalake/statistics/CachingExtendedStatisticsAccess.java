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
import com.google.inject.Inject;
import io.trino.collect.cache.EvictableCacheBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;

import javax.inject.Qualifier;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.time.Duration;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.collect.cache.CacheUtils.uncheckedCacheGet;
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
    private final Cache<String, Optional<ExtendedStatistics>> cache = EvictableCacheBuilder.newBuilder()
            .expireAfterWrite(CACHE_EXPIRATION)
            .maximumSize(CACHE_MAX_SIZE)
            .build();

    @Inject
    public CachingExtendedStatisticsAccess(@ForCachingExtendedStatisticsAccess ExtendedStatisticsAccess delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Optional<ExtendedStatistics> readExtendedStatistics(ConnectorSession session, String tableLocation)
    {
        try {
            return uncheckedCacheGet(cache, tableLocation, () -> delegate.readExtendedStatistics(session, tableLocation));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error reading statistics from cache", e.getCause());
        }
    }

    @Override
    public void updateExtendedStatistics(ConnectorSession session, String tableLocation, ExtendedStatistics statistics)
    {
        delegate.updateExtendedStatistics(session, tableLocation, statistics);
        cache.invalidate(tableLocation);
    }

    @Override
    public void deleteExtendedStatistics(ConnectorSession session, String tableLocation)
    {
        delegate.deleteExtendedStatistics(session, tableLocation);
        cache.invalidate(tableLocation);
    }

    public void invalidateCache(String tableLocation)
    {
        // for explicit cache invalidation
        cache.invalidate(tableLocation);
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @Qualifier
    public @interface ForCachingExtendedStatisticsAccess {};
}
