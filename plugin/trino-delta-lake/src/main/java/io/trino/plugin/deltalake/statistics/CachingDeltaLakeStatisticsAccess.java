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
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.util.Objects.requireNonNull;

public class CachingDeltaLakeStatisticsAccess
        implements DeltaLakeStatisticsAccess
{
    private static final Duration CACHE_EXPIRATION = Duration.of(1, HOURS);
    private static final long CACHE_MAX_SIZE = 1000;

    private final DeltaLakeStatisticsAccess delegate;
    private final Cache<String, Optional<DeltaLakeStatistics>> cache = EvictableCacheBuilder.newBuilder()
            .expireAfterWrite(CACHE_EXPIRATION)
            .maximumSize(CACHE_MAX_SIZE)
            .build();

    @Inject
    public CachingDeltaLakeStatisticsAccess(@ForCachingDeltaLakeStatisticsAccess DeltaLakeStatisticsAccess delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Optional<DeltaLakeStatistics> readDeltaLakeStatistics(ConnectorSession session, String tableLocation)
    {
        try {
            return cache.get(tableLocation, () -> delegate.readDeltaLakeStatistics(session, tableLocation));
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error reading statistics from cache", e.getCause());
        }
    }

    @Override
    public void updateDeltaLakeStatistics(ConnectorSession session, String tableLocation, DeltaLakeStatistics statistics)
    {
        delegate.updateDeltaLakeStatistics(session, tableLocation, statistics);
        cache.invalidate(tableLocation);
    }

    @Override
    public void deleteDeltaLakeStatistics(ConnectorSession session, String tableLocation)
    {
        delegate.deleteDeltaLakeStatistics(session, tableLocation);
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
    public @interface ForCachingDeltaLakeStatisticsAccess {};
}
