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
package io.prestosql.plugin.jdbc;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.airlift.units.Duration;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.TableStatistics;

import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TransactionCachingJdbcClient
        extends CachingJdbcClient
{
    private final Cache<TableStatisticsCacheKey, TableStatistics> statisticsCache;

    public TransactionCachingJdbcClient(JdbcClient delegate, Duration cachingTtl)
    {
        super(delegate, cachingTtl, true);
        this.statisticsCache = CacheBuilder.newBuilder()
                .expireAfterWrite(cachingTtl.toMillis(), MILLISECONDS)
                .build();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        // session stays the same per transaction, therefore it doesn't need to be part of cache key
        TableStatisticsCacheKey key = new TableStatisticsCacheKey(handle, tupleDomain);
        TableStatistics cachedStatistics = statisticsCache.getIfPresent(key);
        if (cachedStatistics != null) {
            return cachedStatistics;
        }

        TableStatistics statistics = super.getTableStatistics(session, handle, tupleDomain);
        statisticsCache.put(key, statistics);
        return statistics;
    }

    private static final class TableStatisticsCacheKey
    {
        private final JdbcTableHandle tableHandle;
        private final TupleDomain<ColumnHandle> tupleDomain;

        private TableStatisticsCacheKey(JdbcTableHandle tableHandle, TupleDomain<ColumnHandle> tupleDomain)
        {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
            this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TableStatisticsCacheKey that = (TableStatisticsCacheKey) o;
            return Objects.equals(tableHandle, that.tableHandle) &&
                    Objects.equals(tupleDomain, that.tupleDomain);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableHandle, tupleDomain);
        }
    }
}
