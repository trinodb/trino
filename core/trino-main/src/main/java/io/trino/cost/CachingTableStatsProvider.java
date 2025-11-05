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
package io.trino.cost;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.statistics.TableStatistics;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class CachingTableStatsProvider
        implements TableStatsProvider
{
    private final Metadata metadata;
    private final Session session;
    private final Supplier<Boolean> isQueryDone;

    private final Map<TableHandle, TableStatistics> cache = new WeakHashMap<>();

    public CachingTableStatsProvider(Metadata metadata, Session session, Supplier<Boolean> isQueryDone)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
        this.isQueryDone = requireNonNull(isQueryDone, "isQueryDone is null");
    }

    @Override
    public TableStatistics getTableStatistics(TableHandle tableHandle)
    {
        return cache.computeIfAbsent(tableHandle, this::getTableStatisticsInternal);
    }

    private TableStatistics getTableStatisticsInternal(TableHandle tableHandle)
    {
        try {
            return metadata.getTableStatistics(session, tableHandle);
        }
        catch (RuntimeException e) {
            if (isQueryDone.get()) {
                // getting statistics for finished query may result in many different exceptions being thrown.
                // As we do not care about the result anyway mask it by returning empty statistics.
                return TableStatistics.empty();
            }
            throw e;
        }
    }

    public Map<TableHandle, TableStatistics> getCachedTableStatistics()
    {
        return ImmutableMap.copyOf(cache);
    }
}
