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

import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.statistics.TableStatistics;

import java.util.Map;
import java.util.WeakHashMap;

import static java.util.Objects.requireNonNull;

public class CachingTableStatsProvider
        implements TableStatsProvider
{
    private final Metadata metadata;
    private final Session session;

    private final Map<TableHandle, TableStatistics> cache = new WeakHashMap<>();

    public CachingTableStatsProvider(Metadata metadata, Session session)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public TableStatistics getTableStatistics(TableHandle tableHandle)
    {
        TableStatistics stats = cache.get(tableHandle);
        if (stats == null) {
            stats = metadata.getTableStatistics(session, tableHandle);
            cache.put(tableHandle, stats);
        }
        return stats;
    }
}
