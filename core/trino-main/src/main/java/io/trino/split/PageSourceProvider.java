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
package io.trino.split;

import io.trino.Session;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;

import java.util.List;
import java.util.Optional;

public interface PageSourceProvider
{
    ConnectorPageSource createPageSource(
            Session session,
            Split split,
            TableHandle table,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            MemoryContext memoryContext);

    // TODO (https://github.com/trinodb/trino/issues/29955) replace with MemoryContext
    default long getMemoryUsage()
    {
        return 0;
    }

    /**
     * Creates a tracker for reporting {@link #getMemoryUsage()} into an operator-local memory context.
     * A provider instance is shared by all drivers of a pipeline, and each driver polls the provider
     * memory usage into its own memory context. Trackers coordinate so that shared provider state
     * (e.g. loaded Iceberg equality delete filters) is reported by at most one live tracker at a time
     * instead of being counted once per driver. When the reporting tracker is closed, the role passes
     * to the next tracker that polls.
     */
    default MemoryUsageTracker createMemoryUsageTracker()
    {
        return this::getMemoryUsage;
    }

    interface MemoryUsageTracker
            extends AutoCloseable
    {
        long getMemoryUsage();

        @Override
        default void close() {}
    }
}
