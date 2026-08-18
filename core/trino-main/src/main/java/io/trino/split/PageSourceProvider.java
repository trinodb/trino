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

    /**
     * Registers a memory context as a candidate target for reporting usage of memory owned by this
     * page source provider and shared across all its page sources (e.g. loaded Iceberg equality
     * delete filters). A provider instance is shared by all drivers of a pipeline, so the shared
     * usage is reported into at most one of the tracked contexts at a time, instead of being
     * counted once per driver. A tracked context claims the reporting role when it first calls
     * {@link #updateMemoryUsage(MemoryContext)} while there is no active reporter.
     */
    default void trackMemoryUsage(MemoryContext memoryContext) {}

    /**
     * Stops reporting into a context previously registered with {@link #trackMemoryUsage} and
     * resets it to zero. If the context was the current reporting target, another tracked context
     * takes over on a subsequent {@link #updateMemoryUsage(MemoryContext)}. Must be called before the
     * underlying memory context is released.
     */
    default void untrackMemoryUsage(MemoryContext memoryContext) {}

    /**
     * Reports the current shared memory usage of this provider into the supplied context when it
     * is the active reporting context, or claims the reporting role for it when there is no active
     * reporter. Calls for other tracked contexts do not poll or report the shared usage.
     */
    default void updateMemoryUsage(MemoryContext memoryContext) {}
}
