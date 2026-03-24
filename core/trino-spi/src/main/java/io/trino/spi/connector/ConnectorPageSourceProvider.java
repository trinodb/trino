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
package io.trino.spi.connector;

import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public interface ConnectorPageSourceProvider
{
    default ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        return createPageSource(transaction, session, split, table, columns, dynamicFilter);
    }

    /**
     * @param columns columns that should show up in the output page, in this order
     * @param dynamicFilter optionally remove rows that don't satisfy this predicate
     *
     * @deprecated Implement {@link #createPageSource(ConnectorTransactionHandle, ConnectorSession, ConnectorSplit, ConnectorTableHandle, Optional, List, DynamicFilter)} instead.
     */
    @Deprecated(forRemoval = true)
    default ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support reading tables");
    }

    /**
     * Get the total memory that needs to be reserved in the memory pool.
     * This should include any memory used in the page source provider that is shared across all page sources created by this provider.
     *
     * @return the memory used so far in table read
     */
    default long getMemoryUsage()
    {
        return 0;
    }
}
