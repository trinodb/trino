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
package io.prestosql.spi.connector;

import io.prestosql.spi.predicate.TupleDomain;

import java.util.List;

public interface ConnectorPageSourceProvider
{
    /**
     * @param columns columns that should show up in the output page, in this order
     * @deprecated Use {@link #createPageSource(ConnectorTransactionHandle, ConnectorSession, ConnectorSplit, ConnectorTableHandle, List, DynamicFilter)}
     */
    @Deprecated
    default ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns)
    {
        throw new UnsupportedOperationException("createPageSource() must be implemented");
    }

    /**
     * @param columns columns that should show up in the output page, in this order
     * @param dynamicFilter optionally remove rows that don't satisfy this predicate
     * @deprecated Use {@link #createPageSource(ConnectorTransactionHandle, ConnectorSession, ConnectorSplit, ConnectorTableHandle, List, DynamicFilter)}
     */
    @Deprecated
    default ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            TupleDomain<ColumnHandle> dynamicFilter)
    {
        // By default, ignore dynamic filter (as it is an optimization and doesn't affect correctness).
        return createPageSource(transaction, session, split, table, columns);
    }

    /**
     * @param columns columns that should show up in the output page, in this order
     * @param dynamicFilter optionally remove rows that don't satisfy this predicate
     */
    default ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        // By default, poll dynamic filtering without blocking for collection to complete.
        return createPageSource(transaction, session, split, table, columns, dynamicFilter.getCurrentPredicate());
    }
}
