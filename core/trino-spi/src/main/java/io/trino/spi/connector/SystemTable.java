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

import io.trino.spi.predicate.TupleDomain;

import java.util.Optional;
import java.util.Set;

/**
 * Exactly one of {@link #cursor} or {@link #pageSource} must be implemented.
 * <p>
 * If {@link #splitSource} is implemented, the {@link Connector}'s {@link ConnectorPageSourceProvider} must handle the {@link ConnectorSplit}s it generates.
 */
public interface SystemTable
{
    enum Distribution
    {
        ALL_NODES, ALL_COORDINATORS, SINGLE_COORDINATOR
    }

    Distribution getDistribution();

    ConnectorTableMetadata getTableMetadata();

    /**
     * Create a cursor for the data in this table.
     *
     * @param session the session to use for creating the data
     * @param constraint the constraints for the table columns (indexed from 0)
     */
    default RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        throw new UnsupportedOperationException();
    }

    default RecordCursor cursor(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            TupleDomain<Integer> constraint,
            Set<Integer> requiredColumns,
            ConnectorSplit split)
    {
        return cursor(transactionHandle, session, constraint);
    }

    default RecordCursor cursor(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            TupleDomain<Integer> constraint,
            Set<Integer> requiredColumns,
            ConnectorSplit split,
            ConnectorAccessControl accessControl)
    {
        return cursor(transactionHandle, session, constraint, requiredColumns, split);
    }

    /**
     * Create a page source for the data in this table.
     *
     * @param session the session to use for creating the data
     * @param constraint the constraints for the table columns (indexed from 0)
     */
    default ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        throw new UnsupportedOperationException();
    }

    default ConnectorPageSource pageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            TupleDomain<Integer> constraint,
            ConnectorAccessControl accessControl)
    {
        return pageSource(transactionHandle, session, constraint);
    }

    /**
     * Try and create a {@link ConnectorSplitSource} for the {@link SystemTable}.
     * <p>
     * Implementing this method in a plugin context causes {@link SystemTable#getDistribution()} to have no impact on the actual distribution of splits.
     * The accompanying {@link Connector}'s {@link ConnectorPageSourceProvider} must handle the {@link ConnectorSplit}s.
     *
     * @param connectorSession the session to use for creating the data
     * @param constraint the constraints for the table columns (indexed from 0)
     * @return an optional {@link ConnectorSplitSource}
     */
    default Optional<ConnectorSplitSource> splitSource(ConnectorSession connectorSession, TupleDomain<ColumnHandle> constraint)
    {
        return Optional.empty();
    }
}
