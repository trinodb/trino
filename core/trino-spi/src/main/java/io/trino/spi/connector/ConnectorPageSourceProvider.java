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

import java.util.List;

public interface ConnectorPageSourceProvider
{
    /**
     * @param columns columns that should show up in the output page, in this order
     * @param dynamicFilter optionally remove rows that don't satisfy this predicate
     */
    ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter);

    /**
     * @param splitAddressEnforced true iff the split executed on its preferred node (from its {@link ConnectorSplit#getAddresses()}  result).
     */
    default ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            boolean splitAddressEnforced)
    {
        return createPageSource(
                transaction,
                session,
                split,
                table,
                columns,
                dynamicFilter);
    }

    /**
     * Returns unenforced (effective) predicate that {@link ConnectorPageSource} would use to filter split data.
     * If split is completely filtered out, then this method should return {@link TupleDomain#none}.
     */
    default TupleDomain<ColumnHandle> getUnenforcedPredicate(
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            TupleDomain<ColumnHandle> dynamicFilter)
    {
        return simplifyPredicate(session, split, table, dynamicFilter);
    }

    // for warp compatibility
    @Deprecated
    default TupleDomain<ColumnHandle> simplifyPredicate(
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            TupleDomain<ColumnHandle> predicate)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Prunes columns from predicate that are not effective in filtering split data.
     * If split is completely filtered out by given predicate, then this
     * method must return {@link TupleDomain#none}.
     */
    default TupleDomain<ColumnHandle> prunePredicate(
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            TupleDomain<ColumnHandle> predicate)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns whether the engine should perform dynamic row filtering on top of the returned page source.
     * While dynamic row filtering can be extended to any connector, it is currently restricted to data lake connectors.
     */
    default boolean shouldPerformDynamicRowFiltering()
    {
        return false;
    }
}
