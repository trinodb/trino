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

import io.trino.spi.Experimental;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import java.util.Optional;

public interface ConnectorSplitManager
{
    default ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        throw new UnsupportedOperationException();
    }

    default ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            boolean preferDeterministicSplits,
            Constraint constraint)
    {
        return getSplits(transaction, session, table, dynamicFilter, constraint);
    }

    @Experimental(eta = "2023-07-31")
    default ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableFunctionHandle function)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a split identifier for the purpose of caching with {@link CacheManager}.
     * {@link CacheSplitId} together with {@link CacheTableId} and {@link CacheColumnId}s
     * represents rows produced by {@link ConnectorPageSource} for a given split.
     */
    default Optional<CacheSplitId> getCacheSplitId(ConnectorSplit split)
    {
        return Optional.empty();
    }
}
