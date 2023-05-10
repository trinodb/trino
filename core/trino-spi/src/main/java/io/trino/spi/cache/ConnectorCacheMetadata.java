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
package io.trino.spi.cache;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.Optional;

public interface ConnectorCacheMetadata
{
    /**
     * Returns a table identifier for the purpose of caching with {@link CacheManager}.
     * {@link CacheTableId} together with {@link CacheSplitId} and {@link CacheColumnId}s represents
     * rows produced by {@link ConnectorPageSource} for a given split. Local table properties
     * (e.g. rows order) must be part of {@link CacheTableId} if they are present. List of selected
     * columns should not be part of {@link CacheTableId}. {@link CacheTableId} should not contain
     * elements that can be derived from {@link CacheSplitId} such as predicate on partition column
     * which can filter splits entirely.
     */
    Optional<CacheTableId> getCacheTableId(ConnectorTableHandle tableHandle);

    /**
     * Returns a column identifier for the purpose of caching with {@link CacheManager}.
     * {@link CacheTableId} together with {@link CacheSplitId} and {@link CacheColumnId}s represents
     * rows produced by {@link ConnectorPageSource} for a given split. {@link CacheColumnId} can represent
     * simple, base column or more complex reference (e.g. map or array dereference expressions).
     */
    Optional<CacheColumnId> getCacheColumnId(ConnectorTableHandle tableHandle, ColumnHandle columnHandle);

    /**
     * Returns a canonical {@link ConnectorTableHandle}.
     * If any property of {@link ConnectorTableHandle} affects final query result when underlying table
     * is queried, then such property is considered canonical. Otherwise, the property is non-canonical.
     * Canonical {@link ConnectorTableHandle}s allow to match more similar subqueries that
     * are eligible for caching with {@link CacheManager}. Connector should convert provided
     * {@link ConnectorTableHandle} into canonical one by pruning of every non-canonical field.
     */
    ConnectorTableHandle getCanonicalTableHandle(ConnectorTableHandle handle);
}
