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
package io.trino.sql.planner;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * InternalDynamicFilter is a DynamicFilter that is used internally by the engine.
 * This provides bloom filters through DynamicFilterTupleDomain for usage in dynamic row filtering
 * without exposing the bloom filter to connectors.
 */
public interface InternalDynamicFilter
        extends DynamicFilter
{
    InternalDynamicFilter EMPTY = new InternalDynamicFilter()
    {
        @Override
        public Set<ColumnHandle> getColumnsCovered()
        {
            return Set.of();
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return NOT_BLOCKED;
        }

        @Override
        public boolean isComplete()
        {
            return true;
        }

        @Override
        public boolean isAwaitable()
        {
            return false;
        }

        @Override
        public TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return TupleDomain.all();  // no filtering
        }

        @Override
        public DynamicFilterTupleDomain<ColumnHandle> getCurrentDynamicFilterTupleDomain()
        {
            return DynamicFilterTupleDomain.all();
        }
    };

    // Bloom filter is used to evaluate the dynamic filter in the engine and not exposed to connector
    DynamicFilterTupleDomain<ColumnHandle> getCurrentDynamicFilterTupleDomain();
}
