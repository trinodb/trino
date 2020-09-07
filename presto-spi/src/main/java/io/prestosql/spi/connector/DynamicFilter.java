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

import java.util.concurrent.CompletableFuture;

public interface DynamicFilter
{
    CompletableFuture<?> NOT_BLOCKED = CompletableFuture.completedFuture(null);

    DynamicFilter EMPTY = new DynamicFilter()
    {
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
        public TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return TupleDomain.all();  // no filtering
        }
    };

    /**
     * Block until dynamic filter is narrowed down.
     * Dynamic filter might be narrowed down multiple times during query runtime.
     */
    CompletableFuture<?> isBlocked();

    /**
     * Returns true it dynamic filter cannot be narrowed more.
     */
    boolean isComplete();

    TupleDomain<ColumnHandle> getCurrentPredicate();
}
