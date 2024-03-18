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
package io.trino.plugin.varada.dispatcher;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;

import java.util.Collections;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class CompletedDynamicFilter
        implements DynamicFilter
{
    private static final CompletableFuture<?> isBlocked = CompletableFuture.completedFuture(null);

    private final TupleDomain<ColumnHandle> predicate;

    public CompletedDynamicFilter(TupleDomain<ColumnHandle> predicate)
    {
        this.predicate = requireNonNull(predicate);
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return isBlocked;
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
        return predicate;
    }

    @Override
    public OptionalLong getPreferredDynamicFilterTimeout()
    {
        return OptionalLong.of(0);
    }

    @Override
    public Set<ColumnHandle> getColumnsCovered()
    {
        return Collections.emptySet();
    }
}
