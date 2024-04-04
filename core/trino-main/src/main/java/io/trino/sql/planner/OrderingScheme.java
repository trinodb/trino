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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.SortingProperty;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public record OrderingScheme(
        List<Symbol> orderBy,
        Map<Symbol, SortOrder> orderings)
{
    public OrderingScheme
    {
        requireNonNull(orderBy, "orderBy is null");
        requireNonNull(orderings, "orderings is null");
        checkArgument(!orderBy.isEmpty(), "orderBy is empty");
        checkArgument(orderings.keySet().equals(ImmutableSet.copyOf(orderBy)), "orderBy keys and orderings don't match");
        orderBy = ImmutableList.copyOf(orderBy);
        orderings = ImmutableMap.copyOf(orderings);
    }

    public List<SortOrder> orderingList()
    {
        return orderBy.stream()
                .map(orderings::get)
                .collect(toImmutableList());
    }

    public SortOrder ordering(Symbol symbol)
    {
        checkArgument(orderings.containsKey(symbol), "No ordering for symbol: %s", symbol);
        return orderings.get(symbol);
    }

    public List<SortItem> toSortItems()
    {
        return orderBy().stream()
                .map(symbol -> new SortItem(
                        symbol.name(),
                        SortOrder.valueOf(ordering(symbol).name())))
                .collect(toImmutableList());
    }

    public List<LocalProperty<Symbol>> toLocalProperties()
    {
        return orderBy().stream()
                .map(symbol -> new SortingProperty<>(symbol, ordering(symbol)))
                .collect(toImmutableList());
    }
}
