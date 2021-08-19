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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.SortingProperty;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.SortItem.NullOrdering;
import io.trino.sql.tree.SortItem.Ordering;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class OrderingScheme
{
    private final List<Symbol> orderBy;
    private final Map<Symbol, SortOrder> orderings;

    @JsonCreator
    public OrderingScheme(@JsonProperty("orderBy") List<Symbol> orderBy, @JsonProperty("orderings") Map<Symbol, SortOrder> orderings)
    {
        requireNonNull(orderBy, "orderBy is null");
        requireNonNull(orderings, "orderings is null");
        checkArgument(!orderBy.isEmpty(), "orderBy is empty");
        checkArgument(orderings.keySet().equals(ImmutableSet.copyOf(orderBy)), "orderBy keys and orderings don't match");
        this.orderBy = ImmutableList.copyOf(orderBy);
        this.orderings = ImmutableMap.copyOf(orderings);
    }

    @JsonProperty
    public List<Symbol> getOrderBy()
    {
        return orderBy;
    }

    @JsonProperty
    public Map<Symbol, SortOrder> getOrderings()
    {
        return orderings;
    }

    public List<SortOrder> getOrderingList()
    {
        return orderBy.stream()
                .map(orderings::get)
                .collect(toImmutableList());
    }

    public SortOrder getOrdering(Symbol symbol)
    {
        checkArgument(orderings.containsKey(symbol), "No ordering for symbol: %s", symbol);
        return orderings.get(symbol);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OrderingScheme that = (OrderingScheme) o;
        return Objects.equals(orderBy, that.orderBy) &&
                Objects.equals(orderings, that.orderings);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(orderBy, orderings);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("orderBy", orderBy)
                .add("orderings", orderings)
                .toString();
    }

    public static OrderingScheme fromOrderBy(OrderBy orderBy)
    {
        List<Symbol> orderBySymbols = orderBy.getSortItems().stream()
                .map(SortItem::getSortKey)
                .map(Symbol::from)
                .collect(toImmutableList());

        ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();
        Map<Symbol, SortOrder> orders = new HashMap<>();
        for (int i = 0; i < orderBySymbols.size(); i++) {
            Symbol symbol = orderBySymbols.get(i);
            // for multiple sort items based on the same expression, retain the first one:
            // ORDER BY x DESC, x ASC, y --> ORDER BY x DESC, y
            if (!orders.containsKey(symbol)) {
                symbols.add(symbol);
                orders.put(symbol, sortItemToSortOrder(orderBy.getSortItems().get(i)));
            }
        }

        return new OrderingScheme(symbols.build(), orders);
    }

    public static SortOrder sortItemToSortOrder(SortItem sortItem)
    {
        if (sortItem.getOrdering() == Ordering.ASCENDING) {
            if (sortItem.getNullOrdering() == NullOrdering.FIRST) {
                return SortOrder.ASC_NULLS_FIRST;
            }
            return SortOrder.ASC_NULLS_LAST;
        }

        if (sortItem.getNullOrdering() == NullOrdering.FIRST) {
            return SortOrder.DESC_NULLS_FIRST;
        }
        return SortOrder.DESC_NULLS_LAST;
    }

    public List<io.trino.spi.connector.SortItem> toSortItems()
    {
        return getOrderBy().stream()
                .map(symbol -> new io.trino.spi.connector.SortItem(
                        symbol.getName(),
                        io.trino.spi.connector.SortOrder.valueOf(getOrdering(symbol).name())))
                .collect(toImmutableList());
    }

    public List<LocalProperty<Symbol>> toLocalProperties()
    {
        return getOrderBy().stream()
                .map(symbol -> new SortingProperty<>(symbol, getOrdering(symbol)))
                .collect(toImmutableList());
    }
}
