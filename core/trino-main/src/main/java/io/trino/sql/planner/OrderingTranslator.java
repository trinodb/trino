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
import io.trino.spi.connector.SortOrder;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.SortItem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class OrderingTranslator
{
    private OrderingTranslator() {}

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
        if (sortItem.getOrdering() == SortItem.Ordering.ASCENDING) {
            if (sortItem.getNullOrdering() == SortItem.NullOrdering.FIRST) {
                return SortOrder.ASC_NULLS_FIRST;
            }
            return SortOrder.ASC_NULLS_LAST;
        }

        if (sortItem.getNullOrdering() == SortItem.NullOrdering.FIRST) {
            return SortOrder.DESC_NULLS_FIRST;
        }
        return SortOrder.DESC_NULLS_LAST;
    }
}
