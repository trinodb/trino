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
package io.trino.plugin.elasticsearch.expression;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.SortOrder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record TopN(long limit, List<TopNSortItem> topNSortItems)
{
    public static final long NO_LIMIT = -1;
    public static final TopN EMPTY = fromLimit(NO_LIMIT);

    public static TopN fromLimit(long limit)
    {
        return new TopN(limit, ImmutableList.of());
    }

    public TopN
    {
        requireNonNull(topNSortItems, "topNSortItems is null");
    }

    public TopN addSortItem(TopNSortItem sortItem)
    {
        topNSortItems.add(sortItem);
        return this;
    }

    public boolean isOnlyLimit()
    {
        return limit != NO_LIMIT && topNSortItems.isEmpty();
    }

    public record TopNSortItem(String field, SortOrder order)
    {
        // sorting by _doc (index order) get special treatment in Elasticsearch and is more efficient
        public static final TopNSortItem DEFAULT_SORT_BY_DOC = sortBy("_doc");

        public static TopNSortItem sortBy(String field)
        {
            return new TopNSortItem(field, SortOrder.ASC_NULLS_LAST);
        }

        public static TopNSortItem sortBy(String field, SortOrder order)
        {
            return new TopNSortItem(field, order);
        }

        public SortBuilder<? extends SortBuilder<?>> toSortBuilder()
        {
            if (order.isNullsFirst()) {
                return SortBuilders
                        .fieldSort(field)
                        .order(toEsSortOrder(order))
                        .missing("_first");
            }
            // The missing parameter's default value is _last.
            return SortBuilders
                    .fieldSort(field)
                    .order(toEsSortOrder(order));
        }

        private static org.elasticsearch.search.sort.SortOrder toEsSortOrder(SortOrder sortOrder)
        {
            if (sortOrder.isAscending()) {
                return org.elasticsearch.search.sort.SortOrder.ASC;
            }
            return org.elasticsearch.search.sort.SortOrder.DESC;
        }
    }
}
