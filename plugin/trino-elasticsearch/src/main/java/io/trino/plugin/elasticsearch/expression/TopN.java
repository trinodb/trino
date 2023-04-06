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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.SortOrder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TopN
{
    public static final long NO_LIMIT = -1;
    public static final TopN EMPTY = new TopN(NO_LIMIT, new ArrayList<>());

    private final long limit;
    private final List<TopNSortItem> topNSortItems;

    public static TopN fromLimit(long limit)
    {
        return new TopN(limit, new ArrayList<>());
    }

    @JsonCreator
    public TopN(@JsonProperty("limit") long limit,
            @JsonProperty("topNSortItems") List<TopNSortItem> topNSortItems)
    {
        this.limit = limit;
        this.topNSortItems = requireNonNull(topNSortItems, "topNSortItems is null");
    }

    @JsonProperty("limit")
    public long getLimit()
    {
        return limit;
    }

    @JsonProperty("topNSortItems")
    public List<TopNSortItem> getTopNSortItems()
    {
        return topNSortItems;
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopN that = (TopN) o;
        return limit == that.limit &&
                topNSortItems.equals(that.topNSortItems);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(limit, topNSortItems);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("limit", limit)
                .add("items", topNSortItems)
                .toString();
    }

    public static class TopNSortItem
    {
        // sorting by _doc (index order) get special treatment in Elasticsearch and is more efficient
        public static final TopNSortItem DEFAULT_SORT_BY_DOC = sortBy("_doc");
        private final String field;
        private final SortOrder order;

        public static TopNSortItem sortBy(String field)
        {
            return new TopNSortItem(field, SortOrder.ASC_NULLS_LAST);
        }

        public static TopNSortItem sortBy(String field, SortOrder order)
        {
            return new TopNSortItem(field, order);
        }

        @JsonCreator
        public TopNSortItem(@JsonProperty("field") String field, @JsonProperty("order") SortOrder order)
        {
            this.field = field;
            this.order = order;
        }

        @JsonProperty("field")
        public String getField()
        {
            return field;
        }

        @JsonProperty("order")
        public SortOrder getOrder()
        {
            return order;
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

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TopNSortItem that = (TopNSortItem) o;
            return field.equals(that.field) && order == that.order;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(field, order);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("field", field)
                    .add("order", order)
                    .toString();
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
