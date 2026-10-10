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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.SortOrder;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record TopN(long limit, List<TopNSortItem> topNSortItems)
{
    public static final long NO_LIMIT = -1;

    public static TopN fromLimit(long limit)
    {
        return new TopN(limit, ImmutableList.of());
    }

    public TopN
    {
        requireNonNull(topNSortItems, "topNSortItems is null");
    }

    public boolean isOnlyLimit()
    {
        return limit != NO_LIMIT && topNSortItems.isEmpty();
    }

    public record TopNSortItem(String field, SortOrder order)
    {
        // sorting by _doc (index order) get special treatment in Elasticsearch and is more efficient
        public static final TopNSortItem DEFAULT_SORT_BY_DOC = sortBy("_doc");

        public TopNSortItem
        {
            requireNonNull(field, "field is null");
            requireNonNull(order, "order is null");
        }

        public static TopNSortItem sortBy(String field)
        {
            return new TopNSortItem(field, SortOrder.ASC_NULLS_LAST);
        }

        public static TopNSortItem sortBy(String field, SortOrder order)
        {
            return new TopNSortItem(field, order);
        }

        public ObjectNode toSortQuery()
        {
            ObjectNode sort = JsonNodeFactory.instance.objectNode()
                    .put("order", order.isAscending() ? "asc" : "desc");
            if (order.isNullsFirst()) {
                sort.put("missing", "_first");
            }
            return JsonNodeFactory.instance.objectNode().set(field, sort);
        }
    }
}
