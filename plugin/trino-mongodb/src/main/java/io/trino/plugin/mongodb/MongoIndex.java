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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.mongodb.client.ListIndexesIterable;
import io.trino.spi.connector.SortOrder;
import org.bson.Document;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public record MongoIndex(List<MongodbIndexKey> keys)
{
    public MongoIndex
    {
        keys = ImmutableList.copyOf(keys);
    }

    public static List<MongoIndex> parse(ListIndexesIterable<Document> indexes)
    {
        ImmutableList.Builder<MongoIndex> builder = ImmutableList.builder();
        for (Document index : indexes) {
            // TODO: v, ns, sparse fields
            Document key = (Document) index.get("key");

            if (key.containsKey("_fts")) { // Full Text Search
                continue;
            }
            builder.add(new MongoIndex(parseKey(key)));
        }

        return builder.build();
    }

    private static List<MongodbIndexKey> parseKey(Document key)
    {
        ImmutableList.Builder<MongodbIndexKey> builder = ImmutableList.builder();

        for (String name : key.keySet()) {
            Object value = key.get(name);
            if (value instanceof Number) {
                int order = ((Number) value).intValue();
                checkState(order == 1 || order == -1, "Unknown index sort order");
                SortOrder sortOrder = order == 1 ? SortOrder.ASC_NULLS_LAST : SortOrder.DESC_NULLS_LAST;
                builder.add(new MongodbIndexKey(name, Optional.of(sortOrder)));
            }
            else if (value instanceof String) {
                builder.add(new MongodbIndexKey(name, Optional.empty()));
            }
            else {
                throw new UnsupportedOperationException("Unknown index type: " + value.toString());
            }
        }

        return builder.build();
    }

    public record MongodbIndexKey(String name, Optional<SortOrder> sortOrder)
    {
        public MongodbIndexKey
        {
            requireNonNull(name, "name is null");
            requireNonNull(sortOrder, "sortOrder is null");
        }
    }
}
