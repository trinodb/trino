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
package io.trino.metastore;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.SortOrder;

import static io.trino.metastore.MetastoreErrorCode.HIVE_INVALID_METADATA;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static java.util.Objects.requireNonNull;

public record SortingColumn(String columnName, SortingColumn.Order order)
{
    public enum Order
    {
        ASCENDING(ASC_NULLS_FIRST, 1),
        DESCENDING(DESC_NULLS_LAST, 0);

        private final SortOrder sortOrder;
        private final int hiveOrder;

        Order(SortOrder sortOrder, int hiveOrder)
        {
            this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
            this.hiveOrder = hiveOrder;
        }

        public SortOrder getSortOrder()
        {
            return sortOrder;
        }

        public int getHiveOrder()
        {
            return hiveOrder;
        }

        public static Order fromMetastoreApiOrder(int value, String tablePartitionName)
        {
            for (Order order : values()) {
                if (value == order.getHiveOrder()) {
                    return order;
                }
            }
            throw new TrinoException(HIVE_INVALID_METADATA, "Table/partition metadata has invalid sorting order: " + tablePartitionName);
        }
    }

    public SortingColumn
    {
        requireNonNull(columnName, "columnName is null");
        requireNonNull(order, "order is null");
    }
}
