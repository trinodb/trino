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
package io.trino.plugin.iceberg;

import io.trino.spi.connector.SortOrder;
import org.apache.iceberg.SortField;

import static java.util.Objects.requireNonNull;

public record TrinoSortField(int sourceColumnId, SortOrder sortOrder)
{
    public TrinoSortField
    {
        requireNonNull(sortOrder, "sortOrder is null");
    }

    public static TrinoSortField fromIceberg(SortField sortField)
    {
        SortOrder sortOrder = switch (sortField.direction()) {
            case ASC -> switch (sortField.nullOrder()) {
                case NULLS_FIRST -> SortOrder.ASC_NULLS_FIRST;
                case NULLS_LAST -> SortOrder.ASC_NULLS_LAST;
            };
            case DESC -> switch (sortField.nullOrder()) {
                case NULLS_FIRST -> SortOrder.DESC_NULLS_FIRST;
                case NULLS_LAST -> SortOrder.DESC_NULLS_LAST;
            };
        };

        return new TrinoSortField(sortField.sourceId(), sortOrder);
    }
}
