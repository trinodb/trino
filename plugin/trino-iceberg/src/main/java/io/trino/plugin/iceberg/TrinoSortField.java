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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.SortOrder;
import org.apache.iceberg.SortField;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TrinoSortField
{
    private final int sourceColumnId;
    private final SortOrder sortOrder;

    @JsonCreator
    public TrinoSortField(@JsonProperty("sourceColumnId") int sourceColumnId, @JsonProperty("sortOrder") SortOrder sortOrder)
    {
        this.sourceColumnId = sourceColumnId;
        this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
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

    @JsonProperty
    public int getSourceColumnId()
    {
        return sourceColumnId;
    }

    @JsonProperty
    public SortOrder getSortOrder()
    {
        return sortOrder;
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
        TrinoSortField that = (TrinoSortField) o;
        return sourceColumnId == that.sourceColumnId && sortOrder == that.sortOrder;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sourceColumnId, sortOrder);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sourceColumnId", sourceColumnId)
                .add("sortOrder", sortOrder)
                .toString();
    }
}
