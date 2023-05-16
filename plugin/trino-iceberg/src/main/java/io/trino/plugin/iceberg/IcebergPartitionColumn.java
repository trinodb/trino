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

import io.trino.spi.type.RowType;

import java.util.List;
import java.util.Objects;

public class IcebergPartitionColumn
{
    private final RowType rowType;
    private final List<Integer> fieldIds;

    public IcebergPartitionColumn(RowType rowType, List<Integer> fieldIds)
    {
        this.rowType = rowType;
        this.fieldIds = fieldIds;
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
        IcebergPartitionColumn that = (IcebergPartitionColumn) o;
        return Objects.equals(rowType, that.rowType()) && Objects.equals(fieldIds, that.fieldIds());
    }

    public RowType rowType()
    {
        return rowType;
    }

    public List<Integer> fieldIds()
    {
        return fieldIds;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rowType, fieldIds);
    }
}
