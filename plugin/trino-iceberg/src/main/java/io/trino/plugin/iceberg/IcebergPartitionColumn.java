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

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.RowType;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Row type spanning the partition fields of all partition specs of a table, along with the partition field id of each row field.
 */
public record IcebergPartitionColumn(RowType rowType, List<Integer> fieldIds)
{
    public IcebergPartitionColumn
    {
        requireNonNull(rowType, "rowType is null");
        fieldIds = ImmutableList.copyOf(requireNonNull(fieldIds, "fieldIds is null"));
        checkArgument(rowType.getFields().size() == fieldIds.size(), "rowType has %s fields but %s field ids were given", rowType.getFields().size(), fieldIds.size());
    }

    public static IcebergPartitionColumn fromColumnHandle(IcebergColumnHandle column)
    {
        checkArgument(column.isPartitionColumn(), "Not a partition column: %s", column);
        List<Integer> fieldIds = column.getBaseColumnIdentity().getChildren().stream()
                .map(ColumnIdentity::getId)
                .collect(toImmutableList());
        return new IcebergPartitionColumn((RowType) column.getType(), fieldIds);
    }
}
