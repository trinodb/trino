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
package io.trino.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class TableToPartitionMapping
{
    public static TableToPartitionMapping empty()
    {
        return new TableToPartitionMapping(Optional.empty(), ImmutableMap.of());
    }

    public static TableToPartitionMapping mapColumnsByIndex(Map<Integer, HiveTypeName> columnCoercions)
    {
        return new TableToPartitionMapping(Optional.empty(), columnCoercions);
    }

    // Overhead of ImmutableMap is not accounted because of its complexity.
    private static final int INSTANCE_SIZE = instanceSize(TableToPartitionMapping.class);
    private static final int INTEGER_INSTANCE_SIZE = instanceSize(Integer.class);
    private static final int OPTIONAL_INSTANCE_SIZE = instanceSize(Optional.class);

    private final Optional<Map<Integer, Integer>> tableToPartitionColumns;
    private final Map<Integer, HiveTypeName> partitionColumnCoercions;

    @JsonCreator
    public TableToPartitionMapping(
            @JsonProperty("tableToPartitionColumns") Optional<Map<Integer, Integer>> tableToPartitionColumns,
            @JsonProperty("partitionColumnCoercions") Map<Integer, HiveTypeName> partitionColumnCoercions)
    {
        if (tableToPartitionColumns.map(TableToPartitionMapping::isIdentityMapping).orElse(true)) {
            this.tableToPartitionColumns = Optional.empty();
        }
        else {
            this.tableToPartitionColumns = tableToPartitionColumns.map(ImmutableMap::copyOf);
        }
        this.partitionColumnCoercions = ImmutableMap.copyOf(requireNonNull(partitionColumnCoercions, "partitionColumnCoercions is null"));
    }

    @VisibleForTesting
    static boolean isIdentityMapping(Map<Integer, Integer> map)
    {
        for (int i = 0; i < map.size(); i++) {
            if (!Objects.equals(map.get(i), i)) {
                return false;
            }
        }
        return true;
    }

    @JsonProperty
    public Map<Integer, HiveTypeName> getPartitionColumnCoercions()
    {
        return partitionColumnCoercions;
    }

    @JsonProperty
    public Optional<Map<Integer, Integer>> getTableToPartitionColumns()
    {
        return tableToPartitionColumns;
    }

    public Optional<HiveType> getCoercion(int tableColumnIndex)
    {
        return getPartitionColumnIndex(tableColumnIndex)
                .flatMap(partitionColumnIndex -> Optional.ofNullable(partitionColumnCoercions.get(partitionColumnIndex)))
                .map(HiveTypeName::toHiveType);
    }

    private Optional<Integer> getPartitionColumnIndex(int tableColumnIndex)
    {
        if (tableToPartitionColumns.isEmpty()) {
            return Optional.of(tableColumnIndex);
        }
        return Optional.ofNullable(tableToPartitionColumns.get().get(tableColumnIndex));
    }

    public int getEstimatedSizeInBytes()
    {
        long result = INSTANCE_SIZE +
                estimatedSizeOf(partitionColumnCoercions, (Integer key) -> INTEGER_INSTANCE_SIZE, HiveTypeName::getEstimatedSizeInBytes) +
                OPTIONAL_INSTANCE_SIZE +
                tableToPartitionColumns
                        .map(tableToPartitionColumns -> estimatedSizeOf(tableToPartitionColumns, (Integer key) -> INTEGER_INSTANCE_SIZE, (Integer value) -> INTEGER_INSTANCE_SIZE))
                        .orElse(0L);
        return toIntExact(result);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnCoercions", partitionColumnCoercions)
                .add("tableToPartitionColumns", tableToPartitionColumns)
                .toString();
    }
}
