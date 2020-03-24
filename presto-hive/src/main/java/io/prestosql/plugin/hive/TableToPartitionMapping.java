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
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.openjdk.jol.info.ClassLayout;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static java.util.Objects.requireNonNull;

public class TableToPartitionMapping
{
    public static TableToPartitionMapping empty()
    {
        return new TableToPartitionMapping(ImmutableMap.of());
    }

    // Overhead of ImmutableMap is not accounted because of its complexity.
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TableToPartitionMapping.class).instanceSize();
    private static final int INTEGER_INSTANCE_SIZE = ClassLayout.parseClass(Integer.class).instanceSize();

    private final Map<Integer, HiveTypeName> columnCoercions;

    @JsonCreator
    public TableToPartitionMapping(
            @JsonProperty("columnCoercions") Map<Integer, HiveTypeName> columnCoercions)
    {
        this.columnCoercions = ImmutableMap.copyOf(requireNonNull(columnCoercions, "columnCoercions is null"));
    }

    @JsonProperty
    public Map<Integer, HiveTypeName> getColumnCoercions()
    {
        return columnCoercions;
    }

    public Optional<HiveType> getCoercion(int columnIndex)
    {
        return Optional.ofNullable(columnCoercions.get(columnIndex))
                .map(HiveTypeName::toHiveType);
    }

    public int getEstimatedSizeInBytes()
    {
        int result = INSTANCE_SIZE;
        result += sizeOfObjectArray(columnCoercions.size());
        for (HiveTypeName hiveTypeName : columnCoercions.values()) {
            result += INTEGER_INSTANCE_SIZE + hiveTypeName.getEstimatedSizeInBytes();
        }
        return result;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnCoercions", columnCoercions)
                .toString();
    }
}
