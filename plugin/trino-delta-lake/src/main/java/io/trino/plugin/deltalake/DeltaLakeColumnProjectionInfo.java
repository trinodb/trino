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
package io.trino.plugin.deltalake;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.deltalake.DeltaHiveTypeTranslator.toHiveType;
import static java.util.Objects.requireNonNull;

public record DeltaLakeColumnProjectionInfo(Type type, List<Integer> dereferenceIndices, List<String> dereferencePhysicalNames)
{
    private static final int INSTANCE_SIZE = instanceSize(DeltaLakeColumnProjectionInfo.class);

    public DeltaLakeColumnProjectionInfo
    {
        requireNonNull(type, "type is null");
        requireNonNull(dereferenceIndices, "dereferenceIndices is null");
        requireNonNull(dereferencePhysicalNames, "dereferencePhysicalNames is null");
        checkArgument(!dereferenceIndices.isEmpty(), "dereferenceIndices should not be empty");
        checkArgument(!dereferencePhysicalNames.isEmpty(), "dereferencePhysicalNames should not be empty");
        checkArgument(dereferenceIndices.size() == dereferencePhysicalNames.size(), "dereferenceIndices and dereferencePhysicalNames should have the same sizes");
        dereferenceIndices = ImmutableList.copyOf(dereferenceIndices);
        dereferencePhysicalNames = ImmutableList.copyOf(dereferencePhysicalNames);
    }

    @JsonIgnore
    public String partialName()
    {
        return String.join("#", dereferencePhysicalNames);
    }

    @JsonIgnore
    public long getRetainedSizeInBytes()
    {
        // type is not accounted for as the instances are cached (by TypeRegistry) and shared
        return INSTANCE_SIZE
                + estimatedSizeOf(dereferenceIndices, SizeOf::sizeOf)
                + estimatedSizeOf(dereferencePhysicalNames, SizeOf::estimatedSizeOf);
    }

    public HiveColumnProjectionInfo toHiveColumnProjectionInfo()
    {
        return new HiveColumnProjectionInfo(dereferenceIndices, dereferencePhysicalNames, toHiveType(type), type);
    }

    @Override
    public String toString()
    {
        return partialName() + ":" + type.getDisplayName();
    }
}
