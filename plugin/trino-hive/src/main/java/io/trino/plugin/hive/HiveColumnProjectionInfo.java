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
import io.airlift.slice.SizeOf;
import io.trino.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.util.Objects.requireNonNull;

public class HiveColumnProjectionInfo
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(HiveColumnProjectionInfo.class).instanceSize();

    private final List<Integer> dereferenceIndices;
    private final List<String> dereferenceNames;
    private final HiveType hiveType;
    private final Type type;
    private final String partialName;

    @JsonCreator
    public HiveColumnProjectionInfo(
            @JsonProperty("dereferenceIndices") List<Integer> dereferenceIndices,
            @JsonProperty("dereferenceNames") List<String> dereferenceNames,
            @JsonProperty("hiveType") HiveType hiveType,
            @JsonProperty("type") Type type)
    {
        this.dereferenceIndices = requireNonNull(dereferenceIndices, "dereferenceIndices is null");
        this.dereferenceNames = requireNonNull(dereferenceNames, "dereferenceNames is null");
        checkArgument(dereferenceIndices.size() > 0, "dereferenceIndices should not be empty");
        checkArgument(dereferenceIndices.size() == dereferenceNames.size(), "dereferenceIndices and dereferenceNames should have the same sizes");

        this.hiveType = requireNonNull(hiveType, "hiveType is null");
        this.type = requireNonNull(type, "type is null");

        this.partialName = generatePartialName(dereferenceNames);
    }

    public String getPartialName()
    {
        return partialName;
    }

    @JsonProperty
    public List<Integer> getDereferenceIndices()
    {
        return dereferenceIndices;
    }

    @JsonProperty
    public List<String> getDereferenceNames()
    {
        return dereferenceNames;
    }

    @JsonProperty
    public HiveType getHiveType()
    {
        return hiveType;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(dereferenceIndices, dereferenceNames, hiveType, type);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        HiveColumnProjectionInfo other = (HiveColumnProjectionInfo) obj;
        return Objects.equals(this.dereferenceIndices, other.dereferenceIndices) &&
                Objects.equals(this.dereferenceNames, other.dereferenceNames) &&
                Objects.equals(this.hiveType, other.hiveType) &&
                Objects.equals(this.type, other.type);
    }

    public static String generatePartialName(List<String> dereferenceNames)
    {
        return dereferenceNames.stream()
                .map(name -> "#" + name)
                .collect(Collectors.joining());
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(dereferenceIndices, SizeOf::sizeOf)
                + estimatedSizeOf(dereferenceNames, SizeOf::estimatedSizeOf)
                + hiveType.getRetainedSizeInBytes()
                // type is not accounted for as the instances are cached (by TypeRegistry) and shared
                + estimatedSizeOf(partialName);
    }
}
