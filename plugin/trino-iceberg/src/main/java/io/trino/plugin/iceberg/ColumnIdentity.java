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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.ARRAY;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.MAP;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.STRUCT;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.VARIANT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ColumnIdentity
{
    private static final int INSTANCE_SIZE = instanceSize(ColumnIdentity.class);

    private final int id;
    private final String name;
    private final TypeCategory typeCategory;
    // Underlying ImmutableMap is used to maintain the column ordering
    private final Map<Integer, ColumnIdentity> children;
    private final Map<Integer, Integer> childFieldIdToIndex;

    @JsonCreator
    public ColumnIdentity(
            @JsonProperty("id") int id,
            @JsonProperty("name") String name,
            @JsonProperty("typeCategory") TypeCategory typeCategory,
            @JsonProperty("children") List<ColumnIdentity> children)
    {
        this.id = id;
        this.name = requireNonNull(name, "name is null");
        this.typeCategory = requireNonNull(typeCategory, "typeCategory is null");
        requireNonNull(children, "children is null");
        checkArgument(
                children.isEmpty() == (typeCategory == PRIMITIVE || typeCategory == VARIANT),
                "Children should be empty if and only if column type is primitive");
        ImmutableMap.Builder<Integer, ColumnIdentity> childrenBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, Integer> childFieldIdToIndex = ImmutableMap.builder();
        for (int i = 0; i < children.size(); i++) {
            ColumnIdentity child = children.get(i);
            childrenBuilder.put(child.getId(), child);
            childFieldIdToIndex.put(child.getId(), i);
        }
        this.children = childrenBuilder.buildOrThrow();
        this.childFieldIdToIndex = childFieldIdToIndex.buildOrThrow();
    }

    @JsonProperty
    public int getId()
    {
        return id;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public TypeCategory getTypeCategory()
    {
        return typeCategory;
    }

    @JsonProperty
    public List<ColumnIdentity> getChildren()
    {
        return ImmutableList.copyOf(children.values());
    }

    @JsonIgnore
    public ColumnIdentity getChildByFieldId(int fieldId)
    {
        checkArgument(children.containsKey(fieldId), "ColumnIdentity %s does not contain child with field id %s", this, fieldId);
        return children.get(fieldId);
    }

    @JsonIgnore
    public int getChildIndexByFieldId(int fieldId)
    {
        checkArgument(childFieldIdToIndex.containsKey(fieldId), "ColumnIdentity %s does not contain child with field id %s", this, fieldId);
        return childFieldIdToIndex.get(fieldId);
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
        ColumnIdentity that = (ColumnIdentity) o;
        return id == that.id &&
                name.equals(that.name) &&
                typeCategory == that.typeCategory &&
                children.equals(that.children);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, name, typeCategory, children);
    }

    @Override
    public String toString()
    {
        return id + ":" + name;
    }

    public long getRetainedSizeInBytes()
    {
        // type is not accounted for as the instances are cached (by TypeRegistry) and shared
        return INSTANCE_SIZE
                + sizeOf(id)
                + estimatedSizeOf(name)
                + estimatedSizeOf(children, SizeOf::sizeOf, ColumnIdentity::getRetainedSizeInBytes)
                + estimatedSizeOf(childFieldIdToIndex, SizeOf::sizeOf, SizeOf::sizeOf);
    }

    public enum TypeCategory
    {
        PRIMITIVE,
        STRUCT,
        ARRAY,
        MAP,
        VARIANT
    }

    public static ColumnIdentity primitiveColumnIdentity(int id, String name)
    {
        return new ColumnIdentity(id, name, PRIMITIVE, ImmutableList.of());
    }

    public static ColumnIdentity createColumnIdentity(Types.NestedField column)
    {
        int id = column.fieldId();
        String name = column.name();
        org.apache.iceberg.types.Type fieldType = column.type();

        if (fieldType.isVariantType()) {
            // todo: confirm do we still needs this? Iceberg does not seem to support non-primitive partition fields
            return new ColumnIdentity(id, name, VARIANT, ImmutableList.of());
        }

        if (!fieldType.isNestedType()) {
            return new ColumnIdentity(id, name, PRIMITIVE, ImmutableList.of());
        }

        if (fieldType.isListType()) {
            ColumnIdentity elementColumn = createColumnIdentity(getOnlyElement(fieldType.asListType().fields()));
            return new ColumnIdentity(id, name, ARRAY, ImmutableList.of(elementColumn));
        }

        if (fieldType.isStructType()) {
            List<ColumnIdentity> fieldColumns = fieldType.asStructType().fields().stream()
                    .map(ColumnIdentity::createColumnIdentity)
                    .collect(toImmutableList());
            return new ColumnIdentity(id, name, STRUCT, fieldColumns);
        }

        if (fieldType.isMapType()) {
            List<ColumnIdentity> keyValueColumns = fieldType.asMapType().fields().stream()
                    .map(ColumnIdentity::createColumnIdentity)
                    .collect(toImmutableList());
            checkArgument(keyValueColumns.size() == 2, "Expected map type to have two fields");
            return new ColumnIdentity(id, name, MAP, keyValueColumns);
        }

        throw new UnsupportedOperationException(format("Iceberg column type %s is not supported", fieldType.typeId()));
    }
}
