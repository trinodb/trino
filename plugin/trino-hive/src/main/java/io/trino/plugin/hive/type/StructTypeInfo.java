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
package io.trino.plugin.hive.type;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.hive.util.SerdeConstants.STRUCT_TYPE_NAME;
import static java.util.Objects.requireNonNull;

// based on org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo
public final class StructTypeInfo
        extends TypeInfo
{
    private static final int INSTANCE_SIZE = instanceSize(StructTypeInfo.class);

    private final List<String> names;
    private final List<TypeInfo> typeInfos;

    StructTypeInfo(List<String> names, List<TypeInfo> typeInfos)
    {
        this.names = ImmutableList.copyOf(requireNonNull(names, "names is null"));
        this.typeInfos = ImmutableList.copyOf(requireNonNull(typeInfos, "typeInfos is null"));
        checkArgument(names.size() == typeInfos.size(), "mismatched sizes");
    }

    @Override
    public String getTypeName()
    {
        StringJoiner joiner = new StringJoiner(",", STRUCT_TYPE_NAME + "<", ">");
        for (int i = 0; i < names.size(); i++) {
            joiner.add(names.get(i) + ":" + typeInfos.get(i).getTypeName());
        }
        return joiner.toString();
    }

    @Override
    public Category getCategory()
    {
        return Category.STRUCT;
    }

    public List<String> getAllStructFieldNames()
    {
        return names;
    }

    public List<TypeInfo> getAllStructFieldTypeInfos()
    {
        return typeInfos;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StructTypeInfo o)) {
            return false;
        }
        Iterator<String> namesIterator = getAllStructFieldNames().iterator();
        Iterator<String> otherNamesIterator = o.getAllStructFieldNames().iterator();

        // Compare the field names using ignore-case semantics
        while (namesIterator.hasNext() && otherNamesIterator.hasNext()) {
            if (!namesIterator.next().equalsIgnoreCase(otherNamesIterator.next())) {
                return false;
            }
        }

        // Different number of field names
        if (namesIterator.hasNext() || otherNamesIterator.hasNext()) {
            return false;
        }

        // Compare the field types
        return o.getAllStructFieldTypeInfos().equals(getAllStructFieldTypeInfos());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(names, typeInfos);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                estimatedSizeOf(names, SizeOf::estimatedSizeOf) +
                estimatedSizeOf(typeInfos, TypeInfo::getRetainedSizeInBytes);
    }
}
