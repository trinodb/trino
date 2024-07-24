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

import java.util.List;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.hive.util.SerdeConstants.UNION_TYPE_NAME;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

// based on org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo
public final class UnionTypeInfo
        extends TypeInfo
{
    private static final int INSTANCE_SIZE = instanceSize(UnionTypeInfo.class);

    private final List<TypeInfo> objectTypeInfos;

    UnionTypeInfo(List<TypeInfo> objectTypeInfos)
    {
        this.objectTypeInfos = ImmutableList.copyOf(requireNonNull(objectTypeInfos, "objectTypeInfos is null"));
    }

    @Override
    public String getTypeName()
    {
        return objectTypeInfos.stream()
                .map(TypeInfo::getTypeName)
                .collect(joining(",", UNION_TYPE_NAME + "<", ">"));
    }

    @Override
    public Category getCategory()
    {
        return Category.UNION;
    }

    public List<TypeInfo> getAllUnionObjectTypeInfos()
    {
        return objectTypeInfos;
    }

    @Override
    public boolean equals(Object other)
    {
        return (other instanceof UnionTypeInfo o) &&
                objectTypeInfos.equals(o.objectTypeInfos);
    }

    @Override
    public int hashCode()
    {
        return objectTypeInfos.hashCode();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(objectTypeInfos, TypeInfo::getRetainedSizeInBytes);
    }
}
