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

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_TYPE_NAME;
import static java.util.Objects.requireNonNull;

// based on org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo
public final class ListTypeInfo
        extends TypeInfo
{
    private static final int INSTANCE_SIZE = instanceSize(UnionTypeInfo.class);

    private final TypeInfo elementTypeInfo;

    ListTypeInfo(TypeInfo elementTypeInfo)
    {
        this.elementTypeInfo = requireNonNull(elementTypeInfo, "elementTypeInfo is null");
    }

    @Override
    public String getTypeName()
    {
        return LIST_TYPE_NAME + "<" + elementTypeInfo.getTypeName() + ">";
    }

    @Override
    public Category getCategory()
    {
        return Category.LIST;
    }

    public TypeInfo getListElementTypeInfo()
    {
        return elementTypeInfo;
    }

    @Override
    public boolean equals(Object other)
    {
        return (other instanceof ListTypeInfo o) &&
                elementTypeInfo.equals(o.elementTypeInfo);
    }

    @Override
    public int hashCode()
    {
        return elementTypeInfo.hashCode();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + elementTypeInfo.getRetainedSizeInBytes();
    }
}
