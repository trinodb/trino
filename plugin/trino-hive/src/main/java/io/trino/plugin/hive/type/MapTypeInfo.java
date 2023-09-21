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

import java.util.Objects;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.hive.util.SerdeConstants.MAP_TYPE_NAME;
import static java.util.Objects.requireNonNull;

// based on org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo
public final class MapTypeInfo
        extends TypeInfo
{
    private static final int INSTANCE_SIZE = instanceSize(UnionTypeInfo.class);

    private final TypeInfo keyTypeInfo;
    private final TypeInfo valueTypeInfo;

    MapTypeInfo(TypeInfo keyTypeInfo, TypeInfo valueTypeInfo)
    {
        this.keyTypeInfo = requireNonNull(keyTypeInfo, "keyTypeInfo is null");
        this.valueTypeInfo = requireNonNull(valueTypeInfo, "valueTypeInfo is null");
    }

    @Override
    public String getTypeName()
    {
        return MAP_TYPE_NAME + "<" + keyTypeInfo.getTypeName() + "," + valueTypeInfo.getTypeName() + ">";
    }

    @Override
    public Category getCategory()
    {
        return Category.MAP;
    }

    public TypeInfo getMapKeyTypeInfo()
    {
        return keyTypeInfo;
    }

    public TypeInfo getMapValueTypeInfo()
    {
        return valueTypeInfo;
    }

    @Override
    public boolean equals(Object other)
    {
        return (other instanceof MapTypeInfo o) &&
                keyTypeInfo.equals(o.keyTypeInfo) &&
                valueTypeInfo.equals(o.valueTypeInfo);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyTypeInfo, valueTypeInfo);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + keyTypeInfo.getRetainedSizeInBytes() + valueTypeInfo.getRetainedSizeInBytes();
    }
}
