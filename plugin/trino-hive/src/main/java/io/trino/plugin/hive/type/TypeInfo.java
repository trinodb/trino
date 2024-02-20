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

// based on org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
public abstract sealed class TypeInfo
        permits ListTypeInfo, MapTypeInfo, PrimitiveTypeInfo, StructTypeInfo, UnionTypeInfo
{
    protected TypeInfo() {}

    public abstract Category getCategory();

    public abstract String getTypeName();

    @Override
    public final String toString()
    {
        return getTypeName();
    }

    @Override
    public abstract boolean equals(Object o);

    @Override
    public abstract int hashCode();

    public abstract long getRetainedSizeInBytes();
}
