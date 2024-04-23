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
package io.trino.plugin.bigquery.type;

import static com.google.cloud.bigquery.StandardSQLTypeName.ARRAY;
import static java.util.Objects.requireNonNull;

public final class ArrayTypeInfo
        extends TypeInfo
{
    private final TypeInfo elementTypeInfo;

    ArrayTypeInfo(TypeInfo elementTypeInfo)
    {
        this.elementTypeInfo = requireNonNull(elementTypeInfo, "elementTypeInfo is null");
    }

    @Override
    public String getTypeName()
    {
        return ARRAY + "<" + elementTypeInfo.getTypeName() + ">";
    }

    @Override
    public Category getCategory()
    {
        return Category.ARRAY;
    }

    public TypeInfo getListElementTypeInfo()
    {
        return elementTypeInfo;
    }

    @Override
    public boolean equals(Object other)
    {
        return (other instanceof ArrayTypeInfo o) &&
                elementTypeInfo.equals(o.elementTypeInfo);
    }

    @Override
    public int hashCode()
    {
        return elementTypeInfo.hashCode();
    }
}
