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

import com.google.cloud.bigquery.StandardSQLTypeName;

public final class TypeInfoFactory
{
    private TypeInfoFactory() {}

    public static PrimitiveTypeInfo getPrimitiveTypeInfo(StandardSQLTypeName typeEntry)
    {
        return new PrimitiveTypeInfo(typeEntry.name());
    }

    public static DecimalTypeInfo getDecimalTypeInfo(int precision, int scale)
    {
        return new DecimalTypeInfo(precision, scale);
    }

    public static BigDecimalTypeInfo getBigDecimalTypeInfo(int precision, int scale)
    {
        return new BigDecimalTypeInfo(precision, scale);
    }

    public static TypeInfo getArrayTypeInfo(TypeInfo elementTypeInfo)
    {
        return new ArrayTypeInfo(elementTypeInfo);
    }
}
