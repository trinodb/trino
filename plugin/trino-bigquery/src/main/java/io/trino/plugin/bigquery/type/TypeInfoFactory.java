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

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.bigquery.type.DecimalTypeInfo.decimalTypeName;
import static io.trino.plugin.bigquery.type.TypeInfoUtils.getBaseName;
import static io.trino.plugin.bigquery.type.TypeInfoUtils.getStandardSqlTypeNameFromTypeName;
import static io.trino.plugin.bigquery.type.TypeInfoUtils.parsePrimitiveParts;
import static java.lang.Integer.parseInt;

public final class TypeInfoFactory
{
    private TypeInfoFactory() {}

    public static PrimitiveTypeInfo getPrimitiveTypeInfo(String typeName)
    {
        String baseName = getBaseName(typeName);
        StandardSQLTypeName typeEntry = getStandardSqlTypeNameFromTypeName(baseName);
        checkArgument(typeEntry != null, "Unknown type: %s", typeName);

        return switch (typeEntry) {
            case NUMERIC, BIGNUMERIC -> {
                TypeInfoUtils.PrimitiveParts parts = parsePrimitiveParts(typeName);
                checkArgument(parts.typeParams().length == 2);
                yield new DecimalTypeInfo(parseInt(parts.typeParams()[0]), parseInt(parts.typeParams()[1]));
            }
            default -> new PrimitiveTypeInfo(typeName);
        };
    }

    public static DecimalTypeInfo getDecimalTypeInfo(int precision, int scale)
    {
        return (DecimalTypeInfo) getPrimitiveTypeInfo(decimalTypeName(precision, scale));
    }

    public static TypeInfo getStructTypeInfo(List<String> names, List<TypeInfo> typeInfos)
    {
        return new StructTypeInfo(names, typeInfos);
    }

    public static TypeInfo getListTypeInfo(TypeInfo elementTypeInfo)
    {
        return new ArrayTypeInfo(elementTypeInfo);
    }
}
