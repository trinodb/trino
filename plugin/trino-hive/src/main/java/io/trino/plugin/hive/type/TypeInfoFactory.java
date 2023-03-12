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

import io.trino.plugin.hive.type.TypeInfoUtils.PrimitiveParts;
import io.trino.plugin.hive.type.TypeInfoUtils.PrimitiveTypeEntry;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.type.BaseCharTypeInfo.charTypeName;
import static io.trino.plugin.hive.type.DecimalTypeInfo.decimalTypeName;
import static io.trino.plugin.hive.type.TypeInfoUtils.getBaseName;
import static io.trino.plugin.hive.type.TypeInfoUtils.getTypeEntryFromTypeName;
import static io.trino.plugin.hive.type.TypeInfoUtils.parsePrimitiveParts;
import static io.trino.plugin.hive.util.SerdeConstants.CHAR_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.VARCHAR_TYPE_NAME;
import static java.lang.Integer.parseInt;

// based on org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
public final class TypeInfoFactory
{
    private TypeInfoFactory() {}

    public static PrimitiveTypeInfo getPrimitiveTypeInfo(String typeName)
    {
        String baseName = getBaseName(typeName);
        PrimitiveTypeEntry typeEntry = getTypeEntryFromTypeName(baseName);
        checkArgument(typeEntry != null, "Unknown type: %s", typeName);

        return switch (typeEntry.primitiveCategory()) {
            case CHAR -> {
                PrimitiveParts parts = parsePrimitiveParts(typeName);
                checkArgument(parts.typeParams().length == 1);
                yield new CharTypeInfo(parseInt(parts.typeParams()[0]));
            }
            case VARCHAR -> {
                PrimitiveParts parts = parsePrimitiveParts(typeName);
                checkArgument(parts.typeParams().length == 1);
                yield new VarcharTypeInfo(parseInt(parts.typeParams()[0]));
            }
            case DECIMAL -> {
                PrimitiveParts parts = parsePrimitiveParts(typeName);
                checkArgument(parts.typeParams().length == 2);
                yield new DecimalTypeInfo(parseInt(parts.typeParams()[0]), parseInt(parts.typeParams()[1]));
            }
            default -> new PrimitiveTypeInfo(typeName);
        };
    }

    public static CharTypeInfo getCharTypeInfo(int length)
    {
        return (CharTypeInfo) getPrimitiveTypeInfo(charTypeName(CHAR_TYPE_NAME, length));
    }

    public static VarcharTypeInfo getVarcharTypeInfo(int length)
    {
        return (VarcharTypeInfo) getPrimitiveTypeInfo(charTypeName(VARCHAR_TYPE_NAME, length));
    }

    public static DecimalTypeInfo getDecimalTypeInfo(int precision, int scale)
    {
        return (DecimalTypeInfo) getPrimitiveTypeInfo(decimalTypeName(precision, scale));
    }

    public static TypeInfo getStructTypeInfo(List<String> names, List<TypeInfo> typeInfos)
    {
        return new StructTypeInfo(names, typeInfos);
    }

    public static TypeInfo getUnionTypeInfo(List<TypeInfo> typeInfos)
    {
        return new UnionTypeInfo(typeInfos);
    }

    public static TypeInfo getListTypeInfo(TypeInfo elementTypeInfo)
    {
        return new ListTypeInfo(elementTypeInfo);
    }

    public static TypeInfo getMapTypeInfo(TypeInfo keyTypeInfo, TypeInfo valueTypeInfo)
    {
        return new MapTypeInfo(keyTypeInfo, valueTypeInfo);
    }
}
