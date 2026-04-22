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
package io.trino.plugin.hudi.testing;

import io.trino.metastore.HiveType;
import io.trino.metastore.type.TypeInfo;

import java.util.ArrayList;
import java.util.List;

import static io.trino.metastore.type.TypeConstants.BIGINT_TYPE_NAME;
import static io.trino.metastore.type.TypeConstants.BOOLEAN_TYPE_NAME;
import static io.trino.metastore.type.TypeConstants.DATE_TYPE_NAME;
import static io.trino.metastore.type.TypeConstants.DOUBLE_TYPE_NAME;
import static io.trino.metastore.type.TypeConstants.INT_TYPE_NAME;
import static io.trino.metastore.type.TypeConstants.STRING_TYPE_NAME;
import static io.trino.metastore.type.TypeConstants.TIMESTAMP_TYPE_NAME;
import static io.trino.metastore.type.TypeInfoFactory.getCharTypeInfo;
import static io.trino.metastore.type.TypeInfoFactory.getDecimalTypeInfo;
import static io.trino.metastore.type.TypeInfoFactory.getListTypeInfo;
import static io.trino.metastore.type.TypeInfoFactory.getMapTypeInfo;
import static io.trino.metastore.type.TypeInfoFactory.getPrimitiveTypeInfo;
import static io.trino.metastore.type.TypeInfoFactory.getStructTypeInfo;
import static io.trino.metastore.type.TypeInfoFactory.getVarcharTypeInfo;

public class TypeInfoHelper
{
    public static final TypeInfo BOOLEAN_TYPE_INFO = getPrimitiveTypeInfo(BOOLEAN_TYPE_NAME);
    public static final TypeInfo INT_TYPE_INFO = getPrimitiveTypeInfo(INT_TYPE_NAME);
    public static final TypeInfo LONG_TYPE_INFO = getPrimitiveTypeInfo(BIGINT_TYPE_NAME);
    public static final TypeInfo DOUBLE_TYPE_INFO = getPrimitiveTypeInfo(DOUBLE_TYPE_NAME);
    public static final TypeInfo STRING_TYPE_INFO = getPrimitiveTypeInfo(STRING_TYPE_NAME);
    public static final TypeInfo TIMESTAMP_TYPE_INFO = getPrimitiveTypeInfo(TIMESTAMP_TYPE_NAME);
    public static final TypeInfo DATE_TYPE_INFO = getPrimitiveTypeInfo(DATE_TYPE_NAME);

    public static final TypeInfo ARRAY_STRING_TYPE_INFO = getListTypeInfo(STRING_TYPE_INFO);
    public static final TypeInfo ARRAY_INT_TYPE_INFO = getListTypeInfo(INT_TYPE_INFO);
    public static final TypeInfo ARRAY_BOOLEAN_TYPE_INFO = getListTypeInfo(BOOLEAN_TYPE_INFO);
    public static final TypeInfo ARRAY_DOUBLE_TYPE_INFO = getListTypeInfo(DOUBLE_TYPE_INFO);

    public static final TypeInfo MAP_STRING_INT_TYPE_INFO = getMapTypeInfo(STRING_TYPE_INFO, INT_TYPE_INFO);
    public static final TypeInfo MAP_STRING_LONG_TYPE_INFO = getMapTypeInfo(STRING_TYPE_INFO, LONG_TYPE_INFO);
    public static final TypeInfo MAP_STRING_DATE_TYPE_INFO = getMapTypeInfo(STRING_TYPE_INFO, DATE_TYPE_INFO);

    private TypeInfoHelper()
    {
    }

    /**
     * Creates a HiveType for a list type.
     *
     * @param elementTypeInfo The TypeInfo of the list elements.
     * @return A HiveType instance for the list.
     */
    public static HiveType listHiveType(TypeInfo elementTypeInfo)
    {
        return HiveType.fromTypeInfo(getListTypeInfo(elementTypeInfo));
    }

    /**
     * Creates a HiveType for a map type.
     *
     * @param keyTypeInfo The TypeInfo of the map keys.
     * @param valueTypeInfo The TypeInfo of the map values.
     * @return A HiveType instance for the map.
     */
    public static HiveType mapHiveType(TypeInfo keyTypeInfo, TypeInfo valueTypeInfo)
    {
        return HiveType.fromTypeInfo(getMapTypeInfo(keyTypeInfo, valueTypeInfo));
    }

    /**
     * Creates a HiveType for a struct type.
     *
     * @param fieldNames List of field names.
     * @param fieldTypeInfos List of corresponding field TypeInfos.
     * @return A HiveType instance for the struct.
     */
    public static HiveType structHiveType(List<String> fieldNames, List<TypeInfo> fieldTypeInfos)
    {
        // ArrayList to preserve ordering
        return HiveType.fromTypeInfo(getStructTypeInfo(new ArrayList<>(fieldNames), new ArrayList<>(fieldTypeInfos)));
    }

    /**
     * Creates a HiveType for a decimal type.
     *
     * @param precision The precision.
     * @param scale The scale.
     * @return A HiveType instance for the decimal.
     */
    public static HiveType decimalHiveType(int precision, int scale)
    {
        return HiveType.fromTypeInfo(getDecimalTypeInfo(precision, scale));
    }

    /**
     * Creates a HiveType for a varchar type.
     *
     * @param length The maximum length.
     * @return A HiveType instance for the varchar.
     */
    public static HiveType varcharHiveType(int length)
    {
        return HiveType.fromTypeInfo(getVarcharTypeInfo(length));
    }

    /**
     * Creates a HiveType for a char type.
     *
     * @param length The fixed length.
     * @return A HiveType instance for the char.
     */
    public static HiveType charHiveType(int length)
    {
        return HiveType.fromTypeInfo(getCharTypeInfo(length));
    }
}
