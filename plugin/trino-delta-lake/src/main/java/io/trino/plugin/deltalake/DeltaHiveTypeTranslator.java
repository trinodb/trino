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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.type.DecimalTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarcharType;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveType.HIVE_BINARY;
import static io.trino.plugin.hive.HiveType.HIVE_BOOLEAN;
import static io.trino.plugin.hive.HiveType.HIVE_BYTE;
import static io.trino.plugin.hive.HiveType.HIVE_DATE;
import static io.trino.plugin.hive.HiveType.HIVE_DOUBLE;
import static io.trino.plugin.hive.HiveType.HIVE_FLOAT;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveType.HIVE_SHORT;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveType.HIVE_TIMESTAMP;
import static io.trino.plugin.hive.type.CharTypeInfo.MAX_CHAR_LENGTH;
import static io.trino.plugin.hive.type.TypeInfoFactory.getCharTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getListTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getMapTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getStructTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getVarcharTypeInfo;
import static io.trino.plugin.hive.type.VarcharTypeInfo.MAX_VARCHAR_LENGTH;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DeltaHiveTypeTranslator
{
    private DeltaHiveTypeTranslator() {}

    public static HiveType toHiveType(Type type)
    {
        return HiveType.toHiveType(translate(type));
    }

    // Copy from HiveTypeTranslator with a custom mapping for TimestampWithTimeZone
    public static TypeInfo translate(Type type)
    {
        requireNonNull(type, "type is null");
        if (BOOLEAN.equals(type)) {
            return HIVE_BOOLEAN.getTypeInfo();
        }
        if (BIGINT.equals(type)) {
            return HIVE_LONG.getTypeInfo();
        }
        if (INTEGER.equals(type)) {
            return HIVE_INT.getTypeInfo();
        }
        if (SMALLINT.equals(type)) {
            return HIVE_SHORT.getTypeInfo();
        }
        if (TINYINT.equals(type)) {
            return HIVE_BYTE.getTypeInfo();
        }
        if (REAL.equals(type)) {
            return HIVE_FLOAT.getTypeInfo();
        }
        if (DOUBLE.equals(type)) {
            return HIVE_DOUBLE.getTypeInfo();
        }
        if (type instanceof VarcharType varcharType) {
            if (varcharType.isUnbounded()) {
                return HIVE_STRING.getTypeInfo();
            }
            if (varcharType.getBoundedLength() <= MAX_VARCHAR_LENGTH) {
                return getVarcharTypeInfo(varcharType.getBoundedLength());
            }
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported Hive type: %s. Supported VARCHAR types: VARCHAR(<=%d), VARCHAR.", type, MAX_VARCHAR_LENGTH));
        }
        if (type instanceof CharType charType) {
            int charLength = charType.getLength();
            if (charLength <= MAX_CHAR_LENGTH) {
                return getCharTypeInfo(charLength);
            }
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported Hive type: %s. Supported CHAR types: CHAR(<=%d).", type, MAX_CHAR_LENGTH));
        }
        if (VARBINARY.equals(type)) {
            return HIVE_BINARY.getTypeInfo();
        }
        if (DATE.equals(type)) {
            return HIVE_DATE.getTypeInfo();
        }
        if (type instanceof TimestampWithTimeZoneType) {
            verify(((TimestampWithTimeZoneType) type).getPrecision() == 3, "Unsupported type: %s", type);
            return HIVE_TIMESTAMP.getTypeInfo();
        }
        if (type instanceof TimestampType) {
            verify(((TimestampType) type).getPrecision() == 3, "Unsupported type: %s", type);
            return HIVE_TIMESTAMP.getTypeInfo();
        }
        if (type instanceof DecimalType decimalType) {
            return new DecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale());
        }
        if (type instanceof ArrayType arrayType) {
            TypeInfo elementType = translate(arrayType.getElementType());
            return getListTypeInfo(elementType);
        }
        if (type instanceof MapType mapType) {
            TypeInfo keyType = translate(mapType.getKeyType());
            TypeInfo valueType = translate(mapType.getValueType());
            return getMapTypeInfo(keyType, valueType);
        }
        if (type instanceof RowType) {
            ImmutableList.Builder<String> fieldNames = ImmutableList.builder();
            for (TypeSignatureParameter parameter : type.getTypeSignature().getParameters()) {
                if (!parameter.isNamedTypeSignature()) {
                    throw new IllegalArgumentException(format("Expected all parameters to be named type, but got %s", parameter));
                }
                fieldNames.add(parameter.getNamedTypeSignature().getName()
                        .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, format("Anonymous row type is not supported in Hive. Please give each field a name: %s", type))));
            }
            return getStructTypeInfo(
                    fieldNames.build(),
                    type.getTypeParameters().stream()
                            .map(DeltaHiveTypeTranslator::translate)
                            .collect(toImmutableList()));
        }
        throw new TrinoException(NOT_SUPPORTED, format("Unsupported Delta Lake type: %s", type));
    }
}
