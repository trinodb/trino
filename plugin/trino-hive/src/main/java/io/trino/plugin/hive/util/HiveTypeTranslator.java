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
package io.trino.plugin.hive.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.trino.plugin.hive.HiveErrorCode;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.plugin.hive.type.CharTypeInfo;
import io.trino.plugin.hive.type.DecimalTypeInfo;
import io.trino.plugin.hive.type.ListTypeInfo;
import io.trino.plugin.hive.type.MapTypeInfo;
import io.trino.plugin.hive.type.PrimitiveTypeInfo;
import io.trino.plugin.hive.type.StructTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.plugin.hive.type.UnionTypeInfo;
import io.trino.plugin.hive.type.VarcharTypeInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hive.formats.UnionToRowCoercionUtils.rowTypeSignatureForUnionOfTypes;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
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
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.TypeSignature.rowType;
import static io.trino.spi.type.TypeSignatureParameter.typeParameter;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class HiveTypeTranslator
{
    private HiveTypeTranslator() {}

    public static TypeInfo toTypeInfo(Type type)
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
        if (type instanceof TimestampType) {
            return HIVE_TIMESTAMP.getTypeInfo();
        }
        if (type instanceof DecimalType decimalType) {
            return new DecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale());
        }
        if (type instanceof ArrayType arrayType) {
            TypeInfo elementType = toTypeInfo(arrayType.getElementType());
            return getListTypeInfo(elementType);
        }
        if (type instanceof MapType mapType) {
            TypeInfo keyType = toTypeInfo(mapType.getKeyType());
            TypeInfo valueType = toTypeInfo(mapType.getValueType());
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
                            .map(HiveTypeTranslator::toTypeInfo)
                            .collect(toImmutableList()));
        }
        throw new TrinoException(NOT_SUPPORTED, format("Unsupported Hive type: %s", type));
    }

    public static TypeSignature toTypeSignature(TypeInfo typeInfo, HiveTimestampPrecision timestampPrecision)
    {
        switch (typeInfo.getCategory()) {
            case PRIMITIVE:
                Type primitiveType = fromPrimitiveType((PrimitiveTypeInfo) typeInfo, timestampPrecision);
                if (primitiveType == null) {
                    break;
                }
                return primitiveType.getTypeSignature();
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                return mapType(
                        toTypeSignature(mapTypeInfo.getMapKeyTypeInfo(), timestampPrecision),
                        toTypeSignature(mapTypeInfo.getMapValueTypeInfo(), timestampPrecision));
            case LIST:
                ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
                TypeSignature elementType = toTypeSignature(listTypeInfo.getListElementTypeInfo(), timestampPrecision);
                return arrayType(typeParameter(elementType));
            case STRUCT:
                StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
                List<TypeInfo> fieldTypes = structTypeInfo.getAllStructFieldTypeInfos();
                List<String> fieldNames = structTypeInfo.getAllStructFieldNames();
                if (fieldTypes.size() != fieldNames.size()) {
                    throw new TrinoException(HiveErrorCode.HIVE_INVALID_METADATA, format("Invalid Hive struct type: %s", typeInfo));
                }
                return rowType(Streams.zip(
                        // We lower case the struct field names.
                        // Otherwise, Trino will refuse to write to columns whose struct type has field names containing upper case characters.
                        // Users can't work around this by casting in their queries because Trino parser always lower case types.
                        // TODO: This is a hack. Trino engine should be able to handle identifiers in a case insensitive way where necessary.
                        fieldNames.stream().map(s -> s.toLowerCase(Locale.US)),
                        fieldTypes.stream().map(type -> toTypeSignature(type, timestampPrecision)),
                        TypeSignatureParameter::namedField)
                        .collect(Collectors.toList()));
            case UNION:
                // Use a row type to represent a union type in Hive for reading
                UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
                List<TypeInfo> unionObjectTypes = unionTypeInfo.getAllUnionObjectTypeInfos();
                return rowTypeSignatureForUnionOfTypes(unionObjectTypes.stream()
                        .map(unionObjectType -> toTypeSignature(unionObjectType, timestampPrecision))
                        .collect(toImmutableList()));
        }
        throw new TrinoException(NOT_SUPPORTED, format("Unsupported Hive type: %s", typeInfo));
    }

    /**
     * @deprecated Prefer {@link #fromPrimitiveType(PrimitiveTypeInfo, HiveTimestampPrecision)}
     */
    @Deprecated
    @Nullable
    public static Type fromPrimitiveType(PrimitiveTypeInfo typeInfo)
    {
        return fromPrimitiveType(typeInfo, DEFAULT_PRECISION);
    }

    @Nullable
    private static Type fromPrimitiveType(PrimitiveTypeInfo typeInfo, HiveTimestampPrecision timestampPrecision)
    {
        switch (typeInfo.getPrimitiveCategory()) {
            case BOOLEAN:
                return BOOLEAN;
            case BYTE:
                return TINYINT;
            case SHORT:
                return SMALLINT;
            case INT:
                return INTEGER;
            case LONG:
                return BIGINT;
            case FLOAT:
                return REAL;
            case DOUBLE:
                return DOUBLE;
            case STRING:
                return createUnboundedVarcharType();
            case VARCHAR:
                return createVarcharType(((VarcharTypeInfo) typeInfo).getLength());
            case CHAR:
                return createCharType(((CharTypeInfo) typeInfo).getLength());
            case DATE:
                return DATE;
            case TIMESTAMP:
                return createTimestampType(timestampPrecision.getPrecision());
            case TIMESTAMPLOCALTZ:
                return createTimestampWithTimeZoneType(timestampPrecision.getPrecision());
            case BINARY:
                return VARBINARY;
            case DECIMAL:
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
                return createDecimalType(decimalTypeInfo.precision(), decimalTypeInfo.scale());
            default:
                return null;
        }
    }
}
