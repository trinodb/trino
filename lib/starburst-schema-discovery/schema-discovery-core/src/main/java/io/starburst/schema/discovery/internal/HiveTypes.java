/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.internal;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.type.DecimalTypeInfo;
import io.trino.plugin.hive.type.ListTypeInfo;
import io.trino.plugin.hive.type.MapTypeInfo;
import io.trino.plugin.hive.type.StructTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.plugin.hive.type.TypeInfoFactory;
import io.trino.plugin.hive.type.VarcharTypeInfo;
import io.trino.plugin.hive.util.SerdeConstants;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.starburst.schema.discovery.infer.NullType.isNullType;
import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;
import static io.trino.plugin.hive.type.TypeInfoFactory.getVarcharTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoUtils.getTypeInfoFromTypeString;

//Here is all trino supported types with type='hive' create table

//type_boolean boolean,
//type_tinyint tinyint,
//type_smallint smallint,
//type_integer integer,
//type_bigint bigint,
//type_real real,
//type_double double,
//type_decimal decimal,
//type_varchar varchar,
//type_char char,
//type_varbinary varbinary,
//type_timestamp timestamp,
//type_timestamp3 timestamp(3),
//type_array array<varchar>,
//type_map map<varchar, varchar>,
//type_row row(row_nested varchar)

//those are not supported in hive:
//--type_json json,
//--type_time time, (all time types)
//--type_timestamp6 timestamp(6),
//--type_timestamp9 timestamp(9),
//--type_timestamp12 timestamp(12),
//--type_ip uuid,
//
public class HiveTypes
{
    // string, converted to varchar in io.starburst.schema.discovery.generation.Mapping
    public static final TypeInfo STRING_TYPE = getTypeInfoFromTypeString(SerdeConstants.STRING_TYPE_NAME);
    // varchar(65535)
    public static final TypeInfo STRING_TYPE_ALT = getVarcharTypeInfo(VarcharTypeInfo.MAX_VARCHAR_LENGTH);
    public static final TypeInfo HIVE_BOOLEAN = getTypeInfoFromTypeString(SerdeConstants.BOOLEAN_TYPE_NAME);
    public static final TypeInfo HIVE_BYTE = getTypeInfoFromTypeString(SerdeConstants.TINYINT_TYPE_NAME);
    public static final TypeInfo HIVE_SHORT = getTypeInfoFromTypeString(SerdeConstants.SMALLINT_TYPE_NAME);
    public static final TypeInfo HIVE_INT = getTypeInfoFromTypeString(SerdeConstants.INT_TYPE_NAME);
    public static final TypeInfo HIVE_LONG = getTypeInfoFromTypeString(SerdeConstants.BIGINT_TYPE_NAME);
    public static final TypeInfo HIVE_FLOAT = getTypeInfoFromTypeString(SerdeConstants.FLOAT_TYPE_NAME);
    public static final TypeInfo HIVE_DOUBLE = getTypeInfoFromTypeString(SerdeConstants.DOUBLE_TYPE_NAME);
    public static final TypeInfo HIVE_STRING = getTypeInfoFromTypeString(SerdeConstants.STRING_TYPE_NAME);
    public static final TypeInfo HIVE_TIMESTAMP = getTypeInfoFromTypeString(SerdeConstants.TIMESTAMP_TYPE_NAME);
    public static final TypeInfo HIVE_DATE = getTypeInfoFromTypeString(SerdeConstants.DATE_TYPE_NAME);
    public static final TypeInfo HIVE_BINARY = getTypeInfoFromTypeString(SerdeConstants.BINARY_TYPE_NAME);
    // inspired by https://github.com/apache/hive/blob/85f6162becb8723ff6c9f85875048ced6ca7ae89/storage-api/src/java/org/apache/hadoop/hive/common/type/HiveDecimal.java
    public static final DecimalTypeInfo HIVE_DECIMAL = new DecimalTypeInfo(DecimalTypeInfo.MAX_PRECISION, 18);

    public static TypeInfo arrayType(TypeInfo type)
    {
        return TypeInfoFactory.getListTypeInfo(type);
    }

    public static TypeInfo mapType(TypeInfo key, TypeInfo value)
    {
        return TypeInfoFactory.getMapTypeInfo(key, value);
    }

    public static TypeInfo structType(Collection<String> names, Collection<TypeInfo> types)
    {
        checkArgument(names.size() == types.size(), "Types and names must be the same size");
        return TypeInfoFactory.getStructTypeInfo(ImmutableList.copyOf(names), ImmutableList.copyOf(types));
    }

    public static TypeInfo adjustType(TypeInfo typeInfo)
    {
        if (isNullType(typeInfo)) {
            return HIVE_STRING;
        }
        if (typeInfo.equals(HIVE_BOOLEAN)) {
            return HIVE_BOOLEAN;
        }
        if (typeInfo.equals(HIVE_BYTE)) {
            return HIVE_BYTE;
        }
        if (typeInfo.equals(HIVE_SHORT)) {
            return HIVE_SHORT;
        }
        if (typeInfo.equals(HIVE_INT)) {
            return HIVE_INT;
        }
        if (typeInfo.equals(HIVE_LONG)) {
            return HIVE_LONG;
        }
        if (typeInfo.equals(HIVE_FLOAT)) {
            return HIVE_FLOAT;
        }
        if (typeInfo.equals(HIVE_DOUBLE)) {
            return HIVE_DOUBLE;
        }
        if (typeInfo.equals(HIVE_STRING)) {
            return HIVE_STRING;
        }
        if (typeInfo.equals(HIVE_TIMESTAMP)) {
            return HIVE_TIMESTAMP;
        }
        if (typeInfo.equals(HIVE_DATE)) {
            return HIVE_DATE;
        }
        if (typeInfo.equals(HIVE_BINARY)) {
            return HIVE_BINARY;
        }
        return typeInfo;
    }

    public static Optional<Column> toColumn(String name, TypeInfo typeInfo)
    {
        TypeInfo adjustedType = adjustType(typeInfo);
        // trino does not support any notion of null/void types, we should skip those kind of columns and nested nulls
        return removeAllRemainingNulls(adjustedType)
                .map(sanitizedType -> new Column(toLowerCase(name), new HiveType(sanitizedType)));
    }

    public static Map<String, TypeInfo> buildNameToInfoMap(List<String> st1FieldNames, List<TypeInfo> st1TypeInfos)
    {
        return IntStream.range(0, st1FieldNames.size())
                .mapToObj(index -> new SimpleEntry<>(st1FieldNames.get(index), st1TypeInfos.get(index)))
                .collect(toImmutableMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    public static boolean isStructType(TypeInfo t)
    {
        return t instanceof StructTypeInfo;
    }

    public static boolean isMapType(TypeInfo t)
    {
        return t instanceof MapTypeInfo;
    }

    public static boolean isArrayType(TypeInfo t)
    {
        return t instanceof ListTypeInfo;
    }

    public static boolean accept(DecimalTypeInfo decimalTypeInfo, TypeInfo other)
    {
        if (other == null || decimalTypeInfo.getClass() != other.getClass()) {
            return false;
        }

        DecimalTypeInfo dti = (DecimalTypeInfo) other;
        // Make sure "this" has enough integer room to accommodate other's integer digits.
        return decimalTypeInfo.precision() - decimalTypeInfo.scale() >= dti.precision() - dti.scale();
    }

    private static Optional<TypeInfo> removeAllRemainingNulls(TypeInfo typeInfo)
    {
        if (isNullType(typeInfo)) {
            return Optional.empty();
        }
        if (isStructType(typeInfo)) {
            StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
            Map<String, TypeInfo> fieldToTypeMap = buildNameToInfoMap(structTypeInfo.getAllStructFieldNames(), structTypeInfo.getAllStructFieldTypeInfos());
            List<SimpleEntry<String, TypeInfo>> sanitizedOrderedFields = fieldToTypeMap.entrySet()
                    .stream()
                    .map(e -> removeAllRemainingNulls(e.getValue()).map(sanitizedTypeInfo -> new SimpleEntry<>(e.getKey(), sanitizedTypeInfo)))
                    .flatMap(Optional::stream)
                    .collect(toImmutableList());

            List<String> fieldNames = sanitizedOrderedFields.stream().map(SimpleEntry::getKey).collect(toImmutableList());
            List<TypeInfo> fieldTypes = sanitizedOrderedFields.stream().map(SimpleEntry::getValue).collect(toImmutableList());

            return Optional.of(structType(fieldNames, fieldTypes));
        }
        if (isMapType(typeInfo)) {
            MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
            if (isNullType(mapTypeInfo.getMapKeyTypeInfo())) {
                return Optional.of(mapTypeInfo);
            }
            Optional<TypeInfo> sanitizedMapValueType = removeAllRemainingNulls(mapTypeInfo.getMapValueTypeInfo());
            return sanitizedMapValueType.map(mapValue -> mapType(mapTypeInfo.getMapKeyTypeInfo(), mapValue));
        }
        if (isArrayType(typeInfo)) {
            ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
            return removeAllRemainingNulls(listTypeInfo.getListElementTypeInfo())
                    .map(HiveTypes::arrayType);
        }
        return Optional.of(typeInfo);
    }

    private HiveTypes()
    {
    }
}
