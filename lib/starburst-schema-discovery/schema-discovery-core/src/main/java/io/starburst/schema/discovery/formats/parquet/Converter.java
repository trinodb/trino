/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.formats.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.trino.plugin.hive.type.TypeInfo;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_BINARY;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_BOOLEAN;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_DOUBLE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_FLOAT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_INT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_LONG;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_TIMESTAMP;
import static io.starburst.schema.discovery.internal.HiveTypes.STRING_TYPE;

class Converter
{
    private static final Map<PrimitiveType.PrimitiveTypeName, TypeInfo> staticConversions = ImmutableMap.<PrimitiveType.PrimitiveTypeName, TypeInfo>builder()
            .put(PrimitiveType.PrimitiveTypeName.BOOLEAN, HIVE_BOOLEAN)
            .put(PrimitiveType.PrimitiveTypeName.FLOAT, HIVE_FLOAT)
            .put(PrimitiveType.PrimitiveTypeName.DOUBLE, HIVE_DOUBLE)
            .put(PrimitiveType.PrimitiveTypeName.BINARY, HIVE_BINARY)
            .put(PrimitiveType.PrimitiveTypeName.INT32, HIVE_INT)
            .put(PrimitiveType.PrimitiveTypeName.INT64, HIVE_LONG)
            .put(PrimitiveType.PrimitiveTypeName.INT96, HIVE_TIMESTAMP)
            .buildOrThrow();

    static TypeInfo fromParquetType(org.apache.parquet.schema.Type type)
    {
        if (type.getLogicalTypeAnnotation() != null) {
            return type.getLogicalTypeAnnotation().accept(new TypeVisitor(type)).orElse(STRING_TYPE);
        }
        if (type.isPrimitive()) {
            return staticConversions.getOrDefault(type.asPrimitiveType().getPrimitiveTypeName(), STRING_TYPE);
        }
        if (type instanceof GroupType) {
            return fromGroupType(type);
        }
        return STRING_TYPE;
    }

    private static TypeInfo fromGroupType(org.apache.parquet.schema.Type type)
    {
        GroupType group = (GroupType) type;
        ImmutableList<String> names = group.getFields()
                .stream()
                .map(Type::getName)
                .collect(toImmutableList());
        ImmutableList<TypeInfo> fields = group.getFields()
                .stream()
                .map(Converter::fromParquetType)
                .collect(toImmutableList());
        return HiveTypes.structType(names, fields);
    }

    private Converter()
    {
    }
}
