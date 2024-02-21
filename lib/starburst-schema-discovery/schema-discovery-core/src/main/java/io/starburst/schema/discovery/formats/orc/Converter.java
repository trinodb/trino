/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.formats.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.SchemaDiscoveryErrorCode;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.trino.orc.OrcColumn;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.OrcType;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.plugin.hive.type.TypeInfoFactory;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.schema.discovery.SchemaDiscoveryErrorCode.UNEXPECTED_COLUMN_STATE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_BINARY;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_BOOLEAN;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_BYTE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_DATE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_DOUBLE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_FLOAT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_INT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_LONG;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_SHORT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_TIMESTAMP;
import static io.starburst.schema.discovery.internal.HiveTypes.STRING_TYPE;
import static io.trino.plugin.hive.type.TypeInfoFactory.getDecimalTypeInfo;

class Converter
{
    private static final Map<OrcType.OrcTypeKind, TypeInfo> staticConversions = ImmutableMap.<OrcType.OrcTypeKind, TypeInfo>builder()
            .put(OrcType.OrcTypeKind.BOOLEAN, HIVE_BOOLEAN)
            .put(OrcType.OrcTypeKind.SHORT, HIVE_SHORT)
            .put(OrcType.OrcTypeKind.INT, HIVE_INT)
            .put(OrcType.OrcTypeKind.LONG, HIVE_LONG)
            .put(OrcType.OrcTypeKind.FLOAT, HIVE_FLOAT)
            .put(OrcType.OrcTypeKind.DOUBLE, HIVE_DOUBLE)
            .put(OrcType.OrcTypeKind.STRING, STRING_TYPE)
            .put(OrcType.OrcTypeKind.VARCHAR, STRING_TYPE)
            .put(OrcType.OrcTypeKind.DATE, HIVE_DATE)
            .put(OrcType.OrcTypeKind.TIMESTAMP, HIVE_TIMESTAMP)
            .put(OrcType.OrcTypeKind.TIMESTAMP_INSTANT, HIVE_TIMESTAMP)
            .put(OrcType.OrcTypeKind.BINARY, HIVE_BINARY)
            .put(OrcType.OrcTypeKind.BYTE, HIVE_BYTE)
            .buildOrThrow();

    static TypeInfo fromOrcType(OrcColumn orcColumn, List<OrcType> typesLookup)
    {
        return switch (orcColumn.getColumnType()) {
            case LIST -> fromOrcListType(orcColumn.getNestedColumns(), typesLookup);
            case MAP -> fromOrcMapType(orcColumn.getNestedColumns(), typesLookup);
            case STRUCT -> fromOrcStructType(orcColumn.getNestedColumns(), typesLookup);
            case UNION -> throw new TrinoException(SchemaDiscoveryErrorCode.UNION_NOT_SUPPORTED, "UNION not supported");
            case CHAR -> fromOrcCharType(orcColumn, typesLookup);
            case DECIMAL -> fromOrcDecimalType(orcColumn, typesLookup);
            default -> staticConversions.getOrDefault(orcColumn.getColumnType(), STRING_TYPE);
        };
    }

    private static TypeInfo fromOrcDecimalType(OrcColumn orcColumn, List<OrcType> typesLookup)
    {
        OrcColumnId orcColumnId = orcColumn.getColumnId();
        if (typesLookup.size() <= orcColumnId.getId()) {
            return STRING_TYPE;
        }
        OrcType orcType = typesLookup.get(orcColumnId.getId());
        Optional<Integer> precision = orcType.getPrecision();
        Optional<Integer> scale = orcType.getScale();
        if (precision.isEmpty() || scale.isEmpty()) {
            return STRING_TYPE;
        }
        return getDecimalTypeInfo(precision.get(), scale.get());
    }

    private static TypeInfo fromOrcCharType(OrcColumn orcColumn, List<OrcType> typesLookup)
    {
        OrcColumnId orcColumnId = orcColumn.getColumnId();
        if (typesLookup.size() <= orcColumnId.getId()) {
            return STRING_TYPE;
        }
        OrcType orcType = typesLookup.get(orcColumnId.getId());
        return orcType.getLength()
                .filter(length -> length >= 1 && length <= 255)
                .<TypeInfo>map(TypeInfoFactory::getCharTypeInfo)
                .orElse(STRING_TYPE);
    }

    private static TypeInfo fromOrcListType(List<OrcColumn> nestedColumns, List<OrcType> typesLookup)
    {
        if (nestedColumns.size() != 1) {
            throw new TrinoException(UNEXPECTED_COLUMN_STATE, "Expected 1 nested column for LIST. Instead found: " + nestedColumns.size());
        }
        return HiveTypes.arrayType(fromOrcType(nestedColumns.get(0), typesLookup));
    }

    private static TypeInfo fromOrcMapType(List<OrcColumn> nestedColumns, List<OrcType> typesLookup)
    {
        if (nestedColumns.size() != 2) {
            throw new TrinoException(UNEXPECTED_COLUMN_STATE, "Expected 2 nested columns for MAP. Instead found: " + nestedColumns.size());
        }
        return HiveTypes.mapType(fromOrcType(nestedColumns.get(0), typesLookup), fromOrcType(nestedColumns.get(1), typesLookup));
    }

    private static TypeInfo fromOrcStructType(List<OrcColumn> nestedColumns, List<OrcType> typesLookup)
    {
        ImmutableList<String> names = nestedColumns.stream()
                .map(OrcColumn::getColumnName)
                .collect(toImmutableList());
        ImmutableList<TypeInfo> fields = nestedColumns.stream()
                .map(orcColumn -> fromOrcType(orcColumn, typesLookup))
                .collect(toImmutableList());
        return HiveTypes.structType(names, fields);
    }

    private Converter()
    {
    }
}
