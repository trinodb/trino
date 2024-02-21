/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.generation;

import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.formats.csv.CsvFlags;
import io.starburst.schema.discovery.infer.TypeCoercion;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.TableFormat;
import io.trino.plugin.hive.type.ListTypeInfo;
import io.trino.plugin.hive.type.MapTypeInfo;
import io.trino.plugin.hive.type.StructTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.spi.type.RealType;
import io.trino.spi.type.VarbinaryType;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.starburst.schema.discovery.generation.SqlType.sqlType;

class Mapping
{
    private Mapping() {}

    private static final Map<TypeInfo, SqlType> simpleTypeConversions = ImmutableMap.of(
            HiveTypes.STRING_TYPE, sqlType("varchar"),
            HiveTypes.HIVE_BINARY, sqlType(VarbinaryType.VARBINARY),
            HiveTypes.HIVE_FLOAT, sqlType(RealType.REAL));

    static SqlType convert(TypeInfo typeInfo)
    {
        return switch (typeInfo.getCategory()) {
            case PRIMITIVE -> simpleTypeConversions.getOrDefault(typeInfo, sqlType(typeInfo));
            case LIST -> new InternalListTypeInfo((ListTypeInfo) typeInfo);
            case MAP -> new InternalMapTypeInfo((MapTypeInfo) typeInfo);
            case STRUCT -> new InternalStructTypeInfo((StructTypeInfo) typeInfo);
            default -> sqlType(typeInfo);
        };
    }

    static String columnType(Column column)
    {
        return convert(column.type().typeInfo()).getSqlType();
    }

    static String tableFormat(DiscoveredTable table)
    {
        if (table.format() == TableFormat.CSV) {
            if (table.columns().flags().contains(CsvFlags.HAS_QUOTED_FIELDS)) {
                return TableFormat.CSV.name();
            }
            return "TEXTFILE";
        }
        return table.format().name();
    }

    static <T> String commaList(Stream<T> values, Function<T, String> mapper)
    {
        return values.map(mapper).map(Mapping::quote).collect(Collectors.joining(", "));
    }

    static String quote(String s)
    {
        return "'" + s + "'";
    }

    static String doubleQuote(String s)
    {
        return "\"" + s + "\"";
    }

    static String partitionValue(Column column, String value)
    {
        if (value == null) {
            return "null";
        }
        if (TypeCoercion.isNumericType(column.type().typeInfo())) {
            return value;
        }
        return quote(value);
    }
}
