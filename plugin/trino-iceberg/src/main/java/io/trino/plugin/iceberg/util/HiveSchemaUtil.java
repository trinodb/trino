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
package io.trino.plugin.iceberg.util;

import io.trino.metastore.type.TypeInfo;
import io.trino.spi.TrinoException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.DecimalType;

import static io.trino.metastore.type.TypeInfoUtils.getTypeInfoFromTypeString;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.stream.Collectors.joining;

// based on org.apache.iceberg.hive.HiveSchemaUtil
public final class HiveSchemaUtil
{
    private HiveSchemaUtil() {}

    public static TypeInfo convert(Type type)
    {
        return getTypeInfoFromTypeString(convertToTypeString(type));
    }

    private static String convertToTypeString(Type type)
    {
        return switch (type.typeId()) {
            case BOOLEAN -> "boolean";
            case INTEGER -> "int";
            case LONG -> "bigint";
            case FLOAT -> "float";
            case DOUBLE -> "double";
            case DATE -> "date";
            case TIME, STRING, UUID -> "string";
            case TIMESTAMP -> "timestamp";
            // TODO https://github.com/trinodb/trino/issues/19753 Support Iceberg timestamp types with nanosecond precision
            case TIMESTAMP_NANO -> throw new TrinoException(NOT_SUPPORTED, "Unsupported Iceberg type: TIMESTAMP_NANO");
            case FIXED, BINARY -> "binary";
            case DECIMAL -> "decimal(%s,%s)".formatted(((DecimalType) type).precision(), ((DecimalType) type).scale());
            case UNKNOWN -> throw new TrinoException(NOT_SUPPORTED, "Unsupported Iceberg type: UNKNOWN");
            // TODO https://github.com/trinodb/trino/issues/24538 Support variant type
            case VARIANT -> throw new TrinoException(NOT_SUPPORTED, "Unsupported Iceberg type: VARIANT");
            case LIST -> "array<%s>".formatted(convert(type.asListType().elementType()));
            case MAP -> "map<%s,%s>".formatted(convert(type.asMapType().keyType()), convert(type.asMapType().valueType()));
            case STRUCT -> "struct<%s>".formatted(type.asStructType().fields().stream()
                    .map(field -> "%s:%s".formatted(field.name(), convert(field.type())))
                    .collect(joining(",")));
        };
    }
}
