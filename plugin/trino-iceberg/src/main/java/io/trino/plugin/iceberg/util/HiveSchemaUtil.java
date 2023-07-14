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

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.DecimalType;

import static java.util.stream.Collectors.joining;

// based on org.apache.iceberg.hive.HiveSchemaUtil
public final class HiveSchemaUtil
{
    private HiveSchemaUtil() {}

    public static String convertToTypeString(Type type)
    {
        return switch (type.typeId()) {
            case BOOLEAN -> "boolean";
            case INTEGER -> "int";
            case LONG -> "bigint";
            case FLOAT -> "float";
            case DOUBLE -> "double";
            case STRING -> "string";
            case DATE -> "date";
            case TIME -> "time";
            case TIMESTAMP -> ((Types.TimestampType) type).shouldAdjustToUTC() ? "timestamp with time zone" : "timestamp";
            case FIXED, BINARY -> "binary";
            case UUID -> "uuid";
            case DECIMAL -> "decimal(%s,%s)".formatted(((DecimalType) type).precision(), ((DecimalType) type).scale());
            case LIST -> "array<%s>".formatted(convertToTypeString(type.asListType().elementType()));
            case MAP -> "map<%s,%s>".formatted(convertToTypeString(type.asMapType().keyType()), convertToTypeString(type.asMapType().valueType()));
            case STRUCT -> "struct<%s>".formatted(type.asStructType().fields().stream()
                    .map(field -> "%s:%s".formatted(field.name(), convertToTypeString(field.type())))
                    .collect(joining(",")));
        };
    }
}
