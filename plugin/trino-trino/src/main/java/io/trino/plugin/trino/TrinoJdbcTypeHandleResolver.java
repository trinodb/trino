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
package io.trino.plugin.trino;

import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.sql.Types;
import java.util.Locale;
import java.util.Optional;

final class TrinoJdbcTypeHandleResolver
{
    private TrinoJdbcTypeHandleResolver() {}

    static JdbcTypeHandle resolve(TypeManager typeManager, String typeName)
    {
        String normalizedTypeName = normalizedTypeName(typeName);
        Type parsedType = TrinoTypeNameParser.parseTypeName(typeName, typeManager);
        int jdbcType = switch (normalizedTypeName) {
            case "boolean" -> Types.BOOLEAN;
            case "tinyint" -> Types.TINYINT;
            case "smallint" -> Types.SMALLINT;
            case "integer" -> Types.INTEGER;
            case "bigint" -> Types.BIGINT;
            case "real" -> Types.REAL;
            case "double" -> Types.DOUBLE;
            case "date" -> Types.DATE;
            case "time with time zone" -> Types.TIME_WITH_TIMEZONE;
            case "timestamp with time zone" -> Types.TIMESTAMP_WITH_TIMEZONE;
            case "varbinary" -> Types.VARBINARY;
            case "number" -> Types.OTHER;
            case "json", "uuid", "ipaddress", "hyperloglog", "p4hyperloglog", "setdigest", "tdigest" -> Types.JAVA_OBJECT;
            default -> inferJdbcType(normalizedTypeName, parsedType);
        };
        return new JdbcTypeHandle(jdbcType, Optional.of(typeName), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    static String normalizedTypeName(String typeName)
    {
        return typeName.trim().toLowerCase(Locale.ENGLISH);
    }

    private static int inferJdbcType(String normalizedTypeName, Type parsedType)
    {
        if (normalizedTypeName.startsWith("varchar")) {
            return Types.VARCHAR;
        }
        if (normalizedTypeName.startsWith("char")) {
            return Types.CHAR;
        }
        if (normalizedTypeName.startsWith("decimal")) {
            return Types.DECIMAL;
        }
        if (normalizedTypeName.startsWith("time(") && normalizedTypeName.contains("with time zone")) {
            return Types.TIME_WITH_TIMEZONE;
        }
        if (normalizedTypeName.startsWith("time(")) {
            return Types.TIME;
        }
        if (normalizedTypeName.startsWith("timestamp(") && normalizedTypeName.contains("with time zone")) {
            return Types.TIMESTAMP_WITH_TIMEZONE;
        }
        if (normalizedTypeName.startsWith("timestamp")) {
            return Types.TIMESTAMP;
        }
        if (normalizedTypeName.startsWith("array(")) {
            return Types.ARRAY;
        }
        if (normalizedTypeName.startsWith("map(") || normalizedTypeName.startsWith("row(") || normalizedTypeName.startsWith("qdigest(")) {
            return Types.JAVA_OBJECT;
        }
        if (parsedType != null && (TrinoTypeClassifier.isIntervalYearToMonthType(parsedType) || TrinoTypeClassifier.isIntervalDayToSecondType(parsedType))) {
            return Types.VARCHAR;
        }
        return Types.JAVA_OBJECT;
    }
}
