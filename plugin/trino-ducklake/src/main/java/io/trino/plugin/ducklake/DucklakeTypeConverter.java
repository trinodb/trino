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
package io.trino.plugin.ducklake;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeParameter;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Converts Ducklake type strings to Trino types.
 * Handles all Ducklake primitive, nested, and special types.
 */
public class DucklakeTypeConverter
{
    private static final Pattern DECIMAL_PATTERN = Pattern.compile("decimal\\((\\d+),\\s*(\\d+)\\)");

    private final TypeManager typeManager;

    @Inject
    public DucklakeTypeConverter(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    /**
     * Convert Ducklake type string to Trino Type
     */
    public Type toTrinoType(String ducklakeType)
    {
        requireNonNull(ducklakeType, "ducklakeType is null");

        String normalizedType = ducklakeType.trim().toLowerCase();

        if (normalizedType.startsWith("list<") && normalizedType.endsWith(">")) {
            String elementType = extractTypeArguments(normalizedType, "list").trim();
            return new ArrayType(toTrinoType(elementType));
        }

        if (normalizedType.startsWith("struct<") && normalizedType.endsWith(">")) {
            String fieldsStr = extractTypeArguments(normalizedType, "struct");
            List<String> fieldParts = splitTopLevelCommas(fieldsStr);
            ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
            for (String fieldPart : fieldParts) {
                int colonIndex = fieldPart.indexOf(':');
                if (colonIndex < 0) {
                    throw new TrinoException(NOT_SUPPORTED, format("Invalid struct field (missing colon): %s", fieldPart));
                }
                String fieldName = fieldPart.substring(0, colonIndex).trim();
                String fieldType = fieldPart.substring(colonIndex + 1).trim();
                fields.add(new RowType.Field(Optional.of(fieldName), toTrinoType(fieldType)));
            }
            return RowType.from(fields.build());
        }

        if (normalizedType.startsWith("map<") && normalizedType.endsWith(">")) {
            String argsStr = extractTypeArguments(normalizedType, "map");
            List<String> parts = splitTopLevelCommas(argsStr);
            if (parts.size() != 2) {
                throw new TrinoException(NOT_SUPPORTED, format("Invalid map type (expected 2 type arguments, got %d): %s", parts.size(), ducklakeType));
            }
            TypeSignature keySignature = toTrinoType(parts.get(0).trim()).getTypeSignature();
            TypeSignature valueSignature = toTrinoType(parts.get(1).trim()).getTypeSignature();
            return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                    TypeParameter.typeParameter(keySignature),
                    TypeParameter.typeParameter(valueSignature)));
        }

        return switch (normalizedType) {
            // Boolean
            case "boolean" -> BooleanType.BOOLEAN;

            // Signed integers
            case "int8" -> TinyintType.TINYINT;
            case "int16" -> SmallintType.SMALLINT;
            case "int32" -> IntegerType.INTEGER;
            case "int64" -> BigintType.BIGINT;

            // Unsigned integers - map to next larger signed type
            // TODO: Add validation to ensure values don't exceed signed range
            case "uint8" -> SmallintType.SMALLINT;  // 0..255 -> -32768..32767
            case "uint16" -> IntegerType.INTEGER;    // 0..65535 -> -2^31..2^31-1
            case "uint32" -> BigintType.BIGINT;      // 0..2^32-1 -> -2^63..2^63-1
            case "uint64" -> DecimalType.createDecimalType(20, 0); // 0..2^64-1 -> decimal(20,0)

            // Floating point
            case "float32" -> RealType.REAL;
            case "float64" -> DoubleType.DOUBLE;

            // Temporal types
            case "date" -> DateType.DATE;
            case "time" -> TimeType.TIME_MICROS;  // Ducklake uses microsecond precision
            case "timetz" -> TimeWithTimeZoneType.TIME_TZ_MICROS;
            case "timestamp" -> TimestampType.TIMESTAMP_MICROS;
            case "timestamptz" -> TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
            case "timestamp_s" -> TimestampType.createTimestampType(0);   // second precision
            case "timestamp_ms" -> TimestampType.createTimestampType(3);  // millisecond precision
            case "timestamp_ns" -> TimestampType.createTimestampType(9);  // nanosecond precision

            // String types
            case "varchar" -> VarcharType.VARCHAR;
            case "blob" -> VarbinaryType.VARBINARY;
            case "uuid" -> UuidType.UUID;

            // ⚠️ DEGRADED SEMANTICS - MVP PLACEHOLDERS ONLY
            // These mappings allow basic data access but DO NOT provide full type support:
            // - No type-specific operators or functions
            // - No validation or type safety
            // - Data is accessible but with reduced functionality
            case "json" -> VarcharType.VARCHAR;  // DEGRADED: No JSON operators/functions, stored as string
            case "variant" -> VarcharType.VARCHAR;  // DEGRADED: No variant support, needs shredding implementation
            case "interval" -> VarcharType.VARCHAR;  // DEGRADED: Interval not supported in Trino, stored as string

            // DEGRADED: Geometry types stored as raw bytes, no spatial functions
            case "geometry", "point", "linestring", "polygon",
                 "multipoint", "multilinestring", "multipolygon",
                 "linestring z", "geometrycollection" -> VarbinaryType.VARBINARY;

            default -> {
                // Check for decimal(P,S)
                Matcher decimalMatcher = DECIMAL_PATTERN.matcher(normalizedType);
                if (decimalMatcher.matches()) {
                    int precision = Integer.parseInt(decimalMatcher.group(1));
                    int scale = Integer.parseInt(decimalMatcher.group(2));
                    yield DecimalType.createDecimalType(precision, scale);
                }

                throw new TrinoException(NOT_SUPPORTED, format("Unsupported Ducklake type: %s", ducklakeType));
            }
        };
    }

    /**
     * Convert Trino type to Ducklake type string (for write operations)
     */
    public String toDucklakeType(Type trinoType)
    {
        requireNonNull(trinoType, "trinoType is null");

        if (trinoType.equals(BooleanType.BOOLEAN)) {
            return "boolean";
        }
        if (trinoType.equals(TinyintType.TINYINT)) {
            return "int8";
        }
        if (trinoType.equals(SmallintType.SMALLINT)) {
            return "int16";
        }
        if (trinoType.equals(IntegerType.INTEGER)) {
            return "int32";
        }
        if (trinoType.equals(BigintType.BIGINT)) {
            return "int64";
        }
        if (trinoType.equals(RealType.REAL)) {
            return "float32";
        }
        if (trinoType.equals(DoubleType.DOUBLE)) {
            return "float64";
        }
        if (trinoType instanceof DecimalType decimalType) {
            return format("decimal(%d,%d)", decimalType.getPrecision(), decimalType.getScale());
        }
        if (trinoType.equals(DateType.DATE)) {
            return "date";
        }
        if (trinoType instanceof TimestampType timestampType) {
            return switch (timestampType.getPrecision()) {
                case 0 -> "timestamp_s";
                case 3 -> "timestamp_ms";
                case 6 -> "timestamp";
                case 9 -> "timestamp_ns";
                default -> "timestamp";
            };
        }
        if (trinoType instanceof TimestampWithTimeZoneType) {
            return "timestamptz";
        }
        if (trinoType instanceof TimeType) {
            return "time";
        }
        if (trinoType instanceof TimeWithTimeZoneType) {
            return "timetz";
        }
        if (trinoType.equals(VarcharType.VARCHAR) || trinoType instanceof VarcharType) {
            return "varchar";
        }
        if (trinoType.equals(VarbinaryType.VARBINARY)) {
            return "blob";
        }
        if (trinoType.equals(UuidType.UUID)) {
            return "uuid";
        }

        // TODO: Handle nested types (ArrayType, RowType, MapType)

        throw new TrinoException(NOT_SUPPORTED, format("Unsupported Trino type: %s", trinoType));
    }

    // Extracts the inner type arguments from a parameterized type string (e.g. the content between the outermost angle brackets)
    private static String extractTypeArguments(String typeString, String prefix)
    {
        // prefix< ... >
        return typeString.substring(prefix.length() + 1, typeString.length() - 1);
    }

    // Splits a string at top-level commas, respecting nested angle brackets
    private static List<String> splitTopLevelCommas(String input)
    {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == '<') {
                depth++;
            }
            else if (c == '>') {
                depth--;
            }
            else if (c == ',' && depth == 0) {
                parts.add(input.substring(start, i).trim());
                start = i + 1;
            }
        }
        parts.add(input.substring(start).trim());
        return parts;
    }
}
