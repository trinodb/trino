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
package io.trino.plugin.doris;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class DorisTypeMapper
{
    private static final int BIGINT_UNSIGNED_PRECISION = 20;
    private static final int MAX_DORIS_DATETIME_PRECISION = 6;

    private final DorisLargeintMapping largeintMapping;

    @Inject
    public DorisTypeMapper(DorisConfig config)
    {
        this.largeintMapping = requireNonNull(config, "config is null").getLargeintMapping();
    }

    public Type toTrinoType(DorisRemoteColumn column)
    {
        String normalizedType = normalizedBaseType(column);

        return switch (normalizedType) {
            case "BOOLEAN", "BOOL" -> BOOLEAN;
            case "TINYINT" -> isBooleanAlias(column) ? BOOLEAN : TINYINT;
            case "SMALLINT" -> SMALLINT;
            case "INT", "INTEGER" -> INTEGER;
            case "BIGINT" -> BIGINT;
            case "BIGINT_UNSIGNED" -> createDecimalType(BIGINT_UNSIGNED_PRECISION);
            case "LARGEINT" -> switch (largeintMapping) {
                // VARCHAR is the safe default because Doris LARGEINT can exceed Trino DECIMAL(38, 0).
                case VARCHAR -> createUnboundedVarcharType();
                case DECIMAL -> createDecimalType(MAX_PRECISION);
            };
            case "DECIMAL", "DECIMALV2", "DECIMALV3", "DECIMAL_V3", "DECIMAL32", "DECIMAL64", "DECIMAL128I", "DECIMAL128", "DECIMAL256" -> toDecimalType(column);
            case "FLOAT" -> REAL;
            case "DOUBLE" -> DOUBLE;
            case "CHAR" -> toCharType(column);
            case "VARCHAR" -> toVarcharType(column);
            case "DATE", "DATEV2", "DATE_V2" -> DATE;
            // Doris can report DATETIMEV2 through JDBC metadata as datetime(p) in COLUMN_TYPE.
            case "DATETIME" -> createTimestampType(legacyTimestampPrecision(column));
            case "DATETIMEV2", "DATETIME_V2" -> createTimestampType(timestampPrecision(column));
            case "STRING", "JSON", "JSONB", "IPV4", "IPV6" -> createUnboundedVarcharType();
            case "ARRAY", "MAP", "STRUCT", "VARIANT", "BITMAP", "HLL", "QUANTILE_STATE", "AGG_STATE" -> throw unsupportedComplexType(column);
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported Doris type '%s' for column '%s'".formatted(fullTypeDeclaration(column), column.columnName()));
        };
    }

    private static boolean isBooleanAlias(DorisRemoteColumn column)
    {
        String typeDefinition = column.typeDefinition()
                .map(value -> value.trim().toUpperCase(Locale.ENGLISH))
                .orElse("");
        return column.columnSize()
                .filter(size -> size <= 1)
                .isPresent() ||
                (column.columnSize().isEmpty() && typeDefinition.equals("TINYINT(1)")) ||
                typeDefinition.equals("BOOLEAN") ||
                typeDefinition.equals("BOOL") ||
                typeDefinition.startsWith("BOOLEAN(") ||
                typeDefinition.startsWith("BOOL(");
    }

    private static Type toDecimalType(DorisRemoteColumn column)
    {
        if (declaredPrecision(column).isEmpty()) {
            return createUnboundedVarcharType();
        }

        int precision = declaredPrecision(column).orElseThrow();
        int scale = max(declaredScale(column), 0);

        if (precision <= 0 || precision > MAX_PRECISION || scale > precision) {
            return createUnboundedVarcharType();
        }

        return createDecimalType(precision, scale);
    }

    private static Type toCharType(DorisRemoteColumn column)
    {
        if (declaredLength(column).isEmpty() || declaredLength(column).orElseThrow() <= 0) {
            return createUnboundedVarcharType();
        }

        int length = declaredLength(column).orElseThrow();
        if (length > CharType.MAX_LENGTH) {
            return createUnboundedVarcharType();
        }
        return createCharType(length);
    }

    private static Type toVarcharType(DorisRemoteColumn column)
    {
        if (declaredLength(column).isEmpty() || declaredLength(column).orElseThrow() <= 0) {
            return createUnboundedVarcharType();
        }

        int length = declaredLength(column).orElseThrow();
        if (length >= VarcharType.MAX_LENGTH) {
            return createUnboundedVarcharType();
        }
        return createVarcharType(length);
    }

    private static int timestampPrecision(DorisRemoteColumn column)
    {
        return min(max(column.decimalDigits().orElseGet(() -> typeParameters(column).stream().findFirst().orElse(0)), 0), MAX_DORIS_DATETIME_PRECISION);
    }

    private static int legacyTimestampPrecision(DorisRemoteColumn column)
    {
        return min(max(typeParameters(column).stream().findFirst().orElse(0), 0), MAX_DORIS_DATETIME_PRECISION);
    }

    private static Optional<Integer> declaredPrecision(DorisRemoteColumn column)
    {
        if (column.columnSize().isPresent()) {
            return column.columnSize();
        }
        return typeParameters(column).stream().findFirst();
    }

    private static int declaredScale(DorisRemoteColumn column)
    {
        if (column.decimalDigits().isPresent()) {
            return column.decimalDigits().orElseThrow();
        }

        List<Integer> parameters = typeParameters(column);
        if (parameters.size() >= 2) {
            return parameters.get(1);
        }
        return 0;
    }

    private static Optional<Integer> declaredLength(DorisRemoteColumn column)
    {
        if (column.columnSize().isPresent()) {
            return column.columnSize();
        }
        return typeParameters(column).stream().findFirst();
    }

    private static String normalizedBaseType(DorisRemoteColumn column)
    {
        String typeDeclaration = fullTypeDeclaration(column);
        int parametersStart = firstTypeParameterStart(typeDeclaration);
        if (parametersStart >= 0) {
            typeDeclaration = typeDeclaration.substring(0, parametersStart);
        }
        return typeDeclaration.trim()
                .toUpperCase(Locale.ENGLISH)
                .replace(' ', '_');
    }

    private static String fullTypeDeclaration(DorisRemoteColumn column)
    {
        return column.typeDefinition()
                .orElse(column.dataType())
                .trim();
    }

    private static int firstTypeParameterStart(String typeDeclaration)
    {
        int parenthesisStart = typeDeclaration.indexOf('(');
        int angleBracketStart = typeDeclaration.indexOf('<');
        if (parenthesisStart < 0) {
            return angleBracketStart;
        }
        if (angleBracketStart < 0) {
            return parenthesisStart;
        }
        return min(parenthesisStart, angleBracketStart);
    }

    private static TrinoException unsupportedComplexType(DorisRemoteColumn column)
    {
        return new TrinoException(
                NOT_SUPPORTED,
                "Doris type '%s' for column '%s' is not supported by the Doris connector yet".formatted(fullTypeDeclaration(column), column.columnName()));
    }

    private static List<Integer> typeParameters(DorisRemoteColumn column)
    {
        String typeDeclaration = column.typeDefinition().orElse(column.dataType()).trim();
        int parametersStart = typeDeclaration.indexOf('(');
        int parametersEnd = typeDeclaration.lastIndexOf(')');
        if (parametersStart < 0 || parametersEnd <= parametersStart) {
            return List.of();
        }

        return Arrays.stream(typeDeclaration.substring(parametersStart + 1, parametersEnd).split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .map(value -> {
                    try {
                        return Integer.parseInt(value);
                    }
                    catch (NumberFormatException ignored) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .toList();
    }
}
