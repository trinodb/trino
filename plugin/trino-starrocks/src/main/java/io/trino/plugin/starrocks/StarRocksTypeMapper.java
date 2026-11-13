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
package io.trino.plugin.starrocks;

import com.google.inject.Inject;
import io.trino.spi.type.CharType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

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
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class StarRocksTypeMapper
{
    private static final int MAX_STARROCKS_TIMESTAMP_PRECISION = 6;

    private final Type jsonType;

    @Inject
    public StarRocksTypeMapper(TypeManager typeManager)
    {
        this.jsonType = requireNonNull(typeManager, "typeManager is null").getType(new TypeSignature(StandardTypes.JSON));
    }

    public Type toTrinoType(StarRocksRemoteColumn column)
    {
        String normalizedType = normalizedBaseType(column);

        return switch (normalizedType) {
            case "BOOLEAN", "BOOL" -> BOOLEAN;
            case "TINYINT" -> isUnsignedIntegral(column) ? SMALLINT : TINYINT;
            case "SMALLINT" -> isUnsignedIntegral(column) ? INTEGER : SMALLINT;
            case "INT", "INTEGER" -> isUnsignedIntegral(column) ? BIGINT : INTEGER;
            case "BIGINT" -> isUnsignedIntegral(column) ? createUnboundedVarcharType() : BIGINT;
            case "LARGEINT" -> createUnboundedVarcharType();
            case "DECIMAL", "DECIMALV2", "DECIMALV3", "DECIMAL32", "DECIMAL64", "DECIMAL128", "DECIMAL128I", "DECIMAL256" -> toDecimalType(column);
            case "FLOAT" -> REAL;
            case "DOUBLE" -> DOUBLE;
            case "CHAR" -> toCharType(column);
            case "VARCHAR" -> toVarcharType(column);
            case "JSON", "VARIANT" -> jsonType;
            case "ARRAY", "MAP", "STRUCT" -> createUnboundedVarcharType();
            case "STRING", "TEXT", "BITMAP", "HLL", "PERCENTILE", "PERCENTILE_UNION" -> createUnboundedVarcharType();
            case "BINARY", "VARBINARY" -> VARBINARY;
            case "DATE" -> DATE;
            case "DATETIME", "DATETIMEV2", "TIMESTAMP" -> createTimestampType(timestampPrecision(column));
            default -> createUnboundedVarcharType();
        };
    }

    private static Type toDecimalType(StarRocksRemoteColumn column)
    {
        Optional<Integer> precision = declaredPrecision(column);
        if (precision.isEmpty()) {
            return createUnboundedVarcharType();
        }

        int scale = max(declaredScale(column), 0);
        if (precision.orElseThrow() <= 0 || precision.orElseThrow() > MAX_PRECISION || scale > precision.orElseThrow()) {
            return createUnboundedVarcharType();
        }
        return createDecimalType(precision.orElseThrow(), scale);
    }

    private static Type toCharType(StarRocksRemoteColumn column)
    {
        if (declaredLength(column).isEmpty() || declaredLength(column).orElseThrow() <= 0) {
            return createUnboundedVarcharType();
        }
        return createCharType(min(declaredLength(column).orElseThrow(), CharType.MAX_LENGTH));
    }

    private static Type toVarcharType(StarRocksRemoteColumn column)
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

    private static int timestampPrecision(StarRocksRemoteColumn column)
    {
        return min(max(column.decimalDigits().orElseGet(() -> typeParameters(column).stream().findFirst().orElse(0)), 0), MAX_STARROCKS_TIMESTAMP_PRECISION);
    }

    private static Optional<Integer> declaredPrecision(StarRocksRemoteColumn column)
    {
        if (column.columnSize().isPresent()) {
            return column.columnSize();
        }
        return typeParameters(column).stream().findFirst();
    }

    private static int declaredScale(StarRocksRemoteColumn column)
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

    private static Optional<Integer> declaredLength(StarRocksRemoteColumn column)
    {
        if (column.columnSize().isPresent()) {
            return column.columnSize();
        }
        return typeParameters(column).stream().findFirst();
    }

    private static String normalizedBaseType(StarRocksRemoteColumn column)
    {
        String typeDeclaration = fullTypeDeclaration(column);
        int parametersStart = firstTypeParameterStart(typeDeclaration);
        if (parametersStart >= 0) {
            typeDeclaration = typeDeclaration.substring(0, parametersStart);
        }
        String normalized = typeDeclaration.trim()
                .toUpperCase(Locale.ENGLISH);
        if (normalized.endsWith(" UNSIGNED")) {
            normalized = normalized.substring(0, normalized.length() - " UNSIGNED".length());
        }
        return normalized
                .replace(' ', '_');
    }

    private static boolean isUnsignedIntegral(StarRocksRemoteColumn column)
    {
        return fullTypeDeclaration(column)
                .trim()
                .toUpperCase(Locale.ENGLISH)
                .contains("UNSIGNED");
    }

    private static String fullTypeDeclaration(StarRocksRemoteColumn column)
    {
        return column.typeDefinition()
                .orElse(column.typeName())
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

    private static List<Integer> typeParameters(StarRocksRemoteColumn column)
    {
        String typeDeclaration = fullTypeDeclaration(column);
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
