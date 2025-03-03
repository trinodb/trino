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

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.type.JsonType;
import org.apache.arrow.vector.FieldVector;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

public class StarrocksTypeMapper
{
    private StarrocksTypeMapper() {}

    public static BlockBuilder convert(FieldVector fieldVector, Type type, int rowCount, int dataPosition, BlockBuilder blockBuilder)
    {
        if (type instanceof DecimalType) {
            int precision = ((DecimalType) type).getPrecision();
            int scale = ((DecimalType) type).getScale();
            type.createBlockBuilder(null, rowCount);
            ArrowFieldConverter converter = new ArrowConverter.DynamicDecimalConverter(precision, scale);
            return converter.convert(fieldVector, type, rowCount, dataPosition, blockBuilder);
        }
        ArrowFieldConverter converter = ArrowConverter.typeConverter.get(type.getClass());
        if (converter == null) {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        blockBuilder = type.createBlockBuilder(null, rowCount);
        return converter.convert(fieldVector, type, rowCount, dataPosition, blockBuilder);
    }

    public static Type toTrinoType(String starrocksType, String starrocksColumnType, int precision, int scale)
    {
        String toLowerString = starrocksType.toLowerCase(Locale.ROOT);
        if (isSemiStructure(toLowerString)) {
            return mappingSemiStructure(starrocksColumnType);
        }
        else {
            return mappingBasicType(starrocksColumnType);
        }
    }

    public static Type mappingBasicType(String starrocksColumnType)
    {
        String type = starrocksColumnType.toLowerCase(Locale.ROOT);
        if (type.startsWith("decimal")) {
            String content = StringUtils.substringBetween(starrocksColumnType, "(", ")");
            int precision = Integer.parseInt(content.split(",")[0].trim());
            int scale = Integer.parseInt(content.split(",")[1].trim());
            return DecimalType.createDecimalType(precision, scale);
        }
        type = removeParentheses(type);
        switch (type) {
            case "boolean" -> {
                return BooleanType.BOOLEAN;
            }
            case "tinyint" -> {
                if (starrocksColumnType.equals("tinyint(1)")) {
                    return BooleanType.BOOLEAN;
                }
                return TinyintType.TINYINT;
            }
            case "smallint" -> {
                return SmallintType.SMALLINT;
            }
            case "int", "integer" -> {
                return IntegerType.INTEGER;
            }
            case "bigint" -> {
                return BigintType.BIGINT;
            }
            case "bigint unsigned" -> {
                return DecimalType.createDecimalType(38, 0);
            }
            case "float" -> {
                return RealType.REAL;
            }
            case "double" -> {
                return DoubleType.DOUBLE;
            }
            case "date" -> {
                return DateType.DATE;
            }
            case "datetime" -> {
                return TimestampType.TIMESTAMP_MILLIS;
            }
            case "char", "varchar", "string" -> {
                return VarcharType.VARCHAR;
            }
            case "varbinary" -> {
                return VarbinaryType.VARBINARY;
            }
            case "largeint" -> {
                return DecimalType.createDecimalType();
            }
            case "json" -> {
                return JsonType.JSON;
            }
            default -> throw new UnsupportedOperationException("Unsupported StarRocks type: " + starrocksColumnType);
        }
    }

    public static Type mappingSemiStructure(String columnType)
    {
        if (columnType.startsWith("map")) {
            Type[] mapElementTypes = getMapElementType(columnType);
            return new MapType(mapElementTypes[0], mapElementTypes[1], new TypeOperators());
        }
        else if (columnType.startsWith("array")) {
            return new ArrayType(getArrayElementType(columnType));
        }
        else if (columnType.startsWith("struct")) {
            List<RowType.Field> rowElementTypes = getRowElementType(columnType);
            return RowType.from(rowElementTypes);
        }
        throw new UnsupportedOperationException("Unsupported StarRocks type: " + columnType);
    }

    static final Set<String> basicType = Set.of("boolean", "tinyint", "smallint", "int",
            "integer", "bigint", "bigint unsigned", "float", "decimal", "decimal32", "decimal64",
            "decimal128", "decimalv2",
            "double", "date", "datetime", "char", "varchar",
            "string", "varbinary", "largeint", "json");

    static final Set<String> semiStructure = Set.of("array", "map", "struct");

    public static boolean isBasicType(String starrocksType)
    {
        return basicType.contains(starrocksType);
    }

    public static boolean isSemiStructure(String starrocksType)
    {
        return starrocksType.startsWith("map") || starrocksType.startsWith("array") || starrocksType.startsWith("struct");
    }

    public static Type getArrayElementType(String statement)
    {
        int start = statement.indexOf('<');
        int end = statement.lastIndexOf('>');
        String type = statement.substring(start + 1, end);
        if (basicType.contains(removeParentheses(type).toLowerCase(Locale.ROOT))) {
            return mappingBasicType(type);
        }
        return mappingSemiStructure(type);
    }

    public static List<RowType.Field> getRowElementType(String statement)
    {
        int start = statement.indexOf('<');
        int end = statement.lastIndexOf('>');
        String type = statement.substring(start + 1, end);
        List<Pair<String, String>> pair = new ArrayList<>();
        char[] typeArray = type.toCharArray();
        Deque<Character> bracket = new ArrayDeque<>();
        HashMap<Character, Character> bracketMap = new HashMap<>();
        bracketMap.put('(', ')');
        bracketMap.put(')', '(');
        bracketMap.put('<', '>');
        bracketMap.put('>', '<');
        int lastIndex = 0;
        for (int i = 0; i < typeArray.length; i++) {
            if (typeArray[i] == ',') {
                if (bracket.isEmpty()) {
                    String tempElement = type.substring(lastIndex, i).trim();
                    pair.add(Pair.of(
                            tempElement.substring(0, tempElement.indexOf(" ")).trim(),
                            tempElement.substring(tempElement.indexOf(" ")).trim()));
                    lastIndex = i + 1;
                }
                continue;
            }
            if (i == typeArray.length - 1) {
                String tempElement = type.substring(lastIndex, i + 1).trim();
                pair.add(Pair.of(
                        tempElement.substring(0, tempElement.indexOf(" ")).trim(),
                        tempElement.substring(tempElement.indexOf(" ")).trim()));
            }
            if (bracketMap.containsKey(typeArray[i])) {
                if (bracket.isEmpty()) {
                    bracket.push(typeArray[i]);
                    continue;
                }
                if (bracket.peek() == bracketMap.get(typeArray[i])) {
                    bracket.pop();
                }
                else {
                    bracket.push(typeArray[i]);
                }
            }
        }
        return pair.stream().map(element -> {
            if (basicType.contains(removeParentheses(element.getRight()))) {
                return new RowType.Field(Optional.of(element.getLeft()), mappingBasicType(element.getRight()));
            }
            return new RowType.Field(Optional.of(element.getLeft()), mappingSemiStructure(element.getRight()));
        }).toList();
    }

    public static Type[] getMapElementType(String statement)
    {
        int start = statement.indexOf('<');
        int end = statement.lastIndexOf('>');
        String type = statement.substring(start + 1, end);
        String[] types = getElementType(type);
        Type[] result = new Type[2];
        for (int i = 0; i < types.length; i++) {
            if (basicType.contains(removeParentheses(types[i]))) {
                result[i] = mappingBasicType(types[i]);
            }
            else {
                result[i] = mappingSemiStructure(types[i]);
            }
        }
        return result;
    }

    private static String[] getElementType(String statement)
    {
        List<String> pair = new ArrayList<>();
        char[] typeArray = statement.toCharArray();
        Deque<Character> bracket = new ArrayDeque<>();
        HashMap<Character, Character> bracketMap = new HashMap<>();
        bracketMap.put('(', ')');
        bracketMap.put(')', '(');
        bracketMap.put('<', '>');
        bracketMap.put('>', '<');
        int lastIndex = 0;
        for (int i = 0; i < typeArray.length; i++) {
            if (typeArray[i] == ',') {
                if (bracket.isEmpty()) {
                    String tempElement = statement.substring(lastIndex, i).trim();
                    pair.add(tempElement);
                    lastIndex = i + 1;
                }
                continue;
            }
            if (i == typeArray.length - 1) {
                String tempElement = statement.substring(lastIndex, i + 1).trim();
                pair.add(tempElement);
            }
            if (bracketMap.containsKey(typeArray[i])) {
                if (bracket.isEmpty()) {
                    bracket.push(typeArray[i]);
                    continue;
                }
                if (bracket.peek() == bracketMap.get(typeArray[i])) {
                    bracket.pop();
                }
                else {
                    bracket.push(typeArray[i]);
                }
            }
        }
        return pair.toArray(new String[0]);
    }

    private static final Pattern PARENTHESES_PATTERN = Pattern.compile("\\([^)]*\\)");

    public static String removeParentheses(String input)
    {
        return PARENTHESES_PATTERN.matcher(input).replaceAll("");
    }
}
