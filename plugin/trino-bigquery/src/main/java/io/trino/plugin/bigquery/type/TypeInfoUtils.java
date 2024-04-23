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
package io.trino.plugin.bigquery.type;

import com.google.cloud.bigquery.StandardSQLTypeName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.google.cloud.bigquery.StandardSQLTypeName.ARRAY;
import static com.google.cloud.bigquery.StandardSQLTypeName.STRUCT;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.bigquery.type.TypeInfoFactory.getArrayTypeInfo;
import static io.trino.plugin.bigquery.type.TypeInfoFactory.getBigDecimalTypeInfo;
import static io.trino.plugin.bigquery.type.TypeInfoFactory.getDecimalTypeInfo;
import static io.trino.plugin.bigquery.type.TypeInfoFactory.getPrimitiveTypeInfo;
import static java.lang.Character.isLetterOrDigit;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

public final class TypeInfoUtils
{
    private static final Map<String, StandardSQLTypeName> TYPES = new HashMap<>();

    private TypeInfoUtils() {}

    static {
        registerType(StandardSQLTypeName.BOOL);
        registerType(StandardSQLTypeName.INT64);
        registerType(StandardSQLTypeName.FLOAT64);
        registerType(StandardSQLTypeName.NUMERIC);
        registerType(StandardSQLTypeName.BIGNUMERIC);
        registerType(StandardSQLTypeName.STRING);
        registerType(StandardSQLTypeName.BYTES);
        registerType(StandardSQLTypeName.DATE);
        registerType(StandardSQLTypeName.DATETIME);
        registerType(StandardSQLTypeName.TIME);
        registerType(StandardSQLTypeName.TIMESTAMP);
        registerType(StandardSQLTypeName.GEOGRAPHY);
        registerType(StandardSQLTypeName.JSON);
    }

    private static void registerType(StandardSQLTypeName entry)
    {
        TYPES.put(entry.name(), entry);
    }

    public static StandardSQLTypeName getStandardSqlTypeNameFromTypeName(String typeName)
    {
        return TYPES.get(typeName);
    }

    private static class TypeInfoParser
    {
        public record Token(int position, String text, boolean type)
        {
            public Token
            {
                requireNonNull(text, "text is null");
            }

            @Override
            public String toString()
            {
                return "%s:%s".formatted(position, text);
            }
        }

        private static boolean isTypeChar(char c)
        {
            return isLetterOrDigit(c) || c == '_' || c == '-';
        }

        private static boolean isDigit(String string)
        {
            return string.chars().allMatch(Character::isDigit);
        }

        private static List<Token> tokenize(String typeInfoString)
        {
            List<Token> tokens = new ArrayList<>();
            int begin = 0;
            int end = 1;
            while (end <= typeInfoString.length()) {
                if (end == typeInfoString.length() ||
                        !isTypeChar(typeInfoString.charAt(end - 1)) ||
                        !isTypeChar(typeInfoString.charAt(end))) {
                    Token token = new Token(
                            begin,
                            typeInfoString.substring(begin, end),
                            isTypeChar(typeInfoString.charAt(begin)));
                    tokens.add(token);
                    begin = end;
                }
                end++;
            }
            return tokens;
        }

        public TypeInfoParser(String typeInfoString)
        {
            this.typeInfoString = typeInfoString;
            typeInfoTokens = tokenize(typeInfoString);
        }

        private final String typeInfoString;
        private final List<Token> typeInfoTokens;
        private int index;

        public TypeInfo parseTypeInfo()
        {
            TypeInfo typeInfo;
            index = 0;
            typeInfo = parseType();
            if (index < typeInfoTokens.size()) {
                throw new IllegalArgumentException("Error: unexpected character at the end of '%s'".formatted(typeInfoString));
            }
            return typeInfo;
        }

        private Token peek()
        {
            if (index < typeInfoTokens.size()) {
                return typeInfoTokens.get(index);
            }
            return null;
        }

        private Token expect(String item)
        {
            if (index >= typeInfoTokens.size()) {
                throw new IllegalArgumentException("Error: %s expected at the end of '%s'".formatted(item, typeInfoString));
            }
            Token token = typeInfoTokens.get(index);

            if (item.equals("type")) {
                if (!ARRAY.name().equals(token.text()) && !STRUCT.name().equals(token.text()) && getStandardSqlTypeNameFromTypeName(token.text()) == null) {
                    throw new IllegalArgumentException("Error: '%s' expected at the position %s of '%s' but '%s' is found.".formatted(item, token.position(), typeInfoString, token.text()));
                }
            }
            else if (item.equals("name")) {
                if (!token.type()) {
                    throw new IllegalArgumentException("Error: '%s' expected at the position %s of '%s' but '%s' is found.".formatted(item, token.position(), typeInfoString, token.text()));
                }
            }
            else if (!item.equals(token.text())) {
                throw new IllegalArgumentException("Error: '%s' expected at the position %s of '%s' but '%s' is found.".formatted(item, token.position(), typeInfoString, token.text()));
            }

            index++;
            return token;
        }

        private String[] parseParams()
        {
            List<String> params = new LinkedList<>();

            Token token = peek();
            if (token != null && token.text().equals("(")) {
                expect("(");
                token = peek();
                while ((token == null || !token.text().equals(")")) && index < typeInfoTokens.size()) {
                    Token name = typeInfoTokens.get(index);
                    if (isDigit(name.text())) {
                        params.add(name.text());
                    }
                    token = name;
                    index++;
                }
                if (params.isEmpty()) {
                    throw new IllegalArgumentException("type parameters expected for type string " + typeInfoString);
                }
            }

            return params.toArray(new String[0]);
        }

        private TypeInfo parseType()
        {
            Token token = expect("type");

            StandardSQLTypeName typeEntry = getStandardSqlTypeNameFromTypeName(token.text());
            if (typeEntry != null) {
                String[] params = parseParams();
                return switch (typeEntry) {
                    case STRING -> {
                        if (params.length != 0 && params.length != 1) {
                            throw new IllegalArgumentException("Type string only takes zero or one parameter, but %s is seen".formatted(params.length));
                        }
                        checkArgument(params.length == 0 || parseInt(params[0]) >= 0, "invalid string length, must be equal or greater than zero");
                        yield getPrimitiveTypeInfo(StandardSQLTypeName.STRING);
                    }
                    case NUMERIC -> {
                        if (params.length == 0) {
                            yield getDecimalTypeInfo(38, 9);
                        }
                        if (params.length == 1) {
                            yield getDecimalTypeInfo(parseInt(params[0]), 0);
                        }
                        if (params.length == 2) {
                            yield getDecimalTypeInfo(parseInt(params[0]), parseInt(params[1]));
                        }
                        throw new IllegalArgumentException("Type decimal only takes two parameters, but %s is seen".formatted(params.length));
                    }
                    case BIGNUMERIC -> {
                        if (params.length == 0) {
                            yield getBigDecimalTypeInfo(76, 38);
                        }
                        if (params.length == 1) {
                            yield getBigDecimalTypeInfo(parseInt(params[0]), 0);
                        }
                        if (params.length == 2) {
                            yield getBigDecimalTypeInfo(parseInt(params[0]), parseInt(params[1]));
                        }
                        throw new IllegalArgumentException("Type decimal only takes two parameters, but %s is seen".formatted(params.length));
                    }
                    default -> getPrimitiveTypeInfo(typeEntry);
                };
            }

            if (ARRAY.name().equals(token.text())) {
                expect("<");
                TypeInfo listElementType = parseType();
                expect(">");
                return getArrayTypeInfo(listElementType);
            }

            if (STRUCT.name().equals(token.text())) {
                throw new UnsupportedTypeException(STRUCT, "STRUCT type is not supported, because it can contain unquoted field names, containing spaces, type names, and characters like '>'.");
            }

            throw new RuntimeException("Internal error parsing position %s of '%s'".formatted(token.position(), typeInfoString));
        }
    }

    public static TypeInfo parseTypeString(String typeString)
    {
        return new TypeInfoParser(typeString).parseTypeInfo();
    }
}
