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
import static io.trino.plugin.bigquery.type.TypeInfoFactory.getDecimalTypeInfo;
import static io.trino.plugin.bigquery.type.TypeInfoFactory.getListTypeInfo;
import static io.trino.plugin.bigquery.type.TypeInfoFactory.getPrimitiveTypeInfo;
import static io.trino.plugin.bigquery.type.TypeInfoFactory.getStructTypeInfo;
import static java.lang.Character.isLetterOrDigit;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

public final class TypeInfoUtils
{
    private static final Map<String, StandardSQLTypeName> TYPES = new HashMap<>();

    private TypeInfoUtils() {}

    static {
        registerType(StandardSQLTypeName.BYTES);
        registerType(StandardSQLTypeName.STRING);
        registerType(StandardSQLTypeName.BOOL);
        registerType(StandardSQLTypeName.INT64);
        registerType(StandardSQLTypeName.FLOAT64);
        registerType(StandardSQLTypeName.DATE);
        registerType(StandardSQLTypeName.DATETIME);
        registerType(StandardSQLTypeName.TIME);
        registerType(StandardSQLTypeName.TIMESTAMP);
        registerType(StandardSQLTypeName.GEOGRAPHY);
        registerType(StandardSQLTypeName.NUMERIC);
        registerType(StandardSQLTypeName.BIGNUMERIC);
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

    public static String getBaseName(String typeName)
    {
        int index = typeName.indexOf('(');
        if (index == -1) {
            return typeName;
        }
        return typeName.substring(0, index);
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
            return isLetterOrDigit(c) || c == '_' || c == '.' || c == '$';
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
                if ((begin > 0) && (typeInfoString.charAt(begin - 1) == '(') && (typeInfoString.charAt(begin) == '\'')) {
                    begin++;
                    do {
                        end++;
                    }
                    while (typeInfoString.charAt(end) != '\'');
                }
                else if ((typeInfoString.charAt(begin) == '\'') && (typeInfoString.charAt(begin + 1) == ')')) {
                    begin++;
                    end++;
                }
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

        public List<TypeInfo> parseTypeInfos()
        {
            List<TypeInfo> typeInfos = new ArrayList<>();
            index = 0;
            while (index < typeInfoTokens.size()) {
                typeInfos.add(parseType());
                if (index < typeInfoTokens.size()) {
                    Token separator = typeInfoTokens.get(index);
                    switch (separator.text()) {
                        case ",", " " -> index++;
                        default -> throw new IllegalArgumentException("Error: ',' or ' ' expected at position %s from '%s' %s".formatted(separator.position(), typeInfoString, typeInfoTokens));
                    }
                }
            }
            return typeInfos;
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
            return expect(item, null);
        }

        private Token expect(String item, String alternative)
        {
            if (index >= typeInfoTokens.size()) {
                throw new IllegalArgumentException("Error: %s expected at the end of '%s'".formatted(item, typeInfoString));
            }
            Token token = typeInfoTokens.get(index);

            if (item.equals("type")) {
                if (!ARRAY.name().equals(token.text()) &&
                        !STRUCT.name().equals(token.text()) &&
                        (getStandardSqlTypeNameFromTypeName(token.text()) == null) &&
                        !token.text().equals(alternative)) {
                    throw new IllegalArgumentException("Error: '%s' expected at the position %s of '%s' but '%s' is found.".formatted(item, token.position(), typeInfoString, token.text()));
                }
            }
            else if (item.equals("name")) {
                if (!token.type() && !token.text().equals(alternative)) {
                    throw new IllegalArgumentException("Error: '%s' expected at the position %s of '%s' but '%s' is found.".formatted(item, token.position(), typeInfoString, token.text()));
                }
            }
            else if (!item.equals(token.text()) && !token.text().equals(alternative)) {
                throw new IllegalArgumentException("Error: '%s' expected at the position %s of '%s' but '%s' is found.".formatted(item, token.position(), typeInfoString, token.text()));
            }

            index++;
            return token;
        }

        private String[] parseParams()
        {
            List<String> params = new LinkedList<>();

            Token token = peek();
            if ((token != null) && token.text().equals("(")) {
                expect("(");
                token = peek();
                while ((token == null) || !token.text().equals(")")) {
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
                switch (typeEntry) {
                    case STRING -> {
                        if (params.length != 0 && params.length != 1) {
                            throw new IllegalArgumentException("%s type is specified without length: %s".formatted(typeEntry.name(), typeInfoString));
                        }
                        return getPrimitiveTypeInfo("STRING");
                    }
                    case NUMERIC, BIGNUMERIC -> {
                        if (params.length == 0) {
                            return getDecimalTypeInfo(10, 0);
                        }
                        if (params.length == 1) {
                            return getDecimalTypeInfo(parseInt(params[0]), 0);
                        }
                        if (params.length == 2) {
                            return getDecimalTypeInfo(parseInt(params[0]), parseInt(params[1]));
                        }
                        throw new IllegalArgumentException("Type decimal only takes two parameter, but %s is seen".formatted(params.length));
                    }
                    default -> {
                        return getPrimitiveTypeInfo(typeEntry.name());
                    }
                }
            }

            if (ARRAY.name().equals(token.text())) {
                expect("<");
                TypeInfo listElementType = parseType();
                expect(">");
                return getListTypeInfo(listElementType);
            }

            if (STRUCT.name().equals(token.text())) {
                List<String> fieldNames = new ArrayList<>();
                List<TypeInfo> fieldTypeInfos = new ArrayList<>();
                boolean first = true;
                do {
                    if (first) {
                        expect("<");
                        first = false;
                    }
                    else {
                        Token separator = expect(">", ",");
                        if (separator.text().equals(">")) {
                            break;
                        }
                        expect(" ");
                    }
                    Token name = expect("name", ">");
                    if (name.text().equals(">")) {
                        break;
                    }
                    fieldNames.add(name.text());
                    expect(" ");
                    fieldTypeInfos.add(parseType());
                }
                while (true);

                return getStructTypeInfo(fieldNames, fieldTypeInfos);
            }

            throw new RuntimeException("Internal error parsing position %s of '%s'".formatted(token.position(), typeInfoString));
        }

        public PrimitiveParts parsePrimitiveParts()
        {
            Token token = expect("type");
            return new PrimitiveParts(token.text(), parseParams());
        }
    }

    public record PrimitiveParts(String typeName, String[] typeParams)
    {
        public PrimitiveParts
        {
            requireNonNull(typeName, "typeName is null");
            requireNonNull(typeParams, "typeParams is null");
        }
    }

    public static PrimitiveParts parsePrimitiveParts(String typeInfoString)
    {
        return new TypeInfoParser(typeInfoString).parsePrimitiveParts();
    }

    public static List<TypeInfo> getTypeInfosFromTypeString(String typeString)
    {
        return new TypeInfoParser(typeString).parseTypeInfos();
    }

    public static TypeInfo getTypeInfoFromTypeString(String typeString)
    {
        return getTypeInfosFromTypeString(typeString).get(0);
    }
}
