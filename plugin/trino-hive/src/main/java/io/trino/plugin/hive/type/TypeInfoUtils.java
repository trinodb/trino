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
package io.trino.plugin.hive.type;

import io.trino.plugin.hive.util.SerdeConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.hive.type.TypeInfoFactory.getCharTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getDecimalTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getListTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getMapTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getPrimitiveTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getStructTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getUnionTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getVarcharTypeInfo;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.MAP_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.STRUCT_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.UNION_TYPE_NAME;
import static java.lang.Character.isLetterOrDigit;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

// based on org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
public final class TypeInfoUtils
{
    private static final Map<String, PrimitiveTypeEntry> TYPES = new HashMap<>();

    private TypeInfoUtils() {}

    static {
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.BINARY, SerdeConstants.BINARY_TYPE_NAME));
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.STRING, SerdeConstants.STRING_TYPE_NAME));
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.CHAR, SerdeConstants.CHAR_TYPE_NAME));
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.VARCHAR, SerdeConstants.VARCHAR_TYPE_NAME));
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.BOOLEAN, SerdeConstants.BOOLEAN_TYPE_NAME));
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.INT, SerdeConstants.INT_TYPE_NAME));
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.LONG, SerdeConstants.BIGINT_TYPE_NAME));
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.FLOAT, SerdeConstants.FLOAT_TYPE_NAME));
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.VOID, SerdeConstants.VOID_TYPE_NAME));
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.DOUBLE, SerdeConstants.DOUBLE_TYPE_NAME));
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.BYTE, SerdeConstants.TINYINT_TYPE_NAME));
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.SHORT, SerdeConstants.SMALLINT_TYPE_NAME));
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.DATE, SerdeConstants.DATE_TYPE_NAME));
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.TIMESTAMP, SerdeConstants.TIMESTAMP_TYPE_NAME));
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.TIMESTAMPLOCALTZ, SerdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME));
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.INTERVAL_YEAR_MONTH, SerdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME));
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.INTERVAL_DAY_TIME, SerdeConstants.INTERVAL_DAY_TIME_TYPE_NAME));
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.DECIMAL, SerdeConstants.DECIMAL_TYPE_NAME));
        registerType(new PrimitiveTypeEntry(PrimitiveCategory.UNKNOWN, "unknown"));
    }

    private static void registerType(PrimitiveTypeEntry entry)
    {
        TYPES.put(entry.typeName(), entry);
    }

    public static PrimitiveTypeEntry getTypeEntryFromTypeName(String typeName)
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
            return isLetterOrDigit(c) || c == '_' || c == '.' || c == ' ' || c == '$';
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
                        case ",", ";", ":" -> index++;
                        default -> throw new IllegalArgumentException("Error: ',', ':', or ';' expected at position %s from '%s' %s".formatted(separator.position(), typeInfoString, typeInfoTokens));
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
                if (!LIST_TYPE_NAME.equals(token.text()) &&
                        !MAP_TYPE_NAME.equals(token.text()) &&
                        !STRUCT_TYPE_NAME.equals(token.text()) &&
                        !UNION_TYPE_NAME.equals(token.text()) &&
                        (getTypeEntryFromTypeName(token.text()) == null) &&
                        !token.text().equals(alternative)) {
                    throw new IllegalArgumentException("Error: %s expected at the position %s of '%s' but '%s' is found.".formatted(item, token.position(), typeInfoString, token.text()));
                }
            }
            else if (item.equals("name")) {
                if (!token.type() && !token.text().equals(alternative)) {
                    throw new IllegalArgumentException("Error: %s expected at the position %s of '%s' but '%s' is found.".formatted(item, token.position(), typeInfoString, token.text()));
                }
            }
            else if (!item.equals(token.text()) && !token.text().equals(alternative)) {
                throw new IllegalArgumentException("Error: %s expected at the position %s of '%s' but '%s' is found.".formatted(item, token.position(), typeInfoString, token.text()));
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
                    token = expect("name");
                    params.add(token.text());
                    token = expect(",", ")");
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

            PrimitiveTypeEntry typeEntry = getTypeEntryFromTypeName(token.text());
            if (typeEntry != null && typeEntry.primitiveCategory() != PrimitiveCategory.UNKNOWN) {
                String[] params = parseParams();
                switch (typeEntry.primitiveCategory()) {
                    case CHAR, VARCHAR -> {
                        if (params.length == 0) {
                            throw new IllegalArgumentException("%s type is specified without length: %s".formatted(typeEntry.typeName(), typeInfoString));
                        }
                        if (params.length == 1) {
                            int length = parseInt(params[0]);
                            return (typeEntry.primitiveCategory() == PrimitiveCategory.VARCHAR) ? getVarcharTypeInfo(length) : getCharTypeInfo(length);
                        }
                        throw new IllegalArgumentException("Type %s only takes one parameter, but %s is seen".formatted(typeEntry.typeName(), params.length));
                    }
                    case DECIMAL -> {
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
                        return getPrimitiveTypeInfo(typeEntry.typeName());
                    }
                }
            }

            if (LIST_TYPE_NAME.equals(token.text())) {
                expect("<");
                TypeInfo listElementType = parseType();
                expect(">");
                return getListTypeInfo(listElementType);
            }

            if (MAP_TYPE_NAME.equals(token.text())) {
                expect("<");
                TypeInfo mapKeyType = parseType();
                expect(",");
                TypeInfo mapValueType = parseType();
                expect(">");
                return getMapTypeInfo(mapKeyType, mapValueType);
            }

            if (STRUCT_TYPE_NAME.equals(token.text())) {
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
                    }
                    Token name = expect("name", ">");
                    if (name.text().equals(">")) {
                        break;
                    }
                    fieldNames.add(name.text());
                    expect(":");
                    fieldTypeInfos.add(parseType());
                }
                while (true);

                return getStructTypeInfo(fieldNames, fieldTypeInfos);
            }

            if (UNION_TYPE_NAME.equals(token.text())) {
                List<TypeInfo> objectTypeInfos = new ArrayList<>();
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
                    }
                    objectTypeInfos.add(parseType());
                }
                while (true);

                return getUnionTypeInfo(objectTypeInfos);
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

    public record PrimitiveTypeEntry(PrimitiveCategory primitiveCategory, String typeName)
    {
        public PrimitiveTypeEntry
        {
            requireNonNull(primitiveCategory, "primitiveCategory is null");
            requireNonNull(typeName, "typeName is null");
        }
    }
}
