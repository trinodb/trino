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

import io.airlift.log.Logger;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.util.ArrayList;
import java.util.List;

final class TrinoTypeNameParser
{
    private static final Logger log = Logger.get(TrinoTypeNameParser.class);

    private TrinoTypeNameParser() {}

    static Type parseTypeName(String name, TypeManager typeManager)
    {
        String trimmedName = name.trim();

        if (trimmedName.startsWith("array(") && trimmedName.endsWith(")")) {
            Type elementType = parseTypeName(trimmedName.substring(6, trimmedName.length() - 1), typeManager);
            return elementType == null ? null : new ArrayType(elementType);
        }
        if (trimmedName.startsWith("map(") && trimmedName.endsWith(")")) {
            String inner = trimmedName.substring(4, trimmedName.length() - 1);
            int comma = findTopLevelComma(inner);
            if (comma < 0) {
                return null;
            }
            Type keyType = parseTypeName(inner.substring(0, comma).trim(), typeManager);
            Type valueType = parseTypeName(inner.substring(comma + 1).trim(), typeManager);
            if (keyType == null || valueType == null) {
                return null;
            }
            return new MapType(keyType, valueType, typeManager.getTypeOperators());
        }
        if (trimmedName.startsWith("row(") && trimmedName.endsWith(")")) {
            List<RowType.Field> fields = parseRowFields(trimmedName.substring(4, trimmedName.length() - 1), typeManager);
            return (fields == null || fields.isEmpty()) ? null : RowType.from(fields);
        }

        try {
            return typeManager.fromSqlType(trimmedName);
        }
        catch (Exception e) {
            log.debug(e, "Unable to resolve type name: %s", trimmedName);
            return null;
        }
    }

    static List<RowType.Field> parseRowFields(String inner, TypeManager typeManager)
    {
        List<RowType.Field> fields = new ArrayList<>();
        for (String fieldDefinition : splitTopLevel(inner)) {
            String fieldDef = fieldDefinition.trim();
            String fieldName;
            String fieldTypeName;
            if (fieldDef.startsWith("\"")) {
                int closingQuote = findClosingQuote(fieldDef, 0);
                if (closingQuote < 0) {
                    return null;
                }
                fieldName = unescapeQuotedIdentifier(fieldDef.substring(1, closingQuote));
                fieldTypeName = fieldDef.substring(closingQuote + 1).trim();
            }
            else {
                int firstSpace = fieldDef.indexOf(' ');
                if (firstSpace < 0) {
                    fieldName = null;
                    fieldTypeName = fieldDef;
                }
                else {
                    String candidateName = fieldDef.substring(0, firstSpace);
                    String candidateType = fieldDef.substring(firstSpace + 1).trim();
                    Type parsedCandidateType = parseTypeName(candidateType, typeManager);
                    if (parsedCandidateType != null) {
                        fieldName = candidateName;
                        fieldTypeName = candidateType;
                    }
                    else if (parseTypeName(fieldDef, typeManager) != null) {
                        fieldName = null;
                        fieldTypeName = fieldDef;
                    }
                    else {
                        String resolvedName = null;
                        String resolvedType = null;
                        int lastSpace = fieldDef.lastIndexOf(' ');
                        while (lastSpace > 0) {
                            String suffix = fieldDef.substring(lastSpace + 1).trim();
                            if (parseTypeName(suffix, typeManager) != null) {
                                resolvedName = fieldDef.substring(0, lastSpace).trim();
                                resolvedType = suffix;
                                break;
                            }
                            lastSpace = fieldDef.lastIndexOf(' ', lastSpace - 1);
                        }
                        if (resolvedType == null) {
                            return null;
                        }
                        fieldName = resolvedName;
                        fieldTypeName = resolvedType;
                    }
                }
            }

            Type fieldType = parseTypeName(fieldTypeName, typeManager);
            if (fieldType == null) {
                return null;
            }
            fields.add(fieldName == null ? RowType.field(fieldType) : RowType.field(fieldName, fieldType));
        }
        return fields;
    }

    static int findTopLevelComma(String value)
    {
        int depth = 0;
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (current == '"') {
                int closingQuote = findClosingQuote(value, index);
                if (closingQuote < 0) {
                    return -1;
                }
                index = closingQuote;
            }
            else if (current == '(') {
                depth++;
            }
            else if (current == ')') {
                depth--;
            }
            else if (current == ',' && depth == 0) {
                return index;
            }
        }
        return -1;
    }

    static List<String> splitTopLevel(String value)
    {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (current == '"') {
                int closingQuote = findClosingQuote(value, index);
                if (closingQuote < 0) {
                    parts.add(value.substring(start));
                    return parts;
                }
                index = closingQuote;
            }
            else if (current == '(') {
                depth++;
            }
            else if (current == ')') {
                depth--;
            }
            else if (current == ',' && depth == 0) {
                parts.add(value.substring(start, index));
                start = index + 1;
            }
        }
        parts.add(value.substring(start));
        return parts;
    }

    private static int findClosingQuote(String value, int openingQuote)
    {
        for (int index = openingQuote + 1; index < value.length(); index++) {
            if (value.charAt(index) == '"') {
                if (index + 1 < value.length() && value.charAt(index + 1) == '"') {
                    index++;
                    continue;
                }
                return index;
            }
        }
        return -1;
    }

    private static String unescapeQuotedIdentifier(String value)
    {
        return value.replace("\"\"", "\"");
    }
}
