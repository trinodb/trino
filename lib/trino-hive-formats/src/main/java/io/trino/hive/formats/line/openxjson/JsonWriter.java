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
package io.trino.hive.formats.line.openxjson;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static io.trino.hive.formats.line.openxjson.OpenXJsonDeserializer.parseDecimalHexOctalLong;
import static java.util.Objects.requireNonNull;

/**
 * Writer only accepts a {@link Map}, {@link List}, JsonString, String, Boolean, or Number.
 */
final class JsonWriter
{
    public static String writeJsonObject(Map<?, ?> jsonObject)
    {
        requireNonNull(jsonObject, "jsonObject is null");

        JsonWriter writer = new JsonWriter();
        writer.value(jsonObject);
        return writer.toString();
    }

    public static String writeJsonArray(List<?> jsonArray)
    {
        requireNonNull(jsonArray, "jsonArray is null");

        JsonWriter writer = new JsonWriter();
        writer.value(jsonArray);
        return writer.toString();
    }

    public static String canonicalizeJsonString(JsonString jsonString)
    {
        if (!jsonString.quoted()) {
            String canonicalUnquotedString = canonicalizeUnquotedString(jsonString.value());
            if (canonicalUnquotedString != null) {
                return canonicalUnquotedString;
            }
        }
        return jsonString.value();
    }

    private final StringBuilder out = new StringBuilder();
    private final List<Scope> scopes = new ArrayList<>();

    private JsonWriter() {}

    private void beginArray()
    {
        openNewScope(Scope.EMPTY_ARRAY, "[");
    }

    private void beginObject()
    {
        openNewScope(Scope.EMPTY_OBJECT, "{");
    }

    private void endArray()
    {
        closeCurrentScope(Scope.EMPTY_ARRAY, Scope.NONEMPTY_ARRAY, "]");
    }

    private void endObject()
    {
        closeCurrentScope(Scope.EMPTY_OBJECT, Scope.NONEMPTY_OBJECT, "}");
    }

    private void fieldName(Object name)
    {
        if (name == null) {
            throw new IllegalStateException("Field name must not be null");
        }

        Scope context = getCurrentScope();
        if (context == Scope.NONEMPTY_OBJECT) {
            // another object entry
            out.append(',');
        }
        else if (context != Scope.EMPTY_OBJECT) {
            throw new IllegalStateException("Nesting problem");
        }
        replaceCurrentScope(Scope.DANGLING_FIELD_NAME);

        writeJsonString(name.toString());
    }

    private void value(Object value)
    {
        if (value instanceof List<?> list) {
            beginArray();
            for (Object entry : list) {
                value(entry);
            }
            endArray();
            return;
        }
        if (value instanceof Map<?, ?> map) {
            beginObject();
            for (Entry<?, ?> entry : map.entrySet()) {
                fieldName(entry.getKey());
                value(entry.getValue());
            }
            endObject();
            return;
        }

        // primitive value without a wrapping array or object is not supported
        if (scopes.isEmpty()) {
            throw new IllegalStateException("Nesting problem");
        }

        beforeValue();

        if (value == null || value instanceof Boolean) {
            out.append(value);
        }
        else if (value instanceof Number number) {
            writeJsonNumber(number);
        }
        else if (value instanceof String string) {
            writeJsonString(string);
        }
        else if (value instanceof JsonString string) {
            if (!string.quoted()) {
                String canonicalUnquotedString = canonicalizeUnquotedString(string.value());
                if (canonicalUnquotedString != null) {
                    out.append(canonicalUnquotedString);
                    return;
                }
            }
            writeJsonString(string.value());
        }
        else {
            throw new IllegalArgumentException("Unsupported value type: " + value.getClass().getSimpleName());
        }
    }

    private static String canonicalizeUnquotedString(String value)
    {
        // canonicalize unquoted values
        // boolean to lower case
        if (value.equalsIgnoreCase("true")) {
            return "true";
        }
        if (value.equalsIgnoreCase("false")) {
            return "false";
        }

        // decimal, hex, and octal numbers rendered as plain decimal
        try {
            return String.valueOf(parseDecimalHexOctalLong(value));
        }
        catch (NumberFormatException ignored) {
        }
        // Use BigDecimal for all other values
        try {
            return new BigDecimal(value).toString();
        }
        catch (NumberFormatException ignored) {
        }
        return null;
    }

    private void writeJsonString(String value)
    {
        out.append("\"");
        char currentChar = 0;

        for (int i = 0, length = value.length(); i < length; i++) {
            char previousChar = currentChar;
            currentChar = value.charAt(i);

            /*
             * From RFC 4627, "All Unicode characters may be placed within the
             * quotation marks except for the characters that must be escaped:
             * quotation mark, reverse solidus, and the control characters
             * (U+0000 through U+001F)."
             */
            switch (currentChar) {
                case '"', '\\' -> out.append('\\').append(currentChar);
                case '/' -> {
                    // it makes life easier for HTML embedding of javascript if we escape </ sequences
                    if (previousChar == '<') {
                        out.append('\\');
                    }
                    out.append(currentChar);
                }
                case '\t' -> out.append("\\t");
                case '\b' -> out.append("\\b");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\f' -> out.append("\\f");
                default -> {
                    if (currentChar <= 0x1F) {
                        out.append(String.format("\\u%04x", (int) currentChar));
                    }
                    else {
                        out.append(currentChar);
                    }
                }
            }
        }
        out.append("\"");
    }

    private void writeJsonNumber(Number number)
    {
        requireNonNull(number, "number is null");

        // for double and float, render as a long if possible without loss of permission
        if (number instanceof Double || number instanceof Float) {
            // the original returns "-0" instead of "-0.0" for negative zero
            if (number.equals(-0.0d)) {
                out.append("-0");
                return;
            }

            // render doubles as a fixed integer if possible
            //noinspection FloatingPointEquality
            if (number.doubleValue() == number.longValue()) {
                out.append(number.longValue());
                return;
            }
        }
        out.append(number);
    }

    private void beforeValue()
    {
        // value without a wrapping array or object is not supported
        if (scopes.isEmpty()) {
            return;
        }

        Scope context = getCurrentScope();
        // first in array
        if (context == Scope.EMPTY_ARRAY) {
            replaceCurrentScope(Scope.NONEMPTY_ARRAY);
            return;
        }
        // another array element
        if (context == Scope.NONEMPTY_ARRAY) {
            out.append(',');
            return;
        }
        // value for a key
        if (context == Scope.DANGLING_FIELD_NAME) {
            out.append(":");
            replaceCurrentScope(Scope.NONEMPTY_OBJECT);
            return;
        }
        throw new IllegalStateException("Nesting problem");
    }

    private void openNewScope(Scope empty, String openBracket)
    {
        if (scopes.isEmpty() && !out.isEmpty()) {
            throw new IllegalStateException("Nesting problem: multiple top-level roots");
        }
        beforeValue();
        scopes.add(empty);
        out.append(openBracket);
    }

    private void closeCurrentScope(Scope empty, Scope nonempty, String closeBracket)
    {
        Scope context = getCurrentScope();
        if (context != nonempty && context != empty) {
            throw new IllegalStateException("Nesting problem");
        }

        scopes.remove(scopes.size() - 1);
        out.append(closeBracket);
    }

    private Scope getCurrentScope()
    {
        if (scopes.isEmpty()) {
            throw new IllegalStateException("Nesting problem");
        }
        return scopes.get(scopes.size() - 1);
    }

    private void replaceCurrentScope(Scope topOfStack)
    {
        scopes.set(scopes.size() - 1, topOfStack);
    }

    @Override
    public String toString()
    {
        return out.isEmpty() ? null : out.toString();
    }

    /**
     * Lexical scoping elements within this stringer, necessary to insert the
     * appropriate separator characters (i.e., commas and colons) and to detect
     * nesting errors.
     */
    private enum Scope
    {
        /**
         * An array with no elements
         */
        EMPTY_ARRAY,

        /**
         * An array with at least one value
         */
        NONEMPTY_ARRAY,

        /**
         * An object with no keys or values
         */
        EMPTY_OBJECT,

        /**
         * An object whose most recent element is a field name
         */
        DANGLING_FIELD_NAME,

        /**
         * An object with at least one entry
         */
        NONEMPTY_OBJECT,
    }
}
