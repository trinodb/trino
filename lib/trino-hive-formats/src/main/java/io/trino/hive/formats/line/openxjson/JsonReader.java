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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

/**
 * Reads a JSON encoded string into the corresponding object.
 * <p>
 * For legacy reasons this parser is lenient, so a successful parse does
 * not indicate that the input string was valid JSON. All the following syntax
 * errors will be ignored:
 * <ul>
 * <li>End of line comments starting with {@code //} or {@code #}
 * <li>C-style comments starting with {@code /*} and ending with
 * {@code *}{@code /}
 * <li>Strings that are unquoted or {@code 'single quoted'}.
 * <li>Hexadecimal integers prefixed with {@code 0x} or {@code 0X}.
 * <li>Octal integers prefixed with {@code 0}.
 * <li>Array elements separated by {@code ;}.
 * <li>Unnecessary array separators. These are interpreted as if null was the
 * omitted value.
 * <li>Key-value pairs separated by {@code =} or {@code =>}.
 * <li>Key-value pairs separated by {@code ;}.
 * </ul>
 */
final class JsonReader<T>
{
    /**
     * Decode the JSON value.
     * <p>
     * This will only combinations of {@code null}, {@code JsonString}, {@code Map<String, Object>}, or
     * {@code List<Object>}. A list may contain nulls, but the map will not contain nulls.
     *
     * @param json valid JSON
     * @return JsonString, Map, List, or null
     * @throws InvalidJsonException if the value is not valid json
     */
    public static <T> Object readJson(String json, Function<String, T> keyMap)
            throws InvalidJsonException
    {
        return new JsonReader<>(json, keyMap).nextValue();
    }

    private final String json;
    private final Function<String, T> keyMap;
    private int position;

    private JsonReader(String json, Function<String, T> keyMap)
    {
        requireNonNull(json, "json is null");
        this.keyMap = keyMap;
        this.json = json;
    }

    private Object nextValue()
            throws InvalidJsonException
    {
        int c = seekNextToken();
        if (c == -1) {
            throw new InvalidJsonException("End of input", position, json);
        }
        if (c == '{') {
            return readObject();
        }
        if (c == '[') {
            return readArray();
        }
        if (c == '\'' || c == '"') {
            return readQuotedString((char) c);
        }

        position--;
        return readLiteral();
    }

    private Map<T, Object> readObject()
            throws InvalidJsonException
    {
        // Peek to see if this is the empty object.
        int first = seekNextToken();
        if (first == '}') {
            return emptyMap();
        }
        else if (first != -1) {
            position--;
        }

        Map<T, Object> jsonObject = new LinkedHashMap<>();
        while (true) {
            Object name = nextValue();
            if (name == null) {
                throw new InvalidJsonException("Field name is null literal", position, json);
            }

            if (name instanceof JsonString jsonString) {
                name = jsonString.value();
            }
            else {
                String message = "Names must be strings, but %s is of type %s".formatted(name, name.getClass().getSimpleName());
                throw new InvalidJsonException(message, position, json);
            }

            // name/value separator can be ':', '=', or '=>'
            int separator = seekNextToken();
            if (separator != ':' && separator != '=') {
                throw new InvalidJsonException("Expected ':' after field " + name, position, json);
            }
            if (separator == '=' && position < json.length() && json.charAt(position) == '>') {
                position++;
            }

            T key = keyMap.apply((String) name);
            // Linked hash map does not replace existing keys that match, which is important for case-insensitive mappings
            jsonObject.remove(key);
            jsonObject.put(key, nextValue());

            int c = seekNextToken();
            if (c == '}') {
                return unmodifiableMap(jsonObject);
            }
            // entry separator can be `,` or `;`
            if (c != ',' && c != ';') {
                throw new InvalidJsonException("Unterminated object", position, json);
            }
        }
    }

    private List<Object> readArray()
            throws InvalidJsonException
    {
        List<Object> jsonArray = new ArrayList<>();

        boolean hasTrailingSeparator = false;
        while (true) {
            int c = seekNextToken();
            if (c == -1) {
                throw new InvalidJsonException("Unterminated array", position, json);
            }
            if (c == ']') {
                if (hasTrailingSeparator) {
                    jsonArray.add(null);
                }
                return unmodifiableList(jsonArray);
            }

            // A separator without a value first means is an implicit null
            if (c == ',' || c == ';') {
                jsonArray.add(null);
                hasTrailingSeparator = true;
                continue;
            }

            // add the next value
            position--;
            jsonArray.add(nextValue());

            // next token must end the array or be a separator
            c = seekNextToken();
            if (c == ']') {
                return unmodifiableList(jsonArray);
            }

            // value separator can be ',' or ';'
            if (c != ',' && c != ';') {
                throw new InvalidJsonException("Unterminated array", position, json);
            }
            hasTrailingSeparator = true;
        }
    }

    private JsonString readQuotedString(char quote)
            throws InvalidJsonException
    {
        // only used when there are escapes
        StringBuilder builder = null;

        int start = position;
        while (position < json.length()) {
            int c = json.charAt(position++);
            if (c == quote) {
                if (builder == null) {
                    // copy directly from source
                    return new JsonString(json.substring(start, position - 1), true);
                }
                builder.append(json, start, position - 1);
                return new JsonString(builder.toString(), true);
            }

            if (c == '\\') {
                if (position == json.length()) {
                    throw new InvalidJsonException("Unterminated escape sequence", position, json);
                }
                if (builder == null) {
                    builder = new StringBuilder();
                }
                builder.append(json, start, position - 1);
                builder.append(decodeEscapeCharacter());
                start = position;
            }
        }

        throw new InvalidJsonException("Unterminated string", position, json);
    }

    private char decodeEscapeCharacter()
            throws InvalidJsonException
    {
        char escaped = json.charAt(position++);
        switch (escaped) {
            case 'u' -> {
                if (position + 4 > json.length()) {
                    throw new InvalidJsonException("Unterminated escape sequence", position, json);
                }
                String hex = json.substring(position, position + 4);
                position += 4;
                try {
                    return (char) Integer.parseInt(hex, 16);
                }
                catch (NumberFormatException nfe) {
                    throw new InvalidJsonException("Invalid escape sequence: " + hex, position, json);
                }
            }
            case 't' -> {
                return '\t';
            }
            case 'b' -> {
                return '\b';
            }
            case 'n' -> {
                return '\n';
            }
            case 'r' -> {
                return '\r';
            }
            case 'f' -> {
                return '\f';
            }
            case 'a' -> {
                return '\007';
            }
            case 'v' -> {
                return '\011';
            }
        }
        return escaped;
    }

    /**
     * Reads a null or unquoted string literal value.
     */
    private JsonString readLiteral()
            throws InvalidJsonException
    {
        String literal = literalToken();
        if (literal.isEmpty()) {
            throw new InvalidJsonException("Expected literal value", position, json);
        }

        if ("null".equalsIgnoreCase(literal)) {
            return null;
        }

        return new JsonString(literal, false);
    }

    private String literalToken()
    {
        int start = position;
        while (position < json.length()) {
            char c = json.charAt(position);
            if (c == ' ' || c == '\r' || c == '\n' || c == '\t' || c == '\f' ||
                    c == '{' || c == '}' || c == '[' || c == ']' ||
                    c == '/' || c == '\\' ||
                    c == ':' || c == ',' || c == '=' || c == ';' || c == '#') {
                return json.substring(start, position);
            }
            position++;
        }
        return json.substring(start);
    }

    /**
     * Seek to the start of the next token, skipping any whitespace and comments.
     */
    private int seekNextToken()
            throws InvalidJsonException
    {
        while (position < json.length()) {
            int c = json.charAt(position++);
            switch (c) {
                case '\t', ' ', '\n', '\r' -> {
                    // ignore whitespace
                }
                case '#' -> skipEndOfLineComment();
                case '/' -> {
                    // possible comment
                    if (position == json.length()) {
                        return c;
                    }
                    char peek = json.charAt(position);
                    if (peek == '*') {
                        // c-style comment
                        position++;
                        int commentEnd = json.indexOf("*/", position);
                        if (commentEnd == -1) {
                            throw new InvalidJsonException("Unterminated c-style comment", position, json);
                        }
                        position = commentEnd + 2;
                        continue;
                    }
                    if (peek == '/') {
                        skipEndOfLineComment();
                        continue;
                    }
                    return c;
                }
                default -> {
                    return c;
                }
            }
        }

        return -1;
    }

    // This will never actually find an end of line character as these are handled by the Hive line format
    private void skipEndOfLineComment()
    {
        while (position < json.length()) {
            char c = json.charAt(position);
            position++;
            if (c == '\r' || c == '\n') {
                return;
            }
        }
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", JsonReader.class.getSimpleName() + "[", "]")
                .add("in='" + json + "'")
                .add("position=" + position)
                .toString();
    }
}
