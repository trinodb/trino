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
package io.trino.plugin.snowflake;

import io.airlift.slice.Slice;
import io.trino.spi.variant.Variant;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;

/**
 * Parses the text form of a Snowflake VARIANT value, as returned by the Snowflake JDBC driver.
 * The text is JSON with a few extensions: {@code undefined}, {@code NaN}, {@code Infinity} and {@code -Infinity}.
 * Numbers without an exponent are Snowflake fixed-point numbers, numbers with an exponent are doubles.
 */
final class SnowflakeVariantParser
{
    private static final int MAX_DECIMAL_PRECISION = 38;

    private final String text;
    private int position;

    private SnowflakeVariantParser(String text)
    {
        this.text = text;
    }

    public static Variant parseVariant(String text)
    {
        SnowflakeVariantParser parser = new SnowflakeVariantParser(text);
        Variant variant = parser.parseValue();
        parser.skipWhitespace();
        if (parser.position != text.length()) {
            throw parser.error("Unexpected trailing data");
        }
        return variant;
    }

    private Variant parseValue()
    {
        skipWhitespace();
        if (position >= text.length()) {
            throw error("Unexpected end of input");
        }
        return switch (text.charAt(position)) {
            case '{' -> parseObject();
            case '[' -> parseArray();
            case '"' -> Variant.ofString(parseString());
            case 't' -> parseKeyword("true", Variant.ofBoolean(true));
            case 'f' -> parseKeyword("false", Variant.ofBoolean(false));
            case 'n' -> parseKeyword("null", Variant.NULL_VALUE);
            // Variant has no undefined value, so it is read as null
            case 'u' -> parseKeyword("undefined", Variant.NULL_VALUE);
            case 'N' -> parseKeyword("NaN", Variant.ofDouble(Double.NaN));
            case 'I' -> parseKeyword("Infinity", Variant.ofDouble(Double.POSITIVE_INFINITY));
            default -> parseNumber();
        };
    }

    private Variant parseObject()
    {
        position++;
        Map<Slice, Variant> fields = new LinkedHashMap<>();
        skipWhitespace();
        if (consume('}')) {
            return Variant.ofObject(fields);
        }
        do {
            skipWhitespace();
            if (position >= text.length() || text.charAt(position) != '"') {
                throw error("Expected field name");
            }
            Slice name = utf8Slice(parseString());
            skipWhitespace();
            if (!consume(':')) {
                throw error("Expected ':'");
            }
            if (fields.put(name, parseValue()) != null) {
                throw error("Duplicate field name: " + name.toStringUtf8());
            }
            skipWhitespace();
        }
        while (consume(','));
        if (!consume('}')) {
            throw error("Expected ',' or '}'");
        }
        return Variant.ofObject(fields);
    }

    private Variant parseArray()
    {
        position++;
        List<Variant> elements = new ArrayList<>();
        skipWhitespace();
        if (consume(']')) {
            return Variant.ofArray(elements);
        }
        do {
            elements.add(parseValue());
            skipWhitespace();
        }
        while (consume(','));
        if (!consume(']')) {
            throw error("Expected ',' or ']'");
        }
        return Variant.ofArray(elements);
    }

    private String parseString()
    {
        position++;
        StringBuilder builder = new StringBuilder();
        while (position < text.length()) {
            char current = text.charAt(position++);
            if (current == '"') {
                return builder.toString();
            }
            if (current != '\\') {
                builder.append(current);
                continue;
            }
            if (position >= text.length()) {
                break;
            }
            char escaped = text.charAt(position++);
            switch (escaped) {
                case '"', '\\', '/' -> builder.append(escaped);
                case 'b' -> builder.append('\b');
                case 'f' -> builder.append('\f');
                case 'n' -> builder.append('\n');
                case 'r' -> builder.append('\r');
                case 't' -> builder.append('\t');
                case 'u' -> {
                    if (position + 4 > text.length()) {
                        throw error("Invalid unicode escape");
                    }
                    try {
                        builder.append((char) Integer.parseInt(text, position, position + 4, 16));
                    }
                    catch (NumberFormatException e) {
                        throw error("Invalid unicode escape");
                    }
                    position += 4;
                }
                default -> throw error("Invalid escape character: " + escaped);
            }
        }
        throw error("Unterminated string");
    }

    private Variant parseNumber()
    {
        int start = position;
        if (consume('-') && text.startsWith("Infinity", position)) {
            position += "Infinity".length();
            return Variant.ofDouble(Double.NEGATIVE_INFINITY);
        }
        boolean exponent = false;
        while (position < text.length()) {
            char current = text.charAt(position);
            if (current == 'e' || current == 'E') {
                exponent = true;
            }
            else if (!(current >= '0' && current <= '9') && current != '.' && current != '-' && current != '+') {
                break;
            }
            position++;
        }
        String number = text.substring(start, position);
        BigDecimal decimal;
        try {
            decimal = new BigDecimal(number);
        }
        catch (NumberFormatException e) {
            position = start;
            throw error("Invalid value");
        }
        if (exponent) {
            return Variant.ofDouble(decimal.doubleValue());
        }
        if (decimal.scale() == 0) {
            BigInteger integer = decimal.unscaledValue();
            if (integer.bitLength() < Integer.SIZE) {
                return Variant.ofInt(integer.intValue());
            }
            if (integer.bitLength() < Long.SIZE) {
                return Variant.ofLong(integer.longValue());
            }
        }
        if (decimal.precision() > MAX_DECIMAL_PRECISION || decimal.scale() < 0 || decimal.scale() > MAX_DECIMAL_PRECISION) {
            return Variant.ofDouble(decimal.doubleValue());
        }
        return Variant.ofDecimal(decimal);
    }

    private Variant parseKeyword(String keyword, Variant value)
    {
        if (!text.startsWith(keyword, position)) {
            throw error("Invalid value");
        }
        position += keyword.length();
        return value;
    }

    private boolean consume(char expected)
    {
        if (position < text.length() && text.charAt(position) == expected) {
            position++;
            return true;
        }
        return false;
    }

    private void skipWhitespace()
    {
        while (position < text.length() && Character.isWhitespace(text.charAt(position))) {
            position++;
        }
    }

    private IllegalArgumentException error(String message)
    {
        return new IllegalArgumentException("%s at position %s in Snowflake VARIANT value".formatted(message, position));
    }
}
