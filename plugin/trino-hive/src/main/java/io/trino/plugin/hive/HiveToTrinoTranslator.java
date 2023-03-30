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
package io.trino.plugin.hive;

import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import io.trino.spi.TrinoException;

import static com.google.common.collect.Iterators.peekingIterator;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_VIEW_TRANSLATION_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Translate statements in Hive QL to Trino SQL.
 * <p>
 * Only translation of quoted literals is currently included.
 */
public final class HiveToTrinoTranslator
{
    // Translation methods consume data from the iterator
    private final PeekingIterator<Character> input;
    private final StringBuilder output = new StringBuilder();

    /**
     * Translate a HiveQL statement to Trino SQL by fixing quoted identifiers
     * and string literals. No other translation is performed.
     *
     * <p>Backquotes are replaced with double quotes, including SQL-style
     * escapes (`` becomes ` and " becomes "").
     *
     * <p>Single and double quotes are replaced with single quotes, with
     * minimal processing of escape sequences to ensure that the strings end in
     * the right place.
     */
    public static String translateHiveViewToTrino(String hiveStatement)
    {
        HiveToTrinoTranslator translator = new HiveToTrinoTranslator(hiveStatement);
        return translator.translateQuotedLiterals();
    }

    private HiveToTrinoTranslator(String hiveQl)
    {
        input = peekingIterator(Lists.charactersOf(hiveQl).iterator());
    }

    private String translateQuotedLiterals()
    {
        // Consume input, passing control to other translation methods when
        // their delimiters are encountered.
        while (input.hasNext()) {
            char c = input.next();
            switch (c) {
                case '"':
                case '\'':
                    translateString(c);
                    break;
                case '`':
                    translateQuotedIdentifier();
                    break;
                default:
                    output.append(c);
                    break;
            }
        }
        return output.toString();
    }

    private void translateString(char delimiter)
    {
        // Build a copy of the string to pass to Hive's string unescaper
        StringBuilder string = new StringBuilder(String.valueOf(delimiter));
        while (input.hasNext()) {
            char c = input.next();

            if (c == delimiter) {
                string.append(delimiter);
                String unescaped = unescapeSqlString(string.toString());
                output.append("'");
                output.append(unescaped.replace("'", "''"));
                output.append("'");
                return;
            }

            string.append(c);

            if (c == '\\') {
                if (!input.hasNext()) {
                    break; // skip to end-of-input error
                }
                string.append(input.next());
            }
        }
        throw hiveViewParseError("unexpected end of input in string");
    }

    private void translateQuotedIdentifier()
    {
        output.append('"');
        while (input.hasNext()) {
            char c = input.next();

            if (c == '"') {
                // escape " as ""
                output.append("\"\"");
            }
            else if (c == '`' && input.hasNext() && input.peek() == '`') {
                // un-escape `` as `
                output.append('`');
                input.next();
            }
            else if (c == '`') {
                // end of identifier
                output.append('"');
                return;
            }
            else {
                // don't change characters besides ` and "
                output.append(c);
            }
        }
        throw hiveViewParseError("unexpected end of input in identifier");
    }

    private static TrinoException hiveViewParseError(String message)
    {
        return new TrinoException(HIVE_VIEW_TRANSLATION_ERROR, "Error translating Hive view to Trino: " + message);
    }

    // copied from org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer#unescapeSQLString
    public static String unescapeSqlString(String b)
    {
        Character enclosure = null;
        StringBuilder sb = new StringBuilder(b.length());
        for (int i = 0; i < b.length(); i++) {
            char currentChar = b.charAt(i);
            if (enclosure == null) {
                if ((currentChar == '\'') || (b.charAt(i) == '\"')) {
                    enclosure = currentChar;
                }
                continue;
            }

            if (enclosure.equals(currentChar)) {
                enclosure = null;
                continue;
            }

            if ((currentChar == '\\') && ((i + 6) < b.length()) && (b.charAt(i + 1) == 'u')) {
                int code = 0;
                int base = i + 2;
                for (int j = 0; j < 4; j++) {
                    int digit = Character.digit(b.charAt(j + base), 16);
                    code = (code << 4) + digit;
                }
                sb.append((char) code);
                i += 5;
                continue;
            }

            if ((currentChar == '\\') && ((i + 4) < b.length())) {
                char i1 = b.charAt(i + 1);
                char i2 = b.charAt(i + 2);
                char i3 = b.charAt(i + 3);
                if ((i1 >= '0' && i1 <= '1') && (i2 >= '0' && i2 <= '7') && (i3 >= '0' && i3 <= '7')) {
                    byte value = (byte) ((i3 - '0') + ((i2 - '0') * 8) + ((i1 - '0') * 8 * 8));
                    sb.append(new String(new byte[] {value}, UTF_8));
                    i += 3;
                    continue;
                }
            }

            if ((currentChar == '\\') && ((i + 2) < b.length())) {
                char n = b.charAt(i + 1);
                switch (n) {
                    case '0' -> sb.append("\0");
                    case '\'' -> sb.append("'");
                    case '"' -> sb.append("\"");
                    case 'b' -> sb.append("\b");
                    case 'n' -> sb.append("\n");
                    case 'r' -> sb.append("\r");
                    case 't' -> sb.append("\t");
                    case 'Z' -> sb.append("\u001A");
                    case '\\' -> sb.append("\\");
                    case '%' -> sb.append("\\%");
                    case '_' -> sb.append("\\_");
                    default -> sb.append(n);
                }
                i++;
            }
            else {
                sb.append(currentChar);
            }
        }
        return sb.toString();
    }
}
