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
package io.trino.metastore;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;

import java.util.HexFormat;
import java.util.List;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Locale.ENGLISH;

public final class Partitions
{
    public static final String HIVE_DEFAULT_DYNAMIC_PARTITION = "__HIVE_DEFAULT_PARTITION__";

    private static final HexFormat HEX_UPPER_FORMAT = HexFormat.of().withUpperCase();

    private static final CharMatcher PATH_CHAR_TO_ESCAPE = CharMatcher.inRange((char) 0, (char) 31)
            .or(CharMatcher.anyOf("\"#%'*/:=?\\\u007F{[]^"))
            .precomputed();

    private Partitions() {}

    public static List<String> toPartitionValues(String partitionName)
    {
        // mimics Warehouse.makeValsFromName
        ImmutableList.Builder<String> resultBuilder = ImmutableList.builder();
        int start = 0;
        while (true) {
            while (start < partitionName.length() && partitionName.charAt(start) != '=') {
                start++;
            }
            start++;
            int end = start;
            while (end < partitionName.length() && partitionName.charAt(end) != '/') {
                end++;
            }
            if (start > partitionName.length()) {
                break;
            }
            resultBuilder.add(unescapePathName(partitionName.substring(start, end)));
            start = end + 1;
        }
        return resultBuilder.build();
    }

    // copy of org.apache.hadoop.hive.common.FileUtils#unescapePathName
    @SuppressWarnings("NumericCastThatLosesPrecision")
    public static String unescapePathName(String path)
    {
        // fast path, no escaped characters and therefore no copying necessary
        int escapedAtIndex = path.indexOf('%');
        if (escapedAtIndex < 0 || escapedAtIndex + 2 >= path.length()) {
            return path;
        }

        // slow path, unescape into a new string copy
        StringBuilder sb = new StringBuilder();
        int fromIndex = 0;
        while (escapedAtIndex >= 0 && escapedAtIndex + 2 < path.length()) {
            // preceding sequence without escaped characters
            if (escapedAtIndex > fromIndex) {
                sb.append(path, fromIndex, escapedAtIndex);
            }
            // try to parse the to digits after the percent sign as hex
            try {
                int code = HexFormat.fromHexDigits(path, escapedAtIndex + 1, escapedAtIndex + 3);
                sb.append((char) code);
                // advance past the percent sign and both hex digits
                fromIndex = escapedAtIndex + 3;
            }
            catch (NumberFormatException e) {
                // invalid escape sequence, only advance past the percent sign
                sb.append('%');
                fromIndex = escapedAtIndex + 1;
            }
            // find next escaped character
            escapedAtIndex = path.indexOf('%', fromIndex);
        }
        // trailing sequence without escaped characters
        if (fromIndex < path.length()) {
            sb.append(path, fromIndex, path.length());
        }
        return sb.toString();
    }

    // copy of org.apache.hadoop.hive.common.FileUtils#escapePathName
    public static String escapePathName(String path)
    {
        if (isNullOrEmpty(path)) {
            return HIVE_DEFAULT_DYNAMIC_PARTITION;
        }

        //  Fast-path detection, no escaping and therefore no copying necessary
        int escapeAtIndex = PATH_CHAR_TO_ESCAPE.indexIn(path);
        if (escapeAtIndex < 0) {
            return path;
        }

        // slow path, escape beyond the first required escape character into a new string
        StringBuilder sb = new StringBuilder();
        int fromIndex = 0;
        while (escapeAtIndex >= 0 && escapeAtIndex < path.length()) {
            // preceding characters without escaping needed
            if (escapeAtIndex > fromIndex) {
                sb.append(path, fromIndex, escapeAtIndex);
            }
            // escape single character
            char c = path.charAt(escapeAtIndex);
            sb.append('%').append(HEX_UPPER_FORMAT.toHighHexDigit(c)).append(HEX_UPPER_FORMAT.toLowHexDigit(c));
            // find next character to escape
            fromIndex = escapeAtIndex + 1;
            if (fromIndex < path.length()) {
                escapeAtIndex = PATH_CHAR_TO_ESCAPE.indexIn(path, fromIndex);
            }
            else {
                escapeAtIndex = -1;
            }
        }
        // trailing characters without escaping needed
        if (fromIndex < path.length()) {
            sb.append(path, fromIndex, path.length());
        }
        return sb.toString();
    }

    // copy of org.apache.hadoop.hive.common.FileUtils#makePartName
    public static String makePartName(List<String> columns, List<String> values)
    {
        StringBuilder name = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                name.append('/');
            }
            name.append(escapePathName(columns.get(i).toLowerCase(ENGLISH)));
            name.append('=');
            name.append(escapePathName(values.get(i)));
        }
        return name.toString();
    }
}
