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
package io.trino.plugin.deltalake.util;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;
import io.trino.spi.TrinoException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.DATA_SKIP_STATS_COLUMN_CONFIGURATION_KEY;
import static java.lang.String.format;

public final class DataSkippingStatsColumnsUtils
{
    private static final String SPECIAL_CHARS = "!@#$%^&*()+-={}|[]:\\\";'<>,.?/";
    private static final Escaper ESCAPER;

    static {
        Escapers.Builder builder = Escapers.builder();
        for (char c : SPECIAL_CHARS.toCharArray()) {
            builder.addEscape(c, "\\" + c);
        }
        ESCAPER = builder.build();
    }

    private DataSkippingStatsColumnsUtils() {}

    // convert Trino names to Delta data_skipping_stats_columns property
    public static String toDataSkippingStatsColumnsString(Iterable<String> dataSkippingStatsColumns)
    {
        ImmutableSet.Builder<String> result = ImmutableSet.builder();
        for (String column : dataSkippingStatsColumns) {
            result.add(toDeltaName(column));
        }
        return Joiner.on(",").join(result.build());
    }

    // parse Delta data_skipping_stats_columns property to Trino names
    public static Set<String> getDataSkippingStatsColumns(Optional<String> dataSkippingStatsColumnsProperty)
    {
        if (dataSkippingStatsColumnsProperty.isEmpty()) {
            return ImmutableSet.of();
        }

        String property = dataSkippingStatsColumnsProperty.get();
        StringBuilder current = new StringBuilder();
        boolean inBackticks = false;

        ImmutableSet.Builder<String> result = ImmutableSet.builder();
        List<String> parts = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (char c : property.toCharArray()) {
            if (c == '`') {
                inBackticks = !inBackticks;
            }
            else if (c == ',' && !inBackticks) {
                addDataSkippingStatsColumn(current, result, parts, seen);
                continue;
            }
            else if (c == '.' && !inBackticks) {
                String token = current.toString().trim();
                if (!token.isEmpty()) {
                    parts.add(toTrinoName(token));
                }
                current.setLength(0);
                continue;
            }
            current.append(c);
        }

        if (inBackticks) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, format("Invalid value for %s property: %s", DATA_SKIP_STATS_COLUMN_CONFIGURATION_KEY, property));
        }

        addDataSkippingStatsColumn(current, result, parts, seen);

        return result.build();
    }

    private static void addDataSkippingStatsColumn(StringBuilder current, ImmutableSet.Builder<String> result, List<String> parts, Set<String> seen)
    {
        String lastToken = current.toString().trim();
        if (!lastToken.isEmpty()) {
            parts.add(toTrinoName(lastToken));
            String name = Joiner.on(".").join(parts);
            if (!seen.add(name)) {
                throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Duplicate data skipping stats column: " + name);
            }
            result.add(name);
        }
        current.setLength(0);
        parts.clear();
    }

    // escape Trino name to Delta name, but without escape ` to ``
    public static String escapeSpecialChars(String columnName)
    {
        return ESCAPER.escape(columnName);
    }

    private static String unescape(String name)
    {
        StringBuilder result = new StringBuilder();
        int length = name.length();

        for (int i = 0; i < length; i++) {
            char c = name.charAt(i);
            if (c != '\\') {
                result.append(c);
                continue;
            }

            i++;
            if (i >= length) {
                throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, format("Invalid column in %s property: %s", DATA_SKIP_STATS_COLUMN_CONFIGURATION_KEY, name));
            }

            char next = name.charAt(i);
            if (next != '\\' && !isSpecialChar(next)) {
                throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, format("Invalid column in %s property: %s", DATA_SKIP_STATS_COLUMN_CONFIGURATION_KEY, name));
            }

            result.append(next);
        }

        return result.toString();
    }

    private static boolean isValidUnquotedName(String s)
    {
        for (char c : s.toCharArray()) {
            if (!isAllowedInUnquoted(c)) {
                return false;
            }
        }
        return true;
    }

    private static String toDeltaName(String name)
    {
        if (isValidUnquotedName(name)) {
            return name;
        }

        // handle edge case that row type name with special chars, i,e,
        // row type column: a\\$xyz.nested converts to `a$xyz`.nested
        ImmutableList.Builder<String> partsBuilder = ImmutableList.builder();
        for (int i = 0; i < name.length(); i++) {
            int j = i;
            while (j < name.length() && name.charAt(j) != '.') {
                if (name.charAt(j) == '\\') {
                    j++;
                }
                j++;
            }
            // need unescaped
            String part = name.substring(i, j);
            String unescaped = unescape(part);
            // ` is a special character in Delta, while escaping the `, we need to quote the name
            if (Objects.equals(part, unescaped) && !unescaped.contains("`")) {
                partsBuilder.add(part.replaceAll("`", "``"));
            }
            else {
                String escapedBackticks = unescaped.replaceAll("`", "``");
                partsBuilder.add('`' + escapedBackticks + '`');
            }
            i = j;
        }
        return Joiner.on(".").join(partsBuilder.build());
    }

    /**
     * Escape special characters which defined in {@link #SPECIAL_CHARS}
     */
    public static String toTrinoName(String name)
    {
        if (name.startsWith("`")) {
            if (!name.endsWith("`")) {
                throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, format("Invalid name in %s property: %s", DATA_SKIP_STATS_COLUMN_CONFIGURATION_KEY, name));
            }

            String content = name.substring(1, name.length() - 1);
            content = content.replaceAll("``", "`");

            return ESCAPER.escape(content);
        }

        for (char c : name.toCharArray()) {
            if (!isAllowedInUnquoted(c)) {
                throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, format("Invalid name in %s property: %s", DATA_SKIP_STATS_COLUMN_CONFIGURATION_KEY, name));
            }
        }
        return name;
    }

    private static boolean isSpecialChar(char c)
    {
        return SPECIAL_CHARS.indexOf(c) >= 0 || c == '.';
    }

    private static boolean isAllowedInUnquoted(char c)
    {
        return Character.isLetterOrDigit(c) || c == '_' || c == '.';
    }
}
