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
package io.prestosql.plugin.hive;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class HiveQlTranslation
{
    /**
     * This pattern matches to quoted with ' or " strings. Also it does support escapes the quote character with \.
     * Like:
     * - 'string'
     * - "string"
     * - 'it\'s an "apple"'
     * - "it's an \"apple\""
     */
    private static final Pattern LITERALS = Pattern.compile("(?s)((?<![\\\\])['\"])((?:.(?!(?<![\\\\])\\1))*.?)\\1");

    private HiveQlTranslation() {}

    /**
     * - Replaces Hive ` as Presto " to wrap identifiers.
     * - Replaces Hive " as Presto ' for string literals.
     * - Removes Hive character escaping with \ in string literals.
     * - Escapes ' with '' in string literals.
     */
    public static String translateHiveQlToPrestoSql(String hiveSql)
    {
        Matcher matcher = LITERALS.matcher(hiveSql);
        StringBuilder prestoSql = new StringBuilder();
        int start = 0;
        while (matcher.find()) {
            prestoSql.append(replaceBackTicks(hiveSql.substring(start, matcher.start())))
                    .append("'")
                    .append(removeHiveEscapes(escapeQuotes(matcher.group().substring(1, matcher.group().length() - 1))))
                    .append("'");
            start = matcher.end();
        }
        prestoSql.append(replaceBackTicks(hiveSql.substring(start)));
        return prestoSql.toString();
    }

    private static String removeHiveEscapes(String value)
    {
        return value.replaceAll("(?s)\\\\(.)", "$1");
    }

    private static String escapeQuotes(String value)
    {
        return value.replace("'", "''");
    }

    private static String replaceBackTicks(String value)
    {
        return value.replace("`", "\"");
    }
}
