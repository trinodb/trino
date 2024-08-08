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
package io.trino.plugin.databend;

public final class DatabendUtil
{
    private DatabendUtil() {}

    public static String convertToQuotedString(Object value)
    {
        return value == null ? "NULL" : '\'' + escape(value.toString(), '\'') + '\'';
    }

    private static String escape(String str, char quote)
    {
        if (str == null) {
            return str;
        }

        int len = str.length();
        StringBuilder sb = new StringBuilder(len + 10);

        for (int i = 0; i < len; ++i) {
            char ch = str.charAt(i);
            if (ch == quote || ch == '\\') {
                sb.append('\\');
            }
            sb.append(ch);
        }

        return sb.toString();
    }
}
