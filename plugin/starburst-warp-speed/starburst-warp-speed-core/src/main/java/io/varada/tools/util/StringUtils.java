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
package io.varada.tools.util;

import java.util.UUID;

public abstract class StringUtils
{
    public static boolean isEmpty(final CharSequence cs)
    {
        return cs == null || cs.isEmpty();
    }

    public static boolean isNotEmpty(final CharSequence cs)
    {
        return !isEmpty(cs);
    }

    public static String randomAlphanumeric(int size)
    {
        StringBuilder builder = new StringBuilder();
        int left = size;
        while (builder.length() < size) {
            String tmp = UUID.randomUUID().toString().replace("-", "");
            if (left - tmp.length() >= 0) {
                builder.append(tmp);
                left -= tmp.length();
            }
            else {
                builder.append(tmp, 0, left);
                left = 0;
            }
        }
        return builder.toString();
    }
}
