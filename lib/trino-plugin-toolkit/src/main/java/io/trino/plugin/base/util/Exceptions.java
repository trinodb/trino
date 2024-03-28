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
package io.trino.plugin.base.util;

public final class Exceptions
{
    private Exceptions() {}

    /**
     * Returns {@link Throwable#getMessage()} is non-null and non-empty,
     * otherwise returns {@link Throwable#toString()}.
     */
    public static String messageOrToString(Throwable e)
    {
        String message = e.getMessage();
        if (message != null && !message.isBlank()) {
            return message.strip();
        }
        return e.toString();
    }
}
