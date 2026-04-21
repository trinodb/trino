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
package io.trino.spi.type;

import com.google.errorprone.annotations.FormatMethod;

import static java.lang.String.format;

final class Verify
{
    private Verify() {}

    @FormatMethod
    public static void verify(
            boolean expression,
            String errorMessageTemplate,
            Object... errorMessageArgs)
    {
        if (!expression) {
            throw new VerifyException(format(errorMessageTemplate, errorMessageArgs));
        }
    }

    public static class VerifyException
            extends RuntimeException
    {
        public VerifyException(String message)
        {
            super(message);
        }
    }
}
