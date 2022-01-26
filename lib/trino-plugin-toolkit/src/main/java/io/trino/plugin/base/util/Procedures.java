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

import com.google.errorprone.annotations.FormatMethod;
import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static java.lang.String.format;

public final class Procedures
{
    private Procedures() {}

    @FormatMethod
    public static void checkProcedureArgument(boolean condition, String message, Object... args)
    {
        if (!condition) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, format(message, args));
        }
    }
}
