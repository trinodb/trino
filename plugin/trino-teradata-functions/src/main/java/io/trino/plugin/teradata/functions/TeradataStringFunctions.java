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
package io.trino.plugin.teradata.functions;

import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.FunctionDependency;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static java.nio.charset.StandardCharsets.UTF_16BE;

public final class TeradataStringFunctions
{
    private TeradataStringFunctions() {}

    @Description("Returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("index")
    @SqlType(StandardTypes.BIGINT)
    public static long index(
            @FunctionDependency(
                    name = "strpos",
                    argumentTypes = {StandardTypes.VARCHAR, StandardTypes.VARCHAR},
                    convention = @Convention(arguments = {NEVER_NULL, NEVER_NULL}, result = FAIL_ON_NULL))
                    MethodHandle method,
            @SqlType(StandardTypes.VARCHAR) Slice string,
            @SqlType(StandardTypes.VARCHAR) Slice substring)
    {
        try {
            return (long) method.invokeExact(string, substring);
        }
        catch (Throwable t) {
            throwIfInstanceOf(t, Error.class);
            throwIfInstanceOf(t, TrinoException.class);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, t);
        }
    }

    @Description("Returns the hexadecimal representation of the UTF-16BE encoding of the argument")
    @ScalarFunction("char2hexint")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice char2HexInt(@SqlType(StandardTypes.VARCHAR) Slice string)
    {
        Slice utf16 = Slices.wrappedHeapBuffer(UTF_16BE.encode(string.toStringUtf8()));
        String encoded = BaseEncoding.base16().encode(utf16.getBytes());
        return Slices.utf8Slice(encoded);
    }
}
