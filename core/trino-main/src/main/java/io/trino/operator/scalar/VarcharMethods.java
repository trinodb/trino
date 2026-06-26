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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.StaticMethod;
import io.trino.spi.type.StandardTypes;

public final class VarcharMethods
{
    private VarcharMethods() {}

    @Description("Convert Unicode code point to a string")
    @ScalarFunction
    @StaticMethod(StandardTypes.VARCHAR)
    @SqlType("varchar(1)")
    public static Slice chr(@SqlType(StandardTypes.BIGINT) long codepoint)
    {
        return StringFunctions.chr(codepoint);
    }

    @Description("Decodes the UTF-8 encoded string")
    @ScalarFunction
    @StaticMethod(StandardTypes.VARCHAR)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fromUtf8(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return StringFunctions.fromUtf8(slice);
    }

    @Description("Decodes the UTF-8 encoded string")
    @ScalarFunction
    @StaticMethod(StandardTypes.VARCHAR)
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fromUtf8(@SqlType(StandardTypes.VARBINARY) Slice slice, @SqlType("varchar(x)") Slice replacementCharacter)
    {
        return StringFunctions.fromUtf8(slice, replacementCharacter);
    }

    @Description("Decodes the UTF-8 encoded string")
    @ScalarFunction
    @StaticMethod(StandardTypes.VARCHAR)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fromUtf8(@SqlType(StandardTypes.VARBINARY) Slice slice, @SqlType(StandardTypes.BIGINT) long replacementCodePoint)
    {
        return StringFunctions.fromUtf8(slice, replacementCodePoint);
    }
}
