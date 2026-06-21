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
import io.trino.spi.function.InstanceMethod;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

public final class CharMethods
{
    private CharMethods() {}

    @Description("Count of code points of the given string")
    @ScalarFunction(value = "length", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long length(@LiteralParameter("x") long x, @SqlType("char(x)") Slice slice)
    {
        return StringFunctions.charLength(x, slice);
    }

    @Description("Reverse all code points in a given string")
    @ScalarFunction(value = "reverse", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType("char(x)")
    public static Slice reverse(@LiteralParameter("x") long x, @SqlType("char(x)") Slice slice)
    {
        return StringFunctions.charReverse(x, slice);
    }

    @Description("Suffix starting at given index")
    @ScalarFunction(value = "substring", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice substring(@LiteralParameter("x") Long x, @SqlType("char(x)") Slice utf8, @SqlType(StandardTypes.BIGINT) long start)
    {
        return StringFunctions.charSubstring(x, utf8, start);
    }

    @Description("Substring of given length starting at an index")
    @ScalarFunction(value = "substring", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice substring(@LiteralParameter("x") Long x, @SqlType("char(x)") Slice utf8, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long length)
    {
        return StringFunctions.charSubstr(x, utf8, start, length);
    }

    @Description("Converts the string to lower case")
    @ScalarFunction(value = "lower", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType("char(x)")
    public static Slice lower(@SqlType("char(x)") Slice slice)
    {
        return StringFunctions.charLower(slice);
    }

    @Description("Converts the string to upper case")
    @ScalarFunction(value = "upper", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType("char(x)")
    public static Slice upper(@SqlType("char(x)") Slice slice)
    {
        return StringFunctions.charUpper(slice);
    }

    @Description("Pads a string on the left")
    @ScalarFunction("lpad")
    @InstanceMethod
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice leftPad(@LiteralParameter("x") long x, @SqlType("char(x)") Slice text, @SqlType(StandardTypes.BIGINT) long targetLength, @SqlType("varchar(y)") Slice padString)
    {
        return StringFunctions.leftPad(x, text, targetLength, padString);
    }

    @Description("Encodes the string to UTF-8")
    @ScalarFunction(value = "to_utf8", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice toUtf8(@LiteralParameter("x") long x, @SqlType("char(x)") Slice slice)
    {
        return StringFunctions.toUtf8(x, slice);
    }
}
