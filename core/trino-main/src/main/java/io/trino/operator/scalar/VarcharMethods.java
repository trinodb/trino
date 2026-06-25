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
import io.trino.spi.block.Block;
import io.trino.spi.function.Constraint;
import io.trino.spi.function.Description;
import io.trino.spi.function.InstanceMethod;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.StaticMethod;
import io.trino.spi.type.StandardTypes;
import io.trino.type.CodePointsType;

public final class VarcharMethods
{
    private VarcharMethods() {}

    @Description("Count of code points of the given string")
    @ScalarFunction(value = "length", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long length(@SqlType("varchar(x)") Slice slice)
    {
        return StringFunctions.length(slice);
    }

    @Description("Greedily removes occurrences of a pattern in a string")
    @ScalarFunction(value = "replace", neverFails = true)
    @InstanceMethod
    @LiteralParameters({"x", "y"})
    @SqlType("varchar(x)")
    public static Slice replace(@SqlType("varchar(x)") Slice str, @SqlType("varchar(y)") Slice search)
    {
        return StringFunctions.replace(str, search);
    }

    @Description("Greedily replaces occurrences of a pattern with a string")
    @ScalarFunction(value = "replace", neverFails = true)
    @InstanceMethod
    @LiteralParameters({"x", "y", "z", "u"})
    @Constraint(variable = "u", expression = "min(2147483647, x + z * (x + 1))")
    @SqlType("varchar(u)")
    public static Slice replace(@SqlType("varchar(x)") Slice str, @SqlType("varchar(y)") Slice search, @SqlType("varchar(z)") Slice replace)
    {
        return StringFunctions.replace(str, search, replace);
    }

    @Description("Reverse all code points in a given string")
    @ScalarFunction(value = "reverse", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice reverse(@SqlType("varchar(x)") Slice slice)
    {
        return StringFunctions.reverse(slice);
    }

    @Description("Returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("strpos")
    @InstanceMethod
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    public static long stringPosition(@SqlType("varchar(x)") Slice string, @SqlType("varchar(y)") Slice substring)
    {
        return StringFunctions.stringPosition(string, substring);
    }

    @Description("Returns index of n-th occurrence of a substring (or 0 if not found)")
    @ScalarFunction("strpos")
    @InstanceMethod
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    public static long stringPosition(@SqlType("varchar(x)") Slice string, @SqlType("varchar(y)") Slice substring, @SqlType(StandardTypes.BIGINT) long instance)
    {
        return StringFunctions.stringPosition(string, substring, instance);
    }

    @Description("Suffix starting at given index")
    @ScalarFunction(value = "substring", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice substring(@SqlType("varchar(x)") Slice utf8, @SqlType(StandardTypes.BIGINT) long start)
    {
        return StringFunctions.substring(utf8, start);
    }

    @Description("Substring of given length starting at an index")
    @ScalarFunction(value = "substring", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice substring(@SqlType("varchar(x)") Slice utf8, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long length)
    {
        return StringFunctions.substring(utf8, start, length);
    }

    @ScalarFunction("split")
    @InstanceMethod
    @LiteralParameters({"x", "y"})
    @SqlType("array(varchar(x))")
    public static Block split(@SqlType("varchar(x)") Slice string, @SqlType("varchar(y)") Slice delimiter)
    {
        return StringFunctions.split(string, delimiter);
    }

    @ScalarFunction("split")
    @InstanceMethod
    @LiteralParameters({"x", "y"})
    @SqlType("array(varchar(x))")
    public static Block split(@SqlType("varchar(x)") Slice string, @SqlType("varchar(y)") Slice delimiter, @SqlType(StandardTypes.BIGINT) long limit)
    {
        return StringFunctions.split(string, delimiter, limit);
    }

    @Description("Removes whitespace from the beginning and end of a string")
    @ScalarFunction(value = "trim", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice trim(@SqlType("varchar(x)") Slice slice)
    {
        return StringFunctions.trim(slice);
    }

    @Description("Remove the longest string containing only given characters from the beginning and end of a string")
    @ScalarFunction(value = "trim", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice trim(@SqlType("varchar(x)") Slice slice, @SqlType(CodePointsType.NAME) int[] codePointsToTrim)
    {
        return StringFunctions.trim(slice, codePointsToTrim);
    }

    @Description("Removes whitespace from the beginning of a string")
    @ScalarFunction(value = "ltrim", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice leftTrim(@SqlType("varchar(x)") Slice slice)
    {
        return StringFunctions.leftTrim(slice);
    }

    @Description("Remove the longest string containing only given characters from the beginning of a string")
    @ScalarFunction(value = "ltrim", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice leftTrim(@SqlType("varchar(x)") Slice slice, @SqlType(CodePointsType.NAME) int[] codePointsToTrim)
    {
        return StringFunctions.leftTrim(slice, codePointsToTrim);
    }

    @Description("Removes whitespace from the end of a string")
    @ScalarFunction(value = "rtrim", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice rightTrim(@SqlType("varchar(x)") Slice slice)
    {
        return StringFunctions.rightTrim(slice);
    }

    @Description("Remove the longest string containing only given characters from the end of a string")
    @ScalarFunction(value = "rtrim", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice rightTrim(@SqlType("varchar(x)") Slice slice, @SqlType(CodePointsType.NAME) int[] codePointsToTrim)
    {
        return StringFunctions.rightTrim(slice, codePointsToTrim);
    }

    @Description("Converts the string to lower case")
    @ScalarFunction(value = "lower", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice lower(@SqlType("varchar(x)") Slice slice)
    {
        return StringFunctions.lower(slice);
    }

    @Description("Converts the string to upper case")
    @ScalarFunction(value = "upper", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice upper(@SqlType("varchar(x)") Slice slice)
    {
        return StringFunctions.upper(slice);
    }

    @Description("Pads a string on the left")
    @ScalarFunction("lpad")
    @InstanceMethod
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice leftPad(@SqlType("varchar(x)") Slice text, @SqlType(StandardTypes.BIGINT) long targetLength, @SqlType("varchar(y)") Slice padString)
    {
        return StringFunctions.leftPad(text, targetLength, padString);
    }

    @Description("Pads a string on the right")
    @ScalarFunction("rpad")
    @InstanceMethod
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice rightPad(@SqlType("varchar(x)") Slice text, @SqlType(StandardTypes.BIGINT) long targetLength, @SqlType("varchar(y)") Slice padString)
    {
        return StringFunctions.rightPad(text, targetLength, padString);
    }

    @Description("Encodes the string to UTF-8")
    @ScalarFunction(value = "to_utf8", neverFails = true)
    @InstanceMethod
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice toUtf8(@SqlType("varchar(x)") Slice slice)
    {
        return StringFunctions.toUtf8(slice);
    }

    @Description("Determine whether source starts with prefix or not")
    @ScalarFunction("starts_with")
    @InstanceMethod
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean startsWith(@SqlType("varchar(x)") Slice source, @SqlType("varchar(y)") Slice prefix)
    {
        return StringFunctions.startsWith(source, prefix);
    }

    @Description("Determine whether source ends with suffix or not")
    @ScalarFunction("ends_with")
    @InstanceMethod
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean endsWith(@SqlType("varchar(x)") Slice source, @SqlType("varchar(y)") Slice suffix)
    {
        return StringFunctions.endsWith(source, suffix);
    }

    @Description("Translate characters from the source string based on original and translations strings")
    @ScalarFunction("translate")
    @InstanceMethod
    @LiteralParameters({"x", "y", "z"})
    @SqlType(StandardTypes.VARCHAR)
    public static Slice translate(@SqlType("varchar(x)") Slice source, @SqlType("varchar(y)") Slice from, @SqlType("varchar(z)") Slice to)
    {
        return StringFunctions.translate(source, from, to);
    }

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
