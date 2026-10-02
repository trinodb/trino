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

import io.airlift.regulator.TrinoRegexpMatcher;
import io.airlift.regulator.TrinoRegexpReplacementException;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Constraint;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.type.RegulatorRegexp;
import io.trino.type.RegulatorRegexpType;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;

public final class RegulatorRegexpFunctions
{
    private RegulatorRegexpFunctions() {}

    @Description("Returns whether the pattern is contained within the string")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean regexpLike(@SqlType("varchar(x)") Slice source, @SqlType(RegulatorRegexpType.NAME) RegulatorRegexp pattern)
    {
        return pattern.regex().contains(source);
    }

    @Description("Removes substrings matching a regular expression")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice regexpReplace(@SqlType("varchar(x)") Slice source, @SqlType(RegulatorRegexpType.NAME) RegulatorRegexp pattern)
    {
        return regexpReplace(source, pattern, Slices.EMPTY_SLICE);
    }

    @Description("Replaces substrings matching a regular expression by given string")
    @ScalarFunction
    @LiteralParameters({"x", "y", "z"})
    // Longest possible output is when the pattern is empty, then the replacement will be placed in between
    // any two letters of source (x + 1) times. As the replacement may be wildcard and the wildcard input that takes two letters
    // can produce (x) length output it max length is (x * y / 2) however for (x < 2), (y) itself (without wildcards)
    // may be longer, so we choose max of (x * y / 2) and (y). We then add the length we've added to basic length of source (x)
    // to get the formula: x + max(x * y / 2, y) * (x + 1)
    @Constraint(variable = "z", expression = "min(2147483647, x + max(x * y / 2, y) * (x + 1))")
    @SqlType("varchar(z)")
    public static Slice regexpReplace(@SqlType("varchar(x)") Slice source, @SqlType(RegulatorRegexpType.NAME) RegulatorRegexp pattern, @SqlType("varchar(y)") Slice replacement)
    {
        try {
            return pattern.regex().replace(source, replacement);
        }
        catch (TrinoRegexpReplacementException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Description("String(s) extracted using the given pattern")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("array(varchar(x))")
    public static Block regexpExtractAll(@SqlType("varchar(x)") Slice source, @SqlType(RegulatorRegexpType.NAME) RegulatorRegexp pattern)
    {
        return regexpExtractAll(source, pattern, 0);
    }

    @Description("Group(s) extracted using the given pattern")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("array(varchar(x))")
    public static Block regexpExtractAll(@SqlType("varchar(x)") Slice source, @SqlType(RegulatorRegexpType.NAME) RegulatorRegexp pattern, @SqlType(StandardTypes.BIGINT) long groupIndex)
    {
        validateGroup(groupIndex, pattern.regex().capturingGroupCount());
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 32);
        TrinoRegexpMatcher matcher = pattern.regex().matcher(source, toIntExact(groupIndex));
        while (matcher.find()) {
            Slice value = matcher.group(toIntExact(groupIndex));
            if (value == null) {
                builder.appendNull();
            }
            else {
                VARCHAR.writeSlice(builder, value);
            }
        }
        return builder.build();
    }

    @SqlNullable
    @Description("String extracted using the given pattern")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice regexpExtract(@SqlType("varchar(x)") Slice source, @SqlType(RegulatorRegexpType.NAME) RegulatorRegexp pattern)
    {
        return regexpExtract(source, pattern, 0);
    }

    @SqlNullable
    @Description("Returns regex group of extracted string with a pattern")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice regexpExtract(@SqlType("varchar(x)") Slice source, @SqlType(RegulatorRegexpType.NAME) RegulatorRegexp pattern, @SqlType(StandardTypes.BIGINT) long groupIndex)
    {
        validateGroup(groupIndex, pattern.regex().capturingGroupCount());
        return pattern.regex().extract(source, toIntExact(groupIndex));
    }

    @ScalarFunction
    @LiteralParameters("x")
    @Description("Returns array of strings split by pattern")
    @SqlType("array(varchar(x))")
    public static Block regexpSplit(@SqlType("varchar(x)") Slice source, @SqlType(RegulatorRegexpType.NAME) RegulatorRegexp pattern)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 32);
        TrinoRegexpMatcher matcher = pattern.regex().matcher(source, 0);
        int lastEnd = 0;
        while (matcher.find()) {
            VARCHAR.writeSlice(builder, source, lastEnd, matcher.start() - lastEnd);
            lastEnd = matcher.end();
        }
        VARCHAR.writeSlice(builder, source, lastEnd, source.length() - lastEnd);
        return builder.build();
    }

    @ScalarFunction
    @Description("Returns the index of the matched substring")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTEGER)
    public static long regexpPosition(@SqlType("varchar(x)") Slice source, @SqlType(RegulatorRegexpType.NAME) RegulatorRegexp pattern)
    {
        return regexpPosition(source, pattern, 1);
    }

    @ScalarFunction
    @Description("Returns the index of the matched substring starting from the specified position")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTEGER)
    public static long regexpPosition(
            @SqlType("varchar(x)") Slice source,
            @SqlType(RegulatorRegexpType.NAME) RegulatorRegexp pattern,
            @SqlType(StandardTypes.INTEGER) long start)
    {
        return regexpPosition(source, pattern, start, 1);
    }

    @ScalarFunction
    @Description("Returns the index of the n-th matched substring starting from the specified position")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTEGER)
    public static long regexpPosition(
            @SqlType("varchar(x)") Slice source,
            @SqlType(RegulatorRegexpType.NAME) RegulatorRegexp pattern,
            @SqlType(StandardTypes.INTEGER) long start,
            @SqlType(StandardTypes.INTEGER) long occurrence)
    {
        try {
            return pattern.regex().position(source, start, occurrence);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @ScalarFunction
    @Description("Returns the number of times that a pattern occurs in a string")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long regexpCount(@SqlType("varchar(x)") Slice source, @SqlType(RegulatorRegexpType.NAME) RegulatorRegexp pattern)
    {
        return pattern.regex().count(source);
    }

    private static void validateGroup(long group, int groupCount)
    {
        if (group < 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Group cannot be negative");
        }
        if (group > groupCount) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Pattern has %d groups. Cannot access group %d".formatted(groupCount, group));
        }
    }
}
