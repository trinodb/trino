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

import io.airlift.joni.Matcher;
import io.airlift.joni.Option;
import io.airlift.joni.Regex;
import io.airlift.joni.Region;
import io.airlift.joni.exception.ValueException;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.type.Constraint;
import io.trino.type.JoniRegexp;
import io.trino.type.JoniRegexpType;

import java.nio.charset.StandardCharsets;

import static io.airlift.slice.SliceUtf8.lengthOfCodePointFromStartByte;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public final class JoniRegexpFunctions
{
    private JoniRegexpFunctions() {}

    @Description("Returns whether the pattern is contained within the string")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean regexpLike(@SqlType("varchar(x)") Slice source, @SqlType(JoniRegexpType.NAME) JoniRegexp pattern)
    {
        int offset = source.byteArrayOffset();
        Matcher matcher = pattern.regex().matcher(source.byteArray(), offset, offset + source.length());
        return getSearchingOffset(matcher, offset, offset + source.length()) != -1;
    }

    private static int getNextStart(Slice source, Matcher matcher)
    {
        if (matcher.getEnd() == matcher.getBegin()) {
            if (matcher.getBegin() < source.length()) {
                return matcher.getEnd() + lengthOfCodePointFromStartByte(source.getByte(matcher.getBegin()));
            }
            // last match is empty and we matched end of source, move past the source length to terminate the loop
            return matcher.getEnd() + 1;
        }
        return matcher.getEnd();
    }

    @Description("Removes substrings matching a regular expression")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice regexpReplace(@SqlType("varchar(x)") Slice source, @SqlType(JoniRegexpType.NAME) JoniRegexp pattern)
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
    public static Slice regexpReplace(@SqlType("varchar(x)") Slice source, @SqlType(JoniRegexpType.NAME) JoniRegexp pattern, @SqlType("varchar(y)") Slice replacement)
    {
        Matcher matcher = pattern.matcher(source.getBytes());
        SliceOutput sliceOutput = new DynamicSliceOutput(source.length() + replacement.length() * 5);

        int lastEnd = 0;
        int nextStart = 0; // nextStart is the same as lastEnd, unless the last match was zero-width. In such case, nextStart is lastEnd + 1.
        while (true) {
            int offset = getSearchingOffset(matcher, nextStart, source.length());
            if (offset == -1) {
                break;
            }
            nextStart = getNextStart(source, matcher);
            Slice sliceBetweenReplacements = source.slice(lastEnd, matcher.getBegin() - lastEnd);
            lastEnd = matcher.getEnd();
            sliceOutput.appendBytes(sliceBetweenReplacements);
            appendReplacement(sliceOutput, source, pattern.regex(), matcher.getEagerRegion(), replacement);
        }
        sliceOutput.appendBytes(source.slice(lastEnd, source.length() - lastEnd));

        return sliceOutput.slice();
    }

    private static void appendReplacement(SliceOutput result, Slice source, Regex pattern, Region region, Slice replacement)
    {
        // Handle the following items:
        // 1. ${name};
        // 2. $0, $1, $123 (group 123, if exists; or group 12, if exists; or group 1);
        // 3. \\, \$, \t (literal 't').
        // 4. Anything that doesn't starts with \ or $ is considered regular bytes

        int idx = 0;

        while (idx < replacement.length()) {
            byte nextByte = replacement.getByte(idx);
            if (nextByte == '$') {
                idx++;
                if (idx == replacement.length()) { // not using checkArgument because `.toStringUtf8` is expensive
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: " + replacement.toStringUtf8());
                }
                nextByte = replacement.getByte(idx);
                int backref;
                if (nextByte == '{') { // case 1 in the above comment
                    idx++;
                    int startCursor = idx;
                    while (idx < replacement.length()) {
                        nextByte = replacement.getByte(idx);
                        if (nextByte == '}') {
                            break;
                        }
                        idx++;
                    }
                    byte[] groupName = replacement.getBytes(startCursor, idx - startCursor);
                    try {
                        backref = pattern.nameToBackrefNumber(groupName, 0, groupName.length, region);
                    }
                    catch (ValueException e) {
                        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: unknown group { " + new String(groupName, StandardCharsets.UTF_8) + " }");
                    }
                    idx++;
                }
                else { // case 2 in the above comment
                    backref = nextByte - '0';
                    if (backref < 0 || backref > 9) { // not using checkArgument because `.toStringUtf8` is expensive
                        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: " + replacement.toStringUtf8());
                    }
                    if (region.numRegs <= backref) {
                        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: unknown group " + backref);
                    }
                    idx++;
                    while (idx < replacement.length()) { // Adaptive group number: find largest group num that is not greater than actual number of groups
                        int nextDigit = replacement.getByte(idx) - '0';
                        if (nextDigit < 0 || nextDigit > 9) {
                            break;
                        }
                        int newBackref = (backref * 10) + nextDigit;
                        if (region.numRegs <= newBackref) {
                            break;
                        }
                        backref = newBackref;
                        idx++;
                    }
                }
                int beg = region.beg[backref];
                int end = region.end[backref];
                if (beg != -1 && end != -1) { // the specific group doesn't exist in the current match, skip
                    result.appendBytes(source.slice(beg, end - beg));
                }
            }
            else { // case 3 and 4 in the above comment
                if (nextByte == '\\') {
                    idx++;
                    if (idx == replacement.length()) { // not using checkArgument because `.toStringUtf8` is expensive
                        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: " + replacement.toStringUtf8());
                    }
                    nextByte = replacement.getByte(idx);
                }
                result.appendByte(nextByte);
                idx++;
            }
        }
    }

    @Description("String(s) extracted using the given pattern")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("array(varchar(x))")
    public static Block regexpExtractAll(@SqlType("varchar(x)") Slice source, @SqlType(JoniRegexpType.NAME) JoniRegexp pattern)
    {
        return regexpExtractAll(source, pattern, 0);
    }

    @Description("Group(s) extracted using the given pattern")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("array(varchar(x))")
    public static Block regexpExtractAll(@SqlType("varchar(x)") Slice source, @SqlType(JoniRegexpType.NAME) JoniRegexp pattern, @SqlType(StandardTypes.BIGINT) long groupIndex)
    {
        Matcher matcher = pattern.matcher(source.getBytes());
        validateGroup(groupIndex, matcher.getEagerRegion());
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 32);
        int group = toIntExact(groupIndex);

        int nextStart = 0;
        while (true) {
            int offset = getSearchingOffset(matcher, nextStart, source.length());
            if (offset == -1) {
                break;
            }
            nextStart = getNextStart(source, matcher);
            Region region = matcher.getEagerRegion();
            int beg = region.beg[group];
            int end = region.end[group];
            if (beg == -1 || end == -1) {
                blockBuilder.appendNull();
            }
            else {
                Slice slice = source.slice(beg, end - beg);
                VARCHAR.writeSlice(blockBuilder, slice);
            }
        }
        return blockBuilder.build();
    }

    @SqlNullable
    @Description("String extracted using the given pattern")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice regexpExtract(@SqlType("varchar(x)") Slice source, @SqlType(JoniRegexpType.NAME) JoniRegexp pattern)
    {
        return regexpExtract(source, pattern, 0);
    }

    @SqlNullable
    @Description("Returns regex group of extracted string with a pattern")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice regexpExtract(@SqlType("varchar(x)") Slice source, @SqlType(JoniRegexpType.NAME) JoniRegexp pattern, @SqlType(StandardTypes.BIGINT) long groupIndex)
    {
        Matcher matcher = pattern.matcher(source.getBytes());
        validateGroup(groupIndex, matcher.getEagerRegion());
        int group = toIntExact(groupIndex);

        int offset = getSearchingOffset(matcher, 0, source.length());
        if (offset == -1) {
            return null;
        }
        Region region = matcher.getEagerRegion();
        int beg = region.beg[group];
        int end = region.end[group];
        if (beg == -1) {
            // end == -1 must be true
            return null;
        }

        return source.slice(beg, end - beg);
    }

    @ScalarFunction
    @LiteralParameters("x")
    @Description("Returns array of strings split by pattern")
    @SqlType("array(varchar(x))")
    public static Block regexpSplit(@SqlType("varchar(x)") Slice source, @SqlType(JoniRegexpType.NAME) JoniRegexp pattern)
    {
        Matcher matcher = pattern.matcher(source.getBytes());
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 32);

        int lastEnd = 0;
        int nextStart = 0;
        while (true) {
            int offset = getSearchingOffset(matcher, nextStart, source.length());
            if (offset == -1) {
                break;
            }
            nextStart = getNextStart(source, matcher);
            Slice slice = source.slice(lastEnd, matcher.getBegin() - lastEnd);
            lastEnd = matcher.getEnd();
            VARCHAR.writeSlice(blockBuilder, slice);
        }
        VARCHAR.writeSlice(blockBuilder, source.slice(lastEnd, source.length() - lastEnd));

        return blockBuilder.build();
    }

    private static void validateGroup(long group, Region region)
    {
        if (group < 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Group cannot be negative");
        }
        if (group > region.numRegs - 1) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Pattern has %d groups. Cannot access group %d", region.numRegs - 1, group));
        }
    }

    @ScalarFunction
    @Description("Returns the index of the matched substring")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTEGER)
    public static long regexpPosition(@SqlType("varchar(x)") Slice source, @SqlType(JoniRegexpType.NAME) JoniRegexp pattern)
    {
        return regexpPosition(source, pattern, 1);
    }

    @ScalarFunction
    @Description("Returns the index of the matched substring starting from the specified position")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTEGER)
    public static long regexpPosition(
            @SqlType("varchar(x)") Slice source,
            @SqlType(JoniRegexpType.NAME) JoniRegexp pattern,
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
            @SqlType(JoniRegexpType.NAME) JoniRegexp pattern,
            @SqlType(StandardTypes.INTEGER) long start,
            @SqlType(StandardTypes.INTEGER) long occurrence)
    {
        // start position cannot be smaller than 1
        if (start < 1) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "start position cannot be smaller than 1");
        }
        // occurrence cannot be smaller than 1
        if (occurrence < 1) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "occurrence cannot be smaller than 1");
        }
        // returns -1 if start is greater than the length of source
        if (start > SliceUtf8.countCodePoints(source)) {
            return -1;
        }

        Matcher matcher = pattern.matcher(source.getBytes());
        long count = 0;
        // convert char position to byte position
        // subtract 1 because codePointCount starts from zero
        int nextStart = SliceUtf8.offsetOfCodePoint(source, (int) start - 1);
        while (true) {
            int offset = getSearchingOffset(matcher, nextStart, source.length());
            // Check whether offset is negative, offset is -1 if no pattern was found
            if (offset < 0) {
                return -1;
            }

            if (++count == occurrence) {
                // Plus 1 because position returned start from 1
                return SliceUtf8.countCodePoints(source, 0, matcher.getBegin()) + 1;
            }

            nextStart = getNextStart(source, matcher);
        }
    }

    @ScalarFunction
    @Description("Returns the number of times that a pattern occurs in a string")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long regexpCount(@SqlType("varchar(x)") Slice source, @SqlType(JoniRegexpType.NAME) JoniRegexp pattern)
    {
        Matcher matcher = pattern.matcher(source.getBytes());

        int count = 0;
        // Start from zero, implies the first byte
        int nextStart = 0;
        while (true) {
            // getSearchingOffset returns `source.length` if `nextStart` equals `source.length - 1`.
            // It should return -1 if `nextStart` is greater than `source.length - 1`.
            int offset = getSearchingOffset(matcher, nextStart, source.length());
            if (offset < 0) {
                break;
            }

            nextStart = getNextStart(source, matcher);
            count++;
        }

        return count;
    }

    public static int getSearchingOffset(Matcher matcher, int at, int range)
    {
        try {
            return matcher.searchInterruptible(at, range, Option.DEFAULT);
        }
        catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new TrinoException(GENERIC_USER_ERROR, "" +
                    "Regular expression matching was interrupted, likely because it took too long. " +
                    "Regular expression in the worst case can have a catastrophic amount of backtracking and having exponential time complexity");
        }
    }
}
