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
package io.trino.type;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.safere.Pattern;
import org.safere.Utf8Input;
import org.safere.Utf8Matcher;
import org.safere.Utf8Sink;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public final class SafeReRegexp
{
    private final String pattern;
    private final Pattern safePattern;
    private final Pattern safePatternWithoutDotStarPrefix;

    public SafeReRegexp(Slice pattern)
    {
        this.pattern = pattern.toStringUtf8();
        this.safePattern = Pattern.compile(this.pattern);

        String patternWithoutDotStarPrefix = removeDotStarPrefix(this.pattern);
        if (!patternWithoutDotStarPrefix.equals(this.pattern)) {
            this.safePatternWithoutDotStarPrefix = Pattern.compile(patternWithoutDotStarPrefix);
        }
        else {
            this.safePatternWithoutDotStarPrefix = safePattern;
        }
    }

    public String pattern()
    {
        return pattern;
    }

    @Override
    public String toString()
    {
        return pattern;
    }

    public Utf8Matcher matcher(Slice source)
    {
        return safePattern.matcher(utf8Input(source));
    }

    public boolean matches(Slice source)
    {
        return safePatternWithoutDotStarPrefix.find(utf8Input(source));
    }

    public Slice replace(Slice source, Slice replacement)
    {
        Utf8Matcher matcher = matcher(source);
        // If there is no match we can simply return the original source without doing a copy.
        if (!matcher.find()) {
            return source;
        }

        Utf8Input replacementInput = utf8Input(replacement);
        SliceOutput output = new DynamicSliceOutput(source.length());
        Utf8Sink sink = output::writeBytes;
        try {
            do {
                matcher.appendReplacement(sink, replacementInput);
            }
            while (matcher.find());
            matcher.appendTail(sink);
        }
        catch (IndexOutOfBoundsException | IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: " + replacement.toStringUtf8());
        }
        return output.slice();
    }

    public Block extractAll(Slice source, long groupIndex)
    {
        Utf8Matcher matcher = matcher(source);
        int group = toIntExact(groupIndex);
        validateGroup(group, matcher.groupCount());

        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 32);
        while (matcher.find()) {
            int start = matcher.start(group);
            if (start < 0) {
                blockBuilder.appendNull();
                continue;
            }
            VARCHAR.writeSlice(blockBuilder, source.slice(start, matcher.end(group) - start));
        }
        return blockBuilder.build();
    }

    public Slice extract(Slice source, long groupIndex)
    {
        Utf8Matcher matcher = matcher(source);
        int group = toIntExact(groupIndex);
        validateGroup(group, matcher.groupCount());

        if (!matcher.find()) {
            return null;
        }

        int start = matcher.start(group);
        if (start < 0) {
            return null;
        }
        return source.slice(start, matcher.end(group) - start);
    }

    public Block split(Slice source)
    {
        Utf8Matcher matcher = matcher(source);
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 32);

        int lastEnd = 0;
        while (matcher.find()) {
            VARCHAR.writeSlice(blockBuilder, source.slice(lastEnd, matcher.start() - lastEnd));
            lastEnd = matcher.end();
        }

        VARCHAR.writeSlice(blockBuilder, source.slice(lastEnd, source.length() - lastEnd));
        return blockBuilder.build();
    }

    public long count(Slice source)
    {
        Utf8Matcher matcher = matcher(source);
        long count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    public long position(Slice source, long start, long occurrence)
    {
        // returns -1 if start is greater than the length of source
        if (start > countCodePoints(source)) {
            return -1;
        }

        int startByteOffset = offsetOfCodePoint(source, toIntExact(start) - 1);
        Slice suffix = source.slice(startByteOffset, source.length() - startByteOffset);
        Utf8Matcher matcher = matcher(suffix);
        long count = 0;
        while (matcher.find()) {
            if (++count == occurrence) {
                // Plus 1 because position is returned starting from 1
                return countCodePoints(source, 0, startByteOffset + matcher.start()) + 1;
            }
        }

        return -1;
    }

    /**
     * Strips a leading {@code .*} or {@code .*?} from the pattern. Anchoring a search on a wildcard prefix is
     * redundant, and carrying it forces the engine to track that state across the whole input on every call.
     */
    private static String removeDotStarPrefix(String pattern)
    {
        if (pattern.startsWith(".*?")) {
            return pattern.substring(3);
        }
        if (pattern.startsWith(".*")) {
            return pattern.substring(2);
        }
        return pattern;
    }

    /**
     * Borrows the slice's UTF-8 storage directly, without copying or validating it. Trino varchar values are
     * already well-formed UTF-8, and SafeRE reports match coordinates as byte offsets that never land inside a
     * scalar, so matches align to code point boundaries without any extra fixup.
     */
    private static Utf8Input utf8Input(Slice slice)
    {
        return Utf8Input.trusted(slice.byteArray(), slice.byteArrayOffset(), slice.length());
    }

    private static void validateGroup(int group, int groupCount)
    {
        if (group < 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Group cannot be negative");
        }
        if (group > groupCount) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Pattern has %d groups. Cannot access group %d", groupCount, group));
        }
    }
}
