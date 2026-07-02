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

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.safere.Matcher;
import org.safere.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public final class SafeReRegexp
{
    private static final java.util.regex.Pattern DOT_STAR_PREFIX_PATTERN = java.util.regex.Pattern.compile("(?s)^(\\.\\*\\??)?(.*)");
    private static final int CORE_PATTERN_INDEX = 2;

    private final String pattern;
    private final Pattern safePattern;
    private final Pattern safePatternWithoutDotStarPrefix;

    public SafeReRegexp(Slice pattern)
    {
        this.pattern = pattern.toStringUtf8();
        this.safePattern = Pattern.compile(this.pattern);

        // Remove leading .* / .*? prefix. For a search (find), `.*foo` is equivalent to `foo`, and keeping the
        // wildcard prefix forces the engine to carry that state across the whole input on every call.
        java.util.regex.Matcher dotStarPrefixMatcher = DOT_STAR_PREFIX_PATTERN.matcher(this.pattern);
        checkState(dotStarPrefixMatcher.matches());
        String patternWithoutDotStarPrefix = dotStarPrefixMatcher.group(CORE_PATTERN_INDEX);

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

    public Matcher matcher(CharSequence source)
    {
        return safePattern.matcher(source);
    }

    public boolean matches(Slice source)
    {
        return safePatternWithoutDotStarPrefix.matcher(source.toStringUtf8()).find();
    }

    public Slice replace(Slice source, Slice replacement)
    {
        Matcher matcher = safePattern.matcher(source.toStringUtf8());
        try {
            return utf8Slice(matcher.replaceAll(replacement.toStringUtf8()));
        }
        catch (IndexOutOfBoundsException | IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Illegal replacement sequence: " + replacement.toStringUtf8());
        }
    }

    public Block extractAll(Slice source, long groupIndex)
    {
        String input = source.toStringUtf8();
        Matcher matcher = safePattern.matcher(input);
        int group = toIntExact(groupIndex);
        validateGroup(group, matcher.groupCount());

        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 32);
        while (findOnCodePointBoundary(input, matcher)) {
            String matchedGroup = matcher.group(group);
            if (matchedGroup == null) {
                blockBuilder.appendNull();
                continue;
            }
            VARCHAR.writeSlice(blockBuilder, utf8Slice(matchedGroup));
        }
        return blockBuilder.build();
    }

    public Slice extract(Slice source, long groupIndex)
    {
        String input = source.toStringUtf8();
        Matcher matcher = safePattern.matcher(input);
        int group = toIntExact(groupIndex);
        validateGroup(group, matcher.groupCount());

        if (!findOnCodePointBoundary(input, matcher)) {
            return null;
        }

        String matchedGroup = matcher.group(group);
        if (matchedGroup == null) {
            return null;
        }
        return utf8Slice(matchedGroup);
    }

    public Block split(Slice source)
    {
        String input = source.toStringUtf8();
        Matcher matcher = safePattern.matcher(input);
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 32);

        int lastEnd = 0;
        while (findOnCodePointBoundary(input, matcher)) {
            VARCHAR.writeSlice(blockBuilder, utf8Slice(input.substring(lastEnd, matcher.start())));
            lastEnd = matcher.end();
        }

        VARCHAR.writeSlice(blockBuilder, utf8Slice(input.substring(lastEnd)));
        return blockBuilder.build();
    }

    public long count(Slice source)
    {
        String input = source.toStringUtf8();
        Matcher matcher = safePattern.matcher(input);
        long count = 0;
        while (findOnCodePointBoundary(input, matcher)) {
            count++;
        }
        return count;
    }

    public long position(Slice source, long start, long occurrence)
    {
        String input = source.toStringUtf8();
        // returns -1 if start is greater than the length of source
        if (start > input.codePointCount(0, input.length())) {
            return -1;
        }

        int startCodePointOffset = input.offsetByCodePoints(0, toIntExact(start) - 1);
        String suffix = input.substring(startCodePointOffset);
        Matcher matcher = safePattern.matcher(suffix);
        long count = 0;
        while (findOnCodePointBoundary(suffix, matcher)) {
            if (++count == occurrence) {
                // Plus 1 because position is returned starting from 1
                return input.codePointCount(0, startCodePointOffset + matcher.start()) + 1;
            }
        }

        return -1;
    }

    /**
     * Advances the matcher to the next match, skipping zero-width matches that fall between the two
     * halves of a surrogate pair. SafeRE operates on UTF-16 code units, but Trino regular expression
     * functions operate on code points, so this keeps empty/zero-width matches aligned to code point
     * boundaries, matching the behavior of the other regex engines.
     */
    private static boolean findOnCodePointBoundary(String input, Matcher matcher)
    {
        while (matcher.find()) {
            int start = matcher.start();
            if (matcher.end() == start
                    && start > 0
                    && start < input.length()
                    && Character.isLowSurrogate(input.charAt(start))
                    && Character.isHighSurrogate(input.charAt(start - 1))) {
                continue;
            }
            return true;
        }
        return false;
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
