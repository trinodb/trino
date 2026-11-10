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
import io.airlift.slice.SliceUtf8;
import io.trino.re2j.Matcher;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.type.Re2JRegexp;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public final class Re2JRegexpFunctions
{
    private Re2JRegexpFunctions() {}

    public static Re2JRegexp compile(int dfaStatesLimit, int dfaRetries, Slice pattern)
    {
        try {
            return new Re2JRegexp(dfaStatesLimit, dfaRetries, pattern);
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    public static boolean regexpLike(Slice source, Re2JRegexp pattern)
    {
        return pattern.matches(source);
    }

    public static Slice regexpReplace(Slice source, Re2JRegexp pattern, Slice replacement)
    {
        return pattern.replace(source, replacement);
    }

    public static Block regexpExtractAll(Slice source, Re2JRegexp pattern, long groupIndex)
    {
        return pattern.extractAll(source, groupIndex);
    }

    public static Slice regexpExtract(Slice source, Re2JRegexp pattern, long groupIndex)
    {
        return pattern.extract(source, groupIndex);
    }

    public static Block regexpSplit(Slice source, Re2JRegexp pattern)
    {
        return pattern.split(source);
    }

    public static long regexpPosition(Slice source, Re2JRegexp pattern, long start, long occurrence)
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

        int startBytePosition = SliceUtf8.offsetOfCodePoint(source, (int) start - 1);
        int length = source.length() - startBytePosition;
        Matcher matcher = pattern.matcher(source.slice(startBytePosition, length));
        long count = 0;
        while (matcher.find()) {
            if (++count == occurrence) {
                // Plus 1 because position returned start from 1
                return SliceUtf8.countCodePoints(source, 0, startBytePosition + matcher.start()) + 1;
            }
        }

        return -1;
    }

    public static long regexpCount(Slice source, Re2JRegexp pattern)
    {
        Matcher matcher = pattern.matcher(source);

        int count = 0;
        while (matcher.find()) {
            count++;
        }

        return count;
    }
}
