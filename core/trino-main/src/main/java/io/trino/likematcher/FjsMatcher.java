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
package io.trino.likematcher;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FjsMatcher
        implements Matcher
{
    private final List<Pattern> pattern;
    private final int start;
    private final int end;
    private final boolean exact;

    private volatile Fjs matcher;

    public FjsMatcher(List<Pattern> pattern, int start, int end, boolean exact)
    {
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.start = start;
        this.end = end;
        this.exact = exact;
    }

    @Override
    public boolean match(byte[] input, int offset, int length)
    {
        Fjs matcher = this.matcher;
        if (matcher == null) {
            matcher = new Fjs(pattern, start, end, exact);
            this.matcher = matcher;
        }

        return matcher.match(input, offset, length);
    }

    private static class Fjs
    {
        private final boolean exact;
        private final List<byte[]> patterns = new ArrayList<>();
        private final List<int[]> bmsShifts = new ArrayList<>();
        private final List<int[]> kmpShifts = new ArrayList<>();

        public Fjs(List<Pattern> pattern, int start, int end, boolean exact)
        {
            this.exact = exact;

            for (int i = start; i <= end; i++) {
                Pattern element = pattern.get(i);

                if (element instanceof Pattern.Literal literal) {
                    checkArgument(i == 0 || !(pattern.get(i - 1) instanceof Pattern.Literal), "Multiple consecutive literals found");
                    byte[] bytes = literal.value().getBytes(StandardCharsets.UTF_8);
                    patterns.add(bytes);
                    bmsShifts.add(computeBmsShifts(bytes));
                    kmpShifts.add(computeKmpShifts(bytes));
                }
                else if (element instanceof Pattern.Any) {
                    throw new IllegalArgumentException("'any' pattern not supported");
                }
            }
        }

        private static int[] computeKmpShifts(byte[] pattern)
        {
            int[] result = new int[pattern.length + 1];
            result[0] = -1;

            int j = -1;
            for (int i = 1; i < result.length; i++) {
                while (j >= 0 && pattern[i - 1] != pattern[j]) {
                    j = result[j];
                }
                j++;
                result[i] = j;
            }

            return result;
        }

        private static int[] computeBmsShifts(byte[] pattern)
        {
            int[] result = new int[256];

            for (int i = 0; i < pattern.length; i++) {
                result[pattern[i] & 0xFF] = i + 1;
            }

            return result;
        }

        private static int find(byte[] input, final int offset, final int length, byte[] pattern, int[] bmsShifts, int[] kmpShifts)
        {
            if (pattern.length > length || pattern.length == 0) {
                return -1;
            }

            final int inputLimit = offset + length;

            int i = offset;
            while (true) {
                // Attempt to match the last position of the pattern
                // As long as it doesn't match, skip ahead based on the Boyer-Moore-Sunday heuristic
                int matchEnd = i + pattern.length - 1;
                while (matchEnd < inputLimit - 1 && input[matchEnd] != pattern[pattern.length - 1]) {
                    int shift = pattern.length + 1 - bmsShifts[input[matchEnd + 1] & 0xFF];
                    matchEnd += shift;
                }

                if (matchEnd == inputLimit - 1 && match(input, inputLimit - pattern.length, pattern)) {
                    return inputLimit - pattern.length;
                }
                else if (matchEnd >= inputLimit - 1) {
                    return -1;
                }

                // At this point, we know the last position of the pattern matches with some
                // position in the input text given by "matchEnd"
                // Use KMP to match the first length-1 characters of the pattern

                i = matchEnd - (pattern.length - 1);

                int j = findLongestMatch(input, i, pattern, 0, pattern.length - 1);

                if (j == pattern.length - 1) {
                    return i;
                }

                i += j;
                j = kmpShifts[j];

                // Continue to match the whole pattern using KMP
                while (j > 0) {
                    int size = findLongestMatch(input, i, pattern, j, Math.min(inputLimit - i, pattern.length - j));
                    i += size;
                    j += size;

                    if (j == pattern.length) {
                        return i - j;
                    }

                    j = kmpShifts[j];
                }

                i++;
            }
        }

        private static int findLongestMatch(byte[] input, int inputOffset, byte[] pattern, int patternOffset, int length)
        {
            for (int i = 0; i < length; i++) {
                if (input[inputOffset + i] != pattern[patternOffset + i]) {
                    return i;
                }
            }

            return length;
        }

        private static boolean match(byte[] input, int offset, byte[] pattern)
        {
            for (int i = 0; i < pattern.length; i++) {
                if (input[offset + i] != pattern[i]) {
                    return false;
                }
            }

            return true;
        }

        public boolean match(byte[] input, int offset, int length)
        {
            int start = offset;
            int remaining = length;

            for (int i = 0; i < patterns.size(); i++) {
                if (remaining == 0) {
                    return false;
                }

                byte[] term = patterns.get(i);

                int position = find(input, start, remaining, term, bmsShifts.get(i), kmpShifts.get(i));
                if (position == -1) {
                    return false;
                }

                position += term.length;
                remaining -= position - start;
                start = position;
            }

            return !exact || remaining == 0;
        }
    }
}
