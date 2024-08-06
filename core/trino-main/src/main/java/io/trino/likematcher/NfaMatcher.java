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

import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;

import java.util.Arrays;
import java.util.List;

import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;

final class NfaMatcher
        implements Matcher
{
    private static final int ANY = -1;
    private static final int NONE = -2;
    private static final int INVALID_CODEPOINT = -1;

    private final boolean exact;

    private final boolean[] loopback;
    private final int[] match;
    private final int acceptState;
    private final int stateCount;

    public NfaMatcher(List<Pattern> pattern, int start, int end, boolean exact)
    {
        this.exact = exact;

        stateCount = calculateStateCount(pattern, start, end);

        loopback = new boolean[stateCount];
        match = new int[stateCount];
        Arrays.fill(match, NONE);
        acceptState = stateCount - 1;

        int state = 0;
        for (int j = start; j <= end; j++) {
            Pattern element = pattern.get(j);
            switch (element) {
                case Pattern.Literal(Slice value) -> {
                    int position = 0;
                    while (position < value.length()) {
                        int character = SliceUtf8.getCodePointAt(value, position);
                        match[state++] = character;
                        position += lengthOfCodePoint(character);
                    }
                }
                case Pattern.Any any -> {
                    for (int i = 0; i < any.length(); i++) {
                        match[state++] = ANY;
                    }
                }
                case Pattern.ZeroOrMore _ -> loopback[state] = true;
            }
        }
    }

    private static int calculateStateCount(List<Pattern> pattern, int start, int end)
    {
        int states = 1;
        for (int i = start; i <= end; i++) {
            Pattern element = pattern.get(i);
            if (element instanceof Pattern.Literal(Slice value)) {
                states += value.length();
            }
            else if (element instanceof Pattern.Any(int length)) {
                states += length;
            }
        }
        return states;
    }

    @Override
    public boolean match(Slice input, int offset, int length)
    {
        boolean[] seen = new boolean[stateCount + 1];
        int[] currentStates = new int[stateCount];
        int[] nextStates = new int[stateCount];
        int currentStatesIndex = 0;
        int nextStatesIndex;

        currentStates[currentStatesIndex++] = 0;

        int limit = offset + length;
        int current = offset;
        boolean accept = false;
        while (current < limit) {
            int codepoint = INVALID_CODEPOINT;

            // decode the next UTF-8 codepoint
            int header = input.getByte(current) & 0xFF;
            if (header < 0x80) {
                // normal ASCII
                // 0xxx_xxxx
                codepoint = header;
                current++;
            }
            else if ((header & 0b1110_0000) == 0b1100_0000) {
                // 110x_xxxx 10xx_xxxx
                if (current + 1 < limit) {
                    codepoint = ((header & 0b0001_1111) << 6) | (input.getByte(current + 1) & 0b0011_1111);
                    current += 2;
                }
            }
            else if ((header & 0b1111_0000) == 0b1110_0000) {
                // 1110_xxxx 10xx_xxxx 10xx_xxxx
                if (current + 2 < limit) {
                    codepoint = ((header & 0b0000_1111) << 12) | ((input.getByte(current + 1) & 0b0011_1111) << 6) | (input.getByte(current + 2) & 0b0011_1111);
                    current += 3;
                }
            }
            else if ((header & 0b1111_1000) == 0b1111_0000) {
                // 1111_0xxx 10xx_xxxx 10xx_xxxx 10xx_xxxx
                if (current + 3 < limit) {
                    codepoint = ((header & 0b0000_0111) << 18) | ((input.getByte(current + 1) & 0b0011_1111) << 12) | ((input.getByte(current + 2) & 0b0011_1111) << 6) | (input.getByte(current + 3) & 0b0011_1111);
                    current += 4;
                }
            }

            if (codepoint == INVALID_CODEPOINT) {
                return false;
            }

            accept = false;
            nextStatesIndex = 0;
            Arrays.fill(seen, false);
            for (int i = 0; i < currentStatesIndex; i++) {
                int state = currentStates[i];
                if (!seen[state] && loopback[state]) {
                    nextStates[nextStatesIndex++] = state;
                    accept |= state == acceptState;
                    seen[state] = true;
                }
                int next = state + 1;
                if (!seen[next] && (match[state] == ANY || match[state] == codepoint)) {
                    nextStates[nextStatesIndex++] = next;
                    accept |= next == acceptState;
                    seen[next] = true;
                }
            }

            if (nextStatesIndex == 0) {
                return false;
            }

            if (!exact && accept) {
                return true;
            }

            int[] tmp = currentStates;
            currentStates = nextStates;
            nextStates = tmp;
            currentStatesIndex = nextStatesIndex;
        }

        return accept;
    }
}
