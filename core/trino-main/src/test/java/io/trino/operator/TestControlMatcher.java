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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.assertj.core.api.Assertions.assertThat;

class TestControlMatcher
{
    private static final byte EMPTY = 0;

    private static List<ControlMatcher> matchers()
    {
        return ImmutableList.of(new ScalarControlMatcher(), new VectorControlMatcher());
    }

    @ParameterizedTest
    @MethodSource("matchers")
    void testVectorLength(ControlMatcher matcher)
    {
        int vectorLength = matcher.vectorLength();
        assertThat(vectorLength).isGreaterThanOrEqualTo(Long.BYTES);
        assertThat(Integer.bitCount(vectorLength)).isEqualTo(1);
        // the match set must fit into a long
        assertThat(vectorLength).isLessThanOrEqualTo(Long.SIZE);
    }

    @ParameterizedTest
    @MethodSource("matchers")
    void testMatchesEveryPrefixInEverySlot(ControlMatcher matcher)
    {
        int vectorLength = matcher.vectorLength();
        byte[] control = new byte[vectorLength];
        for (int prefix = 0; prefix < 128; prefix++) {
            byte hashPrefix = (byte) (prefix | 0x80);
            for (int slot = 0; slot < vectorLength; slot++) {
                control[slot] = hashPrefix;
                assertThat(matchedSlots(matcher, control, hashPrefix, 0)).containsExactly(slot);
                control[slot] = EMPTY;
            }
        }
    }

    @ParameterizedTest
    @MethodSource("matchers")
    void testMatchesAgainstRandomControl(ControlMatcher matcher)
    {
        int vectorLength = matcher.vectorLength();
        Random random = new Random(0);
        // like a hash table of the given capacity, the control array repeats its first vectorLength slots at the end
        int capacity = 1024;
        byte[] control = new byte[capacity + vectorLength];
        for (int slot = 0; slot < capacity; slot++) {
            // leave a quarter of the slots empty, and use few distinct prefixes so that a single vector contains multiple matches
            control[slot] = random.nextInt(4) == 0 ? EMPTY : (byte) (random.nextInt(4) | 0x80);
        }
        System.arraycopy(control, 0, control, capacity, vectorLength);

        for (int start = 0; start < capacity; start++) {
            int position = start;
            for (int prefix = 0; prefix < 4; prefix++) {
                byte hashPrefix = (byte) (prefix | 0x80);
                List<Integer> matched = matchedSlots(matcher, control, hashPrefix, position);
                // a match set contains every matching slot, and only occupied slots
                assertThat(matched).containsAll(expectedSlots(control, position, vectorLength, hashPrefix));
                assertThat(matched).allMatch(slot -> control[position + slot] != EMPTY);
            }
            // matching the empty control byte must be exact, as inserts rely on it to find a free slot
            assertThat(matchedSlots(matcher, control, EMPTY, position))
                    .isEqualTo(expectedSlots(control, position, vectorLength, EMPTY));
        }
    }

    private static List<Integer> matchedSlots(ControlMatcher matcher, byte[] control, byte hashPrefix, int position)
    {
        ImmutableList.Builder<Integer> slots = ImmutableList.builder();
        long matches = matcher.match(control, position, hashPrefix);
        while (matches != 0) {
            slots.add(matcher.firstSlot(matches));
            matches = matcher.clearFirstSlot(matches);
        }
        return slots.build();
    }

    private static List<Integer> expectedSlots(byte[] control, int position, int vectorLength, byte hashPrefix)
    {
        return IntStream.range(0, vectorLength)
                .filter(slot -> control[position + slot] == hashPrefix)
                .boxed()
                .collect(toImmutableList());
    }
}
