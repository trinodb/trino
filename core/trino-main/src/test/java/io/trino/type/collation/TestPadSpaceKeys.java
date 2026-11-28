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
package io.trino.type.collation;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class TestPadSpaceKeys
{
    // Weights chosen to collide with the key's tag bytes (0x01 to 0x04), to have the high bit
    // set so that signed comparison would misorder them, and to put the pad weight at the bottom,
    // in the middle, and at the top of the alphabet
    private static final List<Alphabet> ALPHABETS = ImmutableList.of(
            new Alphabet(1, 0x20, new int[] {0x01, 0x02, 0x20, 0x21, 0xFF}),
            new Alphabet(1, 0x01, new int[] {0x01, 0x02, 0x03, 0x04}),
            new Alphabet(1, 0xFF, new int[] {0x02, 0x03, 0xFE, 0xFF}),
            new Alphabet(2, 0x0500, new int[] {0x0302, 0x04FF, 0x0500, 0x0501, 0xFFFF}),
            new Alphabet(4, 0x80000000, new int[] {0x00000001, 0x03030303, 0x7FFFFFFF, 0x80000000, 0x80000001, 0xFFFFFFFF}));

    @Test
    public void testKeysOrderAsPaddedSequences()
    {
        for (Alphabet alphabet : ALPHABETS) {
            List<int[]> sequences = alphabet.sequences(alphabet.weights().length <= 4 ? 5 : 4);
            List<Slice> keys = sequences.stream().map(sequence -> key(alphabet, sequence)).toList();
            for (int left = 0; left < sequences.size(); left++) {
                for (int right = 0; right < sequences.size(); right++) {
                    int[] leftSequence = sequences.get(left);
                    int[] rightSequence = sequences.get(right);
                    int expected = referenceCompare(leftSequence, rightSequence, alphabet.pad());
                    int compared = Integer.signum(PadSpaceKeys.comparePadded(leftSequence, leftSequence.length, rightSequence, rightSequence.length, alphabet.pad()));
                    int keyCompared = Integer.signum(keys.get(left).compareTo(keys.get(right)));
                    // assert only on a mismatch, since there are millions of pairs
                    if (compared != expected || keyCompared != expected) {
                        String context = "%s vs %s, pad %08x".formatted(hex(leftSequence), hex(rightSequence), alphabet.pad());
                        assertThat(compared).as(context).isEqualTo(expected);
                        assertThat(keyCompared).as(context).isEqualTo(expected);
                    }
                }
            }
        }
    }

    @Test
    public void testTrailingPadsDoNotChangeTheKey()
    {
        for (Alphabet alphabet : ALPHABETS) {
            for (int[] sequence : alphabet.sequences(3)) {
                int[] extended = Arrays.copyOf(sequence, sequence.length + 3);
                Arrays.fill(extended, sequence.length, extended.length, alphabet.pad());
                assertThat(key(alphabet, extended)).as(hex(sequence)).isEqualTo(key(alphabet, sequence));
            }
        }
    }

    @Test
    public void testOnlyTheCountedWeightsAreEncoded()
    {
        // callers pass reused buffers whose tail past the count is garbage
        Alphabet alphabet = ALPHABETS.getFirst();
        int[] buffer = {0x21, 0x20, 0x01, 0xFF, 0xFF};
        DynamicSliceOutput output = new DynamicSliceOutput(16);
        PadSpaceKeys.appendLevel(output, buffer, 2, alphabet.pad(), alphabet.width());
        assertThat(output.slice()).isEqualTo(key(alphabet, new int[] {0x21}));
        assertThat(PadSpaceKeys.comparePadded(buffer, 2, new int[] {0x21}, 1, alphabet.pad())).isZero();
    }

    @Test
    public void testLevelsOrderLexicographically()
    {
        // a key of several levels orders by the first level, then by the next, and so on, as if
        // the levels were compared one after another
        Random random = new Random(11);
        Alphabet first = ALPHABETS.get(4);
        Alphabet second = ALPHABETS.get(3);
        Alphabet third = ALPHABETS.getFirst();
        List<int[][]> values = new ArrayList<>();
        for (int i = 0; i < 2_000; i++) {
            values.add(new int[][] {first.random(random, 4), second.random(random, 4), third.random(random, 4)});
        }
        for (int[][] left : values) {
            for (int[][] right : values.subList(0, 200)) {
                int expected = 0;
                Alphabet[] alphabets = {first, second, third};
                for (int level = 0; level < 3 && expected == 0; level++) {
                    expected = referenceCompare(left[level], right[level], alphabets[level].pad());
                }
                int actual = Integer.signum(multiLevelKey(alphabets, left).compareTo(multiLevelKey(alphabets, right)));
                if (actual != expected) {
                    assertThat(actual).as("%s vs %s", Arrays.deepToString(left), Arrays.deepToString(right)).isEqualTo(expected);
                }
            }
        }
    }

    // compares the sequences extended with pad weights well past the longer one
    private static int referenceCompare(int[] left, int[] right, int pad)
    {
        int length = Math.max(left.length, right.length) + 2;
        int[] paddedLeft = Arrays.copyOf(left, length);
        int[] paddedRight = Arrays.copyOf(right, length);
        Arrays.fill(paddedLeft, left.length, length, pad);
        Arrays.fill(paddedRight, right.length, length, pad);
        return Integer.signum(Arrays.compareUnsigned(paddedLeft, paddedRight));
    }

    private static Slice key(Alphabet alphabet, int[] sequence)
    {
        DynamicSliceOutput output = new DynamicSliceOutput(16);
        PadSpaceKeys.appendLevel(output, sequence, sequence.length, alphabet.pad(), alphabet.width());
        return output.slice();
    }

    private static Slice multiLevelKey(Alphabet[] alphabets, int[][] levels)
    {
        DynamicSliceOutput output = new DynamicSliceOutput(64);
        for (int level = 0; level < levels.length; level++) {
            PadSpaceKeys.appendLevel(output, levels[level], levels[level].length, alphabets[level].pad(), alphabets[level].width());
        }
        return output.slice();
    }

    private static String hex(int[] sequence)
    {
        return Arrays.stream(sequence).mapToObj(Integer::toHexString).toList().toString();
    }

    private record Alphabet(int width, int pad, int[] weights)
    {
        // every sequence of at most maxLength weights
        List<int[]> sequences(int maxLength)
        {
            List<int[]> sequences = new ArrayList<>();
            sequences.add(new int[0]);
            List<int[]> previous = List.of(new int[0]);
            for (int length = 1; length <= maxLength; length++) {
                List<int[]> current = new ArrayList<>();
                for (int[] prefix : previous) {
                    for (int weight : weights) {
                        int[] sequence = Arrays.copyOf(prefix, length);
                        sequence[length - 1] = weight;
                        current.add(sequence);
                    }
                }
                sequences.addAll(current);
                previous = current;
            }
            return sequences;
        }

        int[] random(Random random, int maxLength)
        {
            int[] sequence = new int[random.nextInt(maxLength + 1)];
            for (int i = 0; i < sequence.length; i++) {
                sequence[i] = weights[random.nextInt(weights.length)];
            }
            return sequence;
        }
    }
}
