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

import io.airlift.slice.SliceOutput;

/// Weight sequences compared as though the shorter were extended with a pad weight, and the
/// collation keys that order them so.
///
/// A `PAD SPACE` collation compares two strings as though the shorter were extended with spaces
/// to the length of the longer, so each weight past the end of the shorter sequence is compared
/// with the space's weight. Extending both sequences further does not change the result, so a key
/// encodes the end of a sequence as an unbounded run of pad weights. A level of a key is a
/// sequence of tokens:
///
/// | Token | Bytes |
/// | --- | --- |
/// | a weight below the pad weight | `0x02`, then the weight |
/// | a pad weight followed by a weight below it | `0x03 0x01` |
/// | the end of the level | `0x03 0x02` |
/// | a pad weight followed by a weight above it | `0x03 0x03` |
/// | a weight above the pad weight | `0x04`, then the weight |
///
/// Trailing pad weights are dropped before encoding, so a pad weight is always followed by a
/// weight that is not a pad weight. Where two padded sequences first differ, one has a pad weight
/// and the other a weight, or both have weights. A pad weight token orders against a weight token
/// as the pad weight orders against that weight. A pad weight token orders against another pad
/// weight token, or the end of the level, as the next weight that is not a pad weight orders
/// against the pad weight, which is the comparison at the end of the shorter run of pad weights.
final class PadSpaceKeys
{
    private static final int BELOW_PAD = 0x02;
    private static final int PAD = 0x03;
    private static final int ABOVE_PAD = 0x04;

    private static final int FOLLOWED_BY_BELOW = 0x01;
    private static final int END = 0x02;
    private static final int FOLLOWED_BY_ABOVE = 0x03;

    private PadSpaceKeys() {}

    /// Compares the first `leftCount` weights of `left` with the first `rightCount` weights of
    /// `right` as unsigned integers, extending the shorter sequence with `pad`.
    static int comparePadded(int[] left, int leftCount, int[] right, int rightCount, int pad)
    {
        int length = Math.max(leftCount, rightCount);
        for (int i = 0; i < length; i++) {
            int leftWeight = i < leftCount ? left[i] : pad;
            int rightWeight = i < rightCount ? right[i] : pad;
            if (leftWeight != rightWeight) {
                return Integer.compareUnsigned(leftWeight, rightWeight);
            }
        }
        return 0;
    }

    /// Appends the key of the first `count` weights of `weights`, each an unsigned integer
    /// written in `width` bytes, most significant first.
    static void appendLevel(SliceOutput output, int[] weights, int count, int pad, int width)
    {
        int end = count;
        while (end > 0 && weights[end - 1] == pad) {
            end--;
        }
        int position = 0;
        while (position < end) {
            if (weights[position] != pad) {
                output.writeByte(Integer.compareUnsigned(weights[position], pad) < 0 ? BELOW_PAD : ABOVE_PAD);
                for (int shift = 8 * (width - 1); shift >= 0; shift -= 8) {
                    output.writeByte(weights[position] >>> shift);
                }
                position++;
                continue;
            }
            int next = position;
            while (weights[next] == pad) {
                next++;
            }
            int direction = Integer.compareUnsigned(weights[next], pad) < 0 ? FOLLOWED_BY_BELOW : FOLLOWED_BY_ABOVE;
            for (; position < next; position++) {
                output.writeByte(PAD);
                output.writeByte(direction);
            }
        }
        output.writeByte(PAD);
        output.writeByte(END);
    }
}
