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
package io.trino.hive.formats;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static io.trino.hive.formats.ByteSearch.indexOfByte;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.assertj.core.api.Assertions.assertThat;

public class TestByteSearch
{
    @Test
    public void testSingleByteMatchesNaiveSearch()
    {
        // Cover every alignment of the match relative to the eight byte word, every buffer length
        // around the word boundary, and every start offset within the buffer.
        for (int length = 0; length <= 40; length++) {
            byte[] buffer = new byte[length];
            for (int matchPosition = 0; matchPosition < length; matchPosition++) {
                Arrays.fill(buffer, (byte) 'a');
                buffer[matchPosition] = ';';
                for (int offset = 0; offset <= length; offset++) {
                    assertThat(indexOfByte(buffer, offset, length, (byte) ';'))
                            .describedAs("length=%s matchPosition=%s offset=%s", length, matchPosition, offset)
                            .isEqualTo(naiveIndexOf(buffer, offset, length, (byte) ';'));
                }
            }
        }
    }

    @Test
    public void testNoMatch()
    {
        for (int length = 0; length <= 40; length++) {
            byte[] buffer = new byte[length];
            Arrays.fill(buffer, (byte) 'a');
            assertThat(indexOfByte(buffer, 0, length, (byte) ';')).isEqualTo(-1);
            assertThat(indexOfByte(buffer, 0, length, (byte) ';', (byte) '|')).isEqualTo(-1);
        }
    }

    @Test
    public void testHighBitBytesAreNotFalseMatches()
    {
        // Bytes with the high bit set, which every multi byte UTF-8 sequence contains, must not be
        // mistaken for a match. A zero byte must not match either.
        byte[] buffer = new byte[64];
        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = (byte) (0x80 | (i & 0x7F));
        }
        assertThat(indexOfByte(buffer, 0, buffer.length, (byte) ';')).isEqualTo(-1);
        assertThat(indexOfByte(buffer, 0, buffer.length, (byte) 0)).isEqualTo(-1);

        buffer[37] = ';';
        assertThat(indexOfByte(buffer, 0, buffer.length, (byte) ';')).isEqualTo(37);
    }

    @Test
    public void testTwoBytesMatchesNaiveSearch()
    {
        Random random = new Random(1234);
        for (int length = 0; length <= 64; length++) {
            for (int trial = 0; trial < 20; trial++) {
                byte[] buffer = new byte[length];
                for (int i = 0; i < length; i++) {
                    // draw from a small alphabet so both targets occur often, including adjacently
                    buffer[i] = switch (random.nextInt(5)) {
                        case 0 -> (byte) '\n';
                        case 1 -> (byte) '\r';
                        case 2 -> (byte) 0;
                        case 3 -> (byte) 0xFF;
                        default -> (byte) 'a';
                    };
                }
                for (int offset = 0; offset <= length; offset++) {
                    int expected = naiveIndexOf(buffer, offset, length, (byte) '\n', (byte) '\r');
                    assertThat(indexOfByte(buffer, offset, length, (byte) '\n', (byte) '\r'))
                            .describedAs("length=%s offset=%s", length, offset)
                            .isEqualTo(expected);
                }
            }
        }
    }

    @Test
    public void testSearchIsBoundedByEnd()
    {
        byte[] buffer = "aaaa;aaaa".getBytes(US_ASCII);
        assertThat(indexOfByte(buffer, 0, buffer.length, (byte) ';')).isEqualTo(4);
        // the separator sits at index 4, so an end of 4 must not find it
        assertThat(indexOfByte(buffer, 0, 4, (byte) ';')).isEqualTo(-1);
        assertThat(indexOfByte(buffer, 5, buffer.length, (byte) ';')).isEqualTo(-1);
    }

    private static int naiveIndexOf(byte[] buffer, int offset, int end, byte value)
    {
        for (int i = offset; i < end; i++) {
            if (buffer[i] == value) {
                return i;
            }
        }
        return -1;
    }

    private static int naiveIndexOf(byte[] buffer, int offset, int end, byte first, byte second)
    {
        for (int i = offset; i < end; i++) {
            if (buffer[i] == first || buffer[i] == second) {
                return i;
            }
        }
        return -1;
    }
}
