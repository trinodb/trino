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
package io.trino.spi.type;

import io.airlift.slice.Slice;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.spi.type.Varchars.byteCount;
import static io.trino.spi.type.Varchars.truncateToLength;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestVarchars
{
    @Test
    public void testTruncateToLength()
    {
        // Single byte code points
        assertThat(truncateToLength(utf8Slice("abc"), 0)).isEqualTo(utf8Slice(""));
        assertThat(truncateToLength(utf8Slice("abc"), 1)).isEqualTo(utf8Slice("a"));
        assertThat(truncateToLength(utf8Slice("abc"), 4)).isEqualTo(utf8Slice("abc"));
        assertThat(truncateToLength(utf8Slice("abcde"), 5)).isEqualTo(utf8Slice("abcde"));
        // 2 bytes code points
        assertThat(truncateToLength(utf8Slice("абв"), 0)).isEqualTo(utf8Slice(""));
        assertThat(truncateToLength(utf8Slice("абв"), 1)).isEqualTo(utf8Slice("а"));
        assertThat(truncateToLength(utf8Slice("абв"), 4)).isEqualTo(utf8Slice("абв"));
        assertThat(truncateToLength(utf8Slice("абвгд"), 5)).isEqualTo(utf8Slice("абвгд"));
        // 4 bytes code points
        assertThat(truncateToLength(utf8Slice("\uD841\uDF0E\uD841\uDF31\uD841\uDF79\uD843\uDC53\uD843\uDC78"), 0)).isEqualTo(utf8Slice(""));
        assertThat(truncateToLength(utf8Slice("\uD841\uDF0E\uD841\uDF31\uD841\uDF79\uD843\uDC53\uD843\uDC78"), 1)).isEqualTo(utf8Slice("\uD841\uDF0E"));
        assertThat(truncateToLength(utf8Slice("\uD841\uDF0E\uD841\uDF31\uD841\uDF79"), 4)).isEqualTo(utf8Slice("\uD841\uDF0E\uD841\uDF31\uD841\uDF79"));
        assertThat(truncateToLength(utf8Slice("\uD841\uDF0E\uD841\uDF31\uD841\uDF79\uD843\uDC53\uD843\uDC78"), 5)).isEqualTo(utf8Slice("\uD841\uDF0E\uD841\uDF31\uD841\uDF79\uD843\uDC53\uD843\uDC78"));

        assertThat(truncateToLength(utf8Slice("abc"), createVarcharType(1))).isEqualTo(utf8Slice("a"));
        assertThat(truncateToLength(utf8Slice("abc"), (Type) createVarcharType(1))).isEqualTo(utf8Slice("a"));
    }

    @Test
    public void testByteCount()
    {
        // Single byte code points
        assertByteCount("abc", 0, 0, 1, "");
        assertByteCount("abc", 0, 1, 0, "");
        assertByteCount("abc", 1, 1, 1, "b");
        assertByteCount("abc", 1, 1, 2, "b");
        assertByteCount("abc", 1, 2, 1, "b");
        assertByteCount("abc", 1, 2, 2, "bc");
        assertByteCount("abc", 1, 2, 3, "bc");
        assertByteCount("abc", 0, 3, 1, "a");
        assertByteCount("abc", 0, 3, 5, "abc");
        assertByteCountFailure("abc", 4, 5, 1);
        assertByteCountFailure("abc", 5, 0, 1);
        assertByteCountFailure("abc", -1, 1, 1);
        assertByteCountFailure("abc", 1, -1, 1);
        assertByteCountFailure("abc", 1, 1, -1);

        // 2 bytes code points
        assertByteCount("абв", 0, 0, 1, "");
        assertByteCount("абв", 0, 1, 0, "");
        assertByteCount("абв", 0, 2, 1, "а");
        assertByteCount("абв", 0, 4, 1, "а");
        assertByteCount("абв", 0, 1, 1, utf8Slice("а").getBytes(0, 1));
        assertByteCount("абв", 2, 2, 2, "б");
        assertByteCount("абв", 2, 2, 0, "");
        assertByteCount("абв", 0, 3, 5, utf8Slice("аб").getBytes(0, 3));
        assertByteCountFailure("абв", 8, 5, 1);
        // we do not check if the offset is in the middle of a code point
        assertByteCount("абв", 1, 1, 5, utf8Slice("а").getBytes(1, 1));
        assertByteCount("абв", 2, 1, 5, utf8Slice("б").getBytes(0, 1));

        // 3 bytes code points
        assertByteCount("\u6000\u6001\u6002\u6003", 0, 0, 2, "");
        assertByteCount("\u6000\u6001\u6002\u6003", 0, 1, 1, utf8Slice("\u6000").getBytes(0, 1));
        assertByteCount("\u6000\u6001\u6002\u6003", 0, 2, 1, utf8Slice("\u6000").getBytes(0, 2));
        assertByteCount("\u6000\u6001\u6002\u6003", 0, 3, 1, "\u6000");
        assertByteCount("\u6000\u6001\u6002\u6003", 0, 6, 1, "\u6000");
        assertByteCount("\u6000\u6001\u6002\u6003", 6, 2, 4, utf8Slice("\u6002").getBytes(0, 2));
        assertByteCount("\u6000\u6001\u6002\u6003", 0, 12, 6, "\u6000\u6001\u6002\u6003");
        // we do not check if the offset is in the middle of a code point
        assertByteCount("\u6000\u6001\u6002\u6003", 1, 6, 2, utf8Slice("\u6000\u6001\u6002").getBytes(1, 6));
        assertByteCount("\u6000\u6001\u6002\u6003", 2, 6, 2, utf8Slice("\u6000\u6001\u6002").getBytes(2, 6));
        assertByteCount("\u6000\u6001\u6002\u6003", 3, 6, 2, utf8Slice("\u6000\u6001\u6002").getBytes(3, 6));
        assertByteCountFailure("\u6000\u6001\u6002\u6003", 21, 0, 1);

        // invalid code points; always return the original lengths unless code point count is 0
        assertByteCount(new byte[] {(byte) 0x81, (byte) 0x81, (byte) 0x81}, 0, 2, 0, new byte[] {});
        assertByteCount(new byte[] {(byte) 0x81, (byte) 0x81, (byte) 0x81}, 0, 2, 1, new byte[] {(byte) 0x81, (byte) 0x81});
        assertByteCount(new byte[] {(byte) 0x81, (byte) 0x81, (byte) 0x81}, 0, 2, 3, new byte[] {(byte) 0x81, (byte) 0x81});
    }

    private static void assertByteCountFailure(String string, int offset, int length, int codePointCount)
    {
        assertThatThrownBy(() -> byteCount(utf8Slice(string), offset, length, codePointCount))
                .isInstanceOf(IllegalArgumentException.class)
                // TODO split into individual assertions or provide expected message as a parameter
                .hasMessageMatching("invalid offset/length|length must be greater than or equal to zero|codePointsCount must be greater than or equal to zero");
    }

    private static void assertByteCount(String actual, int offset, int length, int codePointCount, String expected)
    {
        assertByteCount(utf8Slice(actual).getBytes(), offset, length, codePointCount, utf8Slice(expected).getBytes());
    }

    private static void assertByteCount(String actual, int offset, int length, int codePointCount, byte[] expected)
    {
        assertByteCount(utf8Slice(actual).getBytes(), offset, length, codePointCount, expected);
    }

    private static void assertByteCount(byte[] actual, int offset, int length, int codePointCount, byte[] expected)
    {
        Slice slice = wrappedBuffer(actual);
        int truncatedLength = byteCount(slice, offset, length, codePointCount);
        byte[] bytes = slice.getBytes(offset, truncatedLength);
        assertThat(bytes).isEqualTo(expected);
    }
}
