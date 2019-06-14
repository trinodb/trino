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
package io.prestosql.spi.type;

import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.Chars.byteCountWithoutTrailingSpace;
import static io.prestosql.spi.type.Chars.padSpaces;
import static io.prestosql.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestChars
{
    @Test
    public void testPadSpaces()
    {
        String nonBmp = "\uD83D\uDE81"; // surrogate pair
        verify(nonBmp.codePoints().count() == 1, "nonBmp must be a single character (code point) string");
        verify(nonBmp.length() == 2, "nonBmp must be encoded as surrogate pairs (2 Java characters)");

        testPadSpaces("", 0, "");
        testPadSpaces("", 1, " ");
        testPadSpaces("", 3, "   ");
        testPadSpaces("ab", 3, "ab ");
        testPadSpaces("abc", 3, "abc");
        testPadSpaces(nonBmp, 3, nonBmp + "  ");
        testPadSpaces(nonBmp + nonBmp, 3, nonBmp + nonBmp + " ");
        testPadSpaces(nonBmp + nonBmp + nonBmp, 3, nonBmp + nonBmp + nonBmp);

        assertThatThrownBy(() -> padSpaces(nonBmp + nonBmp + nonBmp + nonBmp, createCharType(3))).hasMessage("pad length is smaller than text length");
        assertThatThrownBy(() -> padSpaces(utf8Slice(nonBmp + nonBmp + nonBmp + nonBmp), createCharType(3))).hasMessage("pad length is smaller than slice length");
        assertThatThrownBy(() -> padSpaces(utf8Slice(nonBmp + nonBmp + nonBmp + nonBmp), 3)).hasMessage("pad length is smaller than slice length");
    }

    private void testPadSpaces(String input, int length, String expected)
    {
        assertEquals(padSpaces(input, createCharType(length)), expected);
        assertEquals(padSpaces(utf8Slice(input), createCharType(length)), utf8Slice(expected));
        assertEquals(padSpaces(utf8Slice(input), length), utf8Slice(expected));
    }

    @Test
    public void testTruncateToLengthAndTrimSpaces()
    {
        assertEquals(utf8Slice("a"), truncateToLengthAndTrimSpaces(utf8Slice("a c"), 1));
        assertEquals(utf8Slice("a"), truncateToLengthAndTrimSpaces(utf8Slice("a  "), 1));
        assertEquals(utf8Slice("a"), truncateToLengthAndTrimSpaces(utf8Slice("abc"), 1));
        assertEquals(utf8Slice(""), truncateToLengthAndTrimSpaces(utf8Slice("a c"), 0));
        assertEquals(utf8Slice("a c"), truncateToLengthAndTrimSpaces(utf8Slice("a c "), 3));
        assertEquals(utf8Slice("a c"), truncateToLengthAndTrimSpaces(utf8Slice("a c "), 4));
        assertEquals(utf8Slice("a c"), truncateToLengthAndTrimSpaces(utf8Slice("a c "), 5));
        assertEquals(utf8Slice("a c"), truncateToLengthAndTrimSpaces(utf8Slice("a c"), 3));
        assertEquals(utf8Slice("a c"), truncateToLengthAndTrimSpaces(utf8Slice("a c"), 4));
        assertEquals(utf8Slice("a c"), truncateToLengthAndTrimSpaces(utf8Slice("a c"), 5));
        assertEquals(utf8Slice(""), truncateToLengthAndTrimSpaces(utf8Slice("  "), 1));
        assertEquals(utf8Slice(""), truncateToLengthAndTrimSpaces(utf8Slice(""), 1));
    }

    @Test
    public void testByteCountWithoutTrailingSpaces()
    {
        // single byte code points
        assertByteCountWithoutTrailingSpace("abc def ", 1, 0, "");
        assertByteCountWithoutTrailingSpace("abc def ", 0, 4, "abc");
        assertByteCountWithoutTrailingSpace("abc def ", 1, 3, "bc");
        assertByteCountWithoutTrailingSpace("abc def ", 0, 3, "abc");
        assertByteCountWithoutTrailingSpace("abc def ", 1, 2, "bc");
        assertByteCountWithoutTrailingSpace("abc def ", 0, 6, "abc de");
        assertByteCountWithoutTrailingSpace("abc def ", 1, 7, "bc def");
        assertByteCountWithoutTrailingSpace("abc def ", 0, 7, "abc def");
        assertByteCountWithoutTrailingSpace("abc def ", 1, 6, "bc def");
        assertByteCountWithoutTrailingSpace("abc  def ", 3, 1, "");
        assertByteCountWithoutTrailingSpace("abc  def ", 3, 3, "  d");
        assertByteCountWithoutTrailingSpace("abc  def ", 3, 4, "  de");
        assertByteCountWithoutTrailingSpaceFailure("abc def ", 4, 9);
        assertByteCountWithoutTrailingSpaceFailure("abc def ", 12, 1);
        assertByteCountWithoutTrailingSpaceFailure("abc def ", -1, 1);
        assertByteCountWithoutTrailingSpaceFailure("abc def ", 1, -1);
        assertByteCountWithoutTrailingSpace("       ", 0, 4, "");
        assertByteCountWithoutTrailingSpace("       ", 0, 0, "");

        // invalid code points
        assertByteCountWithoutTrailingSpace(new byte[] {(byte) 0x81, (byte) 0x81, (byte) 0x81}, 0, 2, new byte[] {(byte) 0x81, (byte) 0x81});
        assertByteCountWithoutTrailingSpace(new byte[] {(byte) 0x81, (byte) 0x81, (byte) 0x81}, 0, 1, new byte[] {(byte) 0x81});
        assertByteCountWithoutTrailingSpace(new byte[] {(byte) 0x81, (byte) 0x81, (byte) 0x81}, 0, 0, new byte[] {});
    }

    @Test
    public void testByteCountWithoutTrailingSpacesWithCodePointLimit()
    {
        // single byte code points
        assertByteCountWithoutTrailingSpace("abc def ", 1, 0, 1, "");
        assertByteCountWithoutTrailingSpace("abc def ", 0, 3, 4, "abc");
        assertByteCountWithoutTrailingSpace("abc def ", 0, 4, 4, "abc");
        assertByteCountWithoutTrailingSpace("abc def ", 0, 4, 3, "abc");
        assertByteCountWithoutTrailingSpace("abc def ", 0, 5, 4, "abc");

        // invalid code points
        assertByteCountWithoutTrailingSpace(new byte[] {(byte) 0x81, (byte) 0x81, (byte) ' ', (byte) 0x81}, 0, 3, 3, new byte[] {(byte) 0x81, (byte) 0x81});
        assertByteCountWithoutTrailingSpace(new byte[] {(byte) 0x81, (byte) 0x81, (byte) ' ', (byte) 0x81}, 0, 2, 3, new byte[] {(byte) 0x81, (byte) 0x81});
        assertByteCountWithoutTrailingSpace(new byte[] {(byte) 0x81, (byte) 0x81, (byte) ' ', (byte) 0x81}, 0, 0, 3, new byte[] {});
    }

    private static void assertByteCountWithoutTrailingSpaceFailure(String string, int offset, int maxLength)
    {
        try {
            byteCountWithoutTrailingSpace(utf8Slice(string), offset, maxLength);
            fail("Expected exception");
        }
        catch (IllegalArgumentException expected) {
        }
    }

    private static void assertByteCountWithoutTrailingSpace(String actual, int offset, int length, String expected)
    {
        assertByteCountWithoutTrailingSpace(utf8Slice(actual).getBytes(), offset, length, utf8Slice(expected).getBytes());
    }

    private static void assertByteCountWithoutTrailingSpace(byte[] actual, int offset, int length, byte[] expected)
    {
        Slice slice = wrappedBuffer(actual);
        int trimmedLength = byteCountWithoutTrailingSpace(slice, offset, length);
        byte[] bytes = slice.getBytes(offset, trimmedLength);
        assertEquals(bytes, expected);
    }

    private static void assertByteCountWithoutTrailingSpace(String actual, int offset, int length, int codePointCount, String expected)
    {
        assertByteCountWithoutTrailingSpace(utf8Slice(actual).getBytes(), offset, length, codePointCount, utf8Slice(expected).getBytes());
    }

    private static void assertByteCountWithoutTrailingSpace(byte[] actual, int offset, int length, int codePointCount, byte[] expected)
    {
        Slice slice = wrappedBuffer(actual);
        int truncatedLength = byteCountWithoutTrailingSpace(slice, offset, length, codePointCount);
        byte[] bytes = slice.getBytes(offset, truncatedLength);
        assertEquals(bytes, expected);
    }
}
