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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.slice.SliceUtf8.codePointToUtf8;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.CharType.createCharType;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.MIN_CODE_POINT;
import static java.lang.Character.MIN_SUPPLEMENTARY_CODE_POINT;
import static java.lang.Character.isSupplementaryCodePoint;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCharType
        extends AbstractTestType
{
    private static final CharType CHAR_TYPE = createCharType(100);

    public TestCharType()
    {
        super(CHAR_TYPE, String.class, createTestBlock());
    }

    public static ValueBlock createTestBlock()
    {
        BlockBuilder blockBuilder = CHAR_TYPE.createBlockBuilder(null, 15);
        CHAR_TYPE.writeString(blockBuilder, "apple");
        CHAR_TYPE.writeString(blockBuilder, "apple");
        CHAR_TYPE.writeString(blockBuilder, "apple");
        CHAR_TYPE.writeString(blockBuilder, "banana");
        CHAR_TYPE.writeString(blockBuilder, "banana");
        CHAR_TYPE.writeString(blockBuilder, "banana");
        CHAR_TYPE.writeString(blockBuilder, "banana");
        CHAR_TYPE.writeString(blockBuilder, "banana");
        CHAR_TYPE.writeString(blockBuilder, "cherry");
        CHAR_TYPE.writeString(blockBuilder, "cherry");
        CHAR_TYPE.writeString(blockBuilder, "date");
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return Slices.utf8Slice(((Slice) value).toStringUtf8() + "_");
    }

    @Test
    public void testGetObjectValue()
    {
        CharType charType = createCharType(3);

        for (int codePoint : ImmutableList.of(0, 1, 10, 17, (int) ' ', 127, 1011, 11_000, 65_891, MIN_SUPPLEMENTARY_CODE_POINT, MAX_CODE_POINT)) {
            VariableWidthBlockBuilder blockBuilder = charType.createBlockBuilder(null, 1);
            Slice slice = (codePoint != ' ') ? codePointToUtf8(codePoint) : EMPTY_SLICE;
            blockBuilder.writeEntry(slice);
            Block block = blockBuilder.build();
            int codePointLengthInUtf16 = isSupplementaryCodePoint(codePoint) ? 2 : 1;

            String objectValue = (String) charType.getObjectValue(block, 0);
            assertThat(objectValue).isNotNull();
            assertThat(objectValue.codePointAt(0))
                    .describedAs("first code point")
                    .isEqualTo(codePoint);
            assertThat(objectValue.length())
                    .describedAs("size")
                    .isEqualTo(codePointLengthInUtf16 + 2);
            for (int i = codePointLengthInUtf16; i < objectValue.length(); i++) {
                assertThat(objectValue.codePointAt(i)).isEqualTo(' ');
            }
        }
    }

    @Test
    public void testRange()
    {
        Type.Range range = type.getRange().orElseThrow();
        assertThat(range.getMin()).isEqualTo(Slices.utf8Slice(Character.toString(MIN_CODE_POINT).repeat(((CharType) type).getLength())));
        assertThat(range.getMax()).isEqualTo(Slices.utf8Slice(Character.toString(MAX_CODE_POINT).repeat(((CharType) type).getLength())));
    }

    @Test
    public void testPreviousValue()
    {
        assertThat(type.getPreviousValue(getSampleValue()))
                .isEqualTo(Optional.of(utf8Slice("apple" + " ".repeat(94) + "\u001F")));

        CharType charType = createCharType(3);
        assertThat(charType.getPreviousValue(utf8Slice("abc")))
                .isEqualTo(Optional.of(utf8Slice("abb")));
        assertThat(charType.getPreviousValue(utf8Slice("ab")))
                .isEqualTo(Optional.of(utf8Slice("ab\u001F")));
        assertThat(charType.getPreviousValue(EMPTY_SLICE))
                .isEqualTo(Optional.of(utf8Slice("  \u001F")));
        assertThat(charType.getPreviousValue(utf8Slice("a\0\0")))
                .isEqualTo(Optional.of(utf8Slice("`" + Character.toString(MAX_CODE_POINT).repeat(2))));
        assertThat(charType.getPreviousValue(utf8Slice(Character.toString(MIN_CODE_POINT).repeat(3))))
                .isEmpty();

        // the surrogate range has no UTF-8 encoding, so U+D7FF precedes U+E000
        assertThat(createCharType(2).getPreviousValue(utf8Slice("a\uE000")))
                .isEqualTo(Optional.of(utf8Slice("a\uD7FF")));

        assertThat(createCharType(0).getPreviousValue(EMPTY_SLICE))
                .isEmpty();
        assertThat(createCharType(101).getPreviousValue(utf8Slice("abc")))
                .isEmpty();

        // a value that is not valid UTF-8 has no known neighbors
        assertThat(charType.getPreviousValue(wrappedBuffer((byte) 0xC3)))
                .isEmpty();
        // an overlong encoding of the lowest code point is not valid UTF-8 either
        assertThat(charType.getPreviousValue(wrappedBuffer((byte) 0xC0, (byte) 0x80)))
                .isEmpty();
    }

    @Test
    public void testNextValue()
    {
        assertThat(type.getNextValue(getSampleValue()))
                .isEqualTo(Optional.of(utf8Slice("apple" + " ".repeat(94) + "!")));

        CharType charType = createCharType(3);
        assertThat(charType.getNextValue(utf8Slice("abc")))
                .isEqualTo(Optional.of(utf8Slice("abd")));
        assertThat(charType.getNextValue(utf8Slice("ab")))
                .isEqualTo(Optional.of(utf8Slice("ab!")));
        assertThat(charType.getNextValue(EMPTY_SLICE))
                .isEqualTo(Optional.of(utf8Slice("  !")));
        assertThat(charType.getNextValue(utf8Slice("`" + Character.toString(MAX_CODE_POINT).repeat(2))))
                .isEqualTo(Optional.of(utf8Slice("a\0\0")));
        assertThat(charType.getNextValue(utf8Slice(Character.toString(MAX_CODE_POINT).repeat(3))))
                .isEmpty();

        // the surrogate range has no UTF-8 encoding, so U+E000 follows U+D7FF
        assertThat(createCharType(2).getNextValue(utf8Slice("a\uD7FF")))
                .isEqualTo(Optional.of(utf8Slice("a\uE000")));

        assertThat(createCharType(0).getNextValue(EMPTY_SLICE))
                .isEmpty();
        assertThat(createCharType(101).getNextValue(utf8Slice("abc")))
                .isEmpty();

        // a value that is not valid UTF-8 has no known neighbors
        assertThat(charType.getNextValue(wrappedBuffer((byte) 0xC3)))
                .isEmpty();
        // an overlong encoding of the lowest code point is not valid UTF-8 either
        assertThat(charType.getNextValue(wrappedBuffer((byte) 0xC0, (byte) 0x80)))
                .isEmpty();
    }
}
