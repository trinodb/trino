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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Character.MAX_CODE_POINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBoundedVarcharType
        extends AbstractTestType
{
    public TestBoundedVarcharType()
    {
        super(createVarcharType(6), String.class, createTestBlock(createVarcharType(6)));
    }

    private static ValueBlock createTestBlock(VarcharType type)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 15);
        type.writeString(blockBuilder, "apple");
        type.writeString(blockBuilder, "apple");
        type.writeString(blockBuilder, "apple");
        type.writeString(blockBuilder, "banana");
        type.writeString(blockBuilder, "banana");
        type.writeString(blockBuilder, "banana");
        type.writeString(blockBuilder, "banana");
        type.writeString(blockBuilder, "banana");
        type.writeString(blockBuilder, "cherry");
        type.writeString(blockBuilder, "cherry");
        type.writeString(blockBuilder, "date");
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return Slices.utf8Slice(((Slice) value).toStringUtf8() + "_");
    }

    @Test
    public void testRange()
    {
        Type.Range range = type.getRange().orElseThrow();
        assertThat(range.getMin()).isEqualTo(Slices.utf8Slice(""));
        assertThat(range.getMax()).isEqualTo(Slices.utf8Slice(Character.toString(MAX_CODE_POINT).repeat(((VarcharType) type).getBoundedLength())));
    }

    @Test
    public void testPreviousValue()
    {
        // the greatest lesser value decrements the last code point and fills the remaining length with the highest one
        assertThat(type.getPreviousValue(getSampleValue()))
                .isEqualTo(Optional.of(utf8Slice("appld" + Character.toString(MAX_CODE_POINT))));
        assertThat(type.getPreviousValue(utf8Slice("abcdef")))
                .isEqualTo(Optional.of(utf8Slice("abcdee")));
        assertThat(type.getPreviousValue(utf8Slice("b")))
                .isEqualTo(Optional.of(utf8Slice("a" + Character.toString(MAX_CODE_POINT).repeat(5))));

        // a value ending with the lowest code point is directly preceded by the value without it
        assertThat(type.getPreviousValue(utf8Slice("apple\0")))
                .isEqualTo(Optional.of(utf8Slice("apple")));

        // the empty value is the least
        assertThat(type.getPreviousValue(EMPTY_SLICE))
                .isEmpty();

        // the surrogate range has no UTF-8 encoding, so U+D7FF precedes U+E000
        assertThat(createVarcharType(2).getPreviousValue(utf8Slice("a\uE000")))
                .isEqualTo(Optional.of(utf8Slice("a\uD7FF")));

        assertThat(createVarcharType(101).getPreviousValue(utf8Slice("abc")))
                .isEmpty();

        // a value that is not valid UTF-8 has no known neighbors
        assertThat(type.getPreviousValue(wrappedBuffer((byte) 0xC3)))
                .isEmpty();
        // an overlong encoding of the lowest code point is not valid UTF-8 either
        assertThat(type.getPreviousValue(wrappedBuffer((byte) 0xC0, (byte) 0x80)))
                .isEmpty();
    }

    @Test
    public void testNextValue()
    {
        // the least greater value appends the lowest code point
        assertThat(type.getNextValue(getSampleValue()))
                .isEqualTo(Optional.of(utf8Slice("apple\0")));
        assertThat(type.getNextValue(EMPTY_SLICE))
                .isEqualTo(Optional.of(utf8Slice("\0")));

        // a value of the maximum length cannot be extended, so the last code point is incremented instead
        assertThat(type.getNextValue(utf8Slice("abcdef")))
                .isEqualTo(Optional.of(utf8Slice("abcdeg")));
        assertThat(type.getNextValue(utf8Slice("abcde" + Character.toString(MAX_CODE_POINT))))
                .isEqualTo(Optional.of(utf8Slice("abcdf")));
        assertThat(type.getNextValue(utf8Slice(Character.toString(MAX_CODE_POINT).repeat(6))))
                .isEmpty();

        // the surrogate range has no UTF-8 encoding, so U+E000 follows U+D7FF
        assertThat(createVarcharType(2).getNextValue(utf8Slice("a\uD7FF")))
                .isEqualTo(Optional.of(utf8Slice("a\uE000")));

        // appending does not depend on the length, so it is supported for any type
        assertThat(createVarcharType(101).getNextValue(utf8Slice("abc")))
                .isEqualTo(Optional.of(utf8Slice("abc\0")));

        // a value that is not valid UTF-8 has no known neighbors
        assertThat(type.getNextValue(wrappedBuffer((byte) 0xC3)))
                .isEmpty();
        // an overlong encoding of the lowest code point is not valid UTF-8 either
        assertThat(type.getNextValue(wrappedBuffer((byte) 0xC0, (byte) 0x80)))
                .isEmpty();
    }
}
