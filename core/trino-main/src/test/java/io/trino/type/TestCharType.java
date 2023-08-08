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
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.SliceUtf8.codePointToUtf8;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.MIN_CODE_POINT;
import static java.lang.Character.MIN_SUPPLEMENTARY_CODE_POINT;
import static java.lang.Character.isSupplementaryCodePoint;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestCharType
        extends AbstractTestType
{
    private static final CharType CHAR_TYPE = createCharType(100);

    public TestCharType()
    {
        super(CHAR_TYPE, String.class, createTestBlock());
    }

    public static Block createTestBlock()
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
        return blockBuilder.build();
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

            String objectValue = (String) charType.getObjectValue(SESSION, block, 0);
            assertNotNull(objectValue);
            assertEquals(objectValue.codePointAt(0), codePoint, "first code point");
            assertEquals(objectValue.length(), codePointLengthInUtf16 + 2, "size");
            for (int i = codePointLengthInUtf16; i < objectValue.length(); i++) {
                assertEquals(objectValue.codePointAt(i), ' ');
            }
        }
    }

    @Test
    public void testRange()
    {
        Type.Range range = type.getRange().orElseThrow();
        assertEquals(range.getMin(), Slices.utf8Slice(Character.toString(MIN_CODE_POINT).repeat(((CharType) type).getLength())));
        assertEquals(range.getMax(), Slices.utf8Slice(Character.toString(MAX_CODE_POINT).repeat(((CharType) type).getLength())));
    }

    @Test
    public void testPreviousValue()
    {
        assertThat(type.getPreviousValue(getSampleValue()))
                .isEmpty();
    }

    @Test
    public void testNextValue()
    {
        assertThat(type.getNextValue(getSampleValue()))
                .isEmpty();
    }
}
