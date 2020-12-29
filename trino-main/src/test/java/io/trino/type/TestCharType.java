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
package io.prestosql.type;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.CharType;
import org.testng.annotations.Test;

import static io.airlift.slice.SliceUtf8.codePointToUtf8;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.MIN_SUPPLEMENTARY_CODE_POINT;
import static java.lang.Character.isSupplementaryCodePoint;
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
            BlockBuilder blockBuilder = charType.createBlockBuilder(null, 1);
            Slice slice = (codePoint != ' ') ? codePointToUtf8(codePoint) : EMPTY_SLICE;
            blockBuilder.writeBytes(slice, 0, slice.length());
            blockBuilder.closeEntry();
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
}
