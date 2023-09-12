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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Character.MAX_CODE_POINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestBoundedVarcharType
        extends AbstractTestType
{
    public TestBoundedVarcharType()
    {
        super(createVarcharType(6), String.class, createTestBlock(createVarcharType(6)));
    }

    private static Block createTestBlock(VarcharType type)
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
        return blockBuilder.build();
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
        assertEquals(range.getMin(), Slices.utf8Slice(""));
        assertEquals(range.getMax(), Slices.utf8Slice(Character.toString(MAX_CODE_POINT).repeat(((VarcharType) type).getBoundedLength())));
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
