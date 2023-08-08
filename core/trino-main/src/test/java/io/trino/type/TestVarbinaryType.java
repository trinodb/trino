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
import io.trino.spi.type.SqlVarbinary;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestVarbinaryType
        extends AbstractTestType
{
    public TestVarbinaryType()
    {
        super(VARBINARY, SqlVarbinary.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = VARBINARY.createBlockBuilder(null, 15);
        VARBINARY.writeSlice(blockBuilder, Slices.utf8Slice("apple"));
        VARBINARY.writeSlice(blockBuilder, Slices.utf8Slice("apple"));
        VARBINARY.writeSlice(blockBuilder, Slices.utf8Slice("apple"));
        VARBINARY.writeSlice(blockBuilder, Slices.utf8Slice("banana"));
        VARBINARY.writeSlice(blockBuilder, Slices.utf8Slice("banana"));
        VARBINARY.writeSlice(blockBuilder, Slices.utf8Slice("banana"));
        VARBINARY.writeSlice(blockBuilder, Slices.utf8Slice("banana"));
        VARBINARY.writeSlice(blockBuilder, Slices.utf8Slice("banana"));
        VARBINARY.writeSlice(blockBuilder, Slices.utf8Slice("cherry"));
        VARBINARY.writeSlice(blockBuilder, Slices.utf8Slice("cherry"));
        VARBINARY.writeSlice(blockBuilder, Slices.utf8Slice("date"));
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
        assertThat(type.getRange())
                .isEmpty();
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
