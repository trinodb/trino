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
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestUnboundedVarcharType
        extends AbstractTestType
{
    public TestUnboundedVarcharType()
    {
        super(VARCHAR, String.class, createTestBlock());
    }

    private static Block createTestBlock()
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 15);
        VARCHAR.writeString(blockBuilder, "apple");
        VARCHAR.writeString(blockBuilder, "apple");
        VARCHAR.writeString(blockBuilder, "apple");
        VARCHAR.writeString(blockBuilder, "banana");
        VARCHAR.writeString(blockBuilder, "banana");
        VARCHAR.writeString(blockBuilder, "banana");
        VARCHAR.writeString(blockBuilder, "banana");
        VARCHAR.writeString(blockBuilder, "banana");
        VARCHAR.writeString(blockBuilder, "cherry");
        VARCHAR.writeString(blockBuilder, "cherry");
        VARCHAR.writeString(blockBuilder, "date");
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
