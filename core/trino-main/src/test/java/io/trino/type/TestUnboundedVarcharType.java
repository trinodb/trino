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
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestUnboundedVarcharType
        extends AbstractTestType
{
    public TestUnboundedVarcharType()
    {
        super(VARCHAR, String.class, createTestBlock());
    }

    private static ValueBlock createTestBlock()
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
        assertThat(type.getRange())
                .isEmpty();
    }

    @Test
    public void testPreviousValue()
    {
        // a lesser value can always be extended with the highest code point, so there is no greatest lesser value
        assertThat(type.getPreviousValue(getSampleValue()))
                .isEmpty();
        assertThat(type.getPreviousValue(EMPTY_SLICE))
                .isEmpty();
        assertThat(type.getPreviousValue(utf8Slice("apple\0")))
                .isEmpty();
    }

    @Test
    public void testNextValue()
    {
        assertThat(type.getNextValue(getSampleValue()))
                .isEqualTo(Optional.of(utf8Slice("apple\0")));
        assertThat(type.getNextValue(EMPTY_SLICE))
                .isEqualTo(Optional.of(utf8Slice("\0")));

        // a value that is not valid UTF-8 has no known neighbors
        assertThat(type.getNextValue(wrappedBuffer((byte) 0xC3)))
                .isEmpty();
    }
}
