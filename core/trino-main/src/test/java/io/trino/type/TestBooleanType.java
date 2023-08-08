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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.type.BooleanType;
import org.junit.jupiter.api.Test;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBooleanType
        extends AbstractTestType
{
    public TestBooleanType()
    {
        super(BOOLEAN, Boolean.class, createTestBlock());
    }

    @Test
    public void testBooleanBlockWithoutNullsFromByteArray()
    {
        byte[] booleanBytes = new byte[4];
        BlockBuilder builder = BOOLEAN.createFixedSizeBlockBuilder(booleanBytes.length);
        for (int i = 0; i < booleanBytes.length; i++) {
            boolean value = i % 2 == 0;
            booleanBytes[i] = value ? (byte) 1 : 0;
            BOOLEAN.writeBoolean(builder, value);
        }
        Block wrappedBlock = BooleanType.wrapByteArrayAsBooleanBlockWithoutNulls(booleanBytes);
        Block builderBlock = builder.build();
        // wrapped instances have no nulls
        assertFalse(wrappedBlock.mayHaveNull());
        // wrapped byte array instances and builder based instances both produce ByteArrayBlock
        assertTrue(wrappedBlock instanceof ByteArrayBlock);
        assertTrue(builderBlock instanceof ByteArrayBlock);
        assertBlockEquals(BOOLEAN, wrappedBlock, builderBlock);
        // the wrapping instance does not copy the byte array defensively
        assertTrue(BOOLEAN.getBoolean(wrappedBlock, 0));
        booleanBytes[0] = 0;
        assertFalse(BOOLEAN.getBoolean(wrappedBlock, 0));
    }

    @Test
    public void testBooleanBlockWithSingleNonNullValue()
    {
        assertTrue(BooleanType.createBlockForSingleNonNullValue(true) instanceof ByteArrayBlock);
        assertTrue(BOOLEAN.getBoolean(BooleanType.createBlockForSingleNonNullValue(true), 0));
        assertFalse(BOOLEAN.getBoolean(BooleanType.createBlockForSingleNonNullValue(false), 0));
        assertFalse(BooleanType.createBlockForSingleNonNullValue(false).mayHaveNull());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = BOOLEAN.createBlockBuilder(null, 15);
        BOOLEAN.writeBoolean(blockBuilder, true);
        BOOLEAN.writeBoolean(blockBuilder, true);
        BOOLEAN.writeBoolean(blockBuilder, true);
        BOOLEAN.writeBoolean(blockBuilder, false);
        BOOLEAN.writeBoolean(blockBuilder, false);
        BOOLEAN.writeBoolean(blockBuilder, false);
        BOOLEAN.writeBoolean(blockBuilder, false);
        BOOLEAN.writeBoolean(blockBuilder, false);
        BOOLEAN.writeBoolean(blockBuilder, true);
        BOOLEAN.writeBoolean(blockBuilder, true);
        BOOLEAN.writeBoolean(blockBuilder, false);
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return true;
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
