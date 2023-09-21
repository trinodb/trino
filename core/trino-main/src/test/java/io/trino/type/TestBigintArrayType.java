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
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static io.trino.util.StructuralTestUtil.arrayBlockOf;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigintArrayType
        extends AbstractTestType
{
    public TestBigintArrayType()
    {
        super(TESTING_TYPE_MANAGER.getType(arrayType(BIGINT.getTypeSignature())), List.class, createTestBlock(TESTING_TYPE_MANAGER.getType(arrayType(BIGINT.getTypeSignature()))));
    }

    public static Block createTestBlock(Type arrayType)
    {
        BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 4);
        arrayType.writeObject(blockBuilder, arrayBlockOf(BIGINT, 1, 2));
        arrayType.writeObject(blockBuilder, arrayBlockOf(BIGINT, 1, 2, 3));
        arrayType.writeObject(blockBuilder, arrayBlockOf(BIGINT, 1, 2, 3));
        arrayType.writeObject(blockBuilder, arrayBlockOf(BIGINT, 100, 200, 300));
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        Block block = (Block) value;
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, block.getPositionCount() + 1);
        for (int i = 0; i < block.getPositionCount(); i++) {
            BIGINT.appendTo(block, i, blockBuilder);
        }
        BIGINT.writeLong(blockBuilder, 1L);

        return blockBuilder.build();
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
