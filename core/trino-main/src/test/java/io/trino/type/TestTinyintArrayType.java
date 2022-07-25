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

import java.util.List;

import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static io.trino.util.StructuralTestUtil.arrayBlockOf;

public class TestTinyintArrayType
        extends AbstractTestType
{
    public TestTinyintArrayType()
    {
        super(TESTING_TYPE_MANAGER.getType(arrayType(TINYINT.getTypeSignature())), List.class, createTestBlock(TESTING_TYPE_MANAGER.getType(arrayType(TINYINT.getTypeSignature()))));
    }

    public static Block createTestBlock(Type arrayType)
    {
        BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 4);
        arrayType.writeObject(blockBuilder, arrayBlockOf(TINYINT, 1, 2));
        arrayType.writeObject(blockBuilder, arrayBlockOf(TINYINT, 1, 2, 3));
        arrayType.writeObject(blockBuilder, arrayBlockOf(TINYINT, 1, 2, 3));
        arrayType.writeObject(blockBuilder, arrayBlockOf(TINYINT, 100, 110, 127));
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        Block block = (Block) value;
        BlockBuilder blockBuilder = TINYINT.createBlockBuilder(null, block.getPositionCount() + 1);
        for (int i = 0; i < block.getPositionCount(); i++) {
            TINYINT.appendTo(block, i, blockBuilder);
        }
        TINYINT.writeLong(blockBuilder, 1L);

        return blockBuilder.build();
    }
}
