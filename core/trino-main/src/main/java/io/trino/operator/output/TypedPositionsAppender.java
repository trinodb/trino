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
package io.trino.operator.output;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public class TypedPositionsAppender
        implements BlockTypeAwarePositionsAppender
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TypedPositionsAppender.class).instanceSize();

    protected final Type type;
    protected final BlockBuilder blockBuilder;

    TypedPositionsAppender(Type type, @Nullable BlockBuilderStatus blockBuilderStatus, int expectedPositions)
    {
        this(
                requireNonNull(type, "type is null"),
                type.createBlockBuilder(blockBuilderStatus, expectedPositions));
    }

    TypedPositionsAppender(Type type, BlockBuilder blockBuilder)
    {
        this.type = requireNonNull(type, "type is null");
        this.blockBuilder = requireNonNull(blockBuilder, "blockBuilder is null");
    }

    @Override
    public void append(IntArrayList positions, Block source)
    {
        int[] positionArray = positions.elements();
        for (int i = 0; i < positions.size(); i++) {
            type.appendTo(source, positionArray[i], blockBuilder);
        }
    }

    @Override
    public void appendRle(RunLengthEncodedBlock block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            type.appendTo(block, 0, blockBuilder);
        }
    }

    @Override
    public void appendDictionary(IntArrayList positions, DictionaryBlock source)
    {
        int[] positionArray = positions.elements();
        for (int i = 0; i < positions.size(); i++) {
            type.appendTo(source, positionArray[i], blockBuilder);
        }
    }

    @Override
    public void appendRow(Block source, int position)
    {
        type.appendTo(source, position, blockBuilder);
    }

    @Override
    public Block build()
    {
        return blockBuilder.build();
    }

    @Override
    public BlockTypeAwarePositionsAppender newStateLike(@Nullable BlockBuilderStatus blockBuilderStatus)
    {
        return new TypedPositionsAppender(type, blockBuilder.newBlockBuilderLike(blockBuilderStatus));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + blockBuilder.getRetainedSizeInBytes();
    }
}
