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
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public class BlockTypeDispatchingPositionsAppender
        implements BlockTypeAwarePositionsAppender
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BlockTypeDispatchingPositionsAppender.class).instanceSize();

    private final BlockTypeAwarePositionsAppender delegate;

    public BlockTypeDispatchingPositionsAppender(BlockTypeAwarePositionsAppender delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public void append(IntArrayList positions, Block source)
    {
        if (positions.isEmpty()) {
            return;
        }

        if (source instanceof RunLengthEncodedBlock) {
            delegate.appendRle(flattenRle((RunLengthEncodedBlock) source, positions.size()));
        }
        else if (source instanceof DictionaryBlock) {
            appendDictionary(positions, (DictionaryBlock) source);
        }
        else {
            delegate.append(positions, source);
        }
    }

    private BlockView flattenDictionary(DictionaryBlock source, IntArrayList positions)
    {
        Block dictionary = source.getDictionary();
        if (!(dictionary instanceof RunLengthEncodedBlock || dictionary instanceof DictionaryBlock)) {
            return new BlockView(source, positions);
        }

        while (dictionary instanceof RunLengthEncodedBlock || dictionary instanceof DictionaryBlock) {
            if (dictionary instanceof RunLengthEncodedBlock) {
                // since at some level, dictionary contains only a single value, it can be flattened to rle
                RunLengthEncodedBlock rleDictionary = flattenRle((RunLengthEncodedBlock) dictionary, positions.size());
                return new BlockView(rleDictionary, positions);
            }
            else {
                // dictionary is a nested dictionary. we need to remap the ids
                DictionaryBlock dictionaryValue = (DictionaryBlock) dictionary;
                int[] newPositions = new int[positions.size()];
                for (int i = 0; i < newPositions.length; i++) {
                    newPositions[i] = source.getId(positions.getInt(i));
                }
                positions = IntArrayList.wrap(newPositions);
                dictionary = dictionaryValue.getDictionary();
                source = dictionaryValue;
            }
        }
        return new BlockView(source, positions);
    }

    private RunLengthEncodedBlock flattenRle(RunLengthEncodedBlock source, int positionCount)
    {
        Block value = source.getValue();
        if (!(value instanceof RunLengthEncodedBlock || value instanceof DictionaryBlock)) {
            if (source.getPositionCount() == positionCount) {
                return source;
            }
            return new RunLengthEncodedBlock(source.getValue(), positionCount);
        }

        int position = 0;
        while (value instanceof RunLengthEncodedBlock || value instanceof DictionaryBlock) {
            if (value instanceof RunLengthEncodedBlock) {
                value = ((RunLengthEncodedBlock) value).getValue();
                position = 0;
            }
            else {
                DictionaryBlock dictionaryValue = (DictionaryBlock) value;
                position = dictionaryValue.getId(position);
                value = dictionaryValue.getDictionary();
            }
        }

        if (value.getPositionCount() > 1 || position != 0) {
            value = value.getSingleValueBlock(position);
        }
        return new RunLengthEncodedBlock(value, positionCount);
    }

    @Override
    public void appendRle(RunLengthEncodedBlock source)
    {
        delegate.appendRle(flattenRle(source, source.getPositionCount()));
    }

    @Override
    public void appendDictionary(IntArrayList positions, DictionaryBlock source)
    {
        BlockView flatDictionary = flattenDictionary(source, positions);
        if (flatDictionary.getBlock() instanceof DictionaryBlock) {
            delegate.appendDictionary(flatDictionary.getPositions(), (DictionaryBlock) flatDictionary.getBlock());
        }
        else {
            delegate.appendRle((RunLengthEncodedBlock) flatDictionary.getBlock());
        }
    }

    @Override
    public Block build()
    {
        return delegate.build();
    }

    @Override
    public BlockTypeAwarePositionsAppender newStateLike(@Nullable BlockBuilderStatus blockBuilderStatus)
    {
        return new BlockTypeDispatchingPositionsAppender((BlockTypeAwarePositionsAppender) delegate.newStateLike(blockBuilderStatus));
    }

    @Override
    public void appendRow(Block source, int position)
    {
        delegate.appendRow(source, position);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + delegate.getRetainedSizeInBytes();
    }

    private static class BlockView
    {
        private final Block block;
        private final IntArrayList positions;

        private BlockView(Block block, IntArrayList positions)
        {
            this.block = requireNonNull(block, "block is null");
            this.positions = requireNonNull(positions, "positions is null");
        }

        public Block getBlock()
        {
            return block;
        }

        public IntArrayList getPositions()
        {
            return positions;
        }
    }
}
