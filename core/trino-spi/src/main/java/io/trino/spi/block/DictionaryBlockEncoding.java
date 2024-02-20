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
package io.trino.spi.block;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.Optional;

public class DictionaryBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "DICTIONARY";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        // The down casts here are safe because it is the block itself the provides this encoding implementation.
        DictionaryBlock dictionaryBlock = (DictionaryBlock) block;

        if (!dictionaryBlock.isCompact()) {
            throw new IllegalArgumentException("Dictionary should be compact");
        }

        // positionCount
        int positionCount = dictionaryBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        // dictionary
        Block dictionary = dictionaryBlock.getDictionary();
        blockEncodingSerde.writeBlock(sliceOutput, dictionary);

        // ids
        sliceOutput.writeInts(dictionaryBlock.getRawIds(), dictionaryBlock.getRawIdsOffset(), dictionaryBlock.getPositionCount());
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        // positionCount
        int positionCount = sliceInput.readInt();

        // dictionary
        Block dictionaryBlock = blockEncodingSerde.readBlock(sliceInput);

        // ids
        int[] ids = new int[positionCount];
        sliceInput.readInts(ids);

        // flatten the dictionary
        return dictionaryBlock.copyPositions(ids, 0, ids.length);
    }

    @Override
    public Optional<Block> replacementBlockForWrite(Block block)
    {
        DictionaryBlock dictionaryBlock = (DictionaryBlock) block;
        if (!dictionaryBlock.isCompact()) {
            return Optional.of(dictionaryBlock.compact());
        }

        if (dictionaryBlock.getUniqueIds() == dictionaryBlock.getPositionCount() && dictionaryBlock.isSequentialIds()) {
            // ids mapping is identity
            return Optional.of(dictionaryBlock.getDictionary());
        }

        return Optional.empty();
    }
}
