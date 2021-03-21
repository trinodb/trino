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
package io.trino.spi.connector;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;

import java.util.Optional;
import java.util.function.Predicate;

import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;

public final class MergeProcessorUtilities
{
    public static final int INSERT_OPERATION_NUMBER = 1;
    public static final int DELETE_OPERATION_NUMBER = 2;
    public static final int UPDATE_OPERATION_NUMBER = 3;

    private MergeProcessorUtilities() {}

    public static MergePage createMergedDeleteAndInsertPages(Page inputPage, int dataColumnCount)
    {
        // Blocks 0..n-3 in inputPage are the data column blocks.  The last block in the
        // inputPage is is the rowId block. The next-to-last block in the inputPage is the
        // IntArrayBlock operation block, with values DELETE_OPERATION_NUMBER
        // and INSERT_OPERATION_NUMBER
        int inputChannelCount = inputPage.getChannelCount();
        if (inputChannelCount != dataColumnCount + 2) {
            throw new IllegalArgumentException(format("inputPage channelCount (%s) == dataColumns size (%s) + 2", inputChannelCount, dataColumnCount));
        }

        int positionCount = inputPage.getPositionCount();
        if (positionCount <= 0) {
            throw new IllegalArgumentException("positionCount should be > 0, but is " + positionCount);
        }
        Block operationBlock = inputPage.getBlock(inputChannelCount - 2);

        Optional<Page> deletePage = Optional.empty();
        int[] deletePositions = getPositionsForPredicate(positionCount, position -> INTEGER.getLong(operationBlock, position) == DELETE_OPERATION_NUMBER);
        if (deletePositions.length > 0) {
            deletePage = Optional.of(inputPage.getPositions(deletePositions, 0, deletePositions.length));
        }
        Optional<Page> insertPage = Optional.empty();
        int[] insertPositions = getPositionsForPredicate(positionCount, position -> INTEGER.getLong(operationBlock, position) == INSERT_OPERATION_NUMBER);
        if (insertPositions.length > 0) {
            insertPage = Optional.of(inputPage.getPositions(insertPositions, 0, insertPositions.length));
        }
        return new MergePage(deletePage, insertPage);
    }

    public static Block getUnderlyingBlock(Block block)
    {
        while (block instanceof DictionaryBlock) {
            block = ((DictionaryBlock) block).getDictionary();
        }
        return block;
    }

    public static int[] getPositionsForPredicate(int positionCount, Predicate<Integer> positionPicker)
    {
        int counter = 0;
        for (int position = 0; position < positionCount; position++) {
            if (positionPicker.test(position)) {
                counter++;
            }
        }
        int[] positions = new int[counter];
        int cursor = 0;
        for (int position = 0; position < positionCount; position++) {
            if (positionPicker.test(position)) {
                positions[cursor] = position;
                cursor++;
            }
        }
        return positions;
    }
}
