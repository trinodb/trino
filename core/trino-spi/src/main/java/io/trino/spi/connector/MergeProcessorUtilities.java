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
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RowBlock;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static io.trino.spi.connector.MergeDetails.DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.MergeDetails.INSERT_OPERATION_NUMBER;
import static java.lang.String.format;

public final class MergeProcessorUtilities
{
    private MergeProcessorUtilities() {}

    public static PagePair createMergedDeleteAndInsertPages(Page inputPage, int dataColumnCount)
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
        int[] deletePositions = getPositionsForPredicate(positionCount, position -> operationBlock.getInt(position, 0) == DELETE_OPERATION_NUMBER);
        if (deletePositions.length > 0) {
            deletePage = Optional.of(getPositionsHandlingRowId(inputPage, deletePositions));
        }
        Optional<Page> insertPage = Optional.empty();
        int[] insertPositions = getPositionsForPredicate(positionCount, position -> operationBlock.getInt(position, 0) == INSERT_OPERATION_NUMBER);
        if (insertPositions.length > 0) {
            insertPage = Optional.of(getPositionsHandlingRowId(inputPage, insertPositions));
        }
        return new PagePair(deletePage, insertPage);
    }

    public static Block getUnderlyingBlock(Block block)
    {
        while (block instanceof DictionaryBlock) {
            block = ((DictionaryBlock) block).getDictionary();
        }
        return block;
    }

    public static Block getAllNullsRowIdBlock(Block rowIdBlock, Block underlyingBlock, int positionCount)
    {
        boolean[] nulls = new boolean[positionCount];
        Arrays.fill(nulls, true);
        if (underlyingBlock instanceof RowBlock) {
            return RowBlock.fromFieldBlocks(positionCount, Optional.of(nulls), rowIdBlock.getChildren().toArray(new Block[]{}));
        }
        else {
            return ArrayBlock.fromElementBlock(positionCount, Optional.of(nulls), new int[positionCount], underlyingBlock);
        }
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

    private static Page getPositionsHandlingRowId(Page page, int[] positions)
    {
        int positionCount = positions.length;
        int channelCount = page.getChannelCount();
        Block[] newPageBlocks = new Block[channelCount];
        for (int channel = 0; channel < channelCount - 1; channel++) {
            newPageBlocks[channel] = page.getBlock(channel).getPositions(positions, 0, positionCount);
        }
        Block rowIdBlock = page.getBlock(channelCount - 1);
        newPageBlocks[channelCount - 1] = extractRowIdBlockPositions(rowIdBlock, positions);
        return new Page(newPageBlocks);
    }

    private static Block extractRowIdBlockPositions(Block rowIdBlock, int[] positions)
    {
        int positionCount = positions.length;
        Block underlyingBlock = MergeProcessorUtilities.getUnderlyingBlock(rowIdBlock);
        if (underlyingBlock.allPositionsAreNull()) {
            return getAllNullsRowIdBlock(rowIdBlock, underlyingBlock, positionCount);
        }
        else {
            List<Block> rowIdChildren = rowIdBlock.getChildren();
            int rowIdChildCount = rowIdChildren.size();
            Block[] rowIdBlocks = new Block[rowIdChildCount];
            for (int channel = 0; channel < rowIdChildCount; channel++) {
                rowIdBlocks[channel] = rowIdChildren.get(channel).getPositions(positions, 0, positionCount);
            }
            return RowBlock.fromFieldBlocks(positionCount, Optional.empty(), rowIdBlocks);
        }
    }
}
