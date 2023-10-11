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
package io.trino.operator.dynamicfiltering;

import com.google.common.annotations.VisibleForTesting;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import jakarta.annotation.Nullable;

import static io.trino.operator.project.SelectedPositions.positionsList;
import static io.trino.operator.project.SelectedPositions.positionsRange;

public class DictionaryAwareBlockFilter
{
    @Nullable
    private Block lastDictionary;
    @Nullable
    private boolean[] lastDictionaryPositionsMask;
    private long lastDictionaryUsageCount;

    public SelectedPositions filter(Block block, DynamicPageFilter.BlockFilter blockFilter, SelectedPositions selectedPositions)
    {
        Block loadedBlock = block.getLoadedBlock();
        if (loadedBlock instanceof DictionaryBlock) {
            DictionaryBlock dictionaryBlock = (DictionaryBlock) loadedBlock;
            // Attempt to process the dictionary.  If dictionary processing has not been considered effective, null will be returned
            boolean[] dictionaryPositionsMask = processDictionary(blockFilter, dictionaryBlock);
            // record the usage count regardless of dictionary processing choice, so we have stats for next time
            lastDictionaryUsageCount += dictionaryBlock.getPositionCount();
            // if dictionary was processed, produce a dictionary block; otherwise do normal processing
            if (dictionaryPositionsMask != null) {
                return selectDictionaryPositions(dictionaryPositionsMask, dictionaryBlock, selectedPositions);
            }
        }
        return blockFilter.filter(loadedBlock, selectedPositions);
    }

    // Should be called after we know that a filter is ineffective
    // to avoid holding on to cached dictionary and mask as they won't be used again
    public void cleanUp()
    {
        lastDictionary = null;
        lastDictionaryPositionsMask = null;
    }

    @VisibleForTesting
    boolean wasLastBlockDictionaryProcessed()
    {
        return lastDictionaryPositionsMask != null;
    }

    private boolean[] processDictionary(DynamicPageFilter.BlockFilter blockFilter, DictionaryBlock dictionaryBlock)
    {
        Block dictionary = dictionaryBlock.getDictionary();
        if (lastDictionary == dictionary) {
            return lastDictionaryPositionsMask;
        }
        // Process dictionary if:
        //   this is the first block
        //   dictionary positions count is smaller than block
        //   the last dictionary was used for more positions than were in the dictionary
        boolean shouldProcessDictionary = lastDictionary == null || dictionary.getPositionCount() < dictionaryBlock.getPositionCount() || lastDictionaryUsageCount >= lastDictionary.getPositionCount();
        lastDictionaryUsageCount = 0;
        lastDictionary = dictionary;

        if (shouldProcessDictionary) {
            lastDictionaryPositionsMask = blockFilter.selectedPositionsMask(dictionary);
        }
        else {
            lastDictionaryPositionsMask = null;
        }
        return lastDictionaryPositionsMask;
    }

    private static SelectedPositions selectDictionaryPositions(boolean[] dictionaryPositionsMask, DictionaryBlock block, SelectedPositions selectedPositions)
    {
        int positionCount = selectedPositions.size();
        int outputPositionsCount = 0;
        int[] outputPositions = new int[positionCount];
        if (selectedPositions.isList()) {
            int[] positions = selectedPositions.getPositions();
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                // extra copying to avoid branch
                outputPositions[outputPositionsCount] = position;
                // cast to byte
                outputPositionsCount += dictionaryPositionsMask[block.getId(position)] ? 1 : 0;
            }
        }
        else {
            for (int position = 0; position < positionCount; position++) {
                // extra copying to avoid branch
                outputPositions[outputPositionsCount] = position;
                // cast to byte
                outputPositionsCount += dictionaryPositionsMask[block.getId(position)] ? 1 : 0;
            }
            // full range was selected
            if (outputPositionsCount == positionCount) {
                return positionsRange(0, outputPositionsCount);
            }
        }
        return positionsList(outputPositions, 0, outputPositionsCount);
    }
}
