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
package io.trino.sql.gen.columnar;

import io.trino.operator.project.InputChannels;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import jakarta.annotation.Nullable;

import static com.google.common.base.Verify.verify;
import static java.lang.System.arraycopy;

public final class DictionaryAwareColumnarFilter
        implements ColumnarFilter
{
    private final ColumnarFilter columnarFilter;

    private Block lastInputDictionary;
    // null when the last dictionary was not processed and its blocks are filtered position by position
    @Nullable
    private boolean[] lastOutputDictionary;
    private long lastDictionaryUsageCount;

    public DictionaryAwareColumnarFilter(ColumnarFilter columnarFilter)
    {
        verify(columnarFilter.getInputChannels().size() == 1, "Dictionary aware filtering must have only one input");
        this.columnarFilter = columnarFilter;
    }

    @Override
    public int filterPositionsRange(ConnectorSession session, int[] outputPositions, int offset, int size, SourcePage loadedPage)
    {
        if (size == 0) {
            return 0;
        }
        Block block = loadedPage.getBlock(0);
        if (block instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
            return processRle(outputPositions, offset, size, selectedDictionaryMask(session, runLengthEncodedBlock.getValue(), size));
        }
        if (block instanceof DictionaryBlock dictionaryBlock) {
            boolean[] dictionaryMask = selectedDictionaryMaskOrNull(session, dictionaryBlock, size);
            if (dictionaryMask != null) {
                return processDictionary(outputPositions, offset, size, dictionaryBlock, dictionaryMask);
            }
            int[] rawIds = dictionaryBlock.getRawIds();
            int rawIdsOffset = dictionaryBlock.getRawIdsOffset() + offset;
            int outputPositionsCount = filterDictionaryEntries(session, outputPositions, dictionaryBlock, rawIds, rawIdsOffset, size);
            for (int index = 0; index < outputPositionsCount; index++) {
                outputPositions[index] += offset;
            }
            return outputPositionsCount;
        }

        return columnarFilter.filterPositionsRange(session, outputPositions, offset, size, loadedPage);
    }

    @Override
    public int filterPositionsList(ConnectorSession session, int[] outputPositions, int[] activePositions, int offset, int size, SourcePage loadedPage)
    {
        if (size == 0) {
            return 0;
        }
        Block block = loadedPage.getBlock(0);
        if (block instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
            return processRle(outputPositions, activePositions, offset, size, selectedDictionaryMask(session, runLengthEncodedBlock.getValue(), size));
        }
        if (block instanceof DictionaryBlock dictionaryBlock) {
            boolean[] dictionaryMask = selectedDictionaryMaskOrNull(session, dictionaryBlock, size);
            if (dictionaryMask != null) {
                return processDictionary(outputPositions, activePositions, offset, size, dictionaryBlock, dictionaryMask);
            }
            int[] rawIds = dictionaryBlock.getRawIds();
            int rawIdsOffset = dictionaryBlock.getRawIdsOffset();
            int[] ids = new int[size];
            for (int index = 0; index < size; index++) {
                ids[index] = rawIds[rawIdsOffset + activePositions[offset + index]];
            }
            int outputPositionsCount = filterDictionaryEntries(session, outputPositions, dictionaryBlock, ids, 0, size);
            for (int index = 0; index < outputPositionsCount; index++) {
                outputPositions[index] = activePositions[offset + outputPositions[index]];
            }
            return outputPositionsCount;
        }

        return columnarFilter.filterPositionsList(session, outputPositions, activePositions, offset, size, loadedPage);
    }

    @Override
    public InputChannels getInputChannels()
    {
        return columnarFilter.getInputChannels();
    }

    private static int processRle(int[] outputPositions, int[] activePositions, int offset, int size, boolean[] selectedPositionsMask)
    {
        if (!selectedPositionsMask[0]) {
            return 0;
        }
        arraycopy(activePositions, offset, outputPositions, 0, size);
        return size;
    }

    private static int processRle(int[] outputPositions, int offset, int size, boolean[] selectedPositionsMask)
    {
        if (!selectedPositionsMask[0]) {
            return 0;
        }
        for (int index = 0; index < size; index++) {
            outputPositions[index] = offset + index;
        }
        return size;
    }

    private static int processDictionary(int[] outputPositions, int offset, int size, DictionaryBlock dictionaryBlock, boolean[] dictionaryMask)
    {
        int selectedPositionsCount = 0;
        for (int position = offset; position < offset + size; position++) {
            outputPositions[selectedPositionsCount] = position;
            selectedPositionsCount += dictionaryMask[dictionaryBlock.getId(position)] ? 1 : 0;
        }
        return selectedPositionsCount;
    }

    private static int processDictionary(int[] outputPositions, int[] activePositions, int offset, int size, DictionaryBlock dictionaryBlock, boolean[] dictionaryMask)
    {
        int selectedPositionsCount = 0;
        for (int index = offset; index < offset + size; index++) {
            int position = activePositions[index];
            outputPositions[selectedPositionsCount] = position;
            selectedPositionsCount += dictionaryMask[dictionaryBlock.getId(position)] ? 1 : 0;
        }
        return selectedPositionsCount;
    }

    /**
     * Filters the dictionary entries referenced by size ids starting at idsOffset and returns the selected indexes into that range.
     */
    private int filterDictionaryEntries(ConnectorSession session, int[] outputPositions, DictionaryBlock dictionaryBlock, int[] ids, int idsOffset, int size)
    {
        ValueBlock values = dictionaryBlock.getDictionary().copyPositions(ids, idsOffset, size);
        return columnarFilter.filterPositionsRange(session, outputPositions, 0, size, SourcePage.create(values));
    }

    @Nullable
    private boolean[] selectedDictionaryMaskOrNull(ConnectorSession session, DictionaryBlock dictionaryBlock, int blockPositionsCount)
    {
        ValueBlock dictionary = dictionaryBlock.getDictionary();
        try {
            return selectedDictionaryMask(session, dictionary, blockPositionsCount);
        }
        catch (Exception _) {
            // Filtering an unused dictionary entry may fail, so the block is filtered on the entries it references
            skipDictionary(dictionary, blockPositionsCount);
            return null;
        }
    }

    /**
     * Returns the filter result for every dictionary entry, or null when filtering the dictionary is not
     * worth it because the last dictionary served fewer positions than it has entries.
     */
    @Nullable
    private boolean[] selectedDictionaryMask(ConnectorSession session, Block dictionary, int blockPositionsCount)
    {
        if (lastInputDictionary == dictionary) {
            lastDictionaryUsageCount += blockPositionsCount;
            return lastOutputDictionary;
        }

        // Process the dictionary when this is the first block, the dictionary is no larger than the block,
        // or the last dictionary was used for at least as many positions as it has entries
        boolean shouldProcessDictionary = lastInputDictionary == null
                || dictionary.getPositionCount() <= blockPositionsCount
                || lastDictionaryUsageCount >= lastInputDictionary.getPositionCount();

        if (!shouldProcessDictionary) {
            skipDictionary(dictionary, blockPositionsCount);
            return null;
        }

        int positionCount = dictionary.getPositionCount();
        int[] selectedPositions = new int[positionCount];
        int selectedPositionsCount = columnarFilter.filterPositionsRange(session, selectedPositions, 0, positionCount, SourcePage.create(dictionary));

        boolean[] positionsMask = new boolean[positionCount];
        for (int index = 0; index < selectedPositionsCount; index++) {
            positionsMask[selectedPositions[index]] = true;
        }
        lastInputDictionary = dictionary;
        lastOutputDictionary = positionsMask;
        lastDictionaryUsageCount = blockPositionsCount;
        return positionsMask;
    }

    /**
     * Records that blocks of this dictionary are filtered on the entries they reference.
     */
    private void skipDictionary(Block dictionary, int blockPositionsCount)
    {
        lastInputDictionary = dictionary;
        lastOutputDictionary = null;
        lastDictionaryUsageCount = blockPositionsCount;
    }
}
