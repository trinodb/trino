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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;

import static com.google.common.base.Verify.verify;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.util.Arrays.fill;

public final class DictionaryAwareColumnarFilter
        implements ColumnarFilter
{
    private final ColumnarFilter columnarFilter;

    private Block lastInputDictionary;
    private boolean[] selectedEntries;
    private boolean[] filteredEntries;
    private boolean allEntriesFiltered;
    private int[] entriesToFilter = new int[0];
    private int[] selectedEntriesScratch = new int[0];

    public DictionaryAwareColumnarFilter(ColumnarFilter columnarFilter)
    {
        verify(columnarFilter.getInputChannels().size() == 1, "Dictionary aware filtering must have only one input");
        this.columnarFilter = columnarFilter;
    }

    @Override
    public int filterPositionsRange(ConnectorSession session, int[] outputPositions, int offset, int size, SourcePage loadedPage)
    {
        Block block = loadedPage.getBlock(0);
        if (block instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
            return processRle(session, outputPositions, offset, size, runLengthEncodedBlock);
        }
        else if (block instanceof DictionaryBlock dictionaryBlock) {
            try {
                return processDictionary(session, outputPositions, offset, size, dictionaryBlock);
            }
            catch (Exception ignored) {
                // Processing of dictionary failed, but we ignore the exception here
                // and force reprocessing of the whole block using the normal code.
                // The second pass may not fail due to filtering.
                // Entries are marked as filtered before they are filtered, so the state of a
                // dictionary that threw is discarded rather than read as a filtered entry
                // that nothing selected.
                lastInputDictionary = null;
            }
        }

        return columnarFilter.filterPositionsRange(session, outputPositions, offset, size, loadedPage);
    }

    @Override
    public int filterPositionsList(ConnectorSession session, int[] outputPositions, int[] activePositions, int offset, int size, SourcePage loadedPage)
    {
        Block block = loadedPage.getBlock(0);
        if (block instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
            return processRle(session, outputPositions, activePositions, offset, size, runLengthEncodedBlock);
        }
        else if (block instanceof DictionaryBlock dictionaryBlock) {
            try {
                return processDictionary(session, outputPositions, activePositions, offset, size, dictionaryBlock);
            }
            catch (Exception ignored) {
                // Processing of dictionary failed, but we ignore the exception here
                // and force reprocessing of the whole block using the normal code.
                // The second pass may not fail due to filtering.
                // Entries are marked as filtered before they are filtered, so the state of a
                // dictionary that threw is discarded rather than read as a filtered entry
                // that nothing selected.
                lastInputDictionary = null;
            }
        }

        return columnarFilter.filterPositionsList(session, outputPositions, activePositions, offset, size, loadedPage);
    }

    @Override
    public InputChannels getInputChannels()
    {
        return columnarFilter.getInputChannels();
    }

    private int processRle(ConnectorSession session, int[] outputPositions, int[] activePositions, int offset, int size, RunLengthEncodedBlock runLengthEncodedBlock)
    {
        Block value = runLengthEncodedBlock.getValue();
        prepareDictionary(value, size);
        filterAllEntries(session, value);
        if (!selectedEntries[0]) {
            return 0;
        }
        arraycopy(activePositions, offset, outputPositions, 0, size);
        return size;
    }

    private int processRle(ConnectorSession session, int[] outputPositions, int offset, int size, RunLengthEncodedBlock runLengthEncodedBlock)
    {
        Block value = runLengthEncodedBlock.getValue();
        prepareDictionary(value, size);
        filterAllEntries(session, value);
        if (!selectedEntries[0]) {
            return 0;
        }
        for (int index = 0; index < size; index++) {
            outputPositions[index] = offset + index;
        }
        return size;
    }

    private int processDictionary(ConnectorSession session, int[] outputPositions, int offset, int size, DictionaryBlock dictionaryBlock)
    {
        Block dictionary = dictionaryBlock.getDictionary();
        prepareDictionary(dictionary, size);
        if (dictionary.getPositionCount() <= size) {
            filterAllEntries(session, dictionary);
        }
        else {
            int entryCount = 0;
            for (int position = offset; position < offset + size; position++) {
                int entry = dictionaryBlock.getId(position);
                if (!filteredEntries[entry]) {
                    filteredEntries[entry] = true;
                    entriesToFilter[entryCount] = entry;
                    entryCount++;
                }
            }
            filterEntries(session, dictionary, entryCount);
        }

        int selectedPositionsCount = 0;
        for (int position = offset; position < offset + size; position++) {
            outputPositions[selectedPositionsCount] = position;
            selectedPositionsCount += selectedEntries[dictionaryBlock.getId(position)] ? 1 : 0;
        }
        return selectedPositionsCount;
    }

    private int processDictionary(ConnectorSession session, int[] outputPositions, int[] activePositions, int offset, int size, DictionaryBlock dictionaryBlock)
    {
        Block dictionary = dictionaryBlock.getDictionary();
        prepareDictionary(dictionary, size);
        if (dictionary.getPositionCount() <= size) {
            filterAllEntries(session, dictionary);
        }
        else {
            int entryCount = 0;
            for (int index = offset; index < offset + size; index++) {
                int entry = dictionaryBlock.getId(activePositions[index]);
                if (!filteredEntries[entry]) {
                    filteredEntries[entry] = true;
                    entriesToFilter[entryCount] = entry;
                    entryCount++;
                }
            }
            filterEntries(session, dictionary, entryCount);
        }

        int selectedPositionsCount = 0;
        for (int index = offset; index < offset + size; index++) {
            int position = activePositions[index];
            outputPositions[selectedPositionsCount] = position;
            selectedPositionsCount += selectedEntries[dictionaryBlock.getId(position)] ? 1 : 0;
        }
        return selectedPositionsCount;
    }

    private void prepareDictionary(Block dictionary, int blockPositionsCount)
    {
        if (lastInputDictionary != dictionary) {
            int positionCount = dictionary.getPositionCount();
            selectedEntries = new boolean[positionCount];
            filteredEntries = new boolean[positionCount];
            allEntriesFiltered = false;
            lastInputDictionary = dictionary;
        }
        // an entry is filtered at most once, so a block never contributes more entries than it has positions
        int maxEntries = min(blockPositionsCount, lastInputDictionary.getPositionCount());
        if (entriesToFilter.length < maxEntries) {
            entriesToFilter = new int[maxEntries];
            selectedEntriesScratch = new int[maxEntries];
        }
    }

    /**
     * Filters every entry of the dictionary at once, which is what a dictionary no larger than
     * the block that encodes it deserves: the block is about to look at most of the entries
     * anyway, and one range filter beats collecting the entries first.
     */
    private void filterAllEntries(ConnectorSession session, Block dictionary)
    {
        if (allEntriesFiltered) {
            return;
        }
        int positionCount = dictionary.getPositionCount();
        int[] selected = new int[positionCount];
        int selectedCount = columnarFilter.filterPositionsRange(session, selected, 0, positionCount, SourcePage.create(dictionary));
        for (int index = 0; index < selectedCount; index++) {
            selectedEntries[selected[index]] = true;
        }
        fill(filteredEntries, true);
        allEntriesFiltered = true;
    }

    /**
     * Filters the entries collected in {@code entriesToFilter}, which are the entries the block
     * uses and nothing has filtered yet. A dictionary far larger than the block that encodes it
     * costs only the entries the block puts in play, rather than one evaluation per entry.
     */
    private void filterEntries(ConnectorSession session, Block dictionary, int entryCount)
    {
        if (entryCount == 0) {
            return;
        }
        int selectedCount = columnarFilter.filterPositionsList(session, selectedEntriesScratch, entriesToFilter, 0, entryCount, SourcePage.create(dictionary));
        for (int index = 0; index < selectedCount; index++) {
            selectedEntries[selectedEntriesScratch[index]] = true;
        }
    }
}
