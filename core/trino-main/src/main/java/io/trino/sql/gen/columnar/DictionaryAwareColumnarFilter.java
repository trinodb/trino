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
import static java.lang.System.arraycopy;

public final class DictionaryAwareColumnarFilter
        implements ColumnarFilter
{
    private final ColumnarFilter columnarFilter;

    private Block lastInputDictionary;
    private boolean[] lastOutputDictionary;

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
                lastOutputDictionary = null;
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
                lastOutputDictionary = null;
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
        boolean[] selectedPositionsMask = selectedDictionaryMask(session, value);
        if (!selectedPositionsMask[0]) {
            return 0;
        }
        arraycopy(activePositions, offset, outputPositions, 0, size);
        return size;
    }

    private int processRle(ConnectorSession session, int[] outputPositions, int offset, int size, RunLengthEncodedBlock runLengthEncodedBlock)
    {
        Block value = runLengthEncodedBlock.getValue();
        boolean[] selectedPositionsMask = selectedDictionaryMask(session, value);
        if (!selectedPositionsMask[0]) {
            return 0;
        }
        for (int index = 0; index < size; index++) {
            outputPositions[index] = offset + index;
        }
        return size;
    }

    private int processDictionary(ConnectorSession session, int[] outputPositions, int offset, int size, DictionaryBlock dictionaryBlock)
    {
        boolean[] dictionaryMask = selectedDictionaryMask(session, dictionaryBlock.getDictionary());
        int selectedPositionsCount = 0;
        for (int position = offset; position < offset + size; position++) {
            outputPositions[selectedPositionsCount] = position;
            selectedPositionsCount += dictionaryMask[dictionaryBlock.getId(position)] ? 1 : 0;
        }
        return selectedPositionsCount;
    }

    private int processDictionary(ConnectorSession session, int[] outputPositions, int[] activePositions, int offset, int size, DictionaryBlock dictionaryBlock)
    {
        boolean[] dictionaryMask = selectedDictionaryMask(session, dictionaryBlock.getDictionary());
        int selectedPositionsCount = 0;
        for (int index = offset; index < offset + size; index++) {
            int position = activePositions[index];
            outputPositions[selectedPositionsCount] = position;
            selectedPositionsCount += dictionaryMask[dictionaryBlock.getId(position)] ? 1 : 0;
        }
        return selectedPositionsCount;
    }

    private boolean[] selectedDictionaryMask(ConnectorSession session, Block dictionary)
    {
        if (lastInputDictionary == dictionary) {
            return lastOutputDictionary;
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
        return positionsMask;
    }
}
