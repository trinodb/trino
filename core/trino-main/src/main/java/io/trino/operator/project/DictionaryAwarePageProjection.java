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
package io.trino.operator.project;

import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.DictionaryId;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;

import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.block.DictionaryBlock.createProjectedDictionaryBlock;
import static io.trino.spi.block.DictionaryId.randomDictionaryId;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

public class DictionaryAwarePageProjection
        implements PageProjection
{
    private final PageProjection projection;
    private final Function<DictionaryBlock, DictionaryId> sourceIdFunction;

    private Block lastInputDictionary;
    private Optional<Block> lastOutputDictionary;
    private long lastDictionaryUsageCount;

    // The projected dictionary that is itself a dictionary, which is copied when another page shares its input dictionary
    private DictionaryBlock lastNestedOutputDictionary;

    public DictionaryAwarePageProjection(PageProjection projection, Function<DictionaryBlock, DictionaryId> sourceIdFunction)
    {
        this.projection = requireNonNull(projection, "projection is null");
        this.sourceIdFunction = sourceIdFunction;
        verify(projection.isDeterministic(), "projection must be deterministic");
        verify(projection.getInputChannels().size() == 1, "projection must have only one input");
    }

    @Override
    public boolean isDeterministic()
    {
        return projection.isDeterministic();
    }

    @Override
    public InputChannels getInputChannels()
    {
        return projection.getInputChannels();
    }

    @Override
    public Block project(ConnectorSession session, SourcePage page, SelectedPositions selectedPositions)
    {
        Block block = page.getBlock(0);
        Optional<Block> dictionary = Optional.empty();
        if (block instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
            dictionary = Optional.of(runLengthEncodedBlock.getValue());
        }
        else if (block instanceof DictionaryBlock dictionaryBlock) {
            dictionary = Optional.of(dictionaryBlock.getDictionary());
        }

        // Try use dictionary processing first; if it fails, fall back to the generic case
        // if the block is RLE or dictionary block, we may use dictionary processing
        Optional<Block> dictionaryOutput = Optional.empty();
        try {
            Block dictionaryProcessingProjection = createDictionaryBlockProjection(session, dictionary, block.getPositionCount());
            if (dictionaryProcessingProjection != null) {
                dictionaryOutput = Optional.of(dictionaryProcessingProjection);
                lastOutputDictionary = dictionaryOutput;
            }
        }
        catch (Exception _) {
            // Processing of dictionary failed, but we ignore the exception here
            // and force reprocessing of the whole block using the normal code.
            // The second pass may not fail due to filtering.
            // todo dictionary processing should be able to tolerate failures of unused elements
            clearOutputDictionary();
        }

        if (block instanceof DictionaryBlock) {
            // Record the usage count regardless of dictionary processing choice, so we have stats for next time.
            // This guarantees recording will happen once and only once regardless of whether dictionary processing was attempted and whether it succeeded.
            lastDictionaryUsageCount += selectedPositions.size();
        }

        if (dictionaryOutput.isPresent()) {
            if (block instanceof RunLengthEncodedBlock) {
                // single value block is always considered effective, but the processing could have thrown
                // in that case we fallback and process again so the correct error message sent
                return RunLengthEncodedBlock.create(dictionaryOutput.get(), selectedPositions.size());
            }

            if (block instanceof DictionaryBlock dictionaryBlock) {
                // if dictionary was processed, produce a dictionary block; otherwise do normal processing
                return projectDictionary(dictionaryBlock, dictionaryOutput.get(), selectedPositions);
            }

            throw new UnsupportedOperationException("unexpected block type " + block.getClass());
        }

        // there is no dictionary handling or dictionary handling failed; fall back to general projection
        if (block instanceof DictionaryBlock dictionaryBlock) {
            // project the dictionary at translated ids so the projection reads the flat block
            int[] outputIds = filterDictionaryIds(dictionaryBlock, selectedPositions);
            return projection.project(session, SourcePage.create(dictionaryBlock.getDictionary()), SelectedPositions.positionsList(outputIds, 0, outputIds.length));
        }
        return projection.project(session, SourcePage.create(block), selectedPositions);
    }

    // This is not part of project, so project is small enough to be inlined into the page processor
    private Block projectDictionary(DictionaryBlock dictionaryBlock, Block outputDictionary, SelectedPositions selectedPositions)
    {
        int[] outputIds = filterDictionaryIds(dictionaryBlock, selectedPositions);
        if (outputDictionary instanceof DictionaryBlock nestedDictionary) {
            // The projected dictionary is itself a dictionary, so each page maps its selected ids to the values
            // under it. Once another page shares the input dictionary, and the pages have selected at least as many
            // positions as it has, copy the projected values, so this and the following pages select them directly,
            // without the mapping. Copying sooner could cost more than the sparse pages it serves.
            if (nestedDictionary != lastNestedOutputDictionary || lastDictionaryUsageCount < nestedDictionary.getPositionCount()) {
                lastNestedOutputDictionary = nestedDictionary;
                int[] nestedIds = nestedDictionary.getRawIds();
                int nestedIdsOffset = nestedDictionary.getRawIdsOffset();
                for (int index = 0; index < outputIds.length; index++) {
                    outputIds[index] = nestedIds[nestedIdsOffset + outputIds[index]];
                }
                return createProjectedDictionaryBlock(outputIds.length, nestedDictionary.getDictionary(), outputIds, randomDictionaryId());
            }
            outputDictionary = nestedDictionary.getDictionary().copyPositions(nestedDictionary.getRawIds(), nestedDictionary.getRawIdsOffset(), nestedDictionary.getPositionCount());
            clearOutputDictionary();
            lastOutputDictionary = Optional.of(outputDictionary);
        }
        return createProjectedDictionaryBlock(outputIds.length, outputDictionary, outputIds, sourceIdFunction.apply(dictionaryBlock));
    }

    private Block createDictionaryBlockProjection(ConnectorSession session, Optional<Block> dictionary, int blockPositionsCount)
    {
        if (dictionary.isEmpty()) {
            clearOutputDictionary();
            return null;
        }

        if (lastInputDictionary == dictionary.get()) {
            // we must have fallen back last time if lastOutputDictionary is null
            return lastOutputDictionary.orElse(null);
        }

        // Process dictionary if:
        //   dictionary positions count is not greater than block positions count
        //   this is the first block
        //   the last dictionary was used for more positions than were in the dictionary
        boolean shouldProcessDictionary = dictionary.get().getPositionCount() <= blockPositionsCount || lastInputDictionary == null || lastDictionaryUsageCount >= lastInputDictionary.getPositionCount();

        // record the usage count regardless of dictionary processing choice, so we have stats for next time
        lastDictionaryUsageCount = 0;
        lastInputDictionary = dictionary.get();
        clearOutputDictionary();

        if (shouldProcessDictionary) {
            return projection.project(session, SourcePage.create(lastInputDictionary), SelectedPositions.positionsRange(0, lastInputDictionary.getPositionCount()));
        }
        return null;
    }

    private void clearOutputDictionary()
    {
        lastOutputDictionary = Optional.empty();
        lastNestedOutputDictionary = null;
    }

    private static int[] filterDictionaryIds(DictionaryBlock dictionaryBlock, SelectedPositions selectedPositions)
    {
        int[] outputIds = new int[selectedPositions.size()];
        if (selectedPositions.isList()) {
            int[] positions = selectedPositions.getPositions();
            int endPosition = selectedPositions.getOffset() + selectedPositions.size();
            int outputIndex = 0;
            for (int position = selectedPositions.getOffset(); position < endPosition; position++) {
                outputIds[outputIndex++] = dictionaryBlock.getId(positions[position]);
            }
        }
        else {
            checkFromIndexSize(selectedPositions.getOffset(), selectedPositions.size(), dictionaryBlock.getPositionCount());
            System.arraycopy(dictionaryBlock.getRawIds(), dictionaryBlock.getRawIdsOffset() + selectedPositions.getOffset(), outputIds, 0, outputIds.length);
        }
        return outputIds;
    }
}
