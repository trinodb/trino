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

import io.trino.operator.CompletedWork;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.Work;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.DictionaryId;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.block.DictionaryBlock.createProjectedDictionaryBlock;
import static java.util.Objects.requireNonNull;

public class DictionaryAwarePageProjection
        implements PageProjection
{
    private static final DriverYieldSignal NON_YIELDING_SIGNAL = new DriverYieldSignal();

    private final PageProjection projection;
    private final Function<DictionaryBlock, DictionaryId> sourceIdFunction;
    private final boolean produceLazyBlock;

    private Block lastInputDictionary;
    private Optional<Block> lastOutputDictionary;
    private long lastDictionaryUsageCount;

    public DictionaryAwarePageProjection(PageProjection projection, Function<DictionaryBlock, DictionaryId> sourceIdFunction, boolean produceLazyBlock)
    {
        this.projection = requireNonNull(projection, "projection is null");
        this.sourceIdFunction = sourceIdFunction;
        this.produceLazyBlock = produceLazyBlock;
        verify(projection.isDeterministic(), "projection must be deterministic");
        verify(projection.getInputChannels().size() == 1, "projection must have only one input");
    }

    @Override
    public Type getType()
    {
        return projection.getType();
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
    public Work<Block> project(ConnectorSession session, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
    {
        return new DictionaryAwarePageProjectionWork(session, yieldSignal, page, selectedPositions);
    }

    private class DictionaryAwarePageProjectionWork
            implements Work<Block>
    {
        private final ConnectorSession session;
        private final DriverYieldSignal yieldSignal;
        private final SelectedPositions selectedPositions;
        private final boolean produceLazyBlock;

        private Block block;
        private Block result;
        // if the block is RLE or dictionary block, we may use dictionary processing
        private Work<Block> dictionaryProcessingProjectionWork;
        // always prepare to fall back to a general block in case the dictionary does not apply or fails
        private Work<Block> fallbackProcessingProjectionWork;

        public DictionaryAwarePageProjectionWork(@Nullable ConnectorSession session, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
        {
            this.session = session;
            this.block = page.getBlock(0);
            this.selectedPositions = requireNonNull(selectedPositions, "selectedPositions is null");
            this.produceLazyBlock = DictionaryAwarePageProjection.this.produceLazyBlock && !block.isLoaded();

            if (produceLazyBlock) {
                this.yieldSignal = NON_YIELDING_SIGNAL;
            }
            else {
                this.yieldSignal = requireNonNull(yieldSignal, "yieldSignal is null");
                setupDictionaryBlockProjection();
            }
        }

        @Override
        public boolean process()
        {
            if (produceLazyBlock) {
                return true;
            }
            return processInternal();
        }

        private boolean processInternal()
        {
            checkState(result == null, "result has been generated");
            if (fallbackProcessingProjectionWork != null) {
                if (fallbackProcessingProjectionWork.process()) {
                    result = fallbackProcessingProjectionWork.getResult();
                    return true;
                }
                return false;
            }

            Optional<Block> dictionaryOutput = Optional.empty();
            if (dictionaryProcessingProjectionWork != null) {
                try {
                    if (!dictionaryProcessingProjectionWork.process()) {
                        // dictionary processing yielded.
                        return false;
                    }
                    dictionaryOutput = Optional.of(dictionaryProcessingProjectionWork.getResult());
                    lastOutputDictionary = dictionaryOutput;
                }
                catch (Exception ignored) {
                    // Processing of dictionary failed, but we ignore the exception here
                    // and force reprocessing of the whole block using the normal code.
                    // The second pass may not fail due to filtering.
                    // todo dictionary processing should be able to tolerate failures of unused elements
                    lastOutputDictionary = Optional.empty();
                    dictionaryProcessingProjectionWork = null;
                }
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
                    result = RunLengthEncodedBlock.create(dictionaryOutput.get(), selectedPositions.size());
                    return true;
                }

                if (block instanceof DictionaryBlock dictionaryBlock) {
                    // if dictionary was processed, produce a dictionary block; otherwise do normal processing
                    int[] outputIds = filterDictionaryIds(dictionaryBlock, selectedPositions);
                    result = createProjectedDictionaryBlock(selectedPositions.size(), dictionaryOutput.get(), outputIds, sourceIdFunction.apply(dictionaryBlock));
                    return true;
                }

                throw new UnsupportedOperationException("unexpected block type " + block.getClass());
            }

            // there is no dictionary handling or dictionary handling failed; fall back to general projection
            verify(dictionaryProcessingProjectionWork == null);
            verify(fallbackProcessingProjectionWork == null);
            fallbackProcessingProjectionWork = projection.project(session, yieldSignal, new Page(block), selectedPositions);
            if (fallbackProcessingProjectionWork.process()) {
                result = fallbackProcessingProjectionWork.getResult();
                return true;
            }
            return false;
        }

        @Override
        public Block getResult()
        {
            if (produceLazyBlock) {
                return new LazyBlock(selectedPositions.size(), () -> {
                    setupDictionaryBlockProjection();
                    checkState(processInternal(), "processInternal() yielded");
                    checkState(result != null, "result has not been generated");
                    return result.getLoadedBlock();
                });
            }
            checkState(result != null, "result has not been generated");
            return result;
        }

        private void setupDictionaryBlockProjection()
        {
            block = block.getLoadedBlock();

            Optional<Block> dictionary = Optional.empty();
            if (block instanceof RunLengthEncodedBlock) {
                dictionary = Optional.of(((RunLengthEncodedBlock) block).getValue());
            }
            else if (block instanceof DictionaryBlock) {
                dictionary = Optional.of(((DictionaryBlock) block).getDictionary());
            }

            // Try use dictionary processing first; if it fails, fall back to the generic case
            dictionaryProcessingProjectionWork = createDictionaryBlockProjection(dictionary, block.getPositionCount());
        }

        private Work<Block> createDictionaryBlockProjection(Optional<Block> dictionary, int blockPositionsCount)
        {
            if (dictionary.isEmpty()) {
                lastOutputDictionary = Optional.empty();
                return null;
            }

            if (lastInputDictionary == dictionary.get()) {
                // we must have fallen back last time if lastOutputDictionary is null
                return lastOutputDictionary.map(CompletedWork::new).orElse(null);
            }

            // Process dictionary if:
            //   dictionary positions count is not greater than block positions count
            //   this is the first block
            //   the last dictionary was used for more positions than were in the dictionary
            boolean shouldProcessDictionary = dictionary.get().getPositionCount() <= blockPositionsCount || lastInputDictionary == null || lastDictionaryUsageCount >= lastInputDictionary.getPositionCount();

            // record the usage count regardless of dictionary processing choice, so we have stats for next time
            lastDictionaryUsageCount = 0;
            lastInputDictionary = dictionary.get();
            lastOutputDictionary = Optional.empty();

            if (shouldProcessDictionary) {
                return projection.project(session, yieldSignal, new Page(lastInputDictionary), SelectedPositions.positionsRange(0, lastInputDictionary.getPositionCount()));
            }
            return null;
        }
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
            int endPosition = selectedPositions.getOffset() + selectedPositions.size();
            int outputIndex = 0;
            for (int position = selectedPositions.getOffset(); position < endPosition; position++) {
                outputIds[outputIndex++] = dictionaryBlock.getId(position);
            }
        }
        return outputIds;
    }
}
