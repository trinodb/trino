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
package io.trino.operator;

import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.project.PageProcessorMetrics;
import io.trino.operator.project.PageProjection;
import io.trino.operator.project.PageProjectionsProcessor;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Consumer;

import static io.trino.operator.project.SelectedPositions.positionsRange;
import static java.util.Objects.requireNonNull;

/**
 * A page whose channels are produced on first access, restricted to a mask of selected positions.
 * Passed from {@link Operator#getMaskedOutput} to {@link Operator#addMaskedInput} so the consumer
 * can decide, from a subset of channels, whether the remaining channels need to be produced at all.
 * A channel accessed via {@link #getBlock} is projected over the whole mask and cached; a later
 * {@link #materialize} reuses those cached blocks and produces the rest, sharing the producer's
 * batch-sizing and oversized-batch splitting through {@link PageProjectionsProcessor}.
 * <p>
 * A masked page is valid only until the driver next requests output from its producer; the
 * consumer must produce or copy everything it needs before returning from
 * {@link Operator#addMaskedInput}.
 */
public final class MaskedPage
{
    private final ConnectorSession session;
    private final SourcePage sourcePage;
    private final PageProjectionsProcessor projectionsProcessor;
    private final LocalMemoryContext memoryContext;
    private final PageProcessorMetrics metrics;
    private final Block[] computedBlocks;
    private Consumer<Page> outputValidator = _ -> {};

    /**
     * Applies {@code selectedPositions} to {@code sourcePage} and wraps it. The source page must
     * not be used directly afterwards.
     */
    public static MaskedPage applyMask(
            ConnectorSession session,
            SourcePage sourcePage,
            SelectedPositions selectedPositions,
            PageProjectionsProcessor projectionsProcessor,
            LocalMemoryContext memoryContext,
            PageProcessorMetrics metrics)
    {
        if (selectedPositions.isList()) {
            // copy: source pages wrap the array into blocks that outlive the filter evaluator's
            // reusable positions buffer
            int[] positions = Arrays.copyOfRange(
                    selectedPositions.getPositions(),
                    selectedPositions.getOffset(),
                    selectedPositions.getOffset() + selectedPositions.size());
            sourcePage.selectPositions(positions, 0, positions.length);
        }
        else if (selectedPositions.getOffset() != 0 || selectedPositions.size() != sourcePage.getPositionCount()) {
            sourcePage.selectPositions(selectedPositions.getOffset(), selectedPositions.size());
        }
        return new MaskedPage(session, sourcePage, projectionsProcessor, memoryContext, metrics);
    }

    private MaskedPage(
            ConnectorSession session,
            SourcePage sourcePage,
            PageProjectionsProcessor projectionsProcessor,
            LocalMemoryContext memoryContext,
            PageProcessorMetrics metrics)
    {
        this.session = requireNonNull(session, "session is null");
        this.sourcePage = requireNonNull(sourcePage, "sourcePage is null");
        this.projectionsProcessor = requireNonNull(projectionsProcessor, "projectionsProcessor is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        this.metrics = requireNonNull(metrics, "metrics is null");
        this.computedBlocks = new Block[projectionsProcessor.getProjectionCount()];
    }

    public int getPositionCount()
    {
        return sourcePage.getPositionCount();
    }

    public int getChannelCount()
    {
        return projectionsProcessor.getProjectionCount();
    }

    /**
     * Sets a validator applied to each page produced by {@link #materialize}. Runs on blocks that
     * are decoded anyway, so deferred channels stay untouched.
     */
    void setOutputValidator(Consumer<Page> outputValidator)
    {
        this.outputValidator = requireNonNull(outputValidator, "outputValidator is null");
    }

    /**
     * Produces the given channel, restricted to the mask. Computed once and cached; a subsequent
     * {@link #selectPositionsFrom} narrows the cached block.
     */
    public Block getBlock(int channel)
    {
        Block block = computedBlocks[channel];
        if (block == null) {
            PageProjection projection = projectionsProcessor.getProjections().get(channel);
            SourcePage inputPage = projection.getInputChannels().getInputChannels(sourcePage);
            block = projection.project(session, inputPage, positionsRange(0, sourcePage.getPositionCount()));
            computedBlocks[channel] = block;
            updateMemoryUsage();
        }
        return block;
    }

    /**
     * Narrows the mask to positions {@code [position, getPositionCount())}. Positions refer to the
     * current mask, not the original page.
     */
    public void selectPositionsFrom(int position)
    {
        if (position == 0) {
            return;
        }
        int size = sourcePage.getPositionCount() - position;
        sourcePage.selectPositions(position, size);
        for (int channel = 0; channel < computedBlocks.length; channel++) {
            Block block = computedBlocks[channel];
            if (block != null) {
                computedBlocks[channel] = block.getRegion(position, size);
            }
        }
        updateMemoryUsage();
    }

    private void updateMemoryUsage()
    {
        long retainedSizeInBytes = 0;
        for (Block block : computedBlocks) {
            if (block != null) {
                retainedSizeInBytes += block.getRetainedSizeInBytes();
            }
        }
        memoryContext.setBytes(retainedSizeInBytes);
    }

    /**
     * Produces all channels restricted to the mask, reusing blocks already produced by
     * {@link #getBlock} and computing the rest in batches bounded by output size. Must be drained
     * while this page is valid.
     */
    public WorkProcessor<Page> materialize()
    {
        return projectionsProcessor.project(session, memoryContext, metrics, sourcePage, positionsRange(0, sourcePage.getPositionCount()), Optional.of(computedBlocks))
                .map(page -> {
                    outputValidator.accept(page);
                    return page;
                });
    }
}
