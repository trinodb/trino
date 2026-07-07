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

import com.google.common.collect.ImmutableMap;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

/**
 * Returns the top N rows from the source sorted according to the specified ordering in the keyChannelIndex channel.
 */
public class TopNProcessor
{
    public static final String SKIPPED_DECODE_POSITIONS = "Skipped decode positions";

    private final LocalMemoryContext localUserMemoryContext;

    // placeholder null value per non-sort channel of a probe page; null for sort channels
    private final Block[] probeNullValues;
    private final GroupedTopNRowNumberBuilder topNBuilder;
    private Iterator<Page> outputIterator;
    private long skippedDecodePositions;

    public TopNProcessor(
            AggregatedMemoryContext aggregatedMemoryContext,
            List<Type> types,
            int n,
            List<Integer> sortChannels,
            PageWithPositionComparator comparator)
    {
        requireNonNull(aggregatedMemoryContext, "aggregatedMemoryContext is null");
        checkArgument(n > 0, "n must be > 0, found: %s", n);
        this.localUserMemoryContext = aggregatedMemoryContext.newLocalMemoryContext(TopNProcessor.class.getSimpleName());
        requireNonNull(sortChannels, "sortChannels is null");
        this.probeNullValues = new Block[types.size()];
        for (int channel = 0; channel < types.size(); channel++) {
            if (!sortChannels.contains(channel)) {
                probeNullValues[channel] = types.get(channel).createNullBlock();
            }
        }

        topNBuilder = new GroupedTopNRowNumberBuilder(
                types,
                comparator,
                n,
                false,
                new int[0],
                new NoChannelGroupByHash());
    }

    public void addInput(Page page)
    {
        boolean done = topNBuilder.processPage(requireNonNull(page, "page is null")).process();
        // there is no grouping so work will always be done
        verify(done);
        updateMemoryReservation();
    }

    /**
     * Decodes only the sort channels to find which positions may enter the top N, then decodes the
     * remaining channels only for those positions.
     */
    public void addMaskedInput(MaskedPage maskedPage)
    {
        int positionCount = maskedPage.getPositionCount();
        GroupedTopNRowNumberBuilder.ProbeResult probeResult = topNBuilder.probe(probePage(maskedPage));
        skippedDecodePositions += positionCount - probeResult.positionCount();
        if (probeResult.positionCount() == 0) {
            return;
        }
        maskedPage.selectPositions(probeResult.positions(), 0, probeResult.positionCount());
        int groupIdsOffset = 0;
        Iterator<Page> batches = maskedPage.materialize().iterator();
        while (batches.hasNext()) {
            Page batch = batches.next();
            topNBuilder.addPage(batch, probeResult.groupIds(), groupIdsOffset);
            groupIdsOffset += batch.getPositionCount();
            updateMemoryReservation();
        }
    }

    private Page probePage(MaskedPage maskedPage)
    {
        int positionCount = maskedPage.getPositionCount();
        Block[] blocks = new Block[probeNullValues.length];
        for (int channel = 0; channel < blocks.length; channel++) {
            if (probeNullValues[channel] == null) {
                blocks[channel] = maskedPage.getBlock(channel);
            }
            else {
                blocks[channel] = RunLengthEncodedBlock.create(probeNullValues[channel], positionCount);
            }
        }
        return new Page(positionCount, blocks);
    }

    public Page getOutput()
    {
        if (outputIterator == null) {
            // start flushing
            outputIterator = topNBuilder.buildResult();
        }

        Page output = null;
        if (outputIterator.hasNext()) {
            output = outputIterator.next();
        }
        else {
            outputIterator = emptyIterator();
        }
        updateMemoryReservation();
        return output;
    }

    public boolean noMoreOutput()
    {
        return outputIterator != null && !outputIterator.hasNext();
    }

    public Metrics getMetrics()
    {
        if (skippedDecodePositions == 0) {
            return Metrics.EMPTY;
        }
        return new Metrics(ImmutableMap.of(SKIPPED_DECODE_POSITIONS, new LongCount(skippedDecodePositions)));
    }

    private void updateMemoryReservation()
    {
        localUserMemoryContext.setBytes(topNBuilder.getEstimatedSizeInBytes());
    }
}
