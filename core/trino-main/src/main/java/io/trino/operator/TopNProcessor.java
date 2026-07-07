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

import com.google.common.collect.ImmutableList;
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

    private final List<Type> types;
    private final List<Integer> sortChannels;
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
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));

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
     * Decodes only the sort channels to find which positions enter the top N; the remaining
     * channels are decoded only for the surviving positions of a contributing page.
     */
    public void addMaskedInput(MaskedPage maskedPage)
    {
        int positionCount = maskedPage.getPositionCount();
        GroupedTopNRowNumberBuilder.ProbeResult probeResult = topNBuilder.probe(probePage(maskedPage));
        int firstPositionToAdd = probeResult.firstPositionToAdd();
        if (firstPositionToAdd < 0) {
            // no position enters the top N, so payload channels are never decoded for this page
            skippedDecodePositions += positionCount;
            return;
        }
        // payload channels are decoded only from firstPositionToAdd onward
        skippedDecodePositions += firstPositionToAdd;
        maskedPage.selectPositionsFrom(firstPositionToAdd);
        int groupIdsOffset = firstPositionToAdd;
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
        Block[] blocks = new Block[types.size()];
        for (int channel = 0; channel < blocks.length; channel++) {
            if (sortChannels.contains(channel)) {
                blocks[channel] = maskedPage.getBlock(channel);
            }
            else {
                blocks[channel] = RunLengthEncodedBlock.create(types.get(channel), null, positionCount);
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
