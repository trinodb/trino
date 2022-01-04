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
package io.trino.operator.window;

import com.google.common.collect.ImmutableList;
import io.trino.operator.PagesHashStrategy;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexComparator;
import io.trino.operator.WindowOperator.FrameBoundKey;
import io.trino.spi.PageBuilder;
import io.trino.spi.function.WindowFunction;
import io.trino.spi.function.WindowIndex;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.WindowOperator.FrameBoundKey.Type.END;
import static io.trino.operator.WindowOperator.FrameBoundKey.Type.START;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;

public final class RegularWindowPartition
        implements WindowPartition
{
    private final PagesIndex pagesIndex;
    private final int partitionStart;
    private final int partitionEnd;

    private final int[] outputChannels;
    private final List<WindowFunction> windowFunctions;

    private final PagesHashStrategy peerGroupHashStrategy;

    private int peerGroupStart;
    private int peerGroupEnd;

    private int currentGroupIndex = -1;
    private int currentPosition;

    private final Map<Integer, Framing> framings = new HashMap<>();

    public RegularWindowPartition(
            PagesIndex pagesIndex,
            int partitionStart,
            int partitionEnd,
            int[] outputChannels,
            List<WindowFunction> windowFunctions,
            List<FrameInfo> frames,
            PagesHashStrategy peerGroupHashStrategy,
            Map<FrameBoundKey, PagesIndexComparator> frameBoundComparators)
    {
        this.pagesIndex = pagesIndex;
        this.partitionStart = partitionStart;
        this.partitionEnd = partitionEnd;
        this.outputChannels = outputChannels;
        this.windowFunctions = ImmutableList.copyOf(windowFunctions);
        this.peerGroupHashStrategy = peerGroupHashStrategy;

        // reset functions for new partition
        WindowIndex windowIndex = new PagesWindowIndex(pagesIndex, partitionStart, partitionEnd);
        for (WindowFunction windowFunction : windowFunctions) {
            windowFunction.reset(windowIndex);
        }

        currentPosition = partitionStart;
        updatePeerGroup();

        for (int i = 0; i < windowFunctions.size(); i++) {
            FrameInfo frame = frames.get(i);

            Framing framing;
            switch (frame.getType()) {
                case RANGE:
                    PagesIndexComparator startComparator = frameBoundComparators.get(new FrameBoundKey(i, START));
                    PagesIndexComparator endComparator = frameBoundComparators.get(new FrameBoundKey(i, END));
                    if (frame.getEndType() == UNBOUNDED_FOLLOWING) {
                        framing = new RangeFraming(
                                frame,
                                partitionStart,
                                partitionEnd,
                                startComparator,
                                endComparator,
                                pagesIndex,
                                peerGroupHashStrategy,
                                new Framing.Range(0, partitionEnd - partitionStart - 1));
                    }
                    else {
                        framing = new RangeFraming(
                                frame,
                                partitionStart,
                                partitionEnd,
                                startComparator,
                                endComparator,
                                pagesIndex,
                                peerGroupHashStrategy,
                                new Framing.Range(0, peerGroupEnd - partitionStart - 1));
                    }
                    break;
                case ROWS:
                    framing = new RowsFraming(frame, partitionStart, partitionEnd, pagesIndex);
                    break;
                case GROUPS:
                    framing = new GroupsFraming(
                            frame,
                            partitionStart,
                            partitionEnd,
                            pagesIndex,
                            peerGroupHashStrategy,
                            peerGroupEnd - partitionStart - 1);
                    break;
                default:
                    throw new UnsupportedOperationException("not yet implemented");
            }

            framings.put(i, framing);
        }
    }

    @Override
    public int getPartitionStart()
    {
        return partitionStart;
    }

    @Override
    public int getPartitionEnd()
    {
        return partitionEnd;
    }

    @Override
    public boolean hasNext()
    {
        return currentPosition < partitionEnd;
    }

    @Override
    public void processNextRow(PageBuilder pageBuilder)
    {
        checkState(hasNext(), "No more rows in partition");

        // copy output channels
        pageBuilder.declarePosition();
        int channel = 0;
        while (channel < outputChannels.length) {
            pagesIndex.appendTo(outputChannels[channel], currentPosition, pageBuilder.getBlockBuilder(channel));
            channel++;
        }

        // check for new peer group
        if (currentPosition == peerGroupEnd) {
            updatePeerGroup();
        }

        for (int i = 0; i < windowFunctions.size(); i++) {
            WindowFunction windowFunction = windowFunctions.get(i);
            Framing.Range range = framings.get(i).getRange(currentPosition, currentGroupIndex, peerGroupStart, peerGroupEnd);
            windowFunction.processRow(
                    pageBuilder.getBlockBuilder(channel),
                    peerGroupStart - partitionStart,
                    peerGroupEnd - partitionStart - 1,
                    range.getStart(),
                    range.getEnd());
            channel++;
        }

        currentPosition++;
    }

    private void updatePeerGroup()
    {
        currentGroupIndex++;
        peerGroupStart = currentPosition;
        // find end of peer group
        peerGroupEnd = peerGroupStart + 1;
        while ((peerGroupEnd < partitionEnd) && pagesIndex.positionNotDistinctFromPosition(peerGroupHashStrategy, peerGroupStart, peerGroupEnd)) {
            peerGroupEnd++;
        }
    }
}
