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
package io.trino.operator.window.pattern;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.operator.PagesIndex;
import io.trino.operator.window.InternalWindowIndex;
import io.trino.operator.window.PagesWindowIndex;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.function.WindowIndex;
import io.trino.spi.type.Type;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.window.pattern.PhysicalValuePointer.CLASSIFIER;
import static io.trino.operator.window.pattern.PhysicalValuePointer.MATCH_NUMBER;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * This class represents a WindowIndex with additional "channels"
 * which are not part of the underlying PagesIndex.
 * <p>
 * The purpose of this class is to provide input for aggregations
 * in a consistent way, regardless of whether the aggregation
 * uses an input channel, or a runtime-evaluated expression as
 * the argument.
 * <p>
 * The latter case applies to aggregate functions within the
 * DEFINE and MEASURES clauses of row pattern recognition.
 * The aggregated argument can be based on CLASSIFIER() or
 * MATCH_NUMBER() results which are not present in the input,
 * and thus needs to be evaluated at runtime while processing
 * a row.
 * <p>
 * E.g. for the aggregate function array_agg(lower(CLASSIFIER(X))),
 * there is no input channel containing the aggregated argument,
 * so it is evaluated row by row at runtime and passed in an additional
 * "channel" available to the aggregation's accumulator by a standard
 * WindowIndex interface.
 */
public class ProjectingPagesWindowIndex
        implements InternalWindowIndex
{
    private final PagesIndex pagesIndex;
    private final int start;
    private final int size;
    private final List<Type> projectedTypes;
    private final List<ArgumentComputation> projections;
    private final int firstProjectedChannel;
    private final List<String> labelNames;
    private int currentPosition;
    private Block label;
    private Block matchNumber;

    // results of projections for the current position, recorded to avoid duplicate computations
    private final Block[] results;

    // View of the underlying PagesIndex used by projections. It does not contain projected channels.
    private final WindowIndex windowIndex;

    public ProjectingPagesWindowIndex(PagesIndex pagesIndex, int start, int end, List<ArgumentComputation> projections, List<String> labelNames)
    {
        requireNonNull(pagesIndex, "pagesIndex is null");
        checkPositionIndex(start, pagesIndex.getPositionCount(), "start");
        checkPositionIndex(end, pagesIndex.getPositionCount(), "end");
        checkArgument(start < end, "start must be before end");
        requireNonNull(projections, "projections is null");
        requireNonNull(labelNames, "labelNames is null");

        this.pagesIndex = pagesIndex;
        this.start = start;
        this.size = end - start;
        this.projections = ImmutableList.copyOf(projections);
        this.projectedTypes = projections.stream()
                .map(ArgumentComputation::getOutputType)
                .collect(toImmutableList());
        this.currentPosition = -1;
        this.firstProjectedChannel = pagesIndex.getTypes().size();
        this.labelNames = labelNames;
        this.results = new Block[projectedTypes.size()];
        this.windowIndex = new PagesWindowIndex(pagesIndex, start, end);
    }

    public void setLabelAndMatchNumber(int currentPosition, int label, long matchNumber)
    {
        this.currentPosition = currentPosition;
        this.label = nativeValueToBlock(VARCHAR, utf8Slice(labelNames.get(label)));
        this.matchNumber = nativeValueToBlock(BIGINT, matchNumber);
        Arrays.fill(results, null);
    }

    private Block compute(int position, int projectedChannel)
    {
        checkArgument(position == currentPosition, "accessing position %s (current position: %s)", position, currentPosition);

        if (results[projectedChannel] != null) {
            return results[projectedChannel];
        }

        ArgumentComputation projection = projections.get(projectedChannel);
        List<Integer> layout = projection.getInputChannels();
        Block[] input = new Block[layout.size()];
        for (int i = 0; i < layout.size(); i++) {
            int channel = layout.get(i);
            if (channel == MATCH_NUMBER) {
                input[i] = matchNumber;
            }
            else if (channel == CLASSIFIER) {
                input[i] = label;
            }
            else {
                input[i] = windowIndex.getSingleValueBlock(channel, position);
            }
        }

        Block result = projection.compute(input);
        results[projectedChannel] = result;
        return result;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public boolean isNull(int channel, int position)
    {
        if (channel < firstProjectedChannel) {
            return pagesIndex.isNull(channel, position(position));
        }
        return compute(position, channel - firstProjectedChannel).isNull(0);
    }

    @Override
    public boolean getBoolean(int channel, int position)
    {
        if (channel < firstProjectedChannel) {
            return pagesIndex.getBoolean(channel, position(position));
        }
        int channelIndex = channel - firstProjectedChannel;
        return projectedTypes.get(channelIndex).getBoolean(compute(position, channelIndex), 0);
    }

    @Override
    public long getLong(int channel, int position)
    {
        if (channel < firstProjectedChannel) {
            return pagesIndex.getLong(channel, position(position));
        }
        int channelIndex = channel - firstProjectedChannel;
        return projectedTypes.get(channelIndex).getLong(compute(position, channelIndex), 0);
    }

    @Override
    public double getDouble(int channel, int position)
    {
        if (channel < firstProjectedChannel) {
            return pagesIndex.getDouble(channel, position(position));
        }
        int channelIndex = channel - firstProjectedChannel;
        return projectedTypes.get(channelIndex).getDouble(compute(position, channelIndex), 0);
    }

    @Override
    public Slice getSlice(int channel, int position)
    {
        if (channel < firstProjectedChannel) {
            return pagesIndex.getSlice(channel, position(position));
        }
        int channelIndex = channel - firstProjectedChannel;
        return projectedTypes.get(channelIndex).getSlice(compute(position, channelIndex), 0);
    }

    @Override
    public Block getSingleValueBlock(int channel, int position)
    {
        if (channel < firstProjectedChannel) {
            return pagesIndex.getSingleValueBlock(channel, position(position));
        }
        return compute(position, channel - firstProjectedChannel);
    }

    @Override
    public Object getObject(int channel, int position)
    {
        if (channel < firstProjectedChannel) {
            return pagesIndex.getObject(channel, position(position));
        }
        int channelIndex = channel - firstProjectedChannel;
        return projectedTypes.get(channelIndex).getObject(compute(position, channelIndex), 0);
    }

    @Override
    public void appendTo(int channel, int position, BlockBuilder output)
    {
        if (channel < firstProjectedChannel) {
            pagesIndex.appendTo(channel, position(position), output);
        }
        int channelIndex = channel - firstProjectedChannel;
        projectedTypes.get(channelIndex).appendTo(compute(position, channelIndex), 0, output);
    }

    @Override
    public Block getRawBlock(int channel, int position)
    {
        if (channel < firstProjectedChannel) {
            return pagesIndex.getRawBlock(channel, position(position));
        }

        int channelIndex = channel - firstProjectedChannel;
        Block compute = compute(position, channelIndex);

        // if there are no non-projected columns, no correction needed
        if (firstProjectedChannel == 0) {
            return compute;
        }

        // projection always creates a single row block, and will not align with the blocks from the pages index,
        // so we use an RLE block of the same length as the raw block
        int rawBlockPositionCount = pagesIndex.getRawBlock(0, position(position)).getPositionCount();
        return RunLengthEncodedBlock.create(compute, rawBlockPositionCount);
    }

    @Override
    public int getRawBlockPosition(int position)
    {
        // if there are no non-projected columns then all blocks will have one position
        if (firstProjectedChannel == 0) {
            return 0;
        }
        return pagesIndex.getRawBlockPosition(position(position));
    }

    /**
     * Compute position relative to the pagesIndex start
     *
     * @param position position relative to partition start
     */
    private int position(int position)
    {
        checkElementIndex(position, size, "position");
        return position + start;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("size", size)
                .add("input channels", firstProjectedChannel)
                .add("projected channels", projectedTypes.size())
                .toString();
    }
}
