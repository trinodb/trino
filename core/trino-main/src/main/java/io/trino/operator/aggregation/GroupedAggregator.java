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
package io.trino.operator.aggregation;

import io.trino.operator.GroupByIdBlock;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.AggregationNode.Step;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

public class GroupedAggregator
        extends AbstractAggregator
{
    private final GroupedAccumulator accumulator;

    public GroupedAggregator(
            GroupedAccumulator accumulator,
            Step step,
            Type intermediateType,
            Type finalType,
            List<Integer> rawInputChannels,
            OptionalInt intermediateStateChannel,
            OptionalInt rawInputMaskChannel,
            OptionalInt maskChannel)
    {
        super(step, intermediateType, finalType, rawInputChannels, intermediateStateChannel, rawInputMaskChannel, maskChannel);
        this.accumulator = requireNonNull(accumulator, "accumulator is null");
    }

    public long getEstimatedSize()
    {
        return accumulator.getEstimatedSize();
    }

    public void processPage(GroupByIdBlock groupIds, Page page)
    {
        processPage(page, new AccumulatorWrapper()
        {
            @Override
            public void addInput(Page page, Optional<Block> maskBlock)
            {
                accumulator.addInput(groupIds, page, maskBlock);
            }

            @Override
            public void addIntermediate(Block block)
            {
                accumulator.addIntermediate(groupIds, block);
            }

            @Override
            public void addIntermediate(IntArrayList positions, Block intermediateStateBlock)
            {
                GroupByIdBlock intermediateGroupIds = groupIds;
                if (positions.size() != intermediateStateBlock.getPositionCount()) {
                    // some rows were eliminated by the filter
                    intermediateStateBlock = intermediateStateBlock.getPositions(positions.elements(), 0, positions.size());
                    intermediateGroupIds = new GroupByIdBlock(
                            groupIds.getGroupCount(),
                            groupIds.getPositions(positions.elements(), 0, positions.size()));
                }

                accumulator.addIntermediate(intermediateGroupIds, intermediateStateBlock);
            }
        });
    }

    public void prepareFinal()
    {
        accumulator.prepareFinal();
    }

    public void evaluate(int groupId, BlockBuilder output)
    {
        if (isOutputPartial()) {
            accumulator.evaluateIntermediate(groupId, output);
        }
        else {
            accumulator.evaluateFinal(groupId, output);
        }
    }
}
