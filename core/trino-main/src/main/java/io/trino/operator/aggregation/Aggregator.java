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

public class Aggregator
        extends AbstractAggregator
{
    private final Accumulator accumulator;

    public Aggregator(
            Accumulator accumulator,
            Step step,
            Type intermediateType,
            Type finalType,
            List<Integer> aggregationRawInputChannels,
            OptionalInt intermediateStateChannel,
            OptionalInt rawInputMaskChannel,
            OptionalInt maskChannel)
    {
        super(step, intermediateType, finalType, aggregationRawInputChannels, intermediateStateChannel, rawInputMaskChannel, maskChannel);
        this.accumulator = requireNonNull(accumulator, "accumulator is null");
    }

    public void processPage(Page page)
    {
        processPage(page, new AccumulatorWrapper()
        {
            @Override
            public void addInput(Page page, Optional<Block> maskBlock)
            {
                accumulator.addInput(page, maskBlock);
            }

            @Override
            public void addIntermediate(Block block)
            {
                accumulator.addIntermediate(block);
            }

            @Override
            public void addIntermediate(IntArrayList positions, Block intermediateStateBlock)
            {
                if (positions.size() != intermediateStateBlock.getPositionCount()) {
                    // some rows were eliminated by the filter
                    intermediateStateBlock = intermediateStateBlock.getPositions(positions.elements(), 0, positions.size());
                }

                accumulator.addIntermediate(intermediateStateBlock);
            }
        });
    }

    public void evaluate(BlockBuilder blockBuilder)
    {
        if (isOutputPartial()) {
            accumulator.evaluateIntermediate(blockBuilder);
        }
        else {
            accumulator.evaluateFinal(blockBuilder);
        }
    }

    public long getEstimatedSize()
    {
        return accumulator.getEstimatedSize();
    }
}
