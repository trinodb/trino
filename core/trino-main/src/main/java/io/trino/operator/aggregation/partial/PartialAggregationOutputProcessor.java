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
package io.trino.operator.aggregation.partial;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

/**
 * Creates final output of the partial aggregation step based on either aggregated or raw input.
 * <p>
 * Partial aggregation step output has additional channels used to send
 * raw input data when the partial aggregation is disabled.
 * Those channels need to be present in the output, albeit empty, even if the
 * partial aggregation is enabled.
 * <p>
 * The additional channels are:
 * - mask channels used by any aggregation
 * - every aggregation input channels that are not already in hash channels
 * - a boolean rawInputMask channel that contains decision which input, aggregated or raw, should be used for a given position.
 */
public class PartialAggregationOutputProcessor
{
    private final List<Type> aggregatorIntermediateOutputTypes;
    private final List<Type> aggregationRawInputTypes;
    private final int[] hashChannels;
    private final int[] aggregationRawInputChannels;
    private final int[] maskChannels;
    private final int finalBlockCount;

    public PartialAggregationOutputProcessor(
            List<Integer> groupByChannels,
            Optional<Integer> inputHashChannel,
            List<AggregatorFactory> aggregatorFactories,
            List<? extends Type> aggregationRawInputTypes,
            List<Integer> aggregationRawInputChannels)
    {
        this.aggregationRawInputTypes = ImmutableList.copyOf(aggregationRawInputTypes);

        this.aggregatorIntermediateOutputTypes = requireNonNull(aggregatorFactories, "aggregatorFactories is null")
                .stream()
                .map(AggregatorFactory::getIntermediateType)
                .collect(toImmutableList());
        this.aggregationRawInputChannels = Ints.toArray(aggregationRawInputChannels);
        this.maskChannels = Ints.toArray(aggregatorFactories
                .stream()
                .map(AggregatorFactory::getMaskChannel)
                .flatMapToInt(OptionalInt::stream)
                .boxed()
                .distinct()
                .collect(toImmutableList()));
        this.hashChannels = new int[groupByChannels.size() + (inputHashChannel.isPresent() ? 1 : 0)];
        for (int i = 0; i < groupByChannels.size(); i++) {
            hashChannels[i] = groupByChannels.get(i);
        }
        inputHashChannel.ifPresent(channelIndex -> hashChannels[groupByChannels.size()] = channelIndex);
        finalBlockCount = hashChannels.length + aggregatorIntermediateOutputTypes.size() + maskChannels.length + aggregationRawInputChannels.size() + 1;
    }

    public Page processAggregatedPage(Page page)
    {
        Block[] finalPage = new Block[finalBlockCount];
        int blockOffset = 0;
        for (int i = 0; i < page.getChannelCount(); i++, blockOffset++) {
            finalPage[blockOffset] = page.getBlock(i);
        }
        int positionCount = page.getPositionCount();

        // mask channels
        for (int i = 0; i < maskChannels.length; i++, blockOffset++) {
            finalPage[blockOffset] = RunLengthEncodedBlock.create(BOOLEAN, null, positionCount);
        }
        // aggregation raw inputs
        for (int i = 0; i < aggregationRawInputTypes.size(); i++, blockOffset++) {
            finalPage[blockOffset] = RunLengthEncodedBlock.create(aggregationRawInputTypes.get(i), null, positionCount);
        }

        // use raw input mask channel
        finalPage[blockOffset] = RunLengthEncodedBlock.create(BOOLEAN, null, positionCount);
        return new Page(positionCount, finalPage);
    }

    public Page processRawInputPage(Page page)
    {
        Block[] finalPage = new Block[finalBlockCount];
        int blockOffset = 0;
        // raw input hash channels
        for (int i = 0; i < hashChannels.length; i++, blockOffset++) {
            finalPage[blockOffset] = page.getBlock(hashChannels[i]);
        }
        // aggregator state channels
        for (int i = 0; i < aggregatorIntermediateOutputTypes.size(); i++, blockOffset++) {
            finalPage[blockOffset] = RunLengthEncodedBlock.create(aggregatorIntermediateOutputTypes.get(i), null, page.getPositionCount());
        }
        // mask channels
        for (int i = 0; i < maskChannels.length; i++, blockOffset++) {
            finalPage[blockOffset] = page.getBlock(maskChannels[i]);
        }
        // aggregation raw inputs
        for (int i = 0; i < aggregationRawInputChannels.length; i++, blockOffset++) {
            finalPage[blockOffset] = page.getBlock(aggregationRawInputChannels[i]);
        }
        // use raw input mask channel
        finalPage[blockOffset] = RunLengthEncodedBlock.create(BOOLEAN, true, page.getPositionCount());
        return new Page(page.getPositionCount(), finalPage);
    }
}
