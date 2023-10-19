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

import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.JoinProbe.JoinProbeFactory;
import io.trino.operator.join.LookupJoinOperatorFactory;
import io.trino.operator.join.LookupSourceFactory;
import io.trino.operator.join.unspilled.JoinProbe;
import io.trino.operator.join.unspilled.PartitionedLookupSourceFactory;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.PartitioningSpillerFactory;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class OperatorFactories
{
    private OperatorFactories() {}

    public static OperatorFactory join(
            JoinOperatorType joinType,
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<? extends PartitionedLookupSourceFactory> lookupSourceFactory,
            boolean hasFilter,
            List<Type> probeTypes,
            List<Integer> probeJoinChannel,
            OptionalInt probeHashChannel,
            Optional<List<Integer>> probeOutputChannelsOptional,
            TypeOperators typeOperators)
    {
        List<Integer> probeOutputChannels = probeOutputChannelsOptional.orElseGet(() -> rangeList(probeTypes.size()));
        List<Type> probeOutputChannelTypes = probeOutputChannels.stream()
                .map(probeTypes::get)
                .collect(toImmutableList());

        return new io.trino.operator.join.unspilled.LookupJoinOperatorFactory(
                operatorId,
                planNodeId,
                lookupSourceFactory,
                probeTypes,
                probeOutputChannelTypes,
                lookupSourceFactory.getBuildOutputTypes(),
                joinType,
                new JoinProbe.JoinProbeFactory(probeOutputChannels, probeJoinChannel, probeHashChannel, hasFilter),
                typeOperators,
                probeJoinChannel,
                probeHashChannel);
    }

    public static OperatorFactory spillingJoin(
            JoinOperatorType joinType,
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactory,
            boolean hasFilter,
            List<Type> probeTypes,
            List<Integer> probeJoinChannel,
            OptionalInt probeHashChannel,
            Optional<List<Integer>> probeOutputChannelsOptional,
            OptionalInt totalOperatorsCount,
            PartitioningSpillerFactory partitioningSpillerFactory,
            TypeOperators typeOperators)
    {
        List<Integer> probeOutputChannels = probeOutputChannelsOptional.orElseGet(() -> rangeList(probeTypes.size()));
        List<Type> probeOutputChannelTypes = probeOutputChannels.stream()
                .map(probeTypes::get)
                .collect(toImmutableList());

        return new LookupJoinOperatorFactory(
                operatorId,
                planNodeId,
                lookupSourceFactory,
                probeTypes,
                probeOutputChannelTypes,
                lookupSourceFactory.getBuildOutputTypes(),
                joinType,
                new JoinProbeFactory(probeOutputChannels.stream().mapToInt(i -> i).toArray(), probeJoinChannel, probeHashChannel),
                typeOperators,
                totalOperatorsCount,
                probeJoinChannel,
                probeHashChannel,
                partitioningSpillerFactory);
    }

    private static List<Integer> rangeList(int endExclusive)
    {
        return IntStream.range(0, endExclusive)
                .boxed()
                .collect(toImmutableList());
    }
}
