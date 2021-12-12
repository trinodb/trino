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

import io.airlift.units.DataSize;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.JoinProbe.JoinProbeFactory;
import io.trino.operator.join.LookupJoinOperatorFactory;
import io.trino.operator.join.LookupJoinOperatorFactory.JoinType;
import io.trino.operator.join.LookupSourceFactory;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.Type;
import io.trino.spiller.PartitioningSpillerFactory;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.join.LookupJoinOperatorFactory.JoinType.FULL_OUTER;
import static io.trino.operator.join.LookupJoinOperatorFactory.JoinType.INNER;
import static io.trino.operator.join.LookupJoinOperatorFactory.JoinType.LOOKUP_OUTER;
import static io.trino.operator.join.LookupJoinOperatorFactory.JoinType.PROBE_OUTER;
import static io.trino.operator.output.PartitionedOutputOperator.PartitionedOutputFactory;

public class TrinoOperatorFactories
        implements OperatorFactories
{
    @Override
    public OperatorFactory innerJoin(
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactory,
            boolean outputSingleMatch,
            boolean waitForBuild,
            boolean hasFilter,
            List<Type> probeTypes,
            List<Integer> probeJoinChannel,
            OptionalInt probeHashChannel,
            Optional<List<Integer>> probeOutputChannels,
            OptionalInt totalOperatorsCount,
            PartitioningSpillerFactory partitioningSpillerFactory,
            BlockTypeOperators blockTypeOperators)
    {
        return createJoinOperatorFactory(
                operatorId,
                planNodeId,
                lookupSourceFactory,
                probeTypes,
                probeJoinChannel,
                probeHashChannel,
                probeOutputChannels.orElse(rangeList(probeTypes.size())),
                INNER,
                outputSingleMatch,
                waitForBuild,
                totalOperatorsCount,
                partitioningSpillerFactory,
                blockTypeOperators);
    }

    @Override
    public OperatorFactory probeOuterJoin(
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactory,
            boolean outputSingleMatch,
            boolean hasFilter,
            List<Type> probeTypes,
            List<Integer> probeJoinChannel,
            OptionalInt probeHashChannel,
            Optional<List<Integer>> probeOutputChannels,
            OptionalInt totalOperatorsCount,
            PartitioningSpillerFactory partitioningSpillerFactory,
            BlockTypeOperators blockTypeOperators)
    {
        return createJoinOperatorFactory(
                operatorId,
                planNodeId,
                lookupSourceFactory,
                probeTypes,
                probeJoinChannel,
                probeHashChannel,
                probeOutputChannels.orElse(rangeList(probeTypes.size())),
                PROBE_OUTER,
                outputSingleMatch,
                false,
                totalOperatorsCount,
                partitioningSpillerFactory,
                blockTypeOperators);
    }

    @Override
    public OperatorFactory lookupOuterJoin(
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactory,
            boolean waitForBuild,
            boolean hasFilter,
            List<Type> probeTypes,
            List<Integer> probeJoinChannel,
            OptionalInt probeHashChannel,
            Optional<List<Integer>> probeOutputChannels,
            OptionalInt totalOperatorsCount,
            PartitioningSpillerFactory partitioningSpillerFactory,
            BlockTypeOperators blockTypeOperators)
    {
        return createJoinOperatorFactory(
                operatorId,
                planNodeId,
                lookupSourceFactory,
                probeTypes,
                probeJoinChannel,
                probeHashChannel,
                probeOutputChannels.orElse(rangeList(probeTypes.size())),
                LOOKUP_OUTER,
                false,
                waitForBuild,
                totalOperatorsCount,
                partitioningSpillerFactory,
                blockTypeOperators);
    }

    @Override
    public OperatorFactory fullOuterJoin(
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactory,
            boolean hasFilter,
            List<Type> probeTypes,
            List<Integer> probeJoinChannel,
            OptionalInt probeHashChannel,
            Optional<List<Integer>> probeOutputChannels,
            OptionalInt totalOperatorsCount,
            PartitioningSpillerFactory partitioningSpillerFactory,
            BlockTypeOperators blockTypeOperators)
    {
        return createJoinOperatorFactory(
                operatorId,
                planNodeId,
                lookupSourceFactory,
                probeTypes,
                probeJoinChannel,
                probeHashChannel,
                probeOutputChannels.orElse(rangeList(probeTypes.size())),
                FULL_OUTER,
                false,
                false,
                totalOperatorsCount,
                partitioningSpillerFactory,
                blockTypeOperators);
    }

    @Override
    public OutputFactory partitionedOutput(
            TaskContext taskContext,
            PartitionFunction partitionFunction,
            List<Integer> partitionChannels,
            List<Optional<NullableValue>> partitionConstants,
            boolean replicateNullsAndAny,
            OptionalInt nullChannel,
            OutputBuffer outputBuffer,
            DataSize maxPagePartitioningBufferSize)
    {
        return new PartitionedOutputFactory(
                partitionFunction,
                partitionChannels,
                partitionConstants,
                replicateNullsAndAny,
                nullChannel,
                outputBuffer,
                maxPagePartitioningBufferSize);
    }

    private static List<Integer> rangeList(int endExclusive)
    {
        return IntStream.range(0, endExclusive)
                .boxed()
                .collect(toImmutableList());
    }

    private OperatorFactory createJoinOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager,
            List<Type> probeTypes,
            List<Integer> probeJoinChannel,
            OptionalInt probeHashChannel,
            List<Integer> probeOutputChannels,
            JoinType joinType,
            boolean outputSingleMatch,
            boolean waitForBuild,
            OptionalInt totalOperatorsCount,
            PartitioningSpillerFactory partitioningSpillerFactory,
            BlockTypeOperators blockTypeOperators)
    {
        List<Type> probeOutputChannelTypes = probeOutputChannels.stream()
                .map(probeTypes::get)
                .collect(toImmutableList());

        return new LookupJoinOperatorFactory(
                operatorId,
                planNodeId,
                lookupSourceFactoryManager,
                probeTypes,
                probeOutputChannelTypes,
                lookupSourceFactoryManager.getBuildOutputTypes(),
                joinType,
                outputSingleMatch,
                waitForBuild,
                new JoinProbeFactory(probeOutputChannels.stream().mapToInt(i -> i).toArray(), probeJoinChannel, probeHashChannel),
                blockTypeOperators,
                totalOperatorsCount,
                probeJoinChannel,
                probeHashChannel,
                partitioningSpillerFactory);
    }
}
