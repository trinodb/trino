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
package io.trino.operator.index;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.trino.operator.FilterAndProjectOperator;
import io.trino.operator.OperatorFactory;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProjection;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.relational.Expressions;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DynamicTupleFilterFactory
{
    private final int filterOperatorId;
    private final PlanNodeId planNodeId;

    private final int[] tupleFilterChannels;
    private final List<Integer> outputFilterChannels;
    private final List<BlockPositionEqual> filterEqualOperators;

    private final List<Type> outputTypes;
    private final List<Supplier<PageProjection>> outputProjections;

    public DynamicTupleFilterFactory(
            int filterOperatorId,
            PlanNodeId planNodeId,
            int[] tupleFilterChannels,
            int[] outputFilterChannels,
            List<Type> outputTypes,
            PageFunctionCompiler pageFunctionCompiler,
            BlockTypeOperators blockTypeOperators)
    {
        requireNonNull(planNodeId, "planNodeId is null");
        requireNonNull(tupleFilterChannels, "tupleFilterChannels is null");
        checkArgument(tupleFilterChannels.length > 0, "Must have at least one tupleFilterChannel");
        requireNonNull(outputFilterChannels, "outputFilterChannels is null");
        checkArgument(outputFilterChannels.length == tupleFilterChannels.length, "outputFilterChannels must have same length as tupleFilterChannels");
        requireNonNull(outputTypes, "outputTypes is null");
        checkArgument(outputTypes.size() >= outputFilterChannels.length, "Must have at least as many output channels as those used for filtering");
        requireNonNull(pageFunctionCompiler, "pageFunctionCompiler is null");
        requireNonNull(blockTypeOperators, "blockTypeOperators is null");

        this.filterOperatorId = filterOperatorId;
        this.planNodeId = planNodeId;

        this.tupleFilterChannels = tupleFilterChannels.clone();
        this.outputFilterChannels = ImmutableList.copyOf(Ints.asList(outputFilterChannels));
        this.filterEqualOperators = IntStream.of(outputFilterChannels)
                .mapToObj(outputTypes::get)
                .map(blockTypeOperators::getEqualOperator)
                .collect(toImmutableList());

        this.outputTypes = ImmutableList.copyOf(outputTypes);
        this.outputProjections = IntStream.range(0, outputTypes.size())
                .mapToObj(field -> pageFunctionCompiler.compileProjection(Expressions.field(field, outputTypes.get(field)), Optional.empty()))
                .collect(toImmutableList());
    }

    public OperatorFactory filterWithTuple(Page tuplePage)
    {
        Supplier<PageProcessor> processor = createPageProcessor(tuplePage.getColumns(tupleFilterChannels), OptionalInt.empty());
        return FilterAndProjectOperator.createOperatorFactory(filterOperatorId, planNodeId, processor, outputTypes, DataSize.ofBytes(0), 0);
    }

    @VisibleForTesting
    public Supplier<PageProcessor> createPageProcessor(Page filterTuple, OptionalInt initialBatchSize)
    {
        TuplePageFilter filter = new TuplePageFilter(filterTuple, filterEqualOperators, outputFilterChannels);
        return () -> new PageProcessor(
                Optional.of(filter),
                outputProjections.stream()
                        .map(Supplier::get)
                        .collect(toImmutableList()), initialBatchSize);
    }
}
