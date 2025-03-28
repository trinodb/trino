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
package io.trino.sql.planner.optimizations.ctereuse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.AggregateCall;
import io.trino.sql.dialect.trino.operation.Aggregation;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.Exchange;
import io.trino.sql.dialect.trino.operation.Join;
import io.trino.sql.dialect.trino.operation.Limit;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.clearspring.analytics.util.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.dialect.trino.Attributes.AGGREGATION_STEP;
import static io.trino.sql.dialect.trino.Attributes.BUCKET_TO_PARTITION;
import static io.trino.sql.dialect.trino.Attributes.DISTINCT;
import static io.trino.sql.dialect.trino.Attributes.DISTRIBUTION_TYPE;
import static io.trino.sql.dialect.trino.Attributes.DYNAMIC_FILTER_IDS;
import static io.trino.sql.dialect.trino.Attributes.EXCHANGE_SCOPE;
import static io.trino.sql.dialect.trino.Attributes.EXCHANGE_TYPE;
import static io.trino.sql.dialect.trino.Attributes.GLOBAL_GROUPING_SETS;
import static io.trino.sql.dialect.trino.Attributes.GROUPING_SETS_COUNT;
import static io.trino.sql.dialect.trino.Attributes.GROUP_ID_INDEX;
import static io.trino.sql.dialect.trino.Attributes.INPUT_REDUCING;
import static io.trino.sql.dialect.trino.Attributes.JOIN_TYPE;
import static io.trino.sql.dialect.trino.Attributes.LIMIT;
import static io.trino.sql.dialect.trino.Attributes.MAY_SKIP_OUTPUT_DUPLICATES;
import static io.trino.sql.dialect.trino.Attributes.NULLABLE_VALUES;
import static io.trino.sql.dialect.trino.Attributes.PARTIAL;
import static io.trino.sql.dialect.trino.Attributes.PARTITIONING_HANDLE;
import static io.trino.sql.dialect.trino.Attributes.PARTITION_COUNT;
import static io.trino.sql.dialect.trino.Attributes.PRE_GROUPED_INDEXES;
import static io.trino.sql.dialect.trino.Attributes.PRE_SORTED_INDEXES;
import static io.trino.sql.dialect.trino.Attributes.REPLICATE_NULLS_AND_ANY;
import static io.trino.sql.dialect.trino.Attributes.RESOLVED_FUNCTION;
import static io.trino.sql.dialect.trino.Attributes.SORT_ORDERS;
import static io.trino.sql.dialect.trino.Attributes.SPILLABLE;
import static io.trino.sql.dialect.trino.Attributes.STATISTICS_AND_COST_SUMMARY;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.rebaseBlock;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.validateMappedTypes;

/**
 * This is a temporary container for code that we will need later for CTE reuse when we implement rebasing / merging operations.
 * These methods will not become part of an interface as they are specific to CTE reuse.
 */
public class RebaseUtilsWIP
{
    private RebaseUtilsWIP()
    {}

    // rebase AggregateCall operation on new base value
    public static Optional<Operation> rebase(AggregateCall aggregateCall, Value baseValue, FieldMapping fieldMapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        Type baseRowType = relationRowType(trinoType(baseValue.type()));

        validateMappedTypes(relationRowType(trinoType(aggregateCall.arguments().get(0).type())), baseRowType, fieldMapping);

        ImmutableList.Builder<Block> rebasedBlocksBuilder = ImmutableList.builder();
        for (Region region : aggregateCall.regions()) {
            Optional<Block> rebasedBlock = rebaseBlock(region.getOnlyBlock(), baseRowType, fieldMapping, nameAllocator);
            if (rebasedBlock.isEmpty()) {
                return Optional.empty();
            }
            rebasedBlocksBuilder.add(rebasedBlock.get());
        }
        List<Block> rebasedBlocks = rebasedBlocksBuilder.build();

        return Optional.of(new AggregateCall(
                nameAllocator.newName(),
                baseValue,
                trinoType(aggregateCall.result().type()),
                rebasedBlocks.get(0),
                rebasedBlocks.get(1),
                rebasedBlocks.get(2),
                rebasedBlocks.get(3),
                Optional.ofNullable(SORT_ORDERS.getAttribute(aggregateCall.attributes())),
                RESOLVED_FUNCTION.getAttribute(aggregateCall.attributes()),
                DISTINCT.getAttribute(aggregateCall.attributes()),
                AGGREGATION_STEP.getAttribute(aggregateCall.attributes())));
    }

    // rebase Aggregation operation on new base operation
    public static Optional<Operation> rebase(Aggregation aggregation, Operation baseOperation, int sourceIndex, FieldMapping fieldMapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        org.assertj.core.util.Preconditions.checkArgument(sourceIndex == 0, "aggregation has one source");

        Type baseType = trinoType(baseOperation.result().type());
        Type baseRowType = relationRowType(baseType);

        validateMappedTypes(relationRowType(trinoType(aggregation.arguments().get(0).type())), baseRowType, fieldMapping);

        // the ^aggregates block cannot be rebased with the rebaseBlock() method because it is based on the relation type, not the row type.
        Block oldAggregatesBlock = aggregation.regions().get(0).getOnlyBlock();

        // validate the ^aggregates block structure
        checkState(oldAggregatesBlock.operations().getLast() instanceof Return);
        Operation rowConstructor = oldAggregatesBlock.operations().get(oldAggregatesBlock.operations().size() - 2);
        checkState(rowConstructor instanceof Row || rowConstructor instanceof Constant);
        checkState(rowConstructor instanceof Row || oldAggregatesBlock.operations().size() == 2);
        checkState(oldAggregatesBlock.operations().subList(0, oldAggregatesBlock.operations().size() - 2).stream()
                .allMatch(AggregateCall.class::isInstance));

        Block.Parameter newAggregatesParameter = new Block.Parameter(nameAllocator.newName(), irType(baseType));
        Block.Builder newAggregatesBlockBuilder = new Block.Builder(oldAggregatesBlock.name(), ImmutableList.of(newAggregatesParameter));
        ImmutableMap.Builder<Value, Operation> rebasedAggregateCallsBuilder = ImmutableMap.builder();
        for (Operation operation : oldAggregatesBlock.operations()) {
            if (operation instanceof AggregateCall aggregateCall) {
                Optional<Operation> rebasedAggregateCall = rebase(aggregateCall, newAggregatesParameter, fieldMapping, nameAllocator);
                if (rebasedAggregateCall.isEmpty()) {
                    return Optional.empty();
                }
                rebasedAggregateCallsBuilder.put(aggregateCall.result(), rebasedAggregateCall.get());
                newAggregatesBlockBuilder.addOperation(rebasedAggregateCall.get());
            }
            else if (operation instanceof Row row) {
                Map<Value, Operation> rebasedAggregateCalls = rebasedAggregateCallsBuilder.buildOrThrow();
                Row newRow = new Row(
                        row.result().name(),
                        row.arguments().stream()
                                .map(rebasedAggregateCalls::get)
                                .map(Operation::result)
                                .collect(toImmutableList()),
                        row.arguments().stream()
                                .map(rebasedAggregateCalls::get)
                                .map(Operation::attributes)
                                .collect(toImmutableList()));
                newAggregatesBlockBuilder.addOperation(newRow);
            }
            else {
                newAggregatesBlockBuilder.addOperation(operation);
            }
        }

        ImmutableList.Builder<Block> rebasedBlocksBuilder = ImmutableList.builder();
        rebasedBlocksBuilder.add(newAggregatesBlockBuilder.build());

        // the remaining blocks can be rebased with the rebaseBlock() method.
        for (Region region : aggregation.regions().subList(1, aggregation.regions().size())) {
            Optional<Block> rebasedBlock = rebaseBlock(region.getOnlyBlock(), baseRowType, fieldMapping, nameAllocator);
            if (rebasedBlock.isEmpty()) {
                return Optional.empty();
            }
            rebasedBlocksBuilder.add(rebasedBlock.get());
        }
        List<Block> rebasedBlocks = rebasedBlocksBuilder.build();

        return Optional.of(new Aggregation(
                nameAllocator.newName(),
                baseOperation.result(),
                rebasedBlocks.get(0),
                rebasedBlocks.get(1),
                rebasedBlocks.get(2),
                GROUPING_SETS_COUNT.getAttribute(aggregation.attributes()),
                GLOBAL_GROUPING_SETS.getAttribute(aggregation.attributes()),
                Optional.ofNullable(GROUP_ID_INDEX.getAttribute(aggregation.attributes())).map(OptionalInt::of).orElse(OptionalInt.empty()),
                PRE_GROUPED_INDEXES.getAttribute(aggregation.attributes()),
                AGGREGATION_STEP.getAttribute(aggregation.attributes()),
                INPUT_REDUCING.getAttribute(aggregation.attributes()),
                baseOperation.attributes()));
    }

    // rebase Exchange operation on new base operation
    // TODO first allocate the result name to keep the order!
    // TODO support rebase on multiple sources at a time so that we can extend the source types and pass-through more fields from all of them
    //  for now, only supporting one source which is pass-through
    public static Optional<Operation> rebase(Exchange exchange, Operation baseOperation, int sourceIndex, FieldMapping fieldMapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        if (exchange.arguments().size() != 1) {
            return Optional.empty();
        }
        org.assertj.core.util.Preconditions.checkArgument(sourceIndex == 0, "expected first and only source");

        if (!AssignmentsUtils.isFullPassthroughFieldSelector(exchange.regions().get(0).getOnlyBlock())) {
            return Optional.empty();
        }
        Type baseRowType = relationRowType(trinoType(baseOperation.result().type()));

        validateMappedTypes(relationRowType(trinoType(exchange.arguments().get(sourceIndex).type())), baseRowType, fieldMapping);

        // the new output type is same as new input type (passthrough) -- rebasing all blocks on the same type
        ImmutableList.Builder<Block> rebasedBlocksBuilder = ImmutableList.builder();
        for (Region region : exchange.regions()) {
            Optional<Block> rebasedBlock = rebaseBlock(region.getOnlyBlock(), baseRowType, fieldMapping, nameAllocator);
            if (rebasedBlock.isEmpty()) {
                return Optional.empty();
            }
            rebasedBlocksBuilder.add(rebasedBlock.get());
        }
        List<Block> rebasedBlocks = rebasedBlocksBuilder.build();

        return Optional.of(new Exchange(
                nameAllocator.newName(),
                ImmutableList.of(baseOperation.result()),
                ImmutableList.of(rebasedBlocks.get(0)),
                rebasedBlocks.get(1),
                rebasedBlocks.get(2),
                rebasedBlocks.get(3),
                EXCHANGE_TYPE.getAttribute(exchange.attributes()),
                EXCHANGE_SCOPE.getAttribute(exchange.attributes()),
                PARTITIONING_HANDLE.getAttribute(exchange.attributes()),
                NULLABLE_VALUES.getAttribute(exchange.attributes()),
                REPLICATE_NULLS_AND_ANY.getAttribute(exchange.attributes()),
                Optional.ofNullable(BUCKET_TO_PARTITION.getAttribute(exchange.attributes())),
                Optional.ofNullable(PARTITION_COUNT.getAttribute(exchange.attributes())),
                Optional.ofNullable(SORT_ORDERS.getAttribute(exchange.attributes())),
                ImmutableList.of(baseOperation.attributes())));
    }

    // rebase Join operation on new base operation
    // TODO do not rebase blocks if old and new types are that same and mapping is identity
    public static Optional<Operation> rebase(Join join, Operation baseOperation, int sourceIndex, FieldMapping fieldMapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        // TODO the output selectors should be extended to pass additional fields, not just remapped!
        org.assertj.core.util.Preconditions.checkArgument(sourceIndex == 0 || sourceIndex == 1, "join has two sources");
        Type baseRowType = relationRowType(trinoType(baseOperation.result().type()));

        validateMappedTypes(relationRowType(trinoType(join.arguments().get(sourceIndex).type())), baseRowType, fieldMapping);

        Region leftCriteriaSelector = join.regions().get(0);
        Region rightCriteriaSelector = join.regions().get(1);
        Region filter = join.regions().get(2);
        Region leftHashSelector = join.regions().get(3);
        Region rightHashSelector = join.regions().get(4);
        Region leftOutputSelector = join.regions().get(5);
        Region rightOutputSelector = join.regions().get(6);
        Region dynamicFilterTargetSelector = join.regions().get(7);

        // rebase all single-parameter blocks
        List<Region> regionsToRebase;
        if (sourceIndex == 0) {
            regionsToRebase = ImmutableList.of(leftCriteriaSelector, leftHashSelector, leftOutputSelector);
        }
        else {
            regionsToRebase = ImmutableList.of(rightCriteriaSelector, rightHashSelector, rightOutputSelector, dynamicFilterTargetSelector);
        }
        ImmutableList.Builder<Block> rebasedBlocksBuilder = ImmutableList.builder();
        for (Region region : regionsToRebase) {
            Optional<Block> rebasedBlock = rebaseBlock(region.getOnlyBlock(), baseRowType, fieldMapping, nameAllocator);
            if (rebasedBlock.isEmpty()) {
                return Optional.empty();
            }
            rebasedBlocksBuilder.add(rebasedBlock.get());
        }
        List<Block> rebasedSingleParameterBlocks = rebasedBlocksBuilder.build();

        // rebase filter
        Optional<Block> rebasedFilter = rebaseBlock(filter.getOnlyBlock(), sourceIndex, baseRowType, fieldMapping, nameAllocator);
        if (rebasedFilter.isEmpty()) {
            return Optional.empty();
        }

        List<Block> rebasedBlocks;
        if (sourceIndex == 0) {
            rebasedBlocks = ImmutableList.of(
                    rebasedSingleParameterBlocks.get(0),
                    rightCriteriaSelector.getOnlyBlock(),
                    rebasedFilter.get(),
                    rebasedSingleParameterBlocks.get(1),
                    rightHashSelector.getOnlyBlock(),
                    rebasedSingleParameterBlocks.get(2),
                    rightOutputSelector.getOnlyBlock(),
                    dynamicFilterTargetSelector.getOnlyBlock());
        }
        else {
            rebasedBlocks = ImmutableList.of(
                    leftCriteriaSelector.getOnlyBlock(),
                    rebasedSingleParameterBlocks.get(0),
                    rebasedFilter.get(),
                    leftHashSelector.getOnlyBlock(),
                    rebasedSingleParameterBlocks.get(1),
                    leftOutputSelector.getOnlyBlock(),
                    rebasedSingleParameterBlocks.get(2),
                    rebasedSingleParameterBlocks.get(3));
        }

        return Optional.of(new Join(
                nameAllocator.newName(),
                sourceIndex == 0 ? baseOperation.result() : join.arguments().get(0),
                sourceIndex == 1 ? baseOperation.result() : join.arguments().get(1),
                rebasedBlocks.get(0),
                rebasedBlocks.get(1),
                rebasedBlocks.get(2),
                rebasedBlocks.get(3),
                rebasedBlocks.get(4),
                rebasedBlocks.get(5),
                rebasedBlocks.get(6),
                rebasedBlocks.get(7),
                JOIN_TYPE.getAttribute(join.attributes()),
                MAY_SKIP_OUTPUT_DUPLICATES.getAttribute(join.attributes()),
                Optional.ofNullable(DISTRIBUTION_TYPE.getAttribute(join.attributes())),
                Optional.ofNullable(SPILLABLE.getAttribute(join.attributes())),
                DYNAMIC_FILTER_IDS.getAttribute(join.attributes()),
                Optional.ofNullable(STATISTICS_AND_COST_SUMMARY.getAttribute(join.attributes())),
                sourceIndex == 0 ? baseOperation.attributes() : ImmutableMap.of(), // TODO with partial rebase we miss the source attributes. Shall we do a copying constructor? That would assume that source attrs are the same for the new sourc which is not correct. Probably pass the other source.
                sourceIndex == 1 ? baseOperation.attributes() : ImmutableMap.of()));
    }

    // rebase Join operation on new base operation
    public static Optional<Operation> rebase(Limit limit, Operation baseOperation, int sourceIndex, FieldMapping fieldMapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        org.assertj.core.util.Preconditions.checkArgument(sourceIndex == 0, "limit has one source");

        Type baseRowType = relationRowType(trinoType(baseOperation.result().type()));
        validateMappedTypes(relationRowType(trinoType(limit.arguments().get(0).type())), baseRowType, fieldMapping);

        Optional<Block> rebasedOrderingSelector = rebaseBlock(limit.regions().get(0).getOnlyBlock(), baseRowType, fieldMapping, nameAllocator);
        if (rebasedOrderingSelector.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new Limit(
                nameAllocator.newName(),
                baseOperation.result(),
                rebasedOrderingSelector.get(),
                Optional.ofNullable(SORT_ORDERS.getAttribute(limit.attributes())),
                LIMIT.getAttribute(limit.attributes()),
                PARTIAL.getAttribute(limit.attributes()),
                PRE_SORTED_INDEXES.getAttribute(limit.attributes()),
                baseOperation.attributes()));
    }
}
