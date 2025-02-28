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
package io.trino.sql.dialect.trino.operation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.Attributes.ExchangeScope;
import io.trino.sql.dialect.trino.Attributes.ExchangeType;
import io.trino.sql.dialect.trino.Attributes.NullableValues;
import io.trino.sql.dialect.trino.Attributes.SortOrderList;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.SystemPartitioningHandle;
import io.trino.sql.planner.optimizations.CteReuse;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Attributes.BUCKET_TO_PARTITION;
import static io.trino.sql.dialect.trino.Attributes.DISTRIBUTION_TYPE;
import static io.trino.sql.dialect.trino.Attributes.DYNAMIC_FILTER_IDS;
import static io.trino.sql.dialect.trino.Attributes.EXCHANGE_SCOPE;
import static io.trino.sql.dialect.trino.Attributes.EXCHANGE_TYPE;
import static io.trino.sql.dialect.trino.Attributes.ExchangeScope.LOCAL;
import static io.trino.sql.dialect.trino.Attributes.ExchangeScope.REMOTE;
import static io.trino.sql.dialect.trino.Attributes.ExchangeType.GATHER;
import static io.trino.sql.dialect.trino.Attributes.ExchangeType.REPARTITION;
import static io.trino.sql.dialect.trino.Attributes.JOIN_TYPE;
import static io.trino.sql.dialect.trino.Attributes.MAY_SKIP_OUTPUT_DUPLICATES;
import static io.trino.sql.dialect.trino.Attributes.NULLABLE_VALUES;
import static io.trino.sql.dialect.trino.Attributes.PARTITIONING_HANDLE;
import static io.trino.sql.dialect.trino.Attributes.PARTITION_COUNT;
import static io.trino.sql.dialect.trino.Attributes.REPLICATE_NULLS_AND_ANY;
import static io.trino.sql.dialect.trino.Attributes.SORT_ORDERS;
import static io.trino.sql.dialect.trino.Attributes.SPILLABLE;
import static io.trino.sql.dialect.trino.Attributes.STATISTICS_AND_COST_SUMMARY;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.assignRelationRowTypeFieldNames;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.operation.SqlOperationsUtil.rebaseBlock;
import static io.trino.sql.dialect.trino.operation.SqlOperationsUtil.validateMappedTypes;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.util.Preconditions.checkArgument;

public class Exchange
        extends Operation
        implements SqlRelationalOperation
{
    private static final String NAME = "exchange";

    private final Result result;
    private final List<Value> inputs;
    private final List<Region> inputFieldSelectors;
    private final Region partitioningBoundArguments;
    private final Region partitioningHashSelector;
    private final Region orderingSelector;
    private final Map<AttributeKey, Object> attributes;

    public Exchange(
            String resultName,
            List<Value> inputs,
            List<Block> inputFieldSelectors,
            Block partitioningBoundArguments,
            Block partitioningHashSelector,
            Block orderingSelector,
            ExchangeType type,
            ExchangeScope scope,
            PartitioningHandle partitioningHandle,
            NullableValues partitioningBoundValues,
            boolean partitioningReplicateNullsAndAny,
            Optional<List<Integer>> partitioningBucketToPartition,
            Optional<Integer> partitionCount,
            Optional<SortOrderList> sortOrders,
            List<Map<AttributeKey, Object>> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(inputs, "inputs is null");
        requireNonNull(inputFieldSelectors, "inputFieldSelectors is null");
        requireNonNull(partitioningBoundArguments, "partitioningBoundArguments is null");
        requireNonNull(partitioningHashSelector, "partitioningHashSelector is null");
        requireNonNull(orderingSelector, "orderingSelector is null");
        requireNonNull(type, "type is null");
        requireNonNull(scope, "scope is null");
        requireNonNull(partitioningHandle, "partitioningHandle is null");
        requireNonNull(partitioningBoundValues, "partitioningBoundValues is null");
        requireNonNull(partitioningBucketToPartition, "partitioningBucketToPartition is null");
        requireNonNull(partitionCount, "partitionCount is null");
        requireNonNull(sortOrders, "sortOrders is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        if (!inputs.stream()
                .allMatch(input -> IS_RELATION.test(trinoType(input.type())))) {
            throw new TrinoException(IR_ERROR, "inputs to the Exchange operation must be of relation type");
        }
        if (inputs.isEmpty()) {
            throw new TrinoException(IR_ERROR, "inputs to the Exchange operation must not be empty");
        }
        if (inputs.size() != inputFieldSelectors.size()) {
            throw new TrinoException(IR_ERROR, "inputs and input field selectors do not match in size");
        }
        for (int i = 0; i < inputFieldSelectors.size(); i++) {
            Block inputSelector = inputFieldSelectors.get(i);
            Value input = inputs.get(i);
            if (inputSelector.parameters().size() != 1 ||
                    !trinoType(inputSelector.parameters().getFirst().type()).equals(relationRowType(trinoType(input.type()))) ||
                    !(trinoType(inputSelector.getReturnedType()) instanceof RowType || trinoType(inputSelector.getReturnedType()).equals(EMPTY_ROW))) {
                throw new TrinoException(IR_ERROR, "invalid input field selector for Exchange operation");
            }
        }
        List<Type> inputFieldTypes = trinoType(inputFieldSelectors.getFirst().getReturnedType()).getTypeParameters();
        for (Block selector : inputFieldSelectors) {
            if (!inputFieldTypes.equals(trinoType(selector.getReturnedType()).getTypeParameters())) {
                throw new TrinoException(IR_ERROR, "all input field selectors for Exchange operation must return the same type");
            }
        }
        this.inputs = ImmutableList.copyOf(inputs);
        this.inputFieldSelectors = inputFieldSelectors.stream()
                .map(Region::singleBlockRegion)
                .collect(toImmutableList());

        Type exchangeRowType = inputFieldTypes.isEmpty() ? EMPTY_ROW : assignRelationRowTypeFieldNames(RowType.anonymous(inputFieldTypes));
        this.result = new Result(resultName, irType(new MultisetType(exchangeRowType)));

        if (partitioningBoundArguments.parameters().size() != 1 ||
                !trinoType(partitioningBoundArguments.parameters().getFirst().type()).equals(exchangeRowType) ||
                !(trinoType(partitioningBoundArguments.getReturnedType()) instanceof RowType || trinoType(partitioningBoundArguments.getReturnedType()).equals(EMPTY_ROW))) {
            throw new TrinoException(IR_ERROR, "invalid partitioning bound arguments for Exchange operation");
        }
        if (trinoType(partitioningBoundArguments.getReturnedType()).getTypeParameters().size() != partitioningBoundValues.nullableValues().length) {
            throw new TrinoException(IR_ERROR, "partitioning bound arguments and bound values for Exchange do not match in size");
        }
        // TODO check that for a LOCAL exchange, all partitioningBoundArguments are variables (FieldSelections). Pass the current Program to resolve backlinks.
        //  see assertion in ExchangeNode
        // TODO check that for partitioningReplicateNullsAndAny, there is at most one partitioning column.
        //  Pass the current Program to resolve backlinks and check which partitioningBoundArguments are columns (FieldSelections).
        //  see assertion in PartitioningScheme
        this.partitioningBoundArguments = singleBlockRegion(partitioningBoundArguments);

        if (partitioningHashSelector.parameters().size() != 1 ||
                !trinoType(partitioningHashSelector.parameters().getFirst().type()).equals(exchangeRowType) ||
                !(trinoType(partitioningHashSelector.getReturnedType()) instanceof RowType || trinoType(partitioningHashSelector.getReturnedType()).equals(EMPTY_ROW)) ||
                trinoType(partitioningHashSelector.getReturnedType()).getTypeParameters().size() > 1) {
            throw new TrinoException(IR_ERROR, "invalid partitioning hash selector for Exchange operation");
        }
        this.partitioningHashSelector = singleBlockRegion(partitioningHashSelector);

        if (partitionCount.isPresent() && !(partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle)) {
            throw new TrinoException(IR_ERROR, "connector partitioning handle should be of type system partitioning when partitionCount is present");
        }

        if (scope == REMOTE && type != REPARTITION && partitioningReplicateNullsAndAny) {
            throw new TrinoException(IR_ERROR, "only REPARTITION can replicate remotely");
        }

        if (orderingSelector.parameters().size() != 1 ||
                !trinoType(orderingSelector.parameters().getFirst().type()).equals(exchangeRowType) ||
                !(trinoType(orderingSelector.getReturnedType()) instanceof RowType || trinoType(orderingSelector.getReturnedType()).equals(EMPTY_ROW))) {
            throw new TrinoException(IR_ERROR, "invalid ordering selector for Exchange operation");
        }
        if (trinoType(orderingSelector.getReturnedType()).getTypeParameters().size() != sortOrders.map(orders -> orders.sortOrders().size()).orElse(0)) {
            throw new TrinoException(IR_ERROR, "ordering fields and sort orders for Exchange do not match in size");
        }
        this.orderingSelector = singleBlockRegion(orderingSelector);

        if (!trinoType(orderingSelector.getReturnedType()).equals(EMPTY_ROW)) {
            if (scope == REMOTE && !partitioningHandle.equals(SINGLE_DISTRIBUTION)) {
                throw new TrinoException(IR_ERROR, "remote merging exchange requires single distribution");
            }
            if (scope == LOCAL && !partitioningHandle.equals(FIXED_PASSTHROUGH_DISTRIBUTION)) {
                throw new TrinoException(IR_ERROR, "local merging exchange requires passthrough distribution");
            }
            if (type != GATHER) {
                throw new TrinoException(IR_ERROR, "merging exchange must be of GATHER type");
            }
            if (inputs.size() != 1) {
                throw new TrinoException(IR_ERROR, "merging exchange must have single input");
            }
        }

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        EXCHANGE_TYPE.putAttribute(attributes, type);
        EXCHANGE_SCOPE.putAttribute(attributes, scope);
        PARTITIONING_HANDLE.putAttribute(attributes, partitioningHandle);
        NULLABLE_VALUES.putAttribute(attributes, partitioningBoundValues);
        REPLICATE_NULLS_AND_ANY.putAttribute(attributes, partitioningReplicateNullsAndAny);
        partitioningBucketToPartition.ifPresent(bucketToPartition -> BUCKET_TO_PARTITION.putAttribute(attributes, bucketToPartition));
        partitionCount.ifPresent(count -> PARTITION_COUNT.putAttribute(attributes, count));
        sortOrders.ifPresent(orders -> SORT_ORDERS.putAttribute(attributes, orders));

        // TODO derive attributes from source attributes
        this.attributes = attributes.buildOrThrow();
    }

    private Exchange(
            Result result,
            List<Value> inputs,
            List<Region> inputFieldSelectors,
            Region partitioningBoundArguments,
            Region partitioningHashSelector,
            Region orderingSelector,
            Map<AttributeKey, Object> attributes)
    {
        // TODO checks!!!
        super(TRINO, NAME);
        this.result = result;
        this.inputs = inputs;
        this.inputFieldSelectors = inputFieldSelectors;
        this.partitioningBoundArguments = partitioningBoundArguments;
        this.partitioningHashSelector = partitioningHashSelector;
        this.orderingSelector = orderingSelector;
        this.attributes = attributes;
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return inputs;
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.<Region>builder()
                .addAll(inputFieldSelectors)
                .add(partitioningBoundArguments)
                .add(partitioningHashSelector)
                .add(orderingSelector)
                .build();
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    // TODO support rebase on multiple sources at a time so that we can extend the source types and pass-through more fields from all of them
    //  for now, only supporting one source which is pass-through
    public Optional<Operation> rebase(Operation baseOperation, int sourceIndex, CteReuse.Mapping mapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        if (inputs.size() != 1) {
            return Optional.empty();
        }
        checkArgument(sourceIndex == 0, "expected first and only source");



        Type baseRowType = relationRowType(trinoType(baseOperation.result().type()));

        validateMappedTypes(relationRowType(trinoType(this.arguments().get(sourceIndex).type())), baseRowType, mapping);

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
            Optional<Block> rebasedBlock = rebaseBlock(region.getOnlyBlock(), baseRowType, mapping, nameAllocator);
            if (rebasedBlock.isEmpty()) {
                return Optional.empty();
            }
            rebasedBlocksBuilder.add(rebasedBlock.get());
        }
        List<Block> rebasedSingleParameterBlocks = rebasedBlocksBuilder.build();

        // rebase filter
        Optional<Block> rebasedFilter = rebaseBlock(filter.getOnlyBlock(), sourceIndex, baseRowType, mapping, nameAllocator);
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
                sourceIndex == 0 ? baseOperation.result() : left,
                sourceIndex == 1 ? baseOperation.result() : right,
                rebasedBlocks.get(0),
                rebasedBlocks.get(1),
                rebasedBlocks.get(2),
                rebasedBlocks.get(3),
                rebasedBlocks.get(4),
                rebasedBlocks.get(5),
                rebasedBlocks.get(6),
                rebasedBlocks.get(7),
                JOIN_TYPE.getAttribute(attributes()),
                MAY_SKIP_OUTPUT_DUPLICATES.getAttribute(attributes()),
                Optional.ofNullable(DISTRIBUTION_TYPE.getAttribute(attributes())),
                Optional.ofNullable(SPILLABLE.getAttribute(attributes())),
                DYNAMIC_FILTER_IDS.getAttribute(attributes()),
                Optional.ofNullable(STATISTICS_AND_COST_SUMMARY.getAttribute(attributes())),
                sourceIndex == 0 ? baseOperation.attributes() : ImmutableMap.of(), // TODO with partial rebase we miss the source attributes. Shall we do a copying constructor? That would assume that source attrs are the same for the new sourc which is not correct. Probably pass the other source.
                sourceIndex == 1 ? baseOperation.attributes() : ImmutableMap.of()));
    }

    @Override
    public Operation withResult(Result newResult)
    {
        checkArgument(this.result.type().equals(newResult.type()), "result type mismatch");

        return new Exchange(
                new Result(newResult.name(), result.type()),
                this.inputs,
                this.inputFieldSelectors,
                this.partitioningBoundArguments,
                this.partitioningHashSelector,
                this.orderingSelector,
                this.attributes);
    }

    @Override
    public Operation withRegions(List<Region> newRegions)
    {
        checkArgument(newRegions.size() == this.regions().size(), "regions lists size mismatch");

        return new Exchange(
                this.result,
                this.inputs,
                newRegions.subList(0, newRegions.size() - 3),
                newRegions.get(newRegions.size() - 3),
                newRegions.get(newRegions.size() - 2),
                newRegions.get(newRegions.size() - 1),
                this.attributes);
    }

    @Override
    public String prettyPrint(int indentLevel, FormatOptions formatOptions)
    {
        return "pretty exchange";
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (Exchange) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.inputs, that.inputs) &&
                Objects.equals(this.inputFieldSelectors, that.inputFieldSelectors) &&
                Objects.equals(this.partitioningBoundArguments, that.partitioningBoundArguments) &&
                Objects.equals(this.partitioningHashSelector, that.partitioningHashSelector) &&
                Objects.equals(this.orderingSelector, that.orderingSelector) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, inputs, inputFieldSelectors, partitioningBoundArguments, partitioningHashSelector, orderingSelector, attributes);
    }
}
