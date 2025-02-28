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
import io.trino.sql.dialect.trino.Attributes.AggregationStep;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.optimizations.CteReuse;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import static com.clearspring.analytics.util.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Attributes.AGGREGATION_STEP;
import static io.trino.sql.dialect.trino.Attributes.GLOBAL_GROUPING_SETS;
import static io.trino.sql.dialect.trino.Attributes.GROUPING_SETS_COUNT;
import static io.trino.sql.dialect.trino.Attributes.GROUP_ID_INDEX;
import static io.trino.sql.dialect.trino.Attributes.INPUT_REDUCING;
import static io.trino.sql.dialect.trino.Attributes.PRE_GROUPED_INDEXES;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.assignRelationRowTypeFieldNames;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.operation.SqlOperationsUtil.rebaseBlock;
import static io.trino.sql.dialect.trino.operation.SqlOperationsUtil.validateMappedTypes;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.util.Preconditions.checkArgument;

public class Aggregation
        extends Operation
        implements SqlRelationalOperation
{
    private static final String NAME = "aggregation";

    private final Result result;
    private final Value input;
    private final Region aggregateCalls;
    private final Region groupingKeysSelector;
    private final Region hashSelector;
    private final Map<AttributeKey, Object> attributes;

    public Aggregation(
            String resultName,
            Value input,
            Block aggregateCalls,
            Block groupingKeysSelector,
            Block hashSelector,
            int groupingSetCount,
            List<Integer> globalGroupingSets,
            OptionalInt groupIdIndex, // index in groupingKeysSelector
            List<Integer> preGroupedIndexes, // indexes in groupingKeysSelector
            AggregationStep step,
            boolean isInputReducing, // note: potentially does not roundtrip to AggregationNode -- it has Optional<Boolean>, but the getter coalesces to false
            Map<AttributeKey, Object> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(aggregateCalls, "aggregateCalls is null");
        requireNonNull(groupingKeysSelector, "groupingKeysSelector is null");
        requireNonNull(hashSelector, "hashSelector is null");
        requireNonNull(globalGroupingSets, "globalGroupingSets is null");
        requireNonNull(groupIdIndex, "groupIdIndex is null");
        requireNonNull(preGroupedIndexes, "preGroupedIndexes is null");
        requireNonNull(step, "step is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        if (!IS_RELATION.test(trinoType(input.type()))) {
            throw new TrinoException(IR_ERROR, "input to the Aggregation operation must be of relation type");
        }
        this.input = input;

        if (aggregateCalls.parameters().size() != 1 ||
                !trinoType(aggregateCalls.parameters().getFirst().type()).equals(trinoType(input.type())) ||
                !(trinoType(aggregateCalls.getReturnedType()) instanceof RowType || trinoType(aggregateCalls.getReturnedType()).equals(EMPTY_ROW))) {
            throw new TrinoException(IR_ERROR, "invalid aggregateCalls for Aggregation operation");
        }
        // TODO check that this is a Row of AggregateCall operations. Pass the current Program to resolve backlinks.
        this.aggregateCalls = singleBlockRegion(aggregateCalls);

        if (groupingKeysSelector.parameters().size() != 1 ||
                !trinoType(groupingKeysSelector.parameters().getFirst().type()).equals(relationRowType(trinoType(input.type()))) ||
                !(trinoType(groupingKeysSelector.getReturnedType()) instanceof RowType || trinoType(groupingKeysSelector.getReturnedType()).equals(EMPTY_ROW))) {
            throw new TrinoException(IR_ERROR, "invalid grouping keys selector for Aggregation operation");
        }
        this.groupingKeysSelector = singleBlockRegion(groupingKeysSelector);

        if (hashSelector.parameters().size() != 1 ||
                !trinoType(hashSelector.parameters().getFirst().type()).equals(relationRowType(trinoType(input.type()))) ||
                !(trinoType(hashSelector.getReturnedType()) instanceof RowType || trinoType(hashSelector.getReturnedType()).equals(EMPTY_ROW)) ||
                trinoType(hashSelector.getReturnedType()).getTypeParameters().size() > 1) {
            throw new TrinoException(IR_ERROR, "invalid hash selector for Aggregation operation");
        }
        this.hashSelector = singleBlockRegion(hashSelector);

        List<Type> outputTypes = ImmutableList.<Type>builder()
                .addAll(trinoType(groupingKeysSelector.getReturnedType()).getTypeParameters())
                .addAll(trinoType(hashSelector.getReturnedType()).getTypeParameters())
                .addAll(trinoType(aggregateCalls.getReturnedType()).getTypeParameters())
                .build();

        if (outputTypes.isEmpty()) {
            this.result = new Result(resultName, irType(new MultisetType(EMPTY_ROW)));
        }
        else {
            this.result = new Result(resultName, irType(new MultisetType(assignRelationRowTypeFieldNames(RowType.anonymous(outputTypes)))));
        }

        int groupingKeysCount = trinoType(groupingKeysSelector.getReturnedType()).getTypeParameters().size();
        groupIdIndex.ifPresent(index -> {
            if (index < 0 || index >= groupingKeysCount) {
                throw new TrinoException(IR_ERROR, "invalid group id for Aggregation operation");
            }
        });
        preGroupedIndexes.stream()
                .forEach(index -> {
                    if (index < 0 || index >= groupingKeysCount) {
                        throw new TrinoException(IR_ERROR, "invalid pre-grouped field for Aggregation operation");
                    }
                });

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        GROUPING_SETS_COUNT.putAttribute(attributes, groupingSetCount);
        GLOBAL_GROUPING_SETS.putAttribute(attributes, globalGroupingSets);
        groupIdIndex.ifPresent(index -> GROUP_ID_INDEX.putAttribute(attributes, index));
        PRE_GROUPED_INDEXES.putAttribute(attributes, preGroupedIndexes);
        AGGREGATION_STEP.putAttribute(attributes, step);
        INPUT_REDUCING.putAttribute(attributes, isInputReducing);

        // TODO derive attributes from source attributes
        // TODO add more external attributes based on AggregationNode, for example: produces distinct rows, is decomposable,...
        this.attributes = attributes.buildOrThrow();
    }

    private Aggregation(Result result, Value input, Region aggregateCalls, Region groupingKeysSelector, Region hashSelector, Map<AttributeKey, Object> attributes)
    {
        super(TRINO, NAME);
        this.result = result;
        this.input = input;
        this.aggregateCalls = aggregateCalls;
        this.groupingKeysSelector = groupingKeysSelector;
        this.hashSelector = hashSelector;
        this.attributes = ImmutableMap.copyOf(attributes);
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return ImmutableList.of(input);
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of(aggregateCalls, groupingKeysSelector, hashSelector);
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public Optional<Operation> rebase(Operation baseOperation, int sourceIndex, CteReuse.Mapping mapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        checkArgument(sourceIndex == 0, "aggregation has one source");

        Type baseType = trinoType(baseOperation.result().type());
        Type baseRowType = relationRowType(baseType);

        validateMappedTypes(relationRowType(trinoType(this.input.type())), baseRowType, mapping);

        // the ^aggregates block cannot be rebased with the rebaseBlock() method because it is based on the relation type, not the row type.
        Block oldAggregatesBlock = aggregateCalls.getOnlyBlock();

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
                Optional<Operation> rebasedAggregateCall = aggregateCall.rebase(newAggregatesParameter, mapping, nameAllocator);
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
        for (Region region : this.regions().subList(1, this.regions().size())) {
            Optional<Block> rebasedBlock = rebaseBlock(region.getOnlyBlock(), baseRowType, mapping, nameAllocator);
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
                GROUPING_SETS_COUNT.getAttribute(this.attributes),
                GLOBAL_GROUPING_SETS.getAttribute(this.attributes),
                Optional.ofNullable(GROUP_ID_INDEX.getAttribute(this.attributes)).map(OptionalInt::of).orElse(OptionalInt.empty()),
                PRE_GROUPED_INDEXES.getAttribute(this.attributes),
                AGGREGATION_STEP.getAttribute(this.attributes),
                INPUT_REDUCING.getAttribute(this.attributes),
                baseOperation.attributes()));
    }

    @Override
    public Operation withResult(Result newResult)
    {
        checkArgument(this.result.type().equals(newResult.type()), "result type mismatch");
        return new Aggregation(newResult, this.input, this.aggregateCalls, this.groupingKeysSelector, this.hashSelector, this.attributes);
    }

    @Override
    public Operation withRegions(List<Region> newRegions)
    {
        // TODO this does not validate the new Regions. Should run the same checks as the constructor.
        checkArgument(newRegions.size() == this.regions().size(), "regions lists size mismatch");
        return new Aggregation(this.result, this.input, newRegions.get(0), newRegions.get(1), newRegions.get(2), this.attributes);
    }

    @Override
    public String prettyPrint(int indentLevel, FormatOptions formatOptions)
    {
        return "pretty aggregation";
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
        var that = (Aggregation) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.input, that.input) &&
                Objects.equals(this.aggregateCalls, that.aggregateCalls) &&
                Objects.equals(this.groupingKeysSelector, that.groupingKeysSelector) &&
                Objects.equals(this.hashSelector, that.hashSelector) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, input, aggregateCalls, groupingKeysSelector, hashSelector, attributes);
    }
}
