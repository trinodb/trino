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
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.spi.TrinoException;
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.Attributes.DistributionType;
import io.trino.sql.dialect.trino.Attributes.JoinType;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Attributes.DISTRIBUTION_TYPE;
import static io.trino.sql.dialect.trino.Attributes.DYNAMIC_FILTER_IDS;
import static io.trino.sql.dialect.trino.Attributes.JOIN_TYPE;
import static io.trino.sql.dialect.trino.Attributes.MAY_SKIP_OUTPUT_DUPLICATES;
import static io.trino.sql.dialect.trino.Attributes.SPILLABLE;
import static io.trino.sql.dialect.trino.Attributes.STATISTICS_AND_COST_SUMMARY;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public final class Join
        extends TrinoOperation
{
    private static final String NAME = "join";

    private final Result result;
    private final Value left;
    private final Value right;
    private final Region leftCriteriaSelector;
    private final Region rightCriteriaSelector;
    private final Region filter;
    private final Region leftHashSelector;
    private final Region rightHashSelector;
    private final Region leftOutputSelector;
    private final Region rightOutputSelector;
    private final Region dynamicFilterTargetSelector;
    private final Map<AttributeKey, Object> attributes;

    public Join(
            String resultName,
            Value left,
            Value right,
            Block leftCriteriaSelector,
            Block rightCriteriaSelector,
            Block filter,
            Block leftHashSelector,
            Block rightHashSelector,
            Block leftOutputSelector,
            Block rightOutputSelector,
            Block dynamicFilterTargetSelector,
            JoinType joinType,
            boolean maySkipOutputDuplicates,
            Optional<DistributionType> distributionType,
            Optional<Boolean> spillable,
            List<String> dynamicFilterIds,
            Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost,
            Map<AttributeKey, Object> leftAttributes,
            Map<AttributeKey, Object> rightAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        requireNonNull(leftCriteriaSelector, "leftCriteriaSelector is null");
        requireNonNull(rightCriteriaSelector, "rightCriteriaSelector is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(leftHashSelector, "leftHashSelector is null");
        requireNonNull(rightHashSelector, "rightHashSelector is null");
        requireNonNull(leftOutputSelector, "leftOutputSelector is null");
        requireNonNull(rightOutputSelector, "rightOutputSelector is null");
        requireNonNull(dynamicFilterTargetSelector, "dynamicFilterTargetSelector is null");
        requireNonNull(joinType, "joinType is null");
        requireNonNull(distributionType, "distributionType is null");
        requireNonNull(spillable, "spillable is null");
        requireNonNull(dynamicFilterIds, "dynamicFilterIds is null");
        requireNonNull(dynamicFilterIds, "dynamicFilterIds is null");
        requireNonNull(reorderJoinStatsAndCost, "reorderJoinStatsAndCost is null");
        requireNonNull(leftAttributes, "leftAttributes is null");
        requireNonNull(rightAttributes, "rightAttributes is null");

        if (!IS_RELATION.test(trinoType(left.type())) || !IS_RELATION.test(trinoType(right.type()))) {
            throw new TrinoException(IR_ERROR, "left and right sources of Join operation must be of relation type");
        }
        this.left = left;
        this.right = right;

        if (leftCriteriaSelector.parameters().size() != 1 ||
                !trinoType(leftCriteriaSelector.parameters().getFirst().type()).equals(relationRowType(trinoType(left.type()))) ||
                !(trinoType(leftCriteriaSelector.getReturnedType()) instanceof RowType || trinoType(leftCriteriaSelector.getReturnedType()).equals(EMPTY_ROW))) {
            throw new TrinoException(IR_ERROR, "invalid left criteria selector for Join operation");
        }
        this.leftCriteriaSelector = singleBlockRegion(leftCriteriaSelector);

        if (rightCriteriaSelector.parameters().size() != 1 ||
                !trinoType(rightCriteriaSelector.parameters().getFirst().type()).equals(relationRowType(trinoType(right.type()))) ||
                !(trinoType(rightCriteriaSelector.getReturnedType()) instanceof RowType || trinoType(rightCriteriaSelector.getReturnedType()).equals(EMPTY_ROW))) {
            throw new TrinoException(IR_ERROR, "invalid right criteria selector for Join operation");
        }
        this.rightCriteriaSelector = singleBlockRegion(rightCriteriaSelector);

        if (!trinoType(leftCriteriaSelector.getReturnedType()).getTypeParameters().equals(trinoType(rightCriteriaSelector.getReturnedType()).getTypeParameters())) {
            throw new TrinoException(IR_ERROR, "left and right criteria selectors for Join operation do not match");
        }

        if (filter.parameters().size() != 2 ||
                !trinoType(filter.parameters().get(0).type()).equals(relationRowType(trinoType(left.type()))) ||
                !trinoType(filter.parameters().get(1).type()).equals(relationRowType(trinoType(right.type()))) ||
                !trinoType(filter.getReturnedType()).equals(BOOLEAN)) {
            throw new TrinoException(IR_ERROR, "invalid filter for Join operation");
        }
        this.filter = singleBlockRegion(filter);

        if (leftHashSelector.parameters().size() != 1 ||
                !trinoType(leftHashSelector.parameters().getFirst().type()).equals(relationRowType(trinoType(left.type()))) ||
                !(trinoType(leftHashSelector.getReturnedType()) instanceof RowType || trinoType(leftHashSelector.getReturnedType()).equals(EMPTY_ROW)) ||
                trinoType(leftHashSelector.getReturnedType()).getTypeParameters().size() > 1) {
            throw new TrinoException(IR_ERROR, "invalid left hash selector for Join operation");
        }
        this.leftHashSelector = singleBlockRegion(leftHashSelector);

        if (rightHashSelector.parameters().size() != 1 ||
                !trinoType(rightHashSelector.parameters().getFirst().type()).equals(relationRowType(trinoType(right.type()))) ||
                !(trinoType(rightHashSelector.getReturnedType()) instanceof RowType || trinoType(rightHashSelector.getReturnedType()).equals(EMPTY_ROW)) ||
                trinoType(rightHashSelector.getReturnedType()).getTypeParameters().size() > 1) {
            throw new TrinoException(IR_ERROR, "invalid right hash selector for Join operation");
        }
        this.rightHashSelector = singleBlockRegion(rightHashSelector);

        if (leftOutputSelector.parameters().size() != 1 ||
                !trinoType(leftOutputSelector.parameters().getFirst().type()).equals(relationRowType(trinoType(left.type()))) ||
                !(trinoType(leftOutputSelector.getReturnedType()) instanceof RowType || trinoType(leftOutputSelector.getReturnedType()).equals(EMPTY_ROW))) {
            throw new TrinoException(IR_ERROR, "invalid left output selector for Join operation");
        }
        this.leftOutputSelector = singleBlockRegion(leftOutputSelector);

        if (rightOutputSelector.parameters().size() != 1 ||
                !trinoType(rightOutputSelector.parameters().getFirst().type()).equals(relationRowType(trinoType(right.type()))) ||
                !(trinoType(rightOutputSelector.getReturnedType()) instanceof RowType || trinoType(rightOutputSelector.getReturnedType()).equals(EMPTY_ROW))) {
            throw new TrinoException(IR_ERROR, "invalid right output selector for Join operation");
        }
        this.rightOutputSelector = singleBlockRegion(rightOutputSelector);

        List<Type> outputTypes = ImmutableList.<Type>builder()
                .addAll(trinoType(leftOutputSelector.getReturnedType()).getTypeParameters())
                .addAll(trinoType(rightOutputSelector.getReturnedType()).getTypeParameters())
                .build();

        if (outputTypes.isEmpty()) {
            this.result = new Result(resultName, irType(new MultisetType(EMPTY_ROW)));
        }
        else {
            this.result = new Result(resultName, irType(new MultisetType(RowType.anonymous(outputTypes))));
        }

        if (dynamicFilterTargetSelector.parameters().size() != 1 ||
                !trinoType(dynamicFilterTargetSelector.parameters().getFirst().type()).equals(relationRowType(trinoType(right.type()))) ||
                !(trinoType(dynamicFilterTargetSelector.getReturnedType()) instanceof RowType || trinoType(dynamicFilterTargetSelector.getReturnedType()).equals(EMPTY_ROW))) {
            throw new TrinoException(IR_ERROR, "invalid dynamic filter target selector for Join operation");
        }
        this.dynamicFilterTargetSelector = singleBlockRegion(dynamicFilterTargetSelector);

        if (trinoType(dynamicFilterTargetSelector.getReturnedType()).getTypeParameters().size() != dynamicFilterIds.size()) {
            throw new TrinoException(IR_ERROR, "dynamic filter target selector for Join operation does not match dynamic filter IDs");
        }

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        JOIN_TYPE.putAttribute(attributes, joinType);
        MAY_SKIP_OUTPUT_DUPLICATES.putAttribute(attributes, maySkipOutputDuplicates);
        distributionType.ifPresent(value -> DISTRIBUTION_TYPE.putAttribute(attributes, value));
        spillable.ifPresent(value -> SPILLABLE.putAttribute(attributes, value));
        DYNAMIC_FILTER_IDS.putAttribute(attributes, dynamicFilterIds);
        reorderJoinStatsAndCost.ifPresent(estimate -> STATISTICS_AND_COST_SUMMARY.putAttribute(attributes, estimate));

        // TODO derive attributes from source attributes
        this.attributes = attributes.buildOrThrow();
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return ImmutableList.of(left, right);
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of(leftCriteriaSelector, rightCriteriaSelector, filter, leftHashSelector, rightHashSelector, leftOutputSelector, rightOutputSelector, dynamicFilterTargetSelector);
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, FormatOptions formatOptions)
    {
        return "pretty join";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new Join(
                result.name(),
                index == 0 ? newArgument : left,
                index == 1 ? newArgument : right,
                leftCriteriaSelector.getOnlyBlock(),
                rightCriteriaSelector.getOnlyBlock(),
                filter.getOnlyBlock(),
                leftHashSelector.getOnlyBlock(),
                rightHashSelector.getOnlyBlock(),
                leftOutputSelector.getOnlyBlock(),
                rightOutputSelector.getOnlyBlock(),
                dynamicFilterTargetSelector.getOnlyBlock(),
                JOIN_TYPE.getAttribute(attributes),
                MAY_SKIP_OUTPUT_DUPLICATES.getAttribute(attributes),
                Optional.ofNullable(DISTRIBUTION_TYPE.getAttribute(attributes)),
                Optional.ofNullable(SPILLABLE.getAttribute(attributes)),
                DYNAMIC_FILTER_IDS.getAttribute(attributes),
                Optional.ofNullable(STATISTICS_AND_COST_SUMMARY.getAttribute(attributes)),
                ImmutableMap.of(),
                ImmutableMap.of());
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
        var that = (Join) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.left, that.left) &&
                Objects.equals(this.right, that.right) &&
                Objects.equals(this.leftCriteriaSelector, that.leftCriteriaSelector) &&
                Objects.equals(this.rightCriteriaSelector, that.rightCriteriaSelector) &&
                Objects.equals(this.filter, that.filter) &&
                Objects.equals(this.leftHashSelector, that.leftHashSelector) &&
                Objects.equals(this.rightHashSelector, that.rightHashSelector) &&
                Objects.equals(this.leftOutputSelector, that.leftOutputSelector) &&
                Objects.equals(this.rightOutputSelector, that.rightOutputSelector) &&
                Objects.equals(this.dynamicFilterTargetSelector, that.dynamicFilterTargetSelector) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, left, right, leftCriteriaSelector, rightCriteriaSelector, filter, leftHashSelector, rightHashSelector, leftOutputSelector, rightOutputSelector, dynamicFilterTargetSelector, attributes);
    }
}
