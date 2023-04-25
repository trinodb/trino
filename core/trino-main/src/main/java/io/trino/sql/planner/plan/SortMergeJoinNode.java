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
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.Expression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SortMergeJoinNode
        extends PlanNode
{
    private final JoinNode.Type type;
    private final PlanNode left;
    private final PlanNode right;
    private final List<JoinNode.EquiJoinClause> criteria;
    private final List<Symbol> leftOutputSymbols;
    private final List<Symbol> rightOutputSymbols;
    private final Optional<Expression> filter;
    private final Optional<JoinNode.DistributionType> distributionType;
    private final Map<DynamicFilterId, Symbol> dynamicFilters;

    private final Boolean needSortLeft;
    private final Boolean needSortRight;

    @JsonCreator
    public SortMergeJoinNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") JoinNode.Type type,
            @JsonProperty("left") PlanNode left,
            @JsonProperty("right") PlanNode right,
            @JsonProperty("criteria") List<JoinNode.EquiJoinClause> criteria,
            @JsonProperty("leftOutputSymbols") List<Symbol> leftOutputSymbols,
            @JsonProperty("rightOutputSymbols") List<Symbol> rightOutputSymbols,
            @JsonProperty("filter") Optional<Expression> filter,
            @JsonProperty("distributionType") Optional<JoinNode.DistributionType> distributionType,
            @JsonProperty("dynamicFilters") Map<DynamicFilterId, Symbol> dynamicFilters,
            @JsonProperty("needSortLeft") Boolean needSortLeft,
            @JsonProperty("needSortRight") Boolean needSortRight)
    {
        super(id);
        requireNonNull(type, "type is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        requireNonNull(criteria, "criteria is null");
        requireNonNull(leftOutputSymbols, "leftOutputSymbols is null");
        requireNonNull(rightOutputSymbols, "rightOutputSymbols is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(distributionType, "distributionType is null");

        this.type = type;
        this.left = left;
        this.right = right;
        this.criteria = ImmutableList.copyOf(criteria);
        this.leftOutputSymbols = ImmutableList.copyOf(leftOutputSymbols);
        this.rightOutputSymbols = ImmutableList.copyOf(rightOutputSymbols);
        this.filter = filter;
        this.distributionType = distributionType;
        this.dynamicFilters = ImmutableMap.copyOf(requireNonNull(dynamicFilters, "dynamicFilters is null"));
        this.needSortLeft = needSortLeft;
        this.needSortRight = needSortRight;

        checkArgument(dynamicFilters.size() == 0, "Dynamic filter was not supported with sort merge join");

        Set<Symbol> leftSymbols = ImmutableSet.copyOf(left.getOutputSymbols());
        Set<Symbol> rightSymbols = ImmutableSet.copyOf(right.getOutputSymbols());

        checkArgument(leftSymbols.containsAll(leftOutputSymbols), "Left source inputs do not contain all left output symbols");
        checkArgument(rightSymbols.containsAll(rightOutputSymbols), "Right source inputs do not contain all right output symbols");

        criteria.forEach(equiJoinClause ->
                checkArgument(
                        leftSymbols.contains(equiJoinClause.getLeft()) &&
                                rightSymbols.contains(equiJoinClause.getRight()),
                        "Equality join criteria should be normalized according to join sides: %s", equiJoinClause));

        for (Symbol symbol : dynamicFilters.values()) {
            checkArgument(rightSymbols.contains(symbol), "Right join input doesn't contain symbol for dynamic filter: %s", symbol);
        }
    }

    public OrderingScheme getLeftOrdering()
    {
        List<Symbol> leftSymbols = Lists.transform(criteria, JoinNode.EquiJoinClause::getLeft);
        Map<Symbol, SortOrder> orders = new HashMap<>();
        leftSymbols.stream().forEach(symbol -> orders.put(symbol, SortOrder.ASC_NULLS_LAST));
        return new OrderingScheme(leftSymbols, orders);
    }

    public OrderingScheme getRightOrdering()
    {
        List<Symbol> rightSymbols = Lists.transform(criteria, JoinNode.EquiJoinClause::getRight);
        Map<Symbol, SortOrder> orders = new HashMap<>();
        rightSymbols.stream().forEach(symbol -> orders.put(symbol, SortOrder.ASC_NULLS_LAST));
        return new OrderingScheme(rightSymbols, orders);
    }

    @JsonProperty("type")
    public JoinNode.Type getType()
    {
        return type;
    }

    @JsonProperty("left")
    public PlanNode getLeft()
    {
        return left;
    }

    @JsonProperty("right")
    public PlanNode getRight()
    {
        return right;
    }

    @JsonProperty("criteria")
    public List<JoinNode.EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    @JsonProperty("leftOutputSymbols")
    public List<Symbol> getLeftOutputSymbols()
    {
        return leftOutputSymbols;
    }

    @JsonProperty("rightOutputSymbols")
    public List<Symbol> getRightOutputSymbols()
    {
        return rightOutputSymbols;
    }

    @JsonProperty("filter")
    public Optional<Expression> getFilter()
    {
        return filter;
    }

    @JsonProperty("needSortLeft")
    public Boolean getNeedSortLeft()
    {
        return needSortLeft;
    }

    @JsonProperty("needSortRight")
    public Boolean getNeedSortRight()
    {
        return needSortRight;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(left, right);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(leftOutputSymbols)
                .addAll(rightOutputSymbols)
                .build();
    }

    @JsonProperty("distributionType")
    public Optional<JoinNode.DistributionType> getDistributionType()
    {
        return distributionType;
    }

    @JsonProperty
    public Map<DynamicFilterId, Symbol> getDynamicFilters()
    {
        return dynamicFilters;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitSortMergeJoin(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return replaceChildren(newChildren, needSortLeft, needSortRight);
    }

    public PlanNode replaceChildren(List<PlanNode> newChildren, Boolean needSortLeft, Boolean needSortRight)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new SortMergeJoinNode(getId(), type, newChildren.get(0), newChildren.get(1), criteria, leftOutputSymbols, rightOutputSymbols, filter, distributionType, dynamicFilters, needSortLeft, needSortRight);
    }
}
