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
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.NullLiteral;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Immutable
public class JoinNode
        extends PlanNode
{
    public enum DistributionType
    {
        PARTITIONED,
        REPLICATED
    }

    public enum Type
    {
        INNER("InnerJoin"),
        LEFT("LeftJoin"),
        RIGHT("RightJoin"),
        FULL("FullJoin");

        private final String joinLabel;

        Type(String joinLabel)
        {
            this.joinLabel = joinLabel;
        }

        public String getJoinLabel()
        {
            return joinLabel;
        }

        public static Type typeConvert(Join.Type joinType)
        {
            return switch (joinType) {
                case CROSS, IMPLICIT, INNER -> Type.INNER;
                case LEFT -> Type.LEFT;
                case RIGHT -> Type.RIGHT;
                case FULL -> Type.FULL;
            };
        }
    }

    private final Type type;
    private final PlanNode left;
    private final PlanNode right;
    private final List<EquiJoinClause> criteria;
    private final List<Symbol> leftOutputSymbols;
    private final List<Symbol> rightOutputSymbols;
    private final boolean maySkipOutputDuplicates;
    private final Optional<Expression> filter;
    private final Optional<Symbol> leftHashSymbol;
    private final Optional<Symbol> rightHashSymbol;
    private final Optional<DistributionType> distributionType;
    private final Optional<Boolean> spillable;
    private final Map<DynamicFilterId, Symbol> dynamicFilters;

    // stats and cost used for join reordering
    private final Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost;

    @JsonCreator
    public JoinNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") Type type,
            @JsonProperty("left") PlanNode left,
            @JsonProperty("right") PlanNode right,
            @JsonProperty("criteria") List<EquiJoinClause> criteria,
            @JsonProperty("leftOutputSymbols") List<Symbol> leftOutputSymbols,
            @JsonProperty("rightOutputSymbols") List<Symbol> rightOutputSymbols,
            @JsonProperty("maySkipOutputDuplicates") boolean maySkipOutputDuplicates,
            @JsonProperty("filter") Optional<Expression> filter,
            @JsonProperty("leftHashSymbol") Optional<Symbol> leftHashSymbol,
            @JsonProperty("rightHashSymbol") Optional<Symbol> rightHashSymbol,
            @JsonProperty("distributionType") Optional<DistributionType> distributionType,
            @JsonProperty("spillable") Optional<Boolean> spillable,
            @JsonProperty("dynamicFilters") Map<DynamicFilterId, Symbol> dynamicFilters,
            @JsonProperty("reorderJoinStatsAndCost") Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost)
    {
        super(id);
        requireNonNull(type, "type is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        requireNonNull(criteria, "criteria is null");
        requireNonNull(leftOutputSymbols, "leftOutputSymbols is null");
        requireNonNull(rightOutputSymbols, "rightOutputSymbols is null");
        requireNonNull(filter, "filter is null");
        // The condition doesn't guarantee that filter is of type boolean, but was found to be a practical way to identify
        // places where JoinNode could be created without appropriate coercions.
        checkArgument(filter.isEmpty() || !(filter.get() instanceof NullLiteral), "Filter must be an expression of boolean type: %s", filter);
        requireNonNull(leftHashSymbol, "leftHashSymbol is null");
        requireNonNull(rightHashSymbol, "rightHashSymbol is null");
        requireNonNull(distributionType, "distributionType is null");
        requireNonNull(spillable, "spillable is null");

        this.type = type;
        this.left = left;
        this.right = right;
        this.criteria = ImmutableList.copyOf(criteria);
        this.leftOutputSymbols = ImmutableList.copyOf(leftOutputSymbols);
        this.rightOutputSymbols = ImmutableList.copyOf(rightOutputSymbols);
        this.maySkipOutputDuplicates = maySkipOutputDuplicates;
        this.filter = filter;
        this.leftHashSymbol = leftHashSymbol;
        this.rightHashSymbol = rightHashSymbol;
        this.distributionType = distributionType;
        this.spillable = spillable;
        this.dynamicFilters = ImmutableMap.copyOf(requireNonNull(dynamicFilters, "dynamicFilters is null"));
        this.reorderJoinStatsAndCost = requireNonNull(reorderJoinStatsAndCost, "reorderJoinStatsAndCost is null");

        Set<Symbol> leftSymbols = ImmutableSet.copyOf(left.getOutputSymbols());
        Set<Symbol> rightSymbols = ImmutableSet.copyOf(right.getOutputSymbols());

        checkArgument(leftSymbols.containsAll(leftOutputSymbols), "Left source inputs do not contain all left output symbols");
        checkArgument(rightSymbols.containsAll(rightOutputSymbols), "Right source inputs do not contain all right output symbols");

        checkArgument(!(criteria.isEmpty() && leftHashSymbol.isPresent()), "Left hash symbol is only valid in an equijoin");
        checkArgument(!(criteria.isEmpty() && rightHashSymbol.isPresent()), "Right hash symbol is only valid in an equijoin");

        criteria.forEach(equiJoinClause ->
                checkArgument(
                        leftSymbols.contains(equiJoinClause.getLeft()) &&
                                rightSymbols.contains(equiJoinClause.getRight()),
                        "Equality join criteria should be normalized according to join sides: %s", equiJoinClause));

        if (distributionType.isPresent()) {
            // The implementation of full outer join only works if the data is hash partitioned.
            checkArgument(
                    !(distributionType.get() == REPLICATED && (type == RIGHT || type == FULL)),
                    "%s join do not work with %s distribution type",
                    type,
                    distributionType.get());
        }

        for (Symbol symbol : dynamicFilters.values()) {
            checkArgument(rightSymbols.contains(symbol), "Right join input doesn't contain symbol for dynamic filter: %s", symbol);
        }
    }

    public JoinNode flipChildren()
    {
        return new JoinNode(
                getId(),
                flipType(type),
                right,
                left,
                flipJoinCriteria(criteria),
                rightOutputSymbols,
                leftOutputSymbols,
                maySkipOutputDuplicates,
                filter,
                rightHashSymbol,
                leftHashSymbol,
                distributionType,
                spillable,
                ImmutableMap.of(), // dynamicFilters are invalid after flipping children
                reorderJoinStatsAndCost);
    }

    private static Type flipType(Type type)
    {
        return switch (type) {
            case INNER -> INNER;
            case FULL -> FULL;
            case LEFT -> RIGHT;
            case RIGHT -> LEFT;
        };
    }

    private static List<EquiJoinClause> flipJoinCriteria(List<EquiJoinClause> joinCriteria)
    {
        return joinCriteria.stream()
                .map(EquiJoinClause::flip)
                .collect(toImmutableList());
    }

    @JsonProperty("type")
    public Type getType()
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
    public List<EquiJoinClause> getCriteria()
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

    @JsonProperty("leftHashSymbol")
    public Optional<Symbol> getLeftHashSymbol()
    {
        return leftHashSymbol;
    }

    @JsonProperty("rightHashSymbol")
    public Optional<Symbol> getRightHashSymbol()
    {
        return rightHashSymbol;
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
    public Optional<DistributionType> getDistributionType()
    {
        return distributionType;
    }

    @JsonProperty("spillable")
    public Optional<Boolean> isSpillable()
    {
        return spillable;
    }

    @JsonProperty("maySkipOutputDuplicates")
    public boolean isMaySkipOutputDuplicates()
    {
        return maySkipOutputDuplicates;
    }

    @JsonProperty
    public Map<DynamicFilterId, Symbol> getDynamicFilters()
    {
        return dynamicFilters;
    }

    @JsonProperty
    public Optional<PlanNodeStatsAndCostSummary> getReorderJoinStatsAndCost()
    {
        return reorderJoinStatsAndCost;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitJoin(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new JoinNode(getId(), type, newChildren.get(0), newChildren.get(1), criteria, leftOutputSymbols, rightOutputSymbols, maySkipOutputDuplicates, filter, leftHashSymbol, rightHashSymbol, distributionType, spillable, dynamicFilters, reorderJoinStatsAndCost);
    }

    public JoinNode withDistributionType(DistributionType distributionType)
    {
        return new JoinNode(getId(), type, left, right, criteria, leftOutputSymbols, rightOutputSymbols, maySkipOutputDuplicates, filter, leftHashSymbol, rightHashSymbol, Optional.of(distributionType), spillable, dynamicFilters, reorderJoinStatsAndCost);
    }

    public JoinNode withSpillable(boolean spillable)
    {
        return new JoinNode(getId(), type, left, right, criteria, leftOutputSymbols, rightOutputSymbols, maySkipOutputDuplicates, filter, leftHashSymbol, rightHashSymbol, distributionType, Optional.of(spillable), dynamicFilters, reorderJoinStatsAndCost);
    }

    public JoinNode withMaySkipOutputDuplicates()
    {
        return new JoinNode(getId(), type, left, right, criteria, leftOutputSymbols, rightOutputSymbols, true, filter, leftHashSymbol, rightHashSymbol, distributionType, spillable, dynamicFilters, reorderJoinStatsAndCost);
    }

    public JoinNode withReorderJoinStatsAndCost(PlanNodeStatsAndCostSummary statsAndCost)
    {
        return new JoinNode(getId(), type, left, right, criteria, leftOutputSymbols, rightOutputSymbols, maySkipOutputDuplicates, filter, leftHashSymbol, rightHashSymbol, distributionType, spillable, dynamicFilters, Optional.of(statsAndCost));
    }

    public JoinNode withoutDynamicFilters()
    {
        return new JoinNode(getId(), type, left, right, criteria, leftOutputSymbols, rightOutputSymbols, maySkipOutputDuplicates, filter, leftHashSymbol, rightHashSymbol, distributionType, spillable, ImmutableMap.of(), reorderJoinStatsAndCost);
    }

    public boolean isCrossJoin()
    {
        return criteria.isEmpty() && filter.isEmpty() && type == INNER;
    }

    public static class EquiJoinClause
    {
        private final Symbol left;
        private final Symbol right;

        @JsonCreator
        public EquiJoinClause(@JsonProperty("left") Symbol left, @JsonProperty("right") Symbol right)
        {
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
        }

        @JsonProperty("left")
        public Symbol getLeft()
        {
            return left;
        }

        @JsonProperty("right")
        public Symbol getRight()
        {
            return right;
        }

        public ComparisonExpression toExpression()
        {
            return new ComparisonExpression(ComparisonExpression.Operator.EQUAL, left.toSymbolReference(), right.toSymbolReference());
        }

        public EquiJoinClause flip()
        {
            return new EquiJoinClause(right, left);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || !this.getClass().equals(obj.getClass())) {
                return false;
            }

            EquiJoinClause other = (EquiJoinClause) obj;

            return Objects.equals(this.left, other.left) &&
                    Objects.equals(this.right, other.right);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(left, right);
        }

        @Override
        public String toString()
        {
            return format("%s = %s", left, right);
        }
    }
}
