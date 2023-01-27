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
package io.trino.sql.planner.assertions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.DistributionType;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NotExpression;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.operator.join.JoinUtils.getJoinDynamicFilters;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.planner.ExpressionExtractor.extractExpressions;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.PlanMatchPattern.DynamicFilterPattern;
import static io.trino.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static java.util.Objects.requireNonNull;

public final class JoinMatcher
        implements Matcher
{
    private final JoinNode.Type joinType;
    private final List<ExpectedValueProvider<JoinNode.EquiJoinClause>> equiCriteria;
    private final Optional<Expression> filter;
    private final Optional<DistributionType> distributionType;
    private final Optional<Boolean> spillable;
    // LEFT_SYMBOL -> RIGHT_SYMBOL
    private final Optional<List<DynamicFilterPattern>> expectedDynamicFilter;

    JoinMatcher(
            JoinNode.Type joinType,
            List<ExpectedValueProvider<JoinNode.EquiJoinClause>> equiCriteria,
            Optional<Expression> filter,
            Optional<DistributionType> distributionType,
            Optional<Boolean> spillable,
            Optional<List<DynamicFilterPattern>> expectedDynamicFilter)
    {
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.equiCriteria = requireNonNull(equiCriteria, "equiCriteria is null");
        this.filter = requireNonNull(filter, "filter cannot be null");
        this.distributionType = requireNonNull(distributionType, "distributionType is null");
        this.spillable = requireNonNull(spillable, "spillable is null");
        this.expectedDynamicFilter = requireNonNull(expectedDynamicFilter, "expectedDynamicFilter is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        if (!(node instanceof JoinNode joinNode)) {
            return false;
        }

        return joinNode.getType() == joinType;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        JoinNode joinNode = (JoinNode) node;

        if (joinNode.getCriteria().size() != equiCriteria.size()) {
            return NO_MATCH;
        }

        if (filter.isPresent()) {
            if (joinNode.getFilter().isEmpty()) {
                return NO_MATCH;
            }
            if (!new ExpressionVerifier(symbolAliases).process(joinNode.getFilter().get(), filter.get())) {
                return NO_MATCH;
            }
        }
        else {
            if (joinNode.getFilter().isPresent()) {
                return NO_MATCH;
            }
        }

        if (distributionType.isPresent() && !distributionType.equals(joinNode.getDistributionType())) {
            return NO_MATCH;
        }

        if (spillable.isPresent() && !spillable.equals(joinNode.isSpillable())) {
            return NO_MATCH;
        }

        /*
         * Have to use order-independent comparison; there are no guarantees what order
         * the equi criteria will have after planning and optimizing.
         */
        Set<JoinNode.EquiJoinClause> actual = ImmutableSet.copyOf(joinNode.getCriteria());
        Set<JoinNode.EquiJoinClause> expected =
                equiCriteria.stream()
                        .map(maker -> maker.getExpectedValue(symbolAliases))
                        .collect(toImmutableSet());

        if (!expected.equals(actual)) {
            return NO_MATCH;
        }

        return new MatchResult(matchDynamicFilters(joinNode, symbolAliases));
    }

    private boolean matchDynamicFilters(JoinNode joinNode, SymbolAliases symbolAliases)
    {
        if (expectedDynamicFilter.isEmpty()) {
            return true;
        }

        Map<DynamicFilterId, Symbol> idToBuildSymbolMap = getJoinDynamicFilters(joinNode);
        Set<DynamicFilterId> dynamicFilterIds = idToBuildSymbolMap.keySet();
        List<DynamicFilters.Descriptor> descriptors = searchFrom(joinNode.getLeft())
                .where(FilterNode.class::isInstance)
                .findAll()
                .stream()
                .flatMap(filterNode -> extractExpressions(filterNode).stream())
                .flatMap(expression -> extractDynamicFilters(expression).getDynamicConjuncts().stream())
                .filter(descriptor -> dynamicFilterIds.contains(descriptor.getId()))
                .collect(toImmutableList());

        Set<Expression> actual = new HashSet<>();
        for (DynamicFilters.Descriptor descriptor : descriptors) {
            Expression probe = descriptor.getInput();
            Symbol build = idToBuildSymbolMap.get(descriptor.getId());
            if (build == null) {
                return false;
            }
            Expression expression;
            if (descriptor.isNullAllowed()) {
                expression = new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, probe, build.toSymbolReference()));
            }
            else {
                expression = new ComparisonExpression(descriptor.getOperator(), probe, build.toSymbolReference());
            }
            actual.add(expression);
        }

        Set<Expression> expected = expectedDynamicFilter.get().stream()
                .map(pattern -> pattern.getExpression(symbolAliases))
                .collect(toImmutableSet());

        return expected.equals(actual);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("type", joinType)
                .add("equiCriteria", equiCriteria)
                .add("filter", filter.orElse(null))
                .add("distributionType", distributionType)
                .add("dynamicFilter", expectedDynamicFilter)
                .toString();
    }

    public static class Builder
    {
        private final JoinNode.Type joinType;
        private Optional<List<ExpectedValueProvider<JoinNode.EquiJoinClause>>> equiCriteria = Optional.empty();
        private Optional<List<PlanMatchPattern.DynamicFilterPattern>> dynamicFilter = Optional.empty();
        private Optional<DistributionType> distributionType = Optional.empty();
        private Optional<Boolean> expectedSpillable = Optional.empty();
        private PlanMatchPattern left;
        private PlanMatchPattern right;
        private Optional<String> filter = Optional.empty();

        public Builder(JoinNode.Type joinType)
        {
            this.joinType = joinType;
        }

        @CanIgnoreReturnValue
        public Builder equiCriteria(List<ExpectedValueProvider<JoinNode.EquiJoinClause>> expectedEquiCriteria)
        {
            this.equiCriteria = Optional.of(expectedEquiCriteria);

            return this;
        }

        @CanIgnoreReturnValue
        public Builder equiCriteria(String left, String right)
        {
            this.equiCriteria = Optional.of(ImmutableList.of(equiJoinClause(left, right)));

            return this;
        }

        @CanIgnoreReturnValue
        public Builder filter(String expectedFilter)
        {
            this.filter = Optional.of(expectedFilter);

            return this;
        }

        @CanIgnoreReturnValue
        public Builder dynamicFilter(Map<String, String> expectedDynamicFilter)
        {
            this.dynamicFilter = Optional.of(expectedDynamicFilter.entrySet().stream()
                    .map(entry -> new PlanMatchPattern.DynamicFilterPattern(entry.getKey(), EQUAL, entry.getValue()))
                    .collect(toImmutableList()));

            return this;
        }

        @CanIgnoreReturnValue
        public Builder dynamicFilter(String key, String value)
        {
            this.dynamicFilter = Optional.of(ImmutableList.of(new PlanMatchPattern.DynamicFilterPattern(key, EQUAL, value)));

            return this;
        }

        @CanIgnoreReturnValue
        public Builder dynamicFilter(List<PlanMatchPattern.DynamicFilterPattern> expectedDynamicFilter)
        {
            this.dynamicFilter = Optional.of(expectedDynamicFilter);

            return this;
        }

        @CanIgnoreReturnValue
        public Builder distributionType(DistributionType expectedDistributionType)
        {
            this.distributionType = Optional.of(expectedDistributionType);

            return this;
        }

        @CanIgnoreReturnValue
        public Builder spillable(Boolean expectedSpillable)
        {
            this.expectedSpillable = Optional.of(expectedSpillable);

            return this;
        }

        @CanIgnoreReturnValue
        public Builder left(PlanMatchPattern left)
        {
            this.left = left;

            return this;
        }

        @CanIgnoreReturnValue
        public Builder right(PlanMatchPattern right)
        {
            this.right = right;

            return this;
        }

        public PlanMatchPattern build()
        {
            return node(JoinNode.class, left, right)
                    .with(
                            new JoinMatcher(
                                    joinType,
                                    equiCriteria.orElse(ImmutableList.of()),
                                    filter.map(PlanBuilder::expression),
                                    distributionType,
                                    expectedSpillable,
                                    dynamicFilter));
        }
    }
}
