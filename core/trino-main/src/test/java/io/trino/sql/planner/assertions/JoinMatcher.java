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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.DistributionType;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.operator.join.JoinUtils.getJoinDynamicFilters;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;
import static io.trino.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static java.util.Objects.requireNonNull;

public final class JoinMatcher
        implements Matcher
{
    private final JoinType joinType;
    private final List<ExpectedValueProvider<JoinNode.EquiJoinClause>> equiCriteria;
    private final boolean ignoreEquiCriteria;
    private final Optional<Expression> filter;
    private final Optional<DistributionType> distributionType;
    private final Optional<Boolean> spillable;
    private final Optional<Boolean> maySkipOutputDuplicates;
    private final Map<DynamicFilterAlias, SymbolAlias> expectedDynamicFilters;

    JoinMatcher(
            JoinType joinType,
            List<ExpectedValueProvider<JoinNode.EquiJoinClause>> equiCriteria,
            boolean ignoreEquiCriteria,
            Optional<Expression> filter,
            Optional<DistributionType> distributionType,
            Optional<Boolean> spillable,
            Optional<Boolean> maySkipOutputDuplicates,
            Map<DynamicFilterAlias, SymbolAlias> expectedDynamicFilters)
    {
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.equiCriteria = requireNonNull(equiCriteria, "equiCriteria is null");
        if (ignoreEquiCriteria && !equiCriteria.isEmpty()) {
            throw new IllegalArgumentException("ignoreEquiCriteria passed with non-empty equiCriteria");
        }
        this.ignoreEquiCriteria = ignoreEquiCriteria;
        this.filter = requireNonNull(filter, "filter cannot be null");
        this.distributionType = requireNonNull(distributionType, "distributionType is null");
        this.spillable = requireNonNull(spillable, "spillable is null");
        this.maySkipOutputDuplicates = requireNonNull(maySkipOutputDuplicates, "MaySkipOutputDuplicates is null");
        this.expectedDynamicFilters = requireNonNull(expectedDynamicFilters, "expectedDynamicFilters is null");
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
    public MatchResult detailMatches(PlanNode node, MatchContext context)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        JoinNode joinNode = (JoinNode) node;

        if (!ignoreEquiCriteria && joinNode.getCriteria().size() != equiCriteria.size()) {
            return NO_MATCH;
        }

        if (filter.isPresent()) {
            if (joinNode.getFilter().isEmpty()) {
                return NO_MATCH;
            }
            if (!new ExpressionVerifier(context.symbolAliases()).process(joinNode.getFilter().get(), filter.get())) {
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

        if (maySkipOutputDuplicates.isPresent() && maySkipOutputDuplicates.get() != joinNode.isMaySkipOutputDuplicates()) {
            return NO_MATCH;
        }

        if (!ignoreEquiCriteria) {
            /*
             * Have to use order-independent comparison; there are no guarantees what order
             * the equi criteria will have after planning and optimizing.
             */
            Set<JoinNode.EquiJoinClause> actual = ImmutableSet.copyOf(joinNode.getCriteria());
            Set<JoinNode.EquiJoinClause> expected =
                    equiCriteria.stream()
                            .map(maker -> maker.getExpectedValue(context.symbolAliases()))
                            .collect(toImmutableSet());

            if (!expected.equals(actual)) {
                return NO_MATCH;
            }
        }

        return matchDynamicFilters(joinNode, context);
    }

    /**
     * Resolves dynamic filter candidates to exactly one {@link DynamicFilterId} per alias.
     * Candidates are collected from source subtrees by {@link DynamicFilterConsumerMatcher} -
     * multiple candidates exist when several DFs have identical consumer properties.
     * This method narrows them by verifying the build symbol matches.
     */
    private MatchResult matchDynamicFilters(JoinNode joinNode, MatchContext context)
    {
        if (expectedDynamicFilters.isEmpty()) {
            return match();
        }

        Map<DynamicFilterId, Symbol> joinProducedFilters = getJoinDynamicFilters(joinNode);

        if (expectedDynamicFilters.size() != joinProducedFilters.size()) {
            return NO_MATCH;
        }

        MatchingDynamicFilters.Builder matchingBuilder = MatchingDynamicFilters.builder();

        for (Map.Entry<DynamicFilterAlias, SymbolAlias> expectedProducer : expectedDynamicFilters.entrySet()) {
            DynamicFilterAlias alias = expectedProducer.getKey();
            Symbol expectedBuildSymbol = expectedProducer.getValue().toSymbol(context.symbolAliases());

            Optional<Set<DynamicFilterId>> candidates = context.dynamicFilters().getCandidates(alias);
            if (candidates.isEmpty()) {
                return NO_MATCH;
            }

            Set<DynamicFilterId> matching = findMatching(candidates.get(), joinProducedFilters, expectedBuildSymbol);
            if (matching.size() != 1) {
                return NO_MATCH;
            }

            matchingBuilder.add(alias, matching);
        }

        return match(matchingBuilder.build());
    }

    private static Set<DynamicFilterId> findMatching(
            Set<DynamicFilterId> candidates,
            Map<DynamicFilterId, Symbol> joinProducedFilters,
            Symbol expectedBuildSymbol)
    {
        return candidates.stream()
                .filter(candidate -> {
                    Symbol actualBuildSymbol = joinProducedFilters.get(candidate);
                    return expectedBuildSymbol.equals(actualBuildSymbol);
                })
                .collect(toImmutableSet());
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
                .add("expectedDynamicFilters", expectedDynamicFilters)
                .toString();
    }

    public static class Builder
    {
        private final JoinType joinType;
        private Optional<List<ExpectedValueProvider<JoinNode.EquiJoinClause>>> equiCriteria = Optional.empty();
        private ImmutableMap.Builder<DynamicFilterAlias, SymbolAlias> dynamicFilters = ImmutableMap.builder();
        private Optional<DistributionType> distributionType = Optional.empty();
        private Optional<Boolean> expectedSpillable = Optional.empty();
        private Optional<Boolean> expectedMaySkipOutputDuplicates = Optional.empty();
        private PlanMatchPattern left;
        private PlanMatchPattern right;
        private Optional<Expression> filter = Optional.empty();
        private boolean ignoreEquiCriteria;

        public Builder(JoinType joinType)
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
        public Builder filter(Expression expectedFilter)
        {
            this.filter = Optional.of(expectedFilter);

            return this;
        }

        @CanIgnoreReturnValue
        public Builder addDynamicFilter(String alias, String buildAlias)
        {
            dynamicFilters.put(new DynamicFilterAlias(alias), new SymbolAlias(buildAlias));

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
        public Builder maySkipOutputDuplicates(Boolean expectedMaySkipOutputDuplicates)
        {
            this.expectedMaySkipOutputDuplicates = Optional.of(expectedMaySkipOutputDuplicates);

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

        public Builder ignoreEquiCriteria()
        {
            this.ignoreEquiCriteria = true;
            return this;
        }

        public PlanMatchPattern build()
        {
            return node(JoinNode.class, left, right)
                    .with(
                            new JoinMatcher(
                                    joinType,
                                    equiCriteria.orElse(ImmutableList.of()),
                                    ignoreEquiCriteria,
                                    filter,
                                    distributionType,
                                    expectedSpillable,
                                    expectedMaySkipOutputDuplicates,
                                    dynamicFilters.buildOrThrow()));
        }
    }
}
