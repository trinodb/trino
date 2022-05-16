package io.trino.util;

import io.trino.sql.planner.assertions.ExpectedValueProvider;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.JoinNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;

public class JoinParamUtil
{
    private final JoinNode.Type joinType;
    private final List<ExpectedValueProvider<JoinNode.EquiJoinClause>> expectedEquiCriteria;
    private Optional<String> expectedFilter;
    private Optional<List<PlanMatchPattern.DynamicFilterPattern>> expectedDynamicFilter;
    private Optional<JoinNode.DistributionType> expectedDistributionType;
    private Optional<Boolean> expectedSpillable;
    private final PlanMatchPattern left;
    private final PlanMatchPattern right;

    public JoinNode.Type getJoinType() {
        return joinType;
    }

    public List<ExpectedValueProvider<JoinNode.EquiJoinClause>> getExpectedEquiCriteria() {
        return expectedEquiCriteria;
    }

    public Optional<String> getExpectedFilter() {
        return expectedFilter;
    }

    public Optional<List<PlanMatchPattern.DynamicFilterPattern>> getExpectedDynamicFilter() {
        return expectedDynamicFilter;
    }

    public Optional<JoinNode.DistributionType> getExpectedDistributionType() {
        return expectedDistributionType;
    }

    public Optional<Boolean> getExpectedSpillable() {
        return expectedSpillable;
    }

    public PlanMatchPattern getLeft() {
        return left;
    }

    public PlanMatchPattern getRight() {
        return right;
    }

    public JoinParamUtil(JoinNode.Type joinType, List<ExpectedValueProvider<JoinNode.EquiJoinClause>> expectedEquiCriteria, Optional<String> expectedFilter,
                         Optional<List<PlanMatchPattern.DynamicFilterPattern>> expectedDynamicFilter, Optional<JoinNode.DistributionType> expectedDistributionType,
                         Optional<Boolean> expectedSpillable, PlanMatchPattern left, PlanMatchPattern right) {
        this.joinType = joinType;
        this.expectedEquiCriteria = expectedEquiCriteria;
        this.expectedFilter = expectedFilter;
        this.expectedDynamicFilter = expectedDynamicFilter;
        this.expectedDistributionType = expectedDistributionType;
        this.expectedSpillable = expectedSpillable;
        this.left = left;
        this.right = right;
    }

    public static class JoinParamBuilder
    {
        private final JoinNode.Type joinType;
        private final List<ExpectedValueProvider<JoinNode.EquiJoinClause>> expectedEquiCriteria;
        private Optional<String> expectedFilter;
        private Optional<List<PlanMatchPattern.DynamicFilterPattern>> expectedDynamicFilter;
        private Optional<JoinNode.DistributionType> expectedDistributionType;
        private Optional<Boolean> expectedSpillable;
        private final PlanMatchPattern left;
        private final PlanMatchPattern right;

        public JoinParamBuilder(JoinNode.Type joinType,
                                List<ExpectedValueProvider<JoinNode.EquiJoinClause>> expectedEquiCriteria,
                                PlanMatchPattern left,
                                PlanMatchPattern right) {
            this.joinType = joinType;
            this.expectedEquiCriteria = expectedEquiCriteria;
            this.left = left;
            this.right = right;
            this.expectedFilter = Optional.empty();
            this.expectedDynamicFilter = Optional.empty();
            this.expectedDistributionType = Optional.empty();
            this.expectedSpillable = Optional.empty();
        }

        public JoinParamBuilder expectedFilter(Optional<String> expectedFilter) {
            this.expectedFilter = expectedFilter;
            return this;
        }

        public JoinParamBuilder expectedDynamicFilter(Optional<List<PlanMatchPattern.DynamicFilterPattern>> expectedDynamicFilter) {
            this.expectedDynamicFilter = expectedDynamicFilter;
            return this;
        }

        public JoinParamBuilder expectedDynamicFilter(List<PlanMatchPattern.DynamicFilterPattern> expectedDynamicFilter) {
            this.expectedDynamicFilter = Optional.of(expectedDynamicFilter);
            return this;
        }

        public JoinParamBuilder expectedDynamicFilter(Map<String, String> expectedDynamicFilter) {
            List<PlanMatchPattern.DynamicFilterPattern> pattern = expectedDynamicFilter.entrySet().stream()
                    .map(entry -> new PlanMatchPattern.DynamicFilterPattern(entry.getKey(), EQUAL, entry.getValue()))
                    .collect(toImmutableList());
            this.expectedDynamicFilter = Optional.of(pattern);
            return this;
        }

        public JoinParamBuilder expectedDistributionType(Optional<JoinNode.DistributionType> expectedDistributionType) {
            this.expectedDistributionType = expectedDistributionType;
            return this;
        }

        public JoinParamBuilder expectedSpillable(Optional<Boolean> expectedSpillable) {
            this.expectedSpillable = expectedSpillable;
            return this;
        }

        public JoinParamUtil build() {
            return new JoinParamUtil(this.joinType,
                    this.expectedEquiCriteria,
                    this.expectedFilter,
                    this.expectedDynamicFilter,
                    this.expectedDistributionType,
                    this.expectedSpillable,
                    this.left,
                    this.right);
        }
    }
}
