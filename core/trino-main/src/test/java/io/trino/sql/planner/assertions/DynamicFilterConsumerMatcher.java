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

import com.google.common.collect.ImmutableSet;
import io.trino.spi.type.Type;
import io.trino.sql.DynamicFilters;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toUnmodifiableSet;

public final class DynamicFilterConsumerMatcher
        implements Matcher
{
    private final Map<DynamicFilterAlias, Set<DynamicFilterConsumer>> expectedConsumers;

    private DynamicFilterConsumerMatcher(List<DynamicFilterConsumer> expectedConsumers)
    {
        this.expectedConsumers = groupByKey(expectedConsumers, consumer -> new DynamicFilterAlias(consumer.alias()));
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        if (!(node instanceof FilterNode filterNode)) {
            return false;
        }
        return expectedConsumers.isEmpty() || !extractDynamicFilters(filterNode.getPredicate()).getDynamicConjuncts().isEmpty();
    }

    @Override
    public MatchResult detailMatches(PlanNode node, MatchContext context)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        FilterNode filterNode = (FilterNode) node;
        Map<DynamicFilterId, Set<DynamicFilters.Descriptor>> actualDynamicFilters = groupByKey(
                extractDynamicFilters(filterNode.getPredicate()).getDynamicConjuncts(),
                DynamicFilters.Descriptor::getId);

        if (actualDynamicFilters.size() != expectedConsumers.size()) {
            return NO_MATCH;
        }

        SymbolMapper symbolMapper = new SymbolMapper(symbol -> Symbol.from(context.symbolAliases().get(symbol.name())));

        MatchingDynamicFilters.Builder matchingBuilder = MatchingDynamicFilters.builder();

        for (Map.Entry<DynamicFilterAlias, Set<DynamicFilterConsumer>> expected : expectedConsumers.entrySet()) {
            Set<DynamicFilterId> candidatesForAlias = actualDynamicFilters.entrySet().stream()
                    .filter(actual -> matches(actual.getValue(), expected.getValue(), symbolMapper))
                    .map(Map.Entry::getKey)
                    .collect(toImmutableSet());

            if (candidatesForAlias.isEmpty()) {
                return NO_MATCH;
            }

            matchingBuilder.add(expected.getKey(), candidatesForAlias);
        }

        MatchingDynamicFilters matching = matchingBuilder.build();

        if (matching.getAllDynamicFilterIds().size() != actualDynamicFilters.size()) {
            return NO_MATCH;
        }

        return match(matching);
    }

    private static boolean matches(Set<DynamicFilters.Descriptor> descriptors, Set<DynamicFilterConsumer> consumers, SymbolMapper symbolMapper)
    {
        if (descriptors.size() != consumers.size()) {
            return false;
        }

        Set<DynamicFilterConsumer> matchedConsumers = new HashSet<>();

        for (DynamicFilters.Descriptor descriptor : descriptors) {
            for (DynamicFilterConsumer consumer : consumers) {
                if (matchedConsumers.contains(consumer)) {
                    continue;
                }
                if (consumer.nullAllowed() != descriptor.isNullAllowed()) {
                    continue;
                }
                if (consumer.operator() != descriptor.getOperator()) {
                    continue;
                }
                if (!symbolMapper.map(consumer.expression()).equals(descriptor.getInput())) {
                    continue;
                }
                matchedConsumers.add(consumer);
                break;
            }
        }

        return matchedConsumers.size() == consumers.size();
    }

    private static <K, V> Map<K, Set<V>> groupByKey(List<V> values, Function<V, K> keyFunction)
    {
        checkArgument(ImmutableSet.copyOf(values).size() == values.size(), "Duplicate values");
        return values.stream()
                .collect(groupingBy(keyFunction, toUnmodifiableSet()));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expectedConsumers", expectedConsumers)
                .toString();
    }

    public static class Builder
    {
        private final PlanMatchPattern source;
        private final List<DynamicFilterConsumer> expectedConsumers = new ArrayList<>();
        private boolean expectNoConsumers;

        public Builder(PlanMatchPattern source)
        {
            this.source = requireNonNull(source, "source is null");
        }

        public Builder addConsumer(Consumer<DynamicFilterConsumer.Builder> consumer)
        {
            DynamicFilterConsumer.Builder builder = new DynamicFilterConsumer.Builder();
            consumer.accept(builder);
            expectedConsumers.add(builder.build());
            return this;
        }

        public Builder noConsumers()
        {
            expectNoConsumers = true;
            return this;
        }

        public PlanMatchPattern build()
        {
            checkState(!expectNoConsumers || expectedConsumers.isEmpty(), "Cannot expect no consumers and have expected consumers");
            return node(FilterNode.class, source)
                    .with(new DynamicFilterConsumerMatcher(expectedConsumers));
        }
    }

    public record DynamicFilterConsumer(
            String alias,
            Comparison.Operator operator,
            Expression expression,
            boolean nullAllowed)
    {
        public DynamicFilterConsumer
        {
            requireNonNull(alias, "alias is null");
            requireNonNull(operator, "operator is null");
            requireNonNull(expression, "expression is null");
        }

        public static class Builder
        {
            private String alias;
            private Comparison.Operator operator = EQUAL;
            private Expression expression;
            private boolean nullAllowed;

            public Builder alias(String alias)
            {
                this.alias = alias;
                return this;
            }

            public Builder operator(Comparison.Operator operator)
            {
                this.operator = operator;
                return this;
            }

            public Builder expression(Expression expression)
            {
                this.expression = expression;
                return this;
            }

            public Builder expression(Type type, String alias)
            {
                this.expression = new Reference(type, alias);
                return this;
            }

            public Builder nullAllowed(boolean nullAllowed)
            {
                this.nullAllowed = nullAllowed;
                return this;
            }

            public DynamicFilterConsumer build()
            {
                return new DynamicFilterConsumer(alias, operator, expression, nullAllowed);
            }
        }
    }
}
