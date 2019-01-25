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

package io.prestosql.sql.planner.iterative.trait;

import com.google.common.collect.ImmutableSet;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.iterative.Rule.Result;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.MarkDistinctNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.ValuesNode;

import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.prestosql.sql.planner.iterative.trait.CardinalityTrait.CARDINALITY;
import static io.prestosql.sql.planner.iterative.trait.CardinalityTrait.atMost;
import static io.prestosql.sql.planner.iterative.trait.CardinalityTrait.exactly;
import static io.prestosql.sql.planner.iterative.trait.CardinalityTrait.scalar;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.sql.planner.iterative.rule.SimpleRule.rule;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static io.prestosql.sql.planner.plan.Patterns.enforceSingleRow;
import static io.prestosql.sql.planner.plan.Patterns.exchange;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.limit;
import static io.prestosql.sql.planner.plan.Patterns.markDistinct;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.values;

public final class CardinalityTraitCalculationRuleSet
{
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                getEnforceSingleRowCardinalityCalculationRule(),
                getValuesCardinalityCalculationRule(),
                getAggregationCardinalityCalculationRule(),
                getProjectCardinalityCalculationRule(),
                getMarkDistinctCardinalityCalculationRule(),
                getExchangeCardinalityCalculationRule(),
                getFilterCardinalityCalculationRule(),
                getLimitCardinalityCalculationRule());
    }

    public Rule<ProjectNode> getProjectCardinalityCalculationRule()
    {
        return propagateTrait(project());
    }

    public Rule<EnforceSingleRowNode> getEnforceSingleRowCardinalityCalculationRule()
    {
        return applyTraitRule(enforceSingleRow(), node -> scalar());
    }

    public Rule<ValuesNode> getValuesCardinalityCalculationRule()
    {
        return applyTraitRule(values(), node -> exactly(node.getRows().size()));
    }

    public Rule<AggregationNode> getAggregationCardinalityCalculationRule()
    {
        return calculateCardinalityRule(aggregation(), (node, context) -> {
            if (node.getStep() == SINGLE && node.getGroupingSets().getGroupingSetCount() == 1 && node.hasEmptyGroupingSet()) {
                return Result.setTrait(scalar());
            }
            return Result.empty();
        });
    }

    public Rule<MarkDistinctNode> getMarkDistinctCardinalityCalculationRule()
    {
        return propagateTrait(markDistinct());
    }

    public Rule<ExchangeNode> getExchangeCardinalityCalculationRule()
    {
        return calculateCardinalityRule(exchange(), (node, context) -> {
            if (node.getSources().size() != 1) {
                return Result.empty();
            }
            Optional<CardinalityTrait> sourceCardinality = getCardinality(context, getOnlyElement(node.getSources()));
            return sourceCardinality.map(Result::setTrait)
                    .orElseGet(Result::empty);
        });
    }

    public Rule<FilterNode> getFilterCardinalityCalculationRule()
    {
        return calculateCardinalityRule(filter(), (node, context) -> {
            Optional<CardinalityTrait> sourceCardinality = getCardinality(context, getOnlyElement(node.getSources()));
            return sourceCardinality.map(cardinalityTrait -> Result.setTrait(atMost(cardinalityTrait.getMaxCardinality())))
                    .orElseGet(Result::empty);
        });
    }

    public Rule<LimitNode> getLimitCardinalityCalculationRule()
    {
        // TODO consider source min cardinality, it requires possibility to updated trait with more specific
        return applyTraitRule(limit(), node -> atMost(node.getCount()));
    }

    private <T extends PlanNode> Rule<T> propagateTrait(Pattern<T> pattern)
    {
        return calculateCardinalityRule(pattern, (node, context) -> {
            Optional<CardinalityTrait> sourceCardinality = getCardinality(context, getOnlyElement(node.getSources()));
            return sourceCardinality.map(Result::setTrait)
                    .orElseGet(Result::empty);
        });
    }

    private static <T extends PlanNode> Rule<T> applyTraitRule(Pattern<T> pattern, Function<T, CardinalityTrait> function)
    {
        return calculateCardinalityRule(pattern, (node, context) -> Result.setTrait(function.apply(node)));
    }

    private static <T extends PlanNode> Rule<T> calculateCardinalityRule(Pattern<T> pattern, BiFunction<T, Rule.Context, Result> function)
    {
        return rule(pattern, (node, captures, traitSet, context) -> {
            if (traitSet.getTrait(CARDINALITY).isPresent()) {
                return Result.empty();
            }
            return function.apply(node, context);
        });
    }

    private static Optional<CardinalityTrait> getCardinality(Rule.Context context, PlanNode planNode)
    {
        return context.getLookup().resolveTrait(planNode, CARDINALITY);
    }
}
