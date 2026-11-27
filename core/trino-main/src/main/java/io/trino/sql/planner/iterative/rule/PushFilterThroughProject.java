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
package io.trino.sql.planner.iterative.rule;

import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.iterative.rule.ProjectionInlining.inlinePredicate;
import static io.trino.sql.planner.iterative.rule.ProjectionInlining.isInliningCandidate;
import static io.trino.sql.planner.plan.Patterns.project;

public final class PushFilterThroughProject
        extends FilterPushdownRule<ProjectNode>
{
    public PushFilterThroughProject(PlannerContext plannerContext, boolean dynamicFiltering)
    {
        super(plannerContext, dynamicFiltering, project());
    }

    @Override
    protected PlanNode pushDown(Pushdown pushdown, ProjectNode node, Expression inheritedPredicate)
    {
        Set<Symbol> deterministicSymbols = node.getAssignments().entrySet().stream()
                .filter(entry -> isDeterministic(entry.getValue()))
                .map(Map.Entry::getKey)
                .collect(toImmutableSet());

        // Push down conjuncts from the inherited predicate that only depend on deterministic assignments and
        // can be inlined without duplicating the evaluation of a complex assignment
        return pushdown.pushEligibleConjuncts(
                node,
                inheritedPredicate,
                conjunct -> deterministicSymbols.containsAll(extractUnique(conjunct)) && isInliningCandidate(conjunct, node),
                conjunct -> inlinePredicate(pushdown.session, pushdown.plannerContext, pushdown.symbolAllocator, node, conjunct));
    }
}
