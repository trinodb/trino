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
package io.prestosql.sql.planner.sanity;

import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanVisitor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.intersection;
import static io.prestosql.sql.DynamicFilters.extractDynamicFilters;

/**
 * When dynamic filter assignments are present on a Join node, they should be consumed by a Filter node on it's probe side
 */
public class DynamicFiltersChecker
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, TypeAnalyzer typeAnalyzer, TypeProvider types, WarningCollector warningCollector)
    {
        plan.accept(new PlanVisitor<Set<String>, Void>()
        {
            @Override
            protected Set<String> visitPlan(PlanNode node, Void context)
            {
                Set<String> consumed = new HashSet<>();
                for (PlanNode source : node.getSources()) {
                    consumed.addAll(source.accept(this, context));
                }
                return consumed;
            }

            @Override
            public Set<String> visitOutput(OutputNode node, Void context)
            {
                Set<String> unmatched = visitPlan(node, context);
                verify(unmatched.isEmpty(), "All consumed dynamic filters could not be matched with a join.");
                return unmatched;
            }

            @Override
            public Set<String> visitJoin(JoinNode node, Void context)
            {
                Set<String> currentJoinDynamicFilters = node.getDynamicFilters().keySet();
                Set<String> consumedProbeSide = node.getLeft().accept(this, context);
                verify(difference(currentJoinDynamicFilters, consumedProbeSide).isEmpty(),
                        "Dynamic filters present in join were not fully consumed by it's probe side.");

                Set<String> consumedBuildSide = node.getRight().accept(this, context);
                verify(intersection(currentJoinDynamicFilters, consumedBuildSide).isEmpty());

                Set<String> unmatched = new HashSet<>(consumedBuildSide);
                unmatched.addAll(consumedProbeSide);
                unmatched.removeAll(currentJoinDynamicFilters);
                return ImmutableSet.copyOf(unmatched);
            }

            @Override
            public Set<String> visitFilter(FilterNode node, Void context)
            {
                ImmutableSet.Builder<String> consumed = ImmutableSet.builder();
                List<DynamicFilters.Descriptor> dynamicFilters = extractDynamicFilters(node.getPredicate()).getDynamicConjuncts();
                dynamicFilters.stream()
                        .map(DynamicFilters.Descriptor::getId)
                        .forEach(consumed::add);
                consumed.addAll(node.getSource().accept(this, context));
                return consumed.build();
            }
        }, null);
    }
}
