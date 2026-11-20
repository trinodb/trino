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
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.PlanNode;

import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.plan.Patterns.assignUniqueId;

public final class PushFilterThroughAssignUniqueId
        extends FilterPushdownRule<AssignUniqueId>
{
    public PushFilterThroughAssignUniqueId(PlannerContext plannerContext, boolean dynamicFiltering)
    {
        super(plannerContext, dynamicFiltering, assignUniqueId());
    }

    @Override
    protected PlanNode pushDown(Pushdown pushdown, AssignUniqueId node, Expression inheritedPredicate)
    {
        // The unique id is assigned by this node, so conjuncts that reference it stay above
        return pushdown.pushEligibleConjuncts(node, inheritedPredicate, conjunct -> !extractUnique(conjunct).contains(node.getIdColumn()));
    }
}
