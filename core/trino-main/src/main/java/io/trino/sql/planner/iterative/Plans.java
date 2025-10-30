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
package io.trino.sql.planner.iterative;

import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;

import static java.util.Objects.requireNonNull;

public final class Plans
{
    public static PlanNode resolveGroupReferences(PlanNode node, Lookup lookup)
    {
        requireNonNull(node, "node is null");
        return node.accept(new ResolvingVisitor(lookup), null);
    }

    private static class ResolvingVisitor
            extends PlanVisitor<PlanNode, Void>
    {
        private final Lookup lookup;

        public ResolvingVisitor(Lookup lookup)
        {
            this.lookup = requireNonNull(lookup, "lookup is null");
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, Void context)
        {
            ImmutableList.Builder<PlanNode> children = ImmutableList.builderWithExpectedSize(node.getSources().size());
            for (PlanNode source : node.getSources()) {
                children.add(source.accept(this, context));
            }
            return node.replaceChildren(children.build());
        }

        @Override
        public PlanNode visitGroupReference(GroupReference node, Void context)
        {
            return lookup.resolve(node).accept(this, context);
        }
    }

    private Plans() {}
}
