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

import com.google.common.collect.Iterables;
import io.trino.sql.planner.plan.PlanNode;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;

public interface Lookup
{
    /**
     * Resolves a node by materializing GroupReference nodes
     * representing symbolic references to other nodes. This method
     * is deprecated since is assumes group contains only one node.
     * <p>
     * If the node is not a GroupReference, it returns the
     * argument as is.
     */
    default PlanNode resolve(PlanNode node)
    {
        if (node instanceof GroupReference) {
            return Iterables.getOnlyElement(resolveGroup(node));
        }
        return node;
    }

    /**
     * Resolves nodes by materializing GroupReference nodes
     * representing symbolic references to other nodes.
     *
     * @throws IllegalArgumentException if the node is not a GroupReference
     */
    Iterable<PlanNode> resolveGroup(PlanNode node);

    /**
     * A Lookup implementation that does not perform lookup. It satisfies contract
     * by rejecting {@link GroupReference}-s.
     */
    static Lookup noLookup()
    {
        return node -> {
            throw new UnsupportedOperationException();
        };
    }

    static Lookup from(Function<GroupReference, Iterable<PlanNode>> resolver)
    {
        return node -> {
            checkArgument(node instanceof GroupReference, "Node '%s' is not a GroupReference", node.getClass().getSimpleName());
            return resolver.apply((GroupReference) node);
        };
    }
}
