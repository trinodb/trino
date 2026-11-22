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

import io.trino.sql.planner.plan.PlanNode;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public interface Lookup
{
    /**
     * Resolves a GroupReference to the node it represents.
     * Other nodes are returned unchanged.
     */
    PlanNode resolve(PlanNode node);

    /**
     * A Lookup implementation that does not perform lookup. It satisfies contract
     * by rejecting {@link GroupReference}-s.
     */
    static Lookup noLookup()
    {
        return from(_ -> {
            throw new UnsupportedOperationException();
        });
    }

    static Lookup from(Function<GroupReference, PlanNode> resolver)
    {
        requireNonNull(resolver, "resolver is null");
        return node -> {
            if (node instanceof GroupReference groupReference) {
                return resolver.apply(groupReference);
            }
            return node;
        };
    }
}
