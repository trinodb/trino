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
package io.trino.sql.planner;

import io.trino.Session;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.plan.PlanNode;

import java.util.IdentityHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/// Computes facts for an immutable plan tree, remembering the facts of every node it visits, so a
/// node is visited once however many joins above it ask for its facts. Nodes are matched by
/// identity, so the plan must not change while the provider is in use.
public final class MemoizingEffectivePredicateProvider
        implements EffectivePredicateProvider
{
    private final EffectivePredicateExtractor.Extraction extraction;
    private final Map<PlanNode, Expression> predicates = new IdentityHashMap<>();

    public MemoizingEffectivePredicateProvider(EffectivePredicateExtractor extractor, Session session, SymbolAllocator symbolAllocator)
    {
        this.extraction = extractor.bind(requireNonNull(session, "session is null"), requireNonNull(symbolAllocator, "symbolAllocator is null"), this);
    }

    @Override
    public Expression getEffectivePredicate(PlanNode node)
    {
        // Not computeIfAbsent: extracting a node's facts requests the facts of its children
        Expression predicate = predicates.get(node);
        if (predicate == null) {
            predicate = extraction.extract(node);
            predicates.put(node, predicate);
        }
        return predicate;
    }
}
