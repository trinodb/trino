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
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.PlanNode;

import static java.util.Objects.requireNonNull;

/// Computes child facts recursively from the current plan without retaining them.
public final class RecursiveEffectivePredicateProvider
        implements EffectivePredicateProvider
{
    private final EffectivePredicateExtractor extractor;
    private final Session session;
    private final SymbolAllocator symbolAllocator;
    private final Lookup lookup;

    public RecursiveEffectivePredicateProvider(EffectivePredicateExtractor extractor, Session session, SymbolAllocator symbolAllocator, Lookup lookup)
    {
        this.extractor = requireNonNull(extractor, "extractor is null");
        this.session = requireNonNull(session, "session is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
    }

    @Override
    public Expression getEffectivePredicate(PlanNode node)
    {
        return extractor.extract(session, symbolAllocator, lookup.resolve(node), this);
    }
}
