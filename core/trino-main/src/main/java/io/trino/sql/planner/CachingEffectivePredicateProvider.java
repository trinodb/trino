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
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Memo;
import io.trino.sql.planner.plan.PlanNode;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/// Supplies facts cached in a memo belonging to one optimizer invocation and session.
/// The extraction policy is fixed for the lifetime of the memo.
/// The memo invalidates a replaced group and every ancestor of that group.
public final class CachingEffectivePredicateProvider
        implements EffectivePredicateProvider
{
    private final EffectivePredicateExtractor extractor;
    private final Session session;
    private final SymbolAllocator symbolAllocator;
    private final Memo memo;

    public CachingEffectivePredicateProvider(EffectivePredicateExtractor extractor, Session session, SymbolAllocator symbolAllocator, Memo memo)
    {
        this.extractor = requireNonNull(extractor, "extractor is null");
        this.session = requireNonNull(session, "session is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.memo = requireNonNull(memo, "memo is null");
    }

    @Override
    public Expression getEffectivePredicate(PlanNode node)
    {
        if (node instanceof GroupReference reference) {
            int group = reference.getGroupId();
            Optional<Expression> cached = memo.getEffectivePredicate(group);
            if (cached.isPresent()) {
                return cached.get();
            }
            Expression predicate = extractor.extract(session, symbolAllocator, memo.resolve(reference), this);
            memo.storeEffectivePredicate(group, predicate);
            return predicate;
        }
        return extractor.extract(session, symbolAllocator, node, this);
    }
}
