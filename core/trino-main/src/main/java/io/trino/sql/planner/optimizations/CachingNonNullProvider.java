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
package io.trino.sql.planner.optimizations;

import io.trino.Session;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Memo;
import io.trino.sql.planner.plan.PlanNode;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

/// Caches non-null derivation results, mirroring [io.trino.cost.CachingStatsProvider]: results for
/// [GroupReference] nodes are cached per-group in the [Memo] (and invalidated there when a group's
/// membership changes), while results for concrete nodes are cached in a per-instance identity map.
public final class CachingNonNullProvider
        implements NonNullProvider
{
    private final NonNullDerivation derivation;
    private final Optional<Memo> memo;
    private final Session session;

    private final Map<PlanNode, Set<Symbol>> cache = new IdentityHashMap<>();

    public CachingNonNullProvider(NonNullDerivation derivation, Optional<Memo> memo, Session session)
    {
        this.derivation = requireNonNull(derivation, "derivation is null");
        this.memo = requireNonNull(memo, "memo is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public Set<Symbol> getNonNullSymbols(PlanNode node)
    {
        requireNonNull(node, "node is null");

        if (node instanceof GroupReference group) {
            return getGroupNonNullSymbols(group);
        }

        Set<Symbol> nonNull = cache.get(node);
        if (nonNull != null) {
            return nonNull;
        }

        nonNull = derivation.calculate(node, this, session);
        verify(cache.put(node, nonNull) == null, "Non-null symbols already set");
        return nonNull;
    }

    private Set<Symbol> getGroupNonNullSymbols(GroupReference groupReference)
    {
        int group = groupReference.getGroupId();
        Memo memo = this.memo.orElseThrow(() -> new IllegalStateException("CachingNonNullProvider without memo cannot handle GroupReferences"));

        Optional<Set<Symbol>> nonNull = memo.getNonNullSymbols(group);
        if (nonNull.isPresent()) {
            return nonNull.get();
        }

        Set<Symbol> groupNonNull = getNonNullSymbols(memo.getNode(group));
        verify(memo.getNonNullSymbols(group).isEmpty(), "Group non-null symbols already set");
        memo.storeNonNullSymbols(group, groupNonNull);
        return groupNonNull;
    }
}
