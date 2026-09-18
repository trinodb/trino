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
package io.trino.sql.planner.assertions;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.sql.planner.plan.DynamicFilterId;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

/**
 * Maps test-defined aliases to candidate {@link DynamicFilterId}s during plan assertion.
 * <p>
 * <b>Problem:</b> Tests need to verify that a dynamic filter's producer (at Join) and
 * consumer (at Filter) are correctly connected, but {@link DynamicFilterId}s are generated
 * internally and unknown to tests.
 * <p>
 * <b>Solution:</b> Tests use string aliases (e.g., "DF1") to name dynamic filters. During
 * bottom-up plan matching:
 * <ol>
 *   <li>Consumer matcher finds candidate IDs that match the expected pattern</li>
 *   <li>Producer matcher narrows to exactly one ID by verifying the build symbol</li>
 * </ol>
 * If the same alias appears in multiple FilterNodes, candidates from each location are
 * intersected - the alias must resolve to an ID that satisfies all locations.
 */
public final class MatchingDynamicFilters
{
    private final Map<DynamicFilterAlias, Set<DynamicFilterId>> matching;

    public MatchingDynamicFilters()
    {
        this(ImmutableMap.of());
    }

    private MatchingDynamicFilters(Map<DynamicFilterAlias, Set<DynamicFilterId>> matching)
    {
        this.matching = matching.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableSet.copyOf(entry.getValue())));
    }

    public MatchingDynamicFilters merge(MatchingDynamicFilters other)
    {
        return new Builder()
                .addAll(this)
                .addAll(other)
                .build();
    }

    public Set<DynamicFilterId> getAllDynamicFilterIds()
    {
        return matching.values().stream()
                .flatMap(Collection::stream)
                .collect(toImmutableSet());
    }

    public Optional<Set<DynamicFilterId>> getCandidates(DynamicFilterAlias alias)
    {
        return Optional.ofNullable(matching.get(alias));
    }

    public boolean containsUnresolvedAliases()
    {
        // An alias is a pointer to a DynamicFilterId. Since IDs must be unique, aliases are resolved
        // when there is a 1:1 mapping between aliases and IDs.

        Set<DynamicFilterId> usedIds = new HashSet<>();

        for (Map.Entry<DynamicFilterAlias, Set<DynamicFilterId>> entry : matching.entrySet()) {
            if (entry.getValue().size() != 1) {
                return true;
            }

            DynamicFilterId id = getOnlyElement(entry.getValue());
            if (!usedIds.add(id)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public String toString()
    {
        return matching.toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final Map<DynamicFilterAlias, Set<Candidates>> dynamicFilters = new HashMap<>();

        public Builder add(DynamicFilterAlias alias, Set<DynamicFilterId> candidates)
        {
            checkArgument(!candidates.isEmpty(), "Cannot add candidates for alias '%s' with empty candidate ids", alias);
            dynamicFilters.computeIfAbsent(alias, _ -> new HashSet<>()).add(new Candidates(candidates));
            return this;
        }

        public Builder addAll(MatchingDynamicFilters source)
        {
            for (Map.Entry<DynamicFilterAlias, Set<DynamicFilterId>> entry : source.matching.entrySet()) {
                add(entry.getKey(), entry.getValue());
            }
            return this;
        }

        public MatchingDynamicFilters build()
        {
            ImmutableMap.Builder<DynamicFilterAlias, Set<DynamicFilterId>> candidates = ImmutableMap.builder();

            for (Map.Entry<DynamicFilterAlias, Set<Candidates>> entry : dynamicFilters.entrySet()) {
                Set<DynamicFilterId> mergedCandidates = entry.getValue().stream()
                        .map(Candidates::candidates)
                        .reduce((candidates1, candidates2) -> Sets.intersection(candidates1, candidates2).immutableCopy())
                        .orElseThrow();
                candidates.put(entry.getKey(), mergedCandidates);
            }

            return new MatchingDynamicFilters(candidates.buildOrThrow());
        }
    }

    private record Candidates(Set<DynamicFilterId> candidates)
    {
        public Candidates
        {
            requireNonNull(candidates, "candidates is null");
        }
    }
}
