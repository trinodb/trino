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
package io.trino.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.trino.Session;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.predicate.TupleDomain;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static io.trino.SystemSessionProperties.isCacheAggregationsEnabled;
import static io.trino.SystemSessionProperties.isCacheCommonSubqueriesEnabled;
import static io.trino.SystemSessionProperties.isCacheProjectionsEnabled;
import static io.trino.cache.CanonicalSubplan.Key;
import static io.trino.cache.CanonicalSubplan.TopNKey;
import static io.trino.cache.CanonicalSubplan.TopNRankingKey;

public class CacheController
{
    /**
     * Logic for cache decision (what to cache, order or caching candidates).
     */
    public List<CacheCandidate> getCachingCandidates(Session session, List<CanonicalSubplan> canonicalSubplans)
    {
        Multimap<SubplanKey, CanonicalSubplan> groupedSubplans = canonicalSubplans.stream()
                .map(subplan -> new SimpleEntry<>(new SubplanKey(subplan), subplan))
                .sorted(Comparator.comparing(entry -> entry.getKey().getPriority()))
                .collect(toImmutableListMultimap(SimpleEntry::getKey, SimpleEntry::getValue));

        List<CacheCandidate> commonSubplans = groupedSubplans.asMap().values().stream()
                .filter(subplans -> subplans.size() > 1)
                // split grouped subplans by intersection of enforced constraints
                .map(this::splitByIntersection)
                .flatMap(Collection::stream)
                // filter out cache candidates which don't share common subplan
                .map(subplans -> new CacheCandidate(ImmutableList.copyOf(subplans), 2))
                .collect(toImmutableList());
        List<CacheCandidate> aggregationSubplans = groupedSubplans.entries().stream()
                .filter(entry -> entry.getKey().aggregation())
                .map(entry -> new CacheCandidate(ImmutableList.of(entry.getValue()), 1))
                .collect(toImmutableList());
        List<CacheCandidate> projectionSubplans = groupedSubplans.entries().stream()
                .filter(entry -> !entry.getKey().aggregation())
                .map(entry -> new CacheCandidate(ImmutableList.of(entry.getValue()), 1))
                .collect(toImmutableList());

        ImmutableList.Builder<CacheCandidate> cacheCandidates = ImmutableList.builder();

        if (isCacheCommonSubqueriesEnabled(session)) {
            cacheCandidates.addAll(commonSubplans);
        }

        if (isCacheAggregationsEnabled(session)) {
            cacheCandidates.addAll(aggregationSubplans);
        }

        if (isCacheProjectionsEnabled(session)) {
            cacheCandidates.addAll(projectionSubplans);
        }

        return cacheCandidates.build();
    }

    private List<List<CanonicalSubplan>> splitByIntersection(Collection<CanonicalSubplan> subplans)
    {
        ImmutableList.Builder<CanonicalSubplan> intersectionBuilder = ImmutableList.builder();
        ImmutableList.Builder<CanonicalSubplan> excludingBuilder = ImmutableList.builder();

        TupleDomain<CacheColumnId> currentConstraint = TupleDomain.all();
        for (CanonicalSubplan subplan : subplans) {
            TupleDomain<CacheColumnId> testConstraint = currentConstraint.intersect(subplan.getEnforcedConstraint());
            if (testConstraint.isNone()) {
                excludingBuilder.add(subplan);
                continue;
            }
            currentConstraint = testConstraint;
            intersectionBuilder.add(subplan);
        }
        List<CanonicalSubplan> excludingSubplans = excludingBuilder.build();
        List<CanonicalSubplan> intersectedSubplans = intersectionBuilder.build();

        ImmutableList.Builder<List<CanonicalSubplan>> intersectedSubplansBuilder = ImmutableList.builder();

        if (intersectedSubplans.size() > 1) {
            intersectedSubplansBuilder.add(intersectedSubplans);
        }
        if (excludingSubplans.size() > 1 && excludingSubplans.size() != subplans.size()) {
            intersectedSubplansBuilder.addAll(splitByIntersection(excludingSubplans));
        }

        return intersectedSubplansBuilder.build();
    }

    record CacheCandidate(List<CanonicalSubplan> subplans, int minSubplans) {}

    record SubplanKey(List<Key> keyChain, boolean aggregation)
    {
        public SubplanKey(CanonicalSubplan subplan)
        {
            this(
                    subplan.getKeyChain(),
                    // TopN and TopNRanking are treated as aggregations because of an assumption of a significant reduction of output rows
                    subplan.getGroupByColumns().isPresent() || subplan.getKey() instanceof TopNKey || subplan.getKey() instanceof TopNRankingKey);
        }

        public int getPriority()
        {
            // prefer deeper plans to be cached first
            return -keyChain.size();
        }
    }
}
