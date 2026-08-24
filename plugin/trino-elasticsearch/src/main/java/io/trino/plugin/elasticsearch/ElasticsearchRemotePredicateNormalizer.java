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
package io.trino.plugin.elasticsearch;

import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Canonicalizes Remote Predicate IR only when Elasticsearch document semantics are provably unchanged.
 *
 * <p>This is the permanent semantic normalization layer shared by planner composition, legacy canonicalization and
 * dynamic filtering. It flattens associative boolean nodes and removes duplicate predicates. It deliberately does not
 * fuse independent same-field range predicates: Elasticsearch fields are multi-valued at document scope, so separate
 * ranges may be satisfied by different values. Range fusion belongs only inside a semantic scope that has already
 * proved same-value/same-element semantics, such as the {@code any_match} translator.</p>
 *
 * <p>Resource-policy-dependent rewrites, such as compacting an OR of terms into bounded {@code terms} batches, belong
 * to the predicate composer.</p>
 */
final class ElasticsearchRemotePredicateNormalizer
{
    private ElasticsearchRemotePredicateNormalizer() {}

    static Optional<ElasticsearchRemotePredicate> and(List<ElasticsearchRemotePredicate> predicates)
    {
        requireNonNull(predicates, "predicates is null");
        List<ElasticsearchRemotePredicate> flattened = new ArrayList<>();
        for (ElasticsearchRemotePredicate predicate : predicates) {
            addConjunct(flattened, normalize(requireNonNull(predicate, "predicate is null")));
        }
        if (flattened.isEmpty()) {
            return Optional.empty();
        }
        if (flattened.size() == 1) {
            return Optional.of(flattened.getFirst());
        }
        return Optional.of(new ElasticsearchRemotePredicate.And(flattened));
    }

    static Optional<ElasticsearchRemotePredicate> or(List<ElasticsearchRemotePredicate> predicates)
    {
        requireNonNull(predicates, "predicates is null");
        List<ElasticsearchRemotePredicate> flattened = new ArrayList<>();
        for (ElasticsearchRemotePredicate predicate : predicates) {
            addDisjunct(flattened, normalize(requireNonNull(predicate, "predicate is null")));
        }
        if (flattened.isEmpty()) {
            return Optional.empty();
        }
        if (flattened.size() == 1) {
            return Optional.of(flattened.getFirst());
        }
        return Optional.of(new ElasticsearchRemotePredicate.Or(flattened));
    }

    static ElasticsearchRemotePredicate normalize(ElasticsearchRemotePredicate predicate)
    {
        requireNonNull(predicate, "predicate is null");
        return switch (predicate) {
            case ElasticsearchRemotePredicate.And and -> and(and.predicates()).orElseThrow();
            case ElasticsearchRemotePredicate.Or or -> or(or.predicates()).orElseThrow();
            case ElasticsearchRemotePredicate.Not not -> new ElasticsearchRemotePredicate.Not(normalize(not.predicate()));
            case ElasticsearchRemotePredicate.Enforced enforced -> new ElasticsearchRemotePredicate.Enforced(
                    normalize(enforced.predicate()),
                    enforced.enforcement());
            default -> predicate;
        };
    }

    private static void addConjunct(List<ElasticsearchRemotePredicate> conjuncts, ElasticsearchRemotePredicate predicate)
    {
        if (predicate instanceof ElasticsearchRemotePredicate.And and) {
            and.predicates().forEach(child -> addConjunct(conjuncts, child));
            return;
        }
        if (!conjuncts.contains(predicate)) {
            conjuncts.add(predicate);
        }
    }

    private static void addDisjunct(List<ElasticsearchRemotePredicate> disjuncts, ElasticsearchRemotePredicate predicate)
    {
        if (predicate instanceof ElasticsearchRemotePredicate.Or or) {
            or.predicates().forEach(child -> addDisjunct(disjuncts, child));
            return;
        }
        if (!disjuncts.contains(predicate)) {
            disjuncts.add(predicate);
        }
    }
}
