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
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement;

import java.util.List;

import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.APPROXIMATE;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.EXACT;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.PREFILTER;
import static java.util.Objects.requireNonNull;

/**
 * Computes semantic properties that are implicit in the Remote Predicate IR tree.
 *
 * <p>The IR keeps enforcement at the subtree where it was proven. Consumers such as predicate planning,
 * diagnostics and statistics need one stable way to summarize the effective enforcement of a composed tree. Keeping
 * this logic here prevents each later feature from inventing a different interpretation of nested
 * {@link ElasticsearchRemotePredicate.Enforced} nodes.</p>
 */
final class ElasticsearchRemotePredicateSemantics
{
    private ElasticsearchRemotePredicateSemantics() {}

    static Enforcement effectiveEnforcement(ElasticsearchRemotePredicate predicate)
    {
        requireNonNull(predicate, "predicate is null");
        return switch (predicate) {
            case ElasticsearchRemotePredicate.Enforced enforced -> stronger(
                    enforced.enforcement(),
                    effectiveEnforcement(enforced.predicate()));
            case ElasticsearchRemotePredicate.And and -> effectiveEnforcement(and.predicates());
            case ElasticsearchRemotePredicate.Or or -> effectiveEnforcement(or.predicates());
            case ElasticsearchRemotePredicate.Not not -> effectiveEnforcement(not.predicate());
            default -> EXACT;
        };
    }

    private static Enforcement effectiveEnforcement(List<ElasticsearchRemotePredicate> predicates)
    {
        Enforcement result = EXACT;
        for (ElasticsearchRemotePredicate predicate : predicates) {
            result = stronger(result, effectiveEnforcement(predicate));
        }
        return result;
    }

    private static Enforcement stronger(Enforcement left, Enforcement right)
    {
        if (left == APPROXIMATE || right == APPROXIMATE) {
            return APPROXIMATE;
        }
        if (left == PREFILTER || right == PREFILTER) {
            return PREFILTER;
        }
        return EXACT;
    }
}
