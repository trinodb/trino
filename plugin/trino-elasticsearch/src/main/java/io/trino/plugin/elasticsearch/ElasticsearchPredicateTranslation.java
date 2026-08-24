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
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforced;
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.elasticsearch.ElasticsearchRemotePredicateSemantics.effectiveEnforcement;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.APPROXIMATE;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.EXACT;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.PREFILTER;
import static java.util.Objects.requireNonNull;

/**
 * Permanent semantic result of translating a connector-owned predicate subtree.
 *
 * <p>{@code remaining} is predicate state that this planner does not own and may still be offered to the legacy
 * compatibility boundary. {@code residual} is predicate state that this planner does own and Trino remains
 * authoritative for. A residual may exist with or without a remote candidate: partial OR, unproven NOT semantics,
 * intentionally disabled or unproven full-text candidates, and resource-budget fallbacks must not be handed back to a
 * legacy path that could bypass the planner's decision.</p>
 *
 * <p>{@code decision} is the durable semantic decision tree for this translation. Boolean composition preserves child
 * decisions instead of collapsing them to a single final reason. Later diagnostics, metrics and statistics consumers
 * can therefore observe why a predicate was accepted, weakened or rejected without replacing the translation
 * contract.</p>
 */
record ElasticsearchPredicateTranslation<R>(
        Optional<ElasticsearchRemotePredicate> remotePredicate,
        Optional<Enforcement> enforcement,
        Optional<R> remaining,
        Optional<R> residual,
        Reason reason,
        Decision decision)
{
    enum Reason
    {
        NOOP,
        EXACT_DOMAIN,
        EXACT_ARRAY,
        EXACT_LIKE,
        EXACT_REGEXP,
        EXACT_PREFIX,
        FULL_TEXT_DISABLED,
        FULL_TEXT_SAFE_PREFILTER,
        FULL_TEXT_SAFE_UNPROVEN,
        FULL_TEXT_UNSAFE_APPROXIMATE,
        BOOLEAN_AND,
        BOOLEAN_OR,
        BOOLEAN_NOT_UNPROVEN,
        UNSUPPORTED_DOMAIN,
        UNSUPPORTED_EXPRESSION,
    }

    /**
     * Immutable semantic snapshot of one translation node. It intentionally contains no SQL or Elasticsearch payload;
     * those remain owned by the translation and Remote Predicate IR. This makes the tree safe for future diagnostics
     * consumers without creating a second predicate representation.
     */
    record Decision(
            Reason reason,
            Optional<Enforcement> enforcement,
            boolean remotePredicatePresent,
            boolean remainingPresent,
            boolean residualPresent,
            List<Decision> children)
    {
        Decision
        {
            requireNonNull(reason, "reason is null");
            requireNonNull(enforcement, "enforcement is null");
            children = List.copyOf(requireNonNull(children, "children is null"));
            checkArgument(remotePredicatePresent == enforcement.isPresent(), "decision remote predicate and enforcement must be present together");
        }
    }

    ElasticsearchPredicateTranslation(
            Optional<ElasticsearchRemotePredicate> remotePredicate,
            Optional<Enforcement> enforcement,
            Optional<R> remaining,
            Optional<R> residual,
            Reason reason)
    {
        this(remotePredicate,
                enforcement,
                remaining,
                residual,
                reason,
                new Decision(
                        reason,
                        enforcement,
                        remotePredicate.isPresent(),
                        remaining.isPresent(),
                        residual.isPresent(),
                        List.of()));
    }

    ElasticsearchPredicateTranslation
    {
        requireNonNull(remotePredicate, "remotePredicate is null");
        requireNonNull(enforcement, "enforcement is null");
        requireNonNull(remaining, "remaining is null");
        requireNonNull(residual, "residual is null");
        requireNonNull(reason, "reason is null");
        requireNonNull(decision, "decision is null");

        checkArgument(remotePredicate.isPresent() == enforcement.isPresent(), "remote predicate and enforcement must be present together");
        if (enforcement.isPresent()) {
            checkArgument(
                    effectiveEnforcement(remotePredicate.orElseThrow()) == enforcement.orElseThrow(),
                    "declared enforcement does not match Remote Predicate IR enforcement");
        }
        if (enforcement.isPresent() && enforcement.orElseThrow() == EXACT) {
            checkArgument(remaining.isEmpty() && residual.isEmpty(), "EXACT translation cannot have remaining or residual state");
        }
        if (enforcement.isPresent() && enforcement.orElseThrow() == PREFILTER) {
            checkArgument(remaining.isPresent() || residual.isPresent(), "PREFILTER translation requires remaining or residual state");
        }

        checkArgument(decision.reason() == reason, "decision reason does not match translation reason");
        checkArgument(decision.enforcement().equals(enforcement), "decision enforcement does not match translation enforcement");
        checkArgument(decision.remotePredicatePresent() == remotePredicate.isPresent(), "decision remote state does not match translation");
        checkArgument(decision.remainingPresent() == remaining.isPresent(), "decision remaining state does not match translation");
        checkArgument(decision.residualPresent() == residual.isPresent(), "decision residual state does not match translation");
    }

    static <R> ElasticsearchPredicateTranslation<R> exact(ElasticsearchRemotePredicate predicate, Reason reason)
    {
        requireNonNull(predicate, "predicate is null");
        checkArgument(effectiveEnforcement(predicate) == EXACT, "exact predicate has non-exact enforcement");
        return new ElasticsearchPredicateTranslation<>(
                Optional.of(predicate),
                Optional.of(EXACT),
                Optional.empty(),
                Optional.empty(),
                reason);
    }

    static <R> ElasticsearchPredicateTranslation<R> prefilter(ElasticsearchRemotePredicate predicate, R residual, Reason reason)
    {
        ElasticsearchRemotePredicate enforced = enforce(predicate, PREFILTER);
        return new ElasticsearchPredicateTranslation<>(
                Optional.of(enforced),
                Optional.of(PREFILTER),
                Optional.empty(),
                Optional.of(requireNonNull(residual, "residual is null")),
                reason);
    }

    static <R> ElasticsearchPredicateTranslation<R> approximate(ElasticsearchRemotePredicate predicate, Reason reason)
    {
        ElasticsearchRemotePredicate enforced = enforce(predicate, APPROXIMATE);
        return new ElasticsearchPredicateTranslation<>(
                Optional.of(enforced),
                Optional.of(APPROXIMATE),
                Optional.empty(),
                Optional.empty(),
                reason);
    }

    static <R> ElasticsearchPredicateTranslation<R> unsupported(R remaining, Reason reason)
    {
        return new ElasticsearchPredicateTranslation<>(
                Optional.empty(),
                Optional.empty(),
                Optional.of(requireNonNull(remaining, "remaining is null")),
                Optional.empty(),
                reason);
    }

    static <R> ElasticsearchPredicateTranslation<R> residual(R residual, Reason reason)
    {
        return new ElasticsearchPredicateTranslation<>(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(requireNonNull(residual, "residual is null")),
                reason);
    }

    static <R> ElasticsearchPredicateTranslation<R> noop(Reason reason)
    {
        return new ElasticsearchPredicateTranslation<>(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                reason);
    }

    static <R> ElasticsearchPredicateTranslation<R> composed(
            Optional<ElasticsearchRemotePredicate> predicate,
            Optional<Enforcement> enforcement,
            Optional<R> remaining,
            Optional<R> residual,
            Reason reason,
            List<? extends ElasticsearchPredicateTranslation<?>> children)
    {
        requireNonNull(predicate, "predicate is null");
        requireNonNull(enforcement, "enforcement is null");
        requireNonNull(remaining, "remaining is null");
        requireNonNull(residual, "residual is null");
        requireNonNull(children, "children is null");
        checkArgument(predicate.isPresent() == enforcement.isPresent(), "remote predicate and enforcement must be present together");

        Optional<ElasticsearchRemotePredicate> enforcedPredicate = predicate.map(value -> enforce(value, enforcement.orElseThrow()));
        return new ElasticsearchPredicateTranslation<>(
                enforcedPredicate,
                enforcement,
                remaining,
                residual,
                reason,
                new Decision(
                        reason,
                        enforcement,
                        enforcedPredicate.isPresent(),
                        remaining.isPresent(),
                        residual.isPresent(),
                        children.stream()
                                .map(ElasticsearchPredicateTranslation::decision)
                                .toList()));
    }

    private static ElasticsearchRemotePredicate enforce(ElasticsearchRemotePredicate predicate, Enforcement enforcement)
    {
        requireNonNull(predicate, "predicate is null");
        requireNonNull(enforcement, "enforcement is null");

        Enforcement current = effectiveEnforcement(predicate);
        if (current == enforcement) {
            return predicate;
        }
        if (enforcement == EXACT) {
            throw new IllegalArgumentException("Cannot strengthen a non-exact predicate to EXACT");
        }
        if (current == APPROXIMATE) {
            throw new IllegalArgumentException("Cannot weaken an APPROXIMATE predicate");
        }
        return new Enforced(predicate, enforcement);
    }
}
