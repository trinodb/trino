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

import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.plugin.elasticsearch.ElasticsearchPredicateTranslation.Reason;
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate;
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement;
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Term;
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Terms;
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Value;
import io.trino.spi.expression.ConnectorExpression;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.APPROXIMATE;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.EXACT;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.PREFILTER;
import static io.trino.spi.expression.Constant.TRUE;
import static java.util.Objects.requireNonNull;

/**
 * Permanent document-scope boolean composition layer for Elasticsearch predicate translations.
 *
 * <p>This class deliberately does not descend into array lambdas. Same-element semantics are proved by
 * {@link ElasticsearchArrayPredicateTranslator} before an array predicate reaches this document-scope composer.</p>
 */
final class ElasticsearchPredicateComposer
{
    private ElasticsearchPredicateComposer() {}

    static ElasticsearchPredicateTranslation<ConnectorExpression> and(
            ConnectorExpression source,
            List<ElasticsearchPredicateTranslation<ConnectorExpression>> children)
    {
        return and(source, children, ElasticsearchPredicateCompositionPolicy.DEFAULT);
    }

    static ElasticsearchPredicateTranslation<ConnectorExpression> and(
            ConnectorExpression source,
            List<ElasticsearchPredicateTranslation<ConnectorExpression>> children,
            ElasticsearchPredicateCompositionPolicy policy)
    {
        requireNonNull(source, "source is null");
        requireNonNull(children, "children is null");
        requireNonNull(policy, "policy is null");

        List<ElasticsearchRemotePredicate> remotePredicates = new ArrayList<>();
        List<ConnectorExpression> remaining = new ArrayList<>();
        List<ConnectorExpression> residual = new ArrayList<>();
        boolean approximate = false;

        for (ElasticsearchPredicateTranslation<ConnectorExpression> child : children) {
            requireNonNull(child, "child is null");
            child.remotePredicate().ifPresent(remotePredicates::add);
            child.remaining().ifPresent(remaining::add);
            child.residual().ifPresent(residual::add);
            approximate |= child.enforcement().filter(value -> value == APPROXIMATE).isPresent();
        }

        Optional<ElasticsearchRemotePredicate> remotePredicate = ElasticsearchRemotePredicateNormalizer.and(remotePredicates);
        Optional<ConnectorExpression> remainingExpression = andExpressions(remaining);
        Optional<ConnectorExpression> residualExpression = andExpressions(residual);

        if (remotePredicate.isEmpty()) {
            return ElasticsearchPredicateTranslation.composed(
                    Optional.empty(),
                    Optional.empty(),
                    remainingExpression,
                    residualExpression,
                    Reason.BOOLEAN_AND,
                    children);
        }
        if (!isWithinRequestBudget(remotePredicate.orElseThrow(), policy)) {
            return ElasticsearchPredicateTranslation.composed(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(source),
                    Reason.BOOLEAN_AND,
                    children);
        }

        Enforcement enforcement;
        if (approximate) {
            enforcement = APPROXIMATE;
        }
        else if (remainingExpression.isPresent() || residualExpression.isPresent()) {
            enforcement = PREFILTER;
        }
        else {
            enforcement = EXACT;
        }

        return ElasticsearchPredicateTranslation.composed(
                remotePredicate,
                Optional.of(enforcement),
                remainingExpression,
                residualExpression,
                Reason.BOOLEAN_AND,
                children);
    }

    static ElasticsearchPredicateTranslation<ConnectorExpression> or(
            ConnectorExpression source,
            List<ElasticsearchPredicateTranslation<ConnectorExpression>> children)
    {
        return or(source, children, ElasticsearchPredicateCompositionPolicy.DEFAULT);
    }

    static ElasticsearchPredicateTranslation<ConnectorExpression> or(
            ConnectorExpression source,
            List<ElasticsearchPredicateTranslation<ConnectorExpression>> children,
            ElasticsearchPredicateCompositionPolicy policy)
    {
        requireNonNull(source, "source is null");
        requireNonNull(children, "children is null");
        requireNonNull(policy, "policy is null");

        // Every OR branch needs a no-false-negative remote candidate. Once the composer owns the OR structure, a
        // partial translation is an owned Trino residual, not compatibility-boundary state that legacy code may retry.
        if (children.stream().anyMatch(child -> child.remaining().isPresent() || child.remotePredicate().isEmpty())) {
            return ElasticsearchPredicateTranslation.composed(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(source),
                    Reason.BOOLEAN_OR,
                    children);
        }

        List<ElasticsearchRemotePredicate> remotePredicates = children.stream()
                .map(child -> child.remotePredicate().orElseThrow())
                .toList();
        Optional<List<ElasticsearchRemotePredicate>> compacted = compactExactTerms(remotePredicates, policy);
        if (compacted.isEmpty()) {
            return ElasticsearchPredicateTranslation.composed(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(source),
                    Reason.BOOLEAN_OR,
                    children);
        }

        Optional<ElasticsearchRemotePredicate> remotePredicate = ElasticsearchRemotePredicateNormalizer.or(compacted.orElseThrow());
        if (remotePredicate.isEmpty() || !isWithinRequestBudget(remotePredicate.orElseThrow(), policy)) {
            return ElasticsearchPredicateTranslation.composed(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(source),
                    Reason.BOOLEAN_OR,
                    children);
        }

        boolean approximate = children.stream()
                .anyMatch(child -> child.enforcement().filter(value -> value == APPROXIMATE).isPresent());
        boolean needsResidual = children.stream().anyMatch(child -> child.residual().isPresent());
        Enforcement enforcement = approximate
                ? APPROXIMATE
                : needsResidual ? PREFILTER : EXACT;

        return ElasticsearchPredicateTranslation.composed(
                remotePredicate,
                Optional.of(enforcement),
                Optional.empty(),
                needsResidual ? Optional.of(source) : Optional.empty(),
                Reason.BOOLEAN_OR,
                children);
    }

    static ElasticsearchPredicateTranslation<ConnectorExpression> not(ConnectorExpression source)
    {
        // The composer owns NOT. Until SQL three-valued logic, missing-field behavior and multi-valued-field semantics
        // are proven equivalent for a form, Trino keeps it as an authoritative residual and legacy pushdown is bypassed.
        return ElasticsearchPredicateTranslation.residual(
                requireNonNull(source, "source is null"),
                Reason.BOOLEAN_NOT_UNPROVEN);
    }

    private static Optional<List<ElasticsearchRemotePredicate>> compactExactTerms(
            List<ElasticsearchRemotePredicate> predicates,
            ElasticsearchPredicateCompositionPolicy policy)
    {
        Optional<ElasticsearchRemotePredicate> normalized = ElasticsearchRemotePredicateNormalizer.or(predicates);
        if (normalized.isEmpty()) {
            return Optional.of(List.of());
        }
        List<ElasticsearchRemotePredicate> disjuncts = normalized.orElseThrow() instanceof ElasticsearchRemotePredicate.Or or
                ? or.predicates()
                : List.of(normalized.orElseThrow());

        Map<String, Set<Value>> valuesByField = new LinkedHashMap<>();
        Map<String, Integer> firstIndexByField = new LinkedHashMap<>();
        int totalTermValues = 0;
        for (int index = 0; index < disjuncts.size(); index++) {
            ElasticsearchRemotePredicate predicate = disjuncts.get(index);
            if (predicate instanceof Term term) {
                Set<Value> values = valuesByField.computeIfAbsent(term.field(), _ -> new LinkedHashSet<>());
                if (values.add(term.value())) {
                    totalTermValues++;
                }
                firstIndexByField.putIfAbsent(term.field(), index);
            }
            else if (predicate instanceof Terms terms) {
                Set<Value> values = valuesByField.computeIfAbsent(terms.field(), _ -> new LinkedHashSet<>());
                for (Value value : terms.values()) {
                    if (values.add(value)) {
                        totalTermValues++;
                    }
                }
                firstIndexByField.putIfAbsent(terms.field(), index);
            }
            if (totalTermValues > policy.maxTermsValues()) {
                return Optional.empty();
            }
        }

        List<ElasticsearchRemotePredicate> result = new ArrayList<>();
        Set<String> emittedFields = new LinkedHashSet<>();
        for (int index = 0; index < disjuncts.size(); index++) {
            ElasticsearchRemotePredicate predicate = disjuncts.get(index);
            String field = switch (predicate) {
                case Term term -> term.field();
                case Terms terms -> terms.field();
                default -> null;
            };
            if (field == null) {
                result.add(predicate);
                continue;
            }
            if (index != firstIndexByField.get(field) || !emittedFields.add(field)) {
                continue;
            }

            List<Value> values = List.copyOf(valuesByField.get(field));
            for (int offset = 0; offset < values.size(); offset += policy.termsBatchSize()) {
                int end = Math.min(offset + policy.termsBatchSize(), values.size());
                List<Value> batch = values.subList(offset, end);
                if (batch.size() == 1) {
                    result.add(new Term(field, batch.getFirst()));
                }
                else {
                    result.add(new Terms(field, batch));
                }
            }
        }

        if (result.size() > policy.maxBooleanClauses()) {
            return Optional.empty();
        }
        return Optional.of(List.copyOf(result));
    }

    private static boolean isWithinRequestBudget(
            ElasticsearchRemotePredicate predicate,
            ElasticsearchPredicateCompositionPolicy policy)
    {
        int bytes = ElasticsearchRemotePredicateQueryBuilder.build(predicate)
        .toString()
        .getBytes(StandardCharsets.UTF_8)
        .length;
        return bytes <= policy.maxQueryBytes();
    }

    private static Optional<ConnectorExpression> andExpressions(List<ConnectorExpression> expressions)
    {
        if (expressions.isEmpty()) {
            return Optional.empty();
        }
        ConnectorExpression expression = ConnectorExpressions.and(expressions);
        if (expression.equals(TRUE)) {
            return Optional.empty();
        }
        return Optional.of(expression);
    }
}
