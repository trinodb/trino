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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.plugin.elasticsearch.client.IndexMetadata.PrimitiveType;
import io.trino.plugin.elasticsearch.expression.ElasticsearchExpressionRewrite;
import io.trino.plugin.elasticsearch.expression.ElasticsearchExpressionTranslator;
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.trino.plugin.elasticsearch.ElasticsearchRemotePredicateTranslator.conjunction;
import static io.trino.plugin.elasticsearch.ElasticsearchRemotePredicateTranslator.translateDomain;
import static io.trino.plugin.elasticsearch.FullTextPushdownMode.DISABLED;
import static io.trino.plugin.elasticsearch.FullTextPushdownMode.SAFE;
import static io.trino.plugin.elasticsearch.FullTextPushdownMode.UNSAFE;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.APPROXIMATE;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.PREFILTER;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Produces the connector-owned predicate plan used by {@link RuleBasedElasticsearchMetadata}.
 *
 * <p>The planner deliberately separates remote predicates from residual predicates. Exact Elasticsearch predicates
 * are removed from the Trino residual. SAFE full-text predicates are retained as residuals because their Elasticsearch
 * counterpart is only a candidate-reducing prefilter. UNSAFE full-text predicates are authoritative by explicit user
 * choice and are marked APPROXIMATE in the IR.</p>
 */
final class ElasticsearchPredicatePushdownPlanner
{
    private static final FunctionName STARTS_WITH_FUNCTION_NAME = new FunctionName("starts_with");
    private static final FunctionName SUBSTR_FUNCTION_NAME = new FunctionName("substr");
    private static final FunctionName SUBSTRING_FUNCTION_NAME = new FunctionName("substring");
    private static final ElasticsearchExpressionTranslator EXPRESSION_TRANSLATOR = new ElasticsearchExpressionTranslator();

    private ElasticsearchPredicatePushdownPlanner() {}

    public static Result plan(ConnectorSession session, Constraint constraint, FullTextPushdownMode fullTextMode)
    {
        requireNonNull(session, "session is null");
        requireNonNull(constraint, "constraint is null");
        requireNonNull(fullTextMode, "fullTextMode is null");

        Constraint normalizedConstraint = fullTextMode == UNSAFE
                ? removeUnsafeAnalyzedPrefixSyntheticDomains(constraint)
                : constraint;

        List<ElasticsearchRemotePredicate> remotePredicates = new ArrayList<>();
        Map<ColumnHandle, Domain> remainingDomains = new HashMap<>(normalizedConstraint.getSummary().getDomains().orElse(Map.of()));
        Map<ColumnHandle, Domain> residualDomains = new HashMap<>();

        for (Map.Entry<ColumnHandle, Domain> entry : normalizedConstraint.getSummary().getDomains().orElse(Map.of()).entrySet()) {
            ElasticsearchColumnHandle column = (ElasticsearchColumnHandle) entry.getKey();
            Domain domain = entry.getValue();

            boolean exactPredicate = column.supportsPredicates();
            boolean fullTextDiscretePredicate = fullTextMode != DISABLED
                    && isAnalyzedTextOnly(column)
                    && domain.getValues().isDiscreteSet();
            if (!exactPredicate && !fullTextDiscretePredicate) {
                continue;
            }

            Optional<ElasticsearchRemotePredicate> translated = translateDomain(column, domain);
            if (translated.isEmpty()) {
                continue;
            }

            ElasticsearchRemotePredicate remotePredicate = translated.orElseThrow();
            if (fullTextDiscretePredicate) {
                remotePredicate = enforceFullText(remotePredicate, fullTextMode);
            }
            remotePredicates.add(remotePredicate);
            remainingDomains.remove(column);
            if (fullTextDiscretePredicate && fullTextMode == SAFE) {
                residualDomains.put(column, domain);
            }
        }

        Constraint expressionConstraint = new Constraint(
                TupleDomain.withColumnDomains(remainingDomains),
                normalizedConstraint.getExpression(),
                normalizedConstraint.getAssignments());

        List<ConnectorExpression> remainingExpressions = new ArrayList<>();
        List<ConnectorExpression> residualExpressions = new ArrayList<>();

        for (ConnectorExpression expression : ConnectorExpressions.extractConjuncts(expressionConstraint.getExpression())) {
            Optional<ElasticsearchRemotePredicate> arrayPredicate = ElasticsearchArrayPredicateTranslator.translate(expression, expressionConstraint.getAssignments());
            if (arrayPredicate.isPresent()) {
                remotePredicates.add(arrayPredicate.orElseThrow());
                continue;
            }

            Optional<ExpressionPushdown> regexpPredicate = translateRegexp(expression, expressionConstraint.getAssignments(), fullTextMode);
            if (regexpPredicate.isPresent()) {
                addExpressionPushdown(regexpPredicate.orElseThrow(), expression, remotePredicates, residualExpressions);
                continue;
            }

            Optional<ElasticsearchRemotePredicate> prefixPredicate = translateExactPrefixCall(expression, expressionConstraint.getAssignments());
            if (prefixPredicate.isPresent()) {
                remotePredicates.add(prefixPredicate.orElseThrow());
                continue;
            }

            Optional<ExpressionPushdown> likePredicate = translateLike(session, expression, expressionConstraint.getAssignments(), fullTextMode);
            if (likePredicate.isPresent()) {
                addExpressionPushdown(likePredicate.orElseThrow(), expression, remotePredicates, residualExpressions);
                continue;
            }

            remainingExpressions.add(expression);
        }

        Constraint remainingConstraint = new Constraint(
                expressionConstraint.getSummary(),
                ConnectorExpressions.and(remainingExpressions),
                expressionConstraint.getAssignments());
        return new Result(
                remainingConstraint,
                conjunction(remotePredicates),
                TupleDomain.withColumnDomains(residualDomains),
                residualExpressions);
    }

    private static void addExpressionPushdown(
            ExpressionPushdown pushdown,
            ConnectorExpression expression,
            List<ElasticsearchRemotePredicate> remotePredicates,
            List<ConnectorExpression> residualExpressions)
    {
        remotePredicates.add(pushdown.remotePredicate());
        if (pushdown.keepResidual()) {
            residualExpressions.add(expression);
        }
    }

    private static Optional<ExpressionPushdown> translateRegexp(
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            FullTextPushdownMode fullTextMode)
    {
        if (fullTextMode == DISABLED
                || !(expression instanceof Call call)
                || !call.getFunctionName().getName().equals("regexp_like")) {
            return Optional.empty();
        }

        List<ConnectorExpression> arguments = call.getArguments();
        if (arguments.size() != 2
                || !(arguments.get(0) instanceof Variable variable)
                || !(arguments.get(1) instanceof Constant constant)
                || !(constant.getValue() instanceof Slice pattern)) {
            return Optional.empty();
        }

        ColumnHandle assigned = assignments.get(variable.getName());
        if (!(assigned instanceof ElasticsearchColumnHandle column) || !(column.type() instanceof VarcharType)) {
            return Optional.empty();
        }

        return CasePreservingElasticsearchMetadata.translateRegexpLike(pattern.toStringUtf8())
                .flatMap(translation -> {
                    boolean safePrefilter = column.supportsPredicates() && translation.quality().safeForPrefilter();
                    if (fullTextMode != UNSAFE && !safePrefilter) {
                        return Optional.empty();
                    }
                    ElasticsearchRemotePredicate predicate = enforceFullText(
                            new ElasticsearchRemotePredicate.Regexp(column.predicateName(), translation.pattern()),
                            fullTextMode);
                    return Optional.of(new ExpressionPushdown(predicate, fullTextMode == SAFE));
                });
    }

    private static Optional<ExpressionPushdown> translateLike(
            ConnectorSession session,
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            FullTextPushdownMode fullTextMode)
    {
        if (!(expression instanceof Call call) || !ElasticsearchMetadata.isSupportedLikeCall(call)) {
            return Optional.empty();
        }

        List<ConnectorExpression> arguments = call.getArguments();
        Variable variable = (Variable) arguments.get(0);
        ElasticsearchColumnHandle column = (ElasticsearchColumnHandle) assignments.get(variable.getName());
        if (column == null || !(column.type() instanceof VarcharType)) {
            return Optional.empty();
        }

        Object patternValue = ((Constant) arguments.get(1)).getValue();
        if (!(patternValue instanceof Slice pattern)) {
            return Optional.empty();
        }

        Optional<Slice> escape = Optional.empty();
        if (arguments.size() == 3) {
            Object escapeValue = ((Constant) arguments.get(2)).getValue();
            if (!(escapeValue instanceof Slice escapeSlice)) {
                return Optional.empty();
            }
            escape = Optional.of(escapeSlice);
        }

        boolean exactLike = supportsExactLikePushdown(column);
        if (exactLike) {
            Optional<String> prefix = ElasticsearchMetadata.likePrefix(pattern, escape);
            ElasticsearchRemotePredicate predicate = prefix
                    .<ElasticsearchRemotePredicate>map(value -> new ElasticsearchRemotePredicate.Prefix(column.predicateName(), value))
                    .orElse(new ElasticsearchRemotePredicate.Regexp(column.predicateName(), ElasticsearchMetadata.likeToRegexp(pattern, escape)));
            return Optional.of(new ExpressionPushdown(predicate, false));
        }

        if (fullTextMode == DISABLED || !isAnalyzedTextOnly(column)) {
            return Optional.empty();
        }

        if (fullTextMode == UNSAFE) {
            Optional<ElasticsearchExpressionRewrite> rewrite = EXPRESSION_TRANSLATOR.rewrite(session, expression, assignments);
            if (rewrite.isPresent()) {
                ElasticsearchExpressionRewrite translated = rewrite.orElseThrow();
                return switch (translated.queryType()) {
                    case MATCH_PHRASE -> Optional.of(new ExpressionPushdown(
                            enforceFullText(
                                    new ElasticsearchRemotePredicate.MatchPhrase(translated.column().remoteName(), translated.value()),
                                    fullTextMode),
                            false));
                };
            }
        }

        Optional<String> prefix = ElasticsearchMetadata.likePrefix(pattern, escape);
        if (prefix.isPresent()) {
            return Optional.of(new ExpressionPushdown(
                    enforceFullText(
                            new ElasticsearchRemotePredicate.MatchPhrasePrefix(column.remoteName(), prefix.orElseThrow()),
                            fullTextMode),
                    fullTextMode == SAFE));
        }

        if (patternSpansTokens(pattern)) {
            return Optional.empty();
        }

        return Optional.of(new ExpressionPushdown(
                enforceFullText(
                        new ElasticsearchRemotePredicate.Regexp(column.remoteName(), ElasticsearchMetadata.likeToRegexp(pattern, escape)),
                        fullTextMode),
                fullTextMode == SAFE));
    }

    private static ElasticsearchRemotePredicate enforceFullText(
            ElasticsearchRemotePredicate predicate,
            FullTextPushdownMode fullTextMode)
    {
        return new ElasticsearchRemotePredicate.Enforced(
                predicate,
                fullTextMode == SAFE ? PREFILTER : APPROXIMATE);
    }

    private static Optional<ElasticsearchRemotePredicate> translateExactPrefixCall(
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments)
    {
        if (!(expression instanceof Call call)) {
            return Optional.empty();
        }

        List<ConnectorExpression> arguments = call.getArguments();
        if (STARTS_WITH_FUNCTION_NAME.equals(call.getFunctionName())
                && arguments.size() == 2
                && arguments.get(0) instanceof Variable variable
                && arguments.get(1) instanceof Constant constant
                && constant.getValue() instanceof Slice prefix) {
            return exactPrefix(variable, prefix, assignments);
        }

        if (!EQUAL_OPERATOR_FUNCTION_NAME.equals(call.getFunctionName()) || arguments.size() != 2) {
            return Optional.empty();
        }

        for (int index = 0; index < 2; index++) {
            if (arguments.get(index) instanceof Call inner
                    && (SUBSTR_FUNCTION_NAME.equals(inner.getFunctionName()) || SUBSTRING_FUNCTION_NAME.equals(inner.getFunctionName()))
                    && inner.getArguments().size() == 3
                    && inner.getArguments().get(0) instanceof Variable variable
                    && inner.getArguments().get(1) instanceof Constant start
                    && start.getValue() instanceof Long from
                    && from == 1L
                    && inner.getArguments().get(2) instanceof Constant length
                    && length.getValue() instanceof Long count
                    && arguments.get(1 - index) instanceof Constant constant
                    && constant.getValue() instanceof Slice prefix
                    && count == countCodePoints(prefix)) {
                return exactPrefix(variable, prefix, assignments);
            }
        }
        return Optional.empty();
    }

    private static Optional<ElasticsearchRemotePredicate> exactPrefix(
            Variable variable,
            Slice prefix,
            Map<String, ColumnHandle> assignments)
    {
        ColumnHandle assigned = assignments.get(variable.getName());
        if (!(assigned instanceof ElasticsearchColumnHandle column) || !supportsExactLikePushdown(column)) {
            return Optional.empty();
        }
        return Optional.of(new ElasticsearchRemotePredicate.Prefix(column.predicateName(), prefix.toStringUtf8()));
    }

    private static boolean supportsExactLikePushdown(ElasticsearchColumnHandle column)
    {
        return column.elasticsearchType() instanceof PrimitiveType primitiveType
                && (primitiveType.name().toLowerCase(ENGLISH).equals("keyword") || primitiveType.keyword().isPresent());
    }

    private static boolean isAnalyzedTextOnly(ElasticsearchColumnHandle column)
    {
        return column != null
                && !column.supportsPredicates()
                && column.type() instanceof VarcharType
                && column.elasticsearchType() instanceof PrimitiveType primitiveType
                && primitiveType.name().equalsIgnoreCase("text")
                && primitiveType.keyword().isEmpty();
    }

    private static boolean patternSpansTokens(Slice pattern)
    {
        return pattern.toStringUtf8().codePoints().anyMatch(Character::isWhitespace);
    }

    /**
     * DomainTranslator represents LIKE 'prefix%' as [prefix, nextPrefix). For analyzed text this range is not an exact
     * Elasticsearch predicate. In UNSAFE mode the LIKE expression itself is authoritative after translation to
     * match_phrase_prefix, so the synthetic range must not survive as a Trino residual.
     */
    private static Constraint removeUnsafeAnalyzedPrefixSyntheticDomains(Constraint constraint)
    {
        Map<ColumnHandle, Domain> domains = new HashMap<>(constraint.getSummary().getDomains().orElse(Map.of()));
        if (domains.isEmpty()) {
            return constraint;
        }

        List<ConnectorExpression> conjuncts = ConnectorExpressions.extractConjuncts(constraint.getExpression());
        boolean changed = false;
        for (ConnectorExpression expression : conjuncts) {
            if (!(expression instanceof Call call) || !ElasticsearchMetadata.isSupportedLikeCall(call)) {
                continue;
            }

            List<ConnectorExpression> arguments = call.getArguments();
            Variable variable = (Variable) arguments.get(0);
            ColumnHandle assigned = constraint.getAssignments().get(variable.getName());
            if (!(assigned instanceof ElasticsearchColumnHandle column)
                    || !isAnalyzedTextOnly(column)
                    || !(arguments.get(1) instanceof Constant constant)
                    || !(constant.getValue() instanceof Slice pattern)) {
                continue;
            }

            Optional<Slice> escape = Optional.empty();
            if (arguments.size() == 3) {
                Object escapeValue = ((Constant) arguments.get(2)).getValue();
                if (!(escapeValue instanceof Slice escapeSlice)) {
                    continue;
                }
                escape = Optional.of(escapeSlice);
            }

            Optional<String> prefix = ElasticsearchMetadata.likePrefix(pattern, escape);
            if (prefix.isEmpty()) {
                continue;
            }

            long conjunctsOnColumn = conjuncts.stream()
                    .filter(conjunct -> referencesVariable(conjunct, variable.getName()))
                    .count();
            if (conjunctsOnColumn != 1) {
                continue;
            }

            Domain actualDomain = domains.get(column);
            if (actualDomain == null) {
                continue;
            }

            Optional<Domain> expectedDomain = RuleBasedElasticsearchMetadata.createLikePrefixDomain(
                    (VarcharType) column.type(),
                    Slices.utf8Slice(prefix.orElseThrow()));
            if (expectedDomain.isPresent() && actualDomain.equals(expectedDomain.orElseThrow())) {
                domains.remove(column);
                changed = true;
            }
        }

        if (!changed) {
            return constraint;
        }

        return new Constraint(
                TupleDomain.withColumnDomains(domains),
                constraint.getExpression(),
                constraint.getAssignments());
    }

    private static boolean referencesVariable(ConnectorExpression expression, String variableName)
    {
        if (expression instanceof Variable variable) {
            return variable.getName().equals(variableName);
        }
        return expression.getChildren().stream()
                .anyMatch(child -> referencesVariable(child, variableName));
    }

    public record Result(
            Constraint remainingConstraint,
            Optional<ElasticsearchRemotePredicate> remotePredicate,
            TupleDomain<ColumnHandle> residualFilter,
            List<ConnectorExpression> residualExpressions)
    {
        public Result
        {
            requireNonNull(remainingConstraint, "remainingConstraint is null");
            requireNonNull(remotePredicate, "remotePredicate is null");
            requireNonNull(residualFilter, "residualFilter is null");
            residualExpressions = List.copyOf(requireNonNull(residualExpressions, "residualExpressions is null"));
        }
    }

    private record ExpressionPushdown(ElasticsearchRemotePredicate remotePredicate, boolean keepResidual)
    {
        private ExpressionPushdown
        {
            requireNonNull(remotePredicate, "remotePredicate is null");
        }
    }
}
