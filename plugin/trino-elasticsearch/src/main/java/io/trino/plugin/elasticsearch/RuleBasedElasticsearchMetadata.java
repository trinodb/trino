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

import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.plugin.elasticsearch.client.IndexMetadata.PrimitiveType;
import io.trino.plugin.elasticsearch.expression.ElasticsearchExpressionRewrite;
import io.trino.plugin.elasticsearch.expression.ElasticsearchExpressionTranslator;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.SliceUtf8.getCodePointAt;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.SliceUtf8.setCodePointAt;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.elasticsearch.ElasticsearchSessionProperties.getFullTextPushdownMode;
import static io.trino.plugin.elasticsearch.FullTextPushdownMode.UNSAFE;

/**
 * Adds rule-based expression translation before the legacy metadata pushdown implementation.
 *
 * <p>The translator is intentionally separated from table-handle mutation. New SQL expression conversions can be
 * implemented as {@code ConnectorExpressionRule}s without growing {@link ElasticsearchMetadata#applyFilter} into a
 * large function-specific conditional. Rewrites are lowered to the existing Elasticsearch table-handle primitives in
 * this class.</p>
 *
 * <p>The rule layer currently runs only in {@link FullTextPushdownMode#UNSAFE}. A successful translation is therefore
 * authoritative: the translated conjunct is removed from the Trino residual expression. Unsupported expressions stay
 * in the residual. The current MATCH_PHRASE lowering uses a synthetic single-value {@link Domain}; consequently, two
 * independent remote predicates on the same field cannot yet be represented. In that case this class deliberately
 * keeps the original expressions until the table handle has a proper multi-predicate remote IR.</p>
 */
public class RuleBasedElasticsearchMetadata
        extends CasePreservingElasticsearchMetadata
{
    private static final ElasticsearchExpressionTranslator EXPRESSION_TRANSLATOR = new ElasticsearchExpressionTranslator();

    @Inject
    public RuleBasedElasticsearchMetadata(TypeManager typeManager, ElasticsearchClient client, ElasticsearchConfig config)
    {
        super(typeManager, client, config);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint constraint)
    {
        if (getFullTextPushdownMode(session) != UNSAFE) {
            return super.applyFilter(session, table, constraint);
        }

        return super.applyFilter(session, table, rewriteUnsafeFullTextConstraint(session, constraint));
    }

    static Constraint rewriteUnsafeFullTextConstraint(ConnectorSession session, Constraint constraint)
    {
        if (constraint.getSummary().isNone()) {
            return constraint;
        }

        Constraint rewrittenConstraint = removeSyntheticPrefixLikeDomains(constraint);

        List<ConnectorExpression> conjuncts = ConnectorExpressions.extractConjuncts(rewrittenConstraint.getExpression());
        List<Optional<ElasticsearchExpressionRewrite>> translations = conjuncts.stream()
                .map(expression -> EXPRESSION_TRANSLATOR.rewrite(session, expression, rewrittenConstraint.getAssignments()))
                .toList();

        Map<ColumnHandle, Domain> originalDomains = rewrittenConstraint.getSummary().getDomains().orElse(Map.of());
        Map<ElasticsearchColumnHandle, Integer> translationsPerColumn = new HashMap<>();
        translations.stream()
                .flatMap(Optional::stream)
                .forEach(rewrite -> translationsPerColumn.merge(rewrite.column(), 1, Integer::sum));

        Map<ColumnHandle, Domain> translatedDomains = new HashMap<>();
        List<ConnectorExpression> remainingExpressions = new ArrayList<>();

        for (int index = 0; index < conjuncts.size(); index++) {
            ConnectorExpression expression = conjuncts.get(index);
            Optional<ElasticsearchExpressionRewrite> translation = translations.get(index);
            if (translation.isEmpty()) {
                remainingExpressions.add(expression);
                continue;
            }

            ElasticsearchExpressionRewrite rewrite = translation.orElseThrow();
            ElasticsearchColumnHandle column = rewrite.column();

            // TupleDomain can hold only one domain per column. Do not encode one translated full-text conjunct when
            // another predicate already targets the same column, because that would corrupt A AND B into a single
            // equality-shaped domain. This is a representation limitation, not a request for exact SQL semantics.
            if (originalDomains.containsKey(column) || translationsPerColumn.getOrDefault(column, 0) != 1) {
                remainingExpressions.add(expression);
                continue;
            }

            switch (rewrite.queryType()) {
                case MATCH_PHRASE -> translatedDomains.put(
                        column,
                        Domain.singleValue(column.type(), utf8Slice(rewrite.value())));
            }
        }

        if (translatedDomains.isEmpty()) {
            return rewrittenConstraint;
        }

        TupleDomain<ColumnHandle> translatedSummary = rewrittenConstraint.getSummary()
                .intersect(TupleDomain.withColumnDomains(translatedDomains));
        return new Constraint(
                translatedSummary,
                ConnectorExpressions.and(remainingExpressions),
                rewrittenConstraint.getAssignments());
    }

    /**
     * DomainTranslator represents {@code LIKE 'prefix%'} as the synthetic range {@code [prefix, nextPrefix)}. The
     * legacy Elasticsearch pushdown recognizes the LIKE expression independently and, in UNSAFE mode, replaces it
     * with {@code match_phrase_prefix}. Leaving the synthetic range in the remaining TupleDomain would therefore add
     * a redundant Trino FilterNode and prevent full pushdown.
     *
     * <p>Only remove a domain when it exactly matches the range DomainTranslator generates for this prefix. Also keep
     * it when another visible connector-expression conjunct references the same column. TupleDomain does not retain
     * predicate provenance, so a predicate that the optimizer has completely absorbed into the same range cannot be
     * distinguished here. Full provenance would require an engine-level representation rather than a connector-only
     * heuristic.</p>
     */
    private static Constraint removeSyntheticPrefixLikeDomains(Constraint constraint)
    {
        Map<ColumnHandle, Domain> domains = new HashMap<>(constraint.getSummary().getDomains().orElse(Map.of()));
        if (domains.isEmpty()) {
            return constraint;
        }

        List<ConnectorExpression> conjuncts = ConnectorExpressions.extractConjuncts(constraint.getExpression());
        boolean changed = false;
        for (ConnectorExpression expression : conjuncts) {
            if (!(expression instanceof Call call) || !isSupportedLikeCall(call)) {
                continue;
            }

            List<ConnectorExpression> arguments = call.getArguments();
            Variable variable = (Variable) arguments.get(0);
            ElasticsearchColumnHandle column = (ElasticsearchColumnHandle) constraint.getAssignments().get(variable.getName());
            if (!isAnalyzedTextOnly(column)
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

            Optional<String> prefix = likePrefix(pattern, escape);
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

            Optional<Domain> expectedDomain = createLikePrefixDomain(
                    (VarcharType) column.type(),
                    utf8Slice(prefix.orElseThrow()));
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

    private static boolean isAnalyzedTextOnly(ElasticsearchColumnHandle column)
    {
        return column != null
                && !column.supportsPredicates()
                && column.type() instanceof VarcharType
                && column.elasticsearchType() instanceof PrimitiveType primitiveType
                && primitiveType.name().equalsIgnoreCase("text")
                && primitiveType.keyword().isEmpty();
    }

    private static boolean referencesVariable(ConnectorExpression expression, String variableName)
    {
        if (expression instanceof Variable variable) {
            return variable.getName().equals(variableName);
        }
        return expression.getChildren().stream()
                .anyMatch(child -> referencesVariable(child, variableName));
    }

    static Optional<Domain> createLikePrefixDomain(VarcharType type, Slice prefix)
    {
        // Keep this byte-for-byte compatible in semantics with DomainTranslator#createRangeDomain. In particular,
        // DomainTranslator increments only ASCII code points so the UTF-8 byte length cannot change.
        int lastIncrementable = -1;
        for (int position = 0; position < prefix.length(); position += lengthOfCodePoint(prefix, position)) {
            if (getCodePointAt(prefix, position) < 127) {
                lastIncrementable = position;
            }
        }

        if (lastIncrementable == -1) {
            return Optional.empty();
        }

        Slice lowerBound = prefix;
        Slice upperBound = prefix.slice(
                        0,
                        lastIncrementable + lengthOfCodePoint(prefix, lastIncrementable))
                .copy();
        setCodePointAt(getCodePointAt(prefix, lastIncrementable) + 1, upperBound, lastIncrementable);

        return Optional.of(Domain.create(
                ValueSet.ofRanges(Range.range(type, lowerBound, true, upperBound, false)),
                false));
    }
}
