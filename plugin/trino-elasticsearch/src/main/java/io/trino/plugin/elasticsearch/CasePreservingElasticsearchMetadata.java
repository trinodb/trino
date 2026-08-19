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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.elasticsearch.ElasticsearchSessionProperties.getFullTextPushdownMode;

/**
 * Exposes Trino-normalized column identifiers while preserving the original Elasticsearch field path in the
 * {@link ElasticsearchColumnHandle}. Trino clients can therefore use the lowercase column names returned by metadata,
 * while every Elasticsearch request continues to target the case-sensitive remote field name stored in the handle.
 */
public class CasePreservingElasticsearchMetadata
        extends ElasticsearchMetadata
{
    private static final FunctionName REGEXP_LIKE_FUNCTION_NAME = new FunctionName("regexp_like");

    @Inject
    public CasePreservingElasticsearchMetadata(TypeManager typeManager, ElasticsearchClient client, ElasticsearchConfig config)
    {
        super(typeManager, client, config);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return normalizeColumnHandles(super.getColumnHandles(session, tableHandle));
    }

    /**
     * Adds aggressive regexp_like pushdown on top of the correctness-oriented base metadata implementation.
     * <p>
     * DISABLED leaves regexp_like to Trino. SAFE only installs a proven full-value pre-filter and retains the Trino
     * residual. UNSAFE accepts Lucene/Joni semantic differences, installs the translated Elasticsearch regexp as the
     * authoritative predicate, and removes the residual expression.
     */
    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> baseResult = super.applyFilter(session, table, constraint);
        FullTextPushdownMode mode = getFullTextPushdownMode(session);
        if (mode == FullTextPushdownMode.DISABLED) {
            return baseResult;
        }

        ElasticsearchTableHandle pushedHandle = (ElasticsearchTableHandle) baseResult
                .map(ConstraintApplicationResult::getHandle)
                .orElse(table);
        ConnectorExpression remainingExpression = baseResult
                .flatMap(ConstraintApplicationResult::getRemainingExpression)
                .orElse(constraint.getExpression());
        TupleDomain<ColumnHandle> remainingFilter = baseResult
                .map(ConstraintApplicationResult::getRemainingFilter)
                .orElse(constraint.getSummary());
        boolean precalculateStatistics = baseResult
                .map(ConstraintApplicationResult::isPrecalculateStatistics)
                .orElse(false);

        Map<String, String> regexes = new HashMap<>(pushedHandle.regexes());
        List<ConnectorExpression> notHandled = new ArrayList<>();
        boolean changed = false;

        for (ConnectorExpression expression : ConnectorExpressions.extractConjuncts(remainingExpression)) {
            Optional<RegexpPushdown> pushdown = regexpPushdown(expression, constraint);
            if (pushdown.isEmpty()) {
                notHandled.add(expression);
                continue;
            }

            RegexpPushdown candidate = pushdown.orElseThrow();
            if (mode == FullTextPushdownMode.SAFE
                    && (!candidate.column().supportsPredicates() || !candidate.translation().quality().safeForPrefilter())) {
                notHandled.add(expression);
                continue;
            }

            String field = candidate.column().predicateName();
            String regexp = candidate.translation().pattern();
            String existingRegexp = regexes.get(field);
            if (existingRegexp != null) {
                // The table handle currently stores one regexp per field. An identical predicate is already pushed;
                // otherwise keep this conjunct in Trino rather than silently replacing the previous condition.
                if (existingRegexp.equals(regexp) && mode == FullTextPushdownMode.UNSAFE) {
                    changed = true;
                    continue;
                }
                notHandled.add(expression);
                continue;
            }
            if (pushedHandle.prefixes().containsKey(field) || pushedHandle.matchPhrasePrefixes().containsKey(field)) {
                notHandled.add(expression);
                continue;
            }

            regexes.put(field, regexp);
            changed = true;
            if (mode == FullTextPushdownMode.SAFE) {
                // SAFE uses Elasticsearch only as a candidate pre-filter. Trino remains authoritative.
                notHandled.add(expression);
            }
        }

        if (!changed) {
            return baseResult;
        }

        ElasticsearchTableHandle newHandle = pushedHandle;
        if (!pushedHandle.regexes().equals(regexes)) {
            newHandle = new ElasticsearchTableHandle(
                    pushedHandle.type(),
                    pushedHandle.schema(),
                    pushedHandle.index(),
                    pushedHandle.constraint(),
                    regexes,
                    pushedHandle.prefixes(),
                    pushedHandle.matchPhrasePrefixes(),
                    pushedHandle.query(),
                    pushedHandle.limit(),
                    pushedHandle.sortOrder(),
                    pushedHandle.columns(),
                    pushedHandle.aggregation());
        }

        return Optional.of(new ConstraintApplicationResult<>(
                newHandle,
                remainingFilter,
                ConnectorExpressions.and(notHandled),
                precalculateStatistics));
    }

    private static Optional<RegexpPushdown> regexpPushdown(ConnectorExpression expression, Constraint constraint)
    {
        if (!(expression instanceof Call call) || !REGEXP_LIKE_FUNCTION_NAME.equals(call.getFunctionName())) {
            return Optional.empty();
        }

        List<ConnectorExpression> arguments = call.getArguments();
        if (arguments.size() != 2
                || !(arguments.get(0) instanceof Variable variable)
                || !(arguments.get(1) instanceof Constant constant)
                || !(constant.getValue() instanceof Slice regexp)) {
            return Optional.empty();
        }

        ElasticsearchColumnHandle column = (ElasticsearchColumnHandle) constraint.getAssignments().get(variable.getName());
        if (column == null || !(column.type() instanceof VarcharType)) {
            return Optional.empty();
        }

        return translateRegexpLike(regexp.toStringUtf8())
                .map(translation -> new RegexpPushdown(column, translation));
    }

    /**
     * Converts Trino regexp_like's substring semantics into Lucene's whole-term regexp semantics and normalizes a
     * deliberately bounded subset of Joni syntax. Unsupported constructs are left in Trino rather than producing an
     * Elasticsearch regexp that can fail the query at runtime.
     */
    static Optional<RegexpTranslation> translateRegexpLike(String source)
    {
        if (source.isEmpty()) {
            return Optional.of(new RegexpTranslation(".*", TranslationQuality.EXACT));
        }

        boolean anchoredStart = source.charAt(0) == '^';
        boolean anchoredEnd = endsWithUnescaped(source, '$');
        int start = anchoredStart ? 1 : 0;
        int end = anchoredEnd ? source.length() - 1 : source.length();
        if (start > end) {
            return Optional.empty();
        }

        String body = source.substring(start, end);
        StringBuilder translated = new StringBuilder();
        boolean approximate = false;
        boolean inCharacterClass = false;
        int groupDepth = 0;

        for (int index = 0; index < body.length(); index++) {
            char current = body.charAt(index);
            if (current == '\\') {
                if (++index >= body.length()) {
                    return Optional.empty();
                }
                char escaped = body.charAt(index);
                switch (escaped) {
                    case 'd' -> {
                        translated.append(inCharacterClass ? "0-9" : "[0-9]");
                        approximate = true;
                    }
                    case 'D' -> {
                        if (inCharacterClass) {
                            return Optional.empty();
                        }
                        translated.append("[^0-9]");
                        approximate = true;
                    }
                    case 'w' -> {
                        translated.append(inCharacterClass ? "A-Za-z0-9_" : "[A-Za-z0-9_]");
                        approximate = true;
                    }
                    case 'W' -> {
                        if (inCharacterClass) {
                            return Optional.empty();
                        }
                        translated.append("[^A-Za-z0-9_]");
                        approximate = true;
                    }
                    // Lucene regexp does not share these Joni/Perl constructs. Do not send an invalid or materially
                    // different expression to Elasticsearch; future translator tiers can add deliberate approximations.
                    case 's', 'S', 'b', 'B', 'p', 'P', 'k', 'Q', 'E', 'A', 'Z', 'z', 'G', 'x', 'u', 't', 'n', 'r', 'f' -> {
                        return Optional.empty();
                    }
                    default -> {
                        if (Character.isDigit(escaped)) {
                            // Backreferences are not part of Lucene regexp syntax.
                            return Optional.empty();
                        }
                        translated.append('\\').append(escaped);
                    }
                }
                continue;
            }

            if (!inCharacterClass && current == '(' && index + 1 < body.length() && body.charAt(index + 1) == '?') {
                if (index + 2 < body.length() && body.charAt(index + 2) == ':') {
                    // Capturing is irrelevant to regexp_like, so a Joni non-capturing group can become a Lucene group.
                    translated.append('(');
                    groupDepth++;
                    index += 2;
                    approximate = true;
                    continue;
                }
                // lookaround, inline flags and other (?...) extensions are not directly representable in Lucene regexp.
                return Optional.empty();
            }

            if (!inCharacterClass && (current == '^' || current == '$')) {
                // Only top-level leading/trailing anchors are normalized. Internal anchors need a real parser.
                return Optional.empty();
            }

            if (current == '[') {
                if (inCharacterClass) {
                    // Nested/intersection character-class syntax needs a real parser before it can be translated safely.
                    return Optional.empty();
                }
                inCharacterClass = true;
                approximate = true;
            }
            else if (current == ']') {
                if (!inCharacterClass) {
                    return Optional.empty();
                }
                inCharacterClass = false;
            }
            else if (!inCharacterClass && current == '(') {
                groupDepth++;
            }
            else if (!inCharacterClass && current == ')') {
                if (groupDepth == 0) {
                    return Optional.empty();
                }
                groupDepth--;
            }

            if (!inCharacterClass && isLuceneOnlyReserved(current)) {
                translated.append('\\');
            }
            translated.append(current);

            if (isRegexpOperator(current)) {
                approximate = true;
            }
        }

        if (inCharacterClass || groupDepth != 0) {
            return Optional.empty();
        }

        String luceneBody = translated.toString();
        StringBuilder lucene = new StringBuilder();
        if (!anchoredStart) {
            lucene.append(".*");
        }
        if (!luceneBody.isEmpty()) {
            lucene.append('(').append(luceneBody).append(')');
        }
        if (!anchoredEnd) {
            lucene.append(".*");
        }
        if (lucene.isEmpty()) {
            return Optional.empty();
        }

        TranslationQuality quality = approximate ? TranslationQuality.APPROXIMATE : TranslationQuality.EXACT;
        return Optional.of(new RegexpTranslation(lucene.toString(), quality));
    }

    private static boolean endsWithUnescaped(String value, char suffix)
    {
        if (value.isEmpty() || value.charAt(value.length() - 1) != suffix) {
            return false;
        }
        int backslashes = 0;
        for (int index = value.length() - 2; index >= 0 && value.charAt(index) == '\\'; index--) {
            backslashes++;
        }
        return backslashes % 2 == 0;
    }

    private static boolean isRegexpOperator(char value)
    {
        return switch (value) {
            case '.', '?', '+', '*', '|', '{', '}', '[', ']', '(', ')' -> true;
            default -> false;
        };
    }

    private static boolean isLuceneOnlyReserved(char value)
    {
        return switch (value) {
            case '"', '#', '@', '&', '<', '>', '~' -> true;
            default -> false;
        };
    }

    static Map<String, ColumnHandle> normalizeColumnHandles(Map<String, ColumnHandle> handles)
    {
        ImmutableMap.Builder<String, ColumnHandle> normalized = ImmutableMap.builder();
        handles.values().stream()
                .map(ElasticsearchColumnHandle.class::cast)
                .forEach(handle -> normalized.put(handle.logicalName(), handle));
        return normalized.buildOrThrow();
    }

    record RegexpTranslation(String pattern, TranslationQuality quality) {}

    private record RegexpPushdown(ElasticsearchColumnHandle column, RegexpTranslation translation) {}

    enum TranslationQuality
    {
        EXACT,
        SUPERSET,
        APPROXIMATE;

        boolean safeForPrefilter()
        {
            return this == EXACT || this == SUPERSET;
        }
    }
}
