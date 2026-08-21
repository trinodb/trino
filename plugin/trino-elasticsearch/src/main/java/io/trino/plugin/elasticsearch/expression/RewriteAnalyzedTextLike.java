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
package io.trino.plugin.elasticsearch.expression;

import io.airlift.slice.Slice;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.elasticsearch.ElasticsearchColumnHandle;
import io.trino.plugin.elasticsearch.client.IndexMetadata.PrimitiveType;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchExpressionRewrite.QueryType.MATCH_PHRASE;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;

/**
 * Rewrites literal and contains-literal LIKE predicates on analyzed text fields into Elasticsearch full-text phrase
 * predicates. This rule is used only by the UNSAFE pushdown path: Elasticsearch analysis semantics are authoritative,
 * so the translated predicate is not followed by a Trino residual filter.
 *
 * <p>A literal may contain one or many tokens. Whitespace is deliberately not a fallback boundary because
 * {@code match_phrase} analyzes the complete query text and preserves token positions. Patterns with internal SQL
 * wildcards are left for other rules because a single phrase predicate cannot represent their wildcard structure.</p>
 */
final class RewriteAnalyzedTextLike
        implements ConnectorExpressionRule<Call, ElasticsearchExpressionRewrite>
{
    private static final Capture<Variable> LIKE_VALUE = newCapture();
    private static final Capture<Constant> LIKE_PATTERN = newCapture();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(LIKE_FUNCTION_NAME))
            .with(type().equalTo(BOOLEAN))
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(variable()
                    .with(type().matching(VarcharType.class::isInstance))
                    .capturedAs(LIKE_VALUE)))
            .with(argument(1).matching(constant()
                    .with(type().matching(VarcharType.class::isInstance))
                    .capturedAs(LIKE_PATTERN)));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<ElasticsearchExpressionRewrite> rewrite(
            Call expression,
            Captures captures,
            RewriteContext<ElasticsearchExpressionRewrite> context)
    {
        Variable variable = captures.get(LIKE_VALUE);
        Constant pattern = captures.get(LIKE_PATTERN);
        if (!(pattern.getValue() instanceof Slice slice)) {
            return Optional.empty();
        }

        ElasticsearchColumnHandle column = (ElasticsearchColumnHandle) context.getAssignment(variable.getName());
        if (!isAnalyzedTextOnly(column)) {
            return Optional.empty();
        }

        return extractMatchPhraseLiteral(slice.toStringUtf8())
                .map(value -> new ElasticsearchExpressionRewrite(column, MATCH_PHRASE, value));
    }

    private static boolean isAnalyzedTextOnly(ElasticsearchColumnHandle column)
    {
        return !column.supportsPredicates()
                && column.elasticsearchType() instanceof PrimitiveType primitiveType
                && primitiveType.name().equalsIgnoreCase("text")
                && primitiveType.keyword().isEmpty();
    }

    /**
     * Extracts the query text for LIKE shapes that can be approximated by one phrase predicate:
     * {@code 'literal'} and {@code '%literal%'}. The literal can contain multiple tokens. Internal {@code %} or
     * {@code _} wildcards require a different translation rule and are therefore not accepted here.
     */
    static Optional<String> extractMatchPhraseLiteral(String pattern)
    {
        if (pattern.isEmpty()) {
            return Optional.empty();
        }

        if (!containsWildcard(pattern)) {
            return Optional.of(pattern);
        }

        if (pattern.length() > 2 && pattern.charAt(0) == '%' && pattern.charAt(pattern.length() - 1) == '%') {
            String literal = pattern.substring(1, pattern.length() - 1);
            if (!literal.isEmpty() && !containsWildcard(literal)) {
                return Optional.of(literal);
            }
        }

        return Optional.empty();
    }

    private static boolean containsWildcard(String value)
    {
        return value.indexOf('%') >= 0 || value.indexOf('_') >= 0;
    }
}
