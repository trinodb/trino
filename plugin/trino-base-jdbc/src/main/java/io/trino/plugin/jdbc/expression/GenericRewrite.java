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
package io.trino.plugin.jdbc.expression;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Matcher.quoteReplacement;

public class GenericRewrite
        implements ConnectorExpressionRule<ConnectorExpression, ParameterizedExpression>
{
    // Matches words in the `rewritePattern`
    private static final Pattern REWRITE_TOKENS = Pattern.compile("(?<![a-zA-Z0-9_$])[a-zA-Z_$][a-zA-Z0-9_$]*(?![a-zA-Z0-9_$])");

    private final ExpressionPattern expressionPattern;
    private final String rewritePattern;

    public GenericRewrite(Map<String, Set<String>> typeClasses, String expressionPattern, String rewritePattern)
    {
        ExpressionMappingParser parser = new ExpressionMappingParser(typeClasses);
        this.expressionPattern = parser.createExpressionPattern(expressionPattern);
        this.rewritePattern = requireNonNull(rewritePattern, "rewritePattern is null");
    }

    @Override
    public io.trino.matching.Pattern<ConnectorExpression> getPattern()
    {
        // TODO make ConnectorExpressionRule.getPattern result type flexible
        //noinspection unchecked
        return (io.trino.matching.Pattern<ConnectorExpression>) expressionPattern.getPattern();
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(ConnectorExpression expression, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        MatchContext matchContext = new MatchContext();
        expressionPattern.resolve(captures, matchContext);

        StringBuilder result = new StringBuilder();
        ImmutableList.Builder<QueryParameter> parameters = ImmutableList.builder();
        Matcher matcher = REWRITE_TOKENS.matcher(rewritePattern);
        while (matcher.find()) {
            String identifier = matcher.group(0);
            Optional<Object> capture = matchContext.getIfPresent(identifier);
            String replacement;
            if (capture.isPresent()) {
                Object value = capture.get();
                if (value instanceof Long) {
                    replacement = Long.toString((Long) value);
                }
                else if (value instanceof ConnectorExpression) {
                    Optional<ParameterizedExpression> rewritten = context.defaultRewrite((ConnectorExpression) value);
                    if (rewritten.isEmpty()) {
                        return Optional.empty();
                    }
                    replacement = format("(%s)", rewritten.get().expression());
                    parameters.addAll(rewritten.get().parameters());
                }
                else {
                    throw new UnsupportedOperationException(format("Unsupported value: %s (%s)", value, value.getClass()));
                }
            }
            else {
                replacement = identifier;
            }
            matcher.appendReplacement(result, quoteReplacement(replacement));
        }
        matcher.appendTail(result);

        return Optional.of(new ParameterizedExpression(result.toString(), parameters.build()));
    }

    @Override
    public String toString()
    {
        return format("%s(%s -> %s)", GenericRewrite.class.getSimpleName(), expressionPattern, rewritePattern);
    }
}
