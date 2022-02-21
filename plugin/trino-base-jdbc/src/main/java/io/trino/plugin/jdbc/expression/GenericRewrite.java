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

import io.trino.matching.Captures;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Matcher.quoteReplacement;

public class GenericRewrite
        implements ConnectorExpressionRule<ConnectorExpression, String>
{
    // Matches words in the `rewritePattern`
    private static final Pattern REWRITE_TOKENS = Pattern.compile("(?<![a-zA-Z0-9_$])[a-zA-Z_$][a-zA-Z0-9_$]*(?![a-zA-Z0-9_$])");

    private final ExpressionPattern expressionPattern;
    private final String rewritePattern;

    public GenericRewrite(String expressionPattern, String rewritePattern)
    {
        ExpressionMappingParser parser = new ExpressionMappingParser();
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
    public Optional<String> rewrite(ConnectorExpression expression, Captures captures, RewriteContext<String> context)
    {
        MatchContext matchContext = new MatchContext();
        expressionPattern.resolve(captures, matchContext);

        StringBuilder rewritten = new StringBuilder();
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
                    Optional<String> rewrittenExpression = context.defaultRewrite((ConnectorExpression) value);
                    if (rewrittenExpression.isEmpty()) {
                        return Optional.empty();
                    }
                    replacement = format("(%s)", rewrittenExpression.get());
                }
                else {
                    throw new UnsupportedOperationException(format("Unsupported value: %s (%s)", value, value.getClass()));
                }
            }
            else {
                replacement = identifier;
            }
            matcher.appendReplacement(rewritten, quoteReplacement(replacement));
        }
        matcher.appendTail(rewritten);

        return Optional.of(rewritten.toString());
    }

    @Override
    public String toString()
    {
        return format("%s(%s -> %s)", GenericRewrite.class.getSimpleName(), expressionPattern, rewritePattern);
    }
}
