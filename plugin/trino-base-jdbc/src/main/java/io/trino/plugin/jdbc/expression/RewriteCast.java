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

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionPatterns;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.ConnectorCast;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.Type;

import java.util.Optional;
import java.util.function.BiFunction;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.connectorCast;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RewriteCast
        implements ConnectorExpressionRule<ConnectorCast, String>
{
    private static final Capture<ConnectorExpression> EXPRESSION = newCapture();
    private final BiFunction<ConnectorSession, Type, String> typeMapping;

    public RewriteCast(BiFunction<ConnectorSession, Type, String> typeMapping)
    {
        this.typeMapping = requireNonNull(typeMapping, "typeMapping is null");
    }

    @Override
    public Pattern<ConnectorCast> getPattern()
    {
        return connectorCast().with(ConnectorExpressionPatterns.castExpression().capturedAs(EXPRESSION));
    }

    @Override
    public Optional<String> rewrite(ConnectorCast expression, Captures captures, RewriteContext<String> context)
    {
        Optional<String> rewrite = context.defaultRewrite(captures.get(EXPRESSION));
        if (rewrite.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(format("CAST(%s AS %s)", rewrite.get(), typeMapping.apply(context.getSession(), expression.getType())));
    }
}
