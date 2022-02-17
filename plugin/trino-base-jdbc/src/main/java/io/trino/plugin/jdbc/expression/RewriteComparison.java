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
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.ConnectorComparison;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.comparisonLeft;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.comparisonRight;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.connectorComparison;
import static java.lang.String.format;

public class RewriteComparison
        implements ConnectorExpressionRule<ConnectorComparison, String>
{
    private static final Capture<ConnectorExpression> LEFT = newCapture();
    private static final Capture<ConnectorExpression> RIGHT = newCapture();

    @Override
    public Pattern<ConnectorComparison> getPattern()
    {
        return connectorComparison()
                .with(comparisonLeft().capturedAs(LEFT))
                .with(comparisonRight().capturedAs(RIGHT));
    }

    @Override
    public Optional<String> rewrite(ConnectorComparison comparison, Captures captures, ConnectorExpressionRule.RewriteContext<String> context)
    {
        Optional<String> left = context.defaultRewrite(captures.get(LEFT));
        Optional<String> right = context.defaultRewrite(captures.get(RIGHT));
        if (left.isEmpty() || right.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(format("(%s %s %s)", left.get(), comparison.getOperatorSymbol(), right.get()));
    }
}
