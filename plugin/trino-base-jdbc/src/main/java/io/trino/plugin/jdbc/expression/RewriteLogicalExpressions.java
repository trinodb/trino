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
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.ConnectorLogicalExpression;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.connectorLogicalExpression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.terms;

public class RewriteLogicalExpressions
        implements ConnectorExpressionRule<ConnectorLogicalExpression, String>
{
    private static final Capture<List<? extends ConnectorExpression>> TERMS = newCapture();

    @Override
    public Pattern<ConnectorLogicalExpression> getPattern()
    {
        return connectorLogicalExpression().with(terms().capturedAs(TERMS));
    }

    @Override
    public Optional<String> rewrite(ConnectorLogicalExpression expression, Captures captures, RewriteContext<String> context)
    {
        ImmutableList.Builder<String> terms = ImmutableList.builder();
        for (ConnectorExpression term : captures.get(TERMS)) {
            Optional<String> rewrite = context.defaultRewrite(term);
            if (rewrite.isEmpty()) {
                return Optional.empty();
            }

            terms.add(rewrite.get());
        }

        return Optional.of(terms.build().stream().collect(Collectors.joining(" " + expression.getOperator() + " ", "(", ")")));
    }
}
