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
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.spi.expression.Call.LIKE_PATTERN_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.lang.String.format;

public class RewriteLike
        implements ConnectorExpressionRule<Call, String>
{
    private static final Capture<ConnectorExpression> LIKE_VALUE = newCapture();
    private static final Capture<ConnectorExpression> LIKE_PATTERN = newCapture();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(LIKE_PATTERN_FUNCTION_NAME))
            .with(type().equalTo(BOOLEAN))
            // TODO support ESCAPE. Currently, LIKE with ESCAPE is not pushed down.
            .with(argumentCount().equalTo(2))
            // TODO support LIKE on char(n)
            .with(argument(0).matching(expression().capturedAs(LIKE_VALUE).with(type().matching(VarcharType.class::isInstance))))
            // Currently, LIKE's pattern must be a varchar.
            .with(argument(1).matching(expression().capturedAs(LIKE_PATTERN).with(type().matching(VarcharType.class::isInstance))));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<String> rewrite(Call call, Captures captures, RewriteContext<String> context)
    {
        Optional<String> value = context.defaultRewrite(captures.get(LIKE_VALUE));
        if (value.isEmpty()) {
            return Optional.empty();
        }
        Optional<String> pattern = context.defaultRewrite(captures.get(LIKE_PATTERN));
        if (pattern.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(format("%s LIKE %s", value.get(), pattern.get()));
    }
}
