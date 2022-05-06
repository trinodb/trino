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
import io.trino.spi.type.Type;

import java.util.Optional;
import java.util.function.Function;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RewriteCast
        implements ConnectorExpressionRule<Call, String>
{
    private static final Capture<ConnectorExpression> ARGUMENT = newCapture();

    private static final Pattern<Call> PATTERN = call()
                .with(functionName().equalTo(CAST_FUNCTION_NAME))
            .with(argument(0).capturedAs(ARGUMENT));

    private final Function<Type, Optional<String>> typeMapping;

    public RewriteCast(Function<Type, Optional<String>> typeMapping)
    {
        this.typeMapping = requireNonNull(typeMapping, "typeMapping is null");
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<String> rewrite(Call expression, Captures captures, RewriteContext<String> context)
    {
        ConnectorExpression argument = captures.get(ARGUMENT);
        Optional<String> typeCast = typeMapping.apply(expression.getType());

        if (typeCast.isEmpty()) {
            return Optional.empty();
        }

        Optional<String> translatedArgument = context.defaultRewrite(argument);
        if (translatedArgument.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(format("CAST(%s AS %s)", translatedArgument.get(), typeCast.get()));
    }
}
