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
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;

public class RewriteOr
        implements ConnectorExpressionRule<Call, String>
{
    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(OR_FUNCTION_NAME))
            .with(type().equalTo(BOOLEAN));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<String> rewrite(Call call, Captures captures, RewriteContext<String> context)
    {
        List<ConnectorExpression> arguments = call.getArguments();
        verify(!arguments.isEmpty(), "no arguments");
        List<String> terms = new ArrayList<>(arguments.size());
        for (ConnectorExpression argument : arguments) {
            verify(argument.getType() == BOOLEAN, "Unexpected type of OR argument: %s", argument.getType());
            Optional<String> rewritten = context.defaultRewrite(argument);
            if (rewritten.isEmpty()) {
                return Optional.empty();
            }
            terms.add(rewritten.get());
        }

        return Optional.of(terms.stream()
                .collect(Collectors.joining(") OR (", "(", ")")));
    }
}
