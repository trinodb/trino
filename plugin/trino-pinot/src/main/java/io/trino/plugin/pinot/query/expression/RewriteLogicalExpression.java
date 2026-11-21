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
package io.trino.plugin.pinot.query.expression;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;

/**
 * Implements {@code $and}, {@code $or} and {@code $not} when all of their arguments can be rewritten.
 */
public class RewriteLogicalExpression
        implements ConnectorExpressionRule<Call, String>
{
    private static final Pattern<Call> PATTERN = call()
            .with(functionName().matching(name -> name.equals(AND_FUNCTION_NAME) || name.equals(OR_FUNCTION_NAME) || name.equals(NOT_FUNCTION_NAME)));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<String> rewrite(Call call, Captures captures, RewriteContext<String> context)
    {
        List<String> arguments = new ArrayList<>(call.getArguments().size());
        for (ConnectorExpression argument : call.getArguments()) {
            Optional<String> rewritten = context.defaultRewrite(argument);
            if (rewritten.isEmpty()) {
                return Optional.empty();
            }
            arguments.add(rewritten.get());
        }
        if (call.getFunctionName().equals(NOT_FUNCTION_NAME)) {
            return Optional.of("NOT(%s)".formatted(arguments.getFirst()));
        }
        String separator = call.getFunctionName().equals(AND_FUNCTION_NAME) ? " AND " : " OR ";
        return Optional.of("(" + String.join(separator, arguments) + ")");
    }
}
