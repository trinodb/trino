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
package io.trino.plugin.mongodb.expression;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Collections;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.mongodb.expression.MongoExpressions.documentOf;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;

public class RewriteNot
        implements ConnectorExpressionRule<Call, FilterExpression>
{
    private final Pattern<Call> pattern;

    public RewriteNot()
    {
        this.pattern = call()
                .with(functionName().equalTo(NOT_FUNCTION_NAME))
                .with(type().equalTo(BOOLEAN))
                .with(argumentCount().equalTo(1));
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return pattern;
    }

    @Override
    public Optional<FilterExpression> rewrite(Call call, Captures captures, RewriteContext<FilterExpression> context)
    {
        ConnectorExpression argumentValue = getOnlyElement(call.getArguments());

        Optional<FilterExpression> rewritten = context.defaultRewrite(argumentValue);
        if (rewritten.isEmpty()) {
            return Optional.empty();
        }
        Object value = rewritten.get().expression();
        return Optional.of(new FilterExpression(documentOf("$not", Collections.singletonList(value)), FilterExpression.ExpressionType.DOCUMENT));
    }
}
