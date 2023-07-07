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

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.mongodb.expression.ExpressionUtils.documentOf;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

class RewriteLogicalExpression
        implements ConnectorExpressionRule<Call, MongoExpression>
{
    private final Pattern<Call> pattern;
    private final String operator;

    RewriteLogicalExpression(FunctionName functionName, String operator)
    {
        this.pattern = call()
                .with(functionName().equalTo(requireNonNull(functionName, "functionName is null")))
                .with(type().equalTo(BOOLEAN));
        this.operator = requireNonNull(operator, "operator is null");
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return pattern;
    }

    @Override
    public Optional<MongoExpression> rewrite(Call call, Captures captures, RewriteContext<MongoExpression> context)
    {
        List<ConnectorExpression> arguments = call.getArguments();
        verify(!arguments.isEmpty(), "no arguments");
        ImmutableList.Builder<Object> terms = ImmutableList.builder();
        for (ConnectorExpression argument : arguments) {
            verify(argument.getType() == BOOLEAN, "Unexpected type of argument: %s", argument.getType());
            Optional<MongoExpression> rewritten = context.defaultRewrite(argument);
            if (rewritten.isEmpty()) {
                return Optional.empty();
            }
            terms.add(rewritten.get().expression());
        }
        return Optional.of(new MongoExpression(documentOf(operator, terms.build())));
    }
}
