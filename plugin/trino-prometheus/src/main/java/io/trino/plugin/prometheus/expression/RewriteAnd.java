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
package io.trino.plugin.prometheus.expression;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;

class RewriteAnd
        implements ConnectorExpressionRule<Call, List<LabelFilterExpression>>
{
    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(AND_FUNCTION_NAME))
            .with(type().equalTo(BOOLEAN));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<List<LabelFilterExpression>> rewrite(Call call, Captures captures, RewriteContext<List<LabelFilterExpression>> context)
    {
        List<ConnectorExpression> arguments = call.getArguments();
        verify(!arguments.isEmpty(), "no arguments");
        ImmutableList.Builder<LabelFilterExpression> terms = ImmutableList.builderWithExpectedSize(arguments.size());
        for (ConnectorExpression argument : arguments) {
            verify(argument.getType() == BOOLEAN, "Unexpected type of argument: %s", argument.getType());
            Optional<List<LabelFilterExpression>> rewritten = context.defaultRewrite(argument);
            if (rewritten.isEmpty()) {
                return Optional.empty();
            }
            terms.addAll(rewritten.get());
        }

        return Optional.of(terms.build());
    }
}
