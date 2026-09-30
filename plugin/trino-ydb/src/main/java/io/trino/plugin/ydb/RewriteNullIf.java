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
package io.trino.plugin.ydb;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.StandardFunctions;

import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static java.lang.String.format;

/**
 * Rewrite <code>$nullif(a, b)</code>, as:
 * <br />
 * <code>CASE WHEN a = b THEN NULL ELSE a END</code>.
 */
public class RewriteNullIf
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private final Pattern<Call> pattern;

    public RewriteNullIf()
    {
        this.pattern = call()
                .with(functionName().matching(name -> name.equals(StandardFunctions.NULLIF_FUNCTION_NAME)))
                .with(argumentCount().matching(count -> count == 2));
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return pattern;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call call, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Optional<ParameterizedExpression> left = context.defaultRewrite(call.getArguments().getFirst());
        Optional<ParameterizedExpression> right = context.defaultRewrite(call.getArguments().get(1));
        if (left.isEmpty() || right.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new ParameterizedExpression(
                format("CASE WHEN %s = %s THEN NULL ELSE %s END", left.get().expression(), right.get().expression(), left.get().expression()),
                ImmutableList.<io.trino.plugin.jdbc.QueryParameter>builder()
                        .addAll(left.get().parameters())
                        .addAll(right.get().parameters())
                        .addAll(left.get().parameters())
                        .build()));
    }
}
