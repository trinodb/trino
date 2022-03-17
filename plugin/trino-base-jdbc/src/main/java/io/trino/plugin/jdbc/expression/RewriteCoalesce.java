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
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.spi.expression.StandardFunctions.COALESCE_FUNCTION_NAME;
import static java.lang.String.format;
import static java.lang.String.join;

public class RewriteCoalesce
        implements ConnectorExpressionRule<Call, String>
{
    private static final Capture<Call> CALL = newCapture();
    private final Pattern<Call> pattern;

    public RewriteCoalesce()
    {
        this.pattern = call()
                .with(functionName().matching(name -> name.equals(COALESCE_FUNCTION_NAME)))
                .capturedAs(CALL);
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return pattern;
    }

    @Override
    public Optional<String> rewrite(Call call, Captures captures, RewriteContext<String> context)
    {
        ImmutableList.Builder<String> arguments = ImmutableList.builderWithExpectedSize(call.getArguments().size());
        for (ConnectorExpression expression : captures.get(CALL).getArguments()) {
            Optional<String> rewritten = context.defaultRewrite(expression);
            if (rewritten.isEmpty()) {
                return Optional.empty();
            }
            arguments.add(rewritten.get());
        }

        return Optional.of(format("COALESCE(%s)", join(",", arguments.build())));
    }
}
