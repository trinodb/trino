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
package io.trino.plugin.cluster;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionPatterns;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RewriteScalar
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final Pattern<Call> PATTERN = ConnectorExpressionPatterns.call();
    private final Function<ConnectorSession, Set<String>> supportedFunctions;

    public RewriteScalar(Function<ConnectorSession, Set<String>> supportedFunctions)
    {
        this.supportedFunctions = requireNonNull(supportedFunctions, "supportedFunctions is null");
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call call, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        if (call.getFunctionName().getCatalogSchema().isPresent()) {
            return Optional.empty();
        }
        String functionName = call.getFunctionName().getName();
        Set<String> supportedFunctions = this.supportedFunctions.apply(context.getSession());
        if (!supportedFunctions.contains(functionName)) {
            return Optional.empty();
        }
        else {
            List<ConnectorExpression> arguments = call.getArguments();
            List<String> terms = new ArrayList<>(arguments.size());
            ImmutableList.Builder<QueryParameter> parameters = ImmutableList.builder();
            for (ConnectorExpression argument : arguments) {
                Optional<ParameterizedExpression> rewritten = context.defaultRewrite(argument);
                if (rewritten.isEmpty()) {
                    return Optional.empty();
                }
                terms.add(rewritten.get().expression());
                parameters.addAll(rewritten.get().parameters());
            }

            return Optional.of(new ParameterizedExpression(
                    format("%s(%s)", functionName, String.join(", ", terms)),
                    parameters.build()));
        }
    }
}
