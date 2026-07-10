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
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.expression.RewriteLikeWithCaseSensitivity;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Optional;

import static java.lang.String.format;

public class ClusterRewriteLikeWithCaseSensitivity
        extends RewriteLikeWithCaseSensitivity
{
    @Override
    public Optional<ParameterizedExpression> rewrite(Call expression, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        ConnectorExpression capturedValue = captures.get(LIKE_VALUE);
        Optional<ParameterizedExpression> value = context.defaultRewrite(capturedValue);
        if (value.isEmpty()) {
            return Optional.empty();
        }

        ImmutableList.Builder<QueryParameter> parameters = ImmutableList.builder();
        parameters.addAll(value.get().parameters());
        Optional<ParameterizedExpression> pattern = context.defaultRewrite(captures.get(LIKE_PATTERN));
        if (pattern.isEmpty()) {
            return Optional.empty();
        }
        parameters.addAll(pattern.get().parameters());
        return Optional.of(new ParameterizedExpression(format("%s LIKE %s", value.get().expression(), pattern.get().expression()), parameters.build()));
    }
}
