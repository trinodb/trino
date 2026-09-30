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
import io.trino.plugin.base.expression.ConnectorExpressionRule.RewriteContext;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import org.jspecify.annotations.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public final class RewriteUtils
{
    private RewriteUtils() {}

    public static Optional<ParameterizedExpression> rewriteBinaryExpression(
            Call call,
            RewriteContext<ParameterizedExpression> context,
            Supplier<Boolean> condition,
            // leftSql, rightSql -> resultSql
            BiFunction<String, String, String> queryCombiner)
    {
        if (!condition.get()) {
            return Optional.empty();
        }
        List<String> sqls = new ArrayList<>();
        ImmutableList.Builder<@NonNull QueryParameter> parameters = ImmutableList.builder();
        for (ConnectorExpression connectorExpression : call.getArguments()) {
            Optional<ParameterizedExpression> expression = context.defaultRewrite(connectorExpression);
            if (expression.isEmpty()) {
                return Optional.empty();
            }
            parameters.addAll(expression.get().parameters());
            sqls.add(expression.get().expression());
        }
        return Optional.of(new ParameterizedExpression(queryCombiner.apply(sqls.get(0), sqls.get(1)), parameters.build()));
    }
}
