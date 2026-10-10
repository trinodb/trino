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
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.projection.ProjectFunctionRule;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.BigintType;
import org.jspecify.annotations.NonNull;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static java.lang.String.format;

/**
 * Rewrite <code>strpos(str, sub)</code>, as:
 * <br />
 * <code>Unicode::Find(str, sub) + 1</code>, with SQL null and missing-substring handling.
 * <br /><br />
 * YQL positions are zero-based; Trino positions are one-based Unicode code points.
 */
public class RewriteStringPosition
        implements ProjectFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private static final Capture<ConnectorExpression> STRING = newCapture();
    private static final Capture<ConnectorExpression> SUBSTRING = newCapture();
    private static final String STRPOS_OPERATOR = "strpos";

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().matching(name -> STRPOS_OPERATOR.equals(name.getName())))
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(expression().capturedAs(STRING)))
            .with(argument(1).matching(expression().capturedAs(SUBSTRING)));

    @Override
    public Pattern<? extends ConnectorExpression> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<JdbcExpression> rewrite(
            ConnectorTableHandle handle,
            ConnectorExpression projectionExpression,
            Captures captures,
            RewriteContext<ParameterizedExpression> context)
    {
        ConnectorExpression stringExpr = captures.get(STRING);
        ConnectorExpression substringExpr = captures.get(SUBSTRING);

        Optional<ParameterizedExpression> rewrittenString = context.rewriteExpression(stringExpr);
        Optional<ParameterizedExpression> rewrittenSubstring = context.rewriteExpression(substringExpr);

        if (rewrittenString.isEmpty() || rewrittenSubstring.isEmpty()) {
            return Optional.empty();
        }

        String strSql = rewrittenString.get().expression();
        String subSql = rewrittenSubstring.get().expression();

        ImmutableList.Builder<@NonNull QueryParameter> parameters = ImmutableList.builder();
        parameters.addAll(rewrittenString.get().parameters());
        parameters.addAll(rewrittenSubstring.get().parameters());
        parameters.addAll(rewrittenString.get().parameters());
        parameters.addAll(rewrittenSubstring.get().parameters());

        String expression = format(
                "CASE WHEN (%s) IS NULL OR (%s) IS NULL THEN CAST(NULL AS Int64) " +
                        "ELSE COALESCE(CAST(Unicode::Find(Unwrap(%s), Unwrap(%s)) AS Int64) + 1, 0) END",
                strSql,
                subSql,
                strSql,
                subSql);

        JdbcExpression result = new JdbcExpression(
                expression,
                parameters.build(),
                YdbTypeUtils.toTypeHandle(BigintType.BIGINT).orElseThrow());
        return Optional.of(result);
    }
}
