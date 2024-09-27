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
package io.trino.plugin.clickhouse.expression;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.CharType;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;
import static io.trino.plugin.clickhouse.ClickHouseClient.supportsPushdown;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.lang.String.format;

public class RewriteLike
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final Capture<Variable> LIKE_VALUE = newCapture();
    // TODO allow Variable as a LIKE_PATTERN: "SELECT * FROM t WHERE column_a LIKE column_b" is a valid query in ClickHouse
    // only Constant is allowed as LIKE_PATTERN, because according to
    // https://clickhouse.com/docs/en/sql-reference/functions/string-search-functions#like
    // ClickHouse requires backslashes in strings to be quoted as well, so you would actually need to write \\%, \\_ and \\\\ to match against literal %, _ and \
    // if "column_a LIKE column_b" is pushed down, it requires more thorough consideration how to process escaping.
    private static final Capture<Constant> LIKE_PATTERN = newCapture();
    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(LIKE_FUNCTION_NAME))
            .with(type().equalTo(BOOLEAN))
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(variable()
                    .with(type().matching(type -> type instanceof CharType || type instanceof VarcharType))
                    .matching((Variable variable, RewriteContext<ParameterizedExpression> context) -> supportsPushdown(variable, context))
                    .capturedAs(LIKE_VALUE)))
            .with(argument(1).matching(constant()
                    .with(type().matching(type -> type instanceof CharType || type instanceof VarcharType))
                    .capturedAs(LIKE_PATTERN)));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call expression, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Variable likeValue = captures.get(LIKE_VALUE);
        Constant likePattern = captures.get(LIKE_PATTERN);

        checkArgument(likePattern.getValue() != null, "When pattern is NULL, optimizer shortcuts to return fast, not reaching connector expression");
        if (((Slice) likePattern.getValue()).toStringUtf8().contains("\\")) {
            // LIKE in trino does not support escape characters.
            // However Clickhouse supports them with \ is for escaping literals %, _ and \.
            // ClickHouse requires backslashes in strings to be quoted as well, so you would actually need to write \\%, \\_ and \\\\ to match against literal %, _ and \
            // So just to translate Trino LIKE '\'  , all \ have to be escaped, and resulting clickhouse should be LIKE '\\\\'
            // TODO escape `\` appropriately and pushdown: .replace("\\", "\\\\\\\\")
            return Optional.empty();
        }

        return context.defaultRewrite(likeValue).flatMap(value ->
                context.defaultRewrite(likePattern).map(pattern ->
                        new ParameterizedExpression(
                                format("%s LIKE %s", value.expression(), pattern.expression()),
                                ImmutableList.<QueryParameter>builder()
                                        .addAll(value.parameters())
                                        .addAll(pattern.parameters())
                                        .build())));
    }
}
