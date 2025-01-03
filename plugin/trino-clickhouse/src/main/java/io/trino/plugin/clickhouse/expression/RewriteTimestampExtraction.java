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
import com.google.common.collect.ImmutableMap;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.projection.ProjectFunctionRule;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;

import java.util.Map;
import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;
import static java.sql.Types.BIGINT;

public class RewriteTimestampExtraction
        implements ProjectFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private static final Capture<Variable> ARGUMENT = newCapture();
    // timezone_hour / timezone_minute(timestamp)
    // Returns the hour/minute of the time zone offset from timestamp
    // are not applicable for Clickhouse as information about time zone is stored within column metadata
    private static final Map<String, String> TRINO_TO_CLICKHOUSE = ImmutableMap.<String, String>builder()
            .put("day", "toDayOfMonth(%s)")
            .put("day_of_month", "toDayOfMonth(%s)")
            .put("day_of_week", "toDayOfWeek(%s)")
            .put("day_of_year", "toDayOfYear(%s)")
            .put("dow", "toDayOfWeek(%s)")
            .put("doy", "toDayOfYear(%s)")
            .put("hour", "toHour(%s)")
            .put("millisecond", "toMillisecond(%s)")
            .put("minute", "toMinute(%s)")
            .put("month", "toMonth(%s)")
            .put("quarter", "toQuarter(%s)")
            .put("second", "toSecond(%s)")
            .put("week", "toWeek(%s, 4)")
            .put("week_of_year", "toWeek(%s, 4)")
            .put("year", "toYear(%s)")
            // TODO no 1:1 mapping ?
            // .put("year_of_week", "toYearWeek(%s, 4)")
            // .put("yow", "toYearWeek(%s, 4)")
            .buildOrThrow();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().matching(x -> TRINO_TO_CLICKHOUSE.containsKey(x.getName())))
            .with(argumentCount().equalTo(1))
            .with(argument(0).matching(variable().capturedAs(ARGUMENT).with(type().matching(type -> type instanceof TimestampType || type instanceof TimestampWithTimeZoneType))));

    @Override
    public Pattern<? extends ConnectorExpression> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<JdbcExpression> rewrite(ConnectorTableHandle handle, ConnectorExpression projectionExpression, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Variable argument = captures.get(ARGUMENT);
        Optional<ParameterizedExpression> translatedArgument = context.rewriteExpression(argument);
        if (translatedArgument.isEmpty()) {
            return Optional.empty();
        }
        String functionName = ((Call) projectionExpression).getFunctionName().getName();
        String clickhouseFunctionPattern = TRINO_TO_CLICKHOUSE.get(functionName);
        JdbcTypeHandle targetJdbcTypeHandle = new JdbcTypeHandle(BIGINT, Optional.of(BigintType.BIGINT.getBaseName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        return Optional.of(new JdbcExpression(
                clickhouseFunctionPattern.formatted(translatedArgument.get().expression()),
                ImmutableList.copyOf(translatedArgument.get().parameters()),
                targetJdbcTypeHandle));
    }
}
