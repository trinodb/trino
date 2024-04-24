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
package io.trino.plugin.pinot.query;

import io.trino.plugin.pinot.PinotColumnHandle;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.pinot.query.DynamicTablePqlExtractor.quoteIdentifier;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public record AggregateExpression(String function, String argument, boolean returnNullOnEmptyGroup)
{
    public AggregateExpression
    {
        requireNonNull(function, "function is null");
        requireNonNull(argument, "argument is null");
    }

    public static AggregateExpression replaceIdentifier(AggregateExpression aggregationExpression, PinotColumnHandle columnHandle)
    {
        return new AggregateExpression(aggregationExpression.function(), stripDoubleQuotes(columnHandle.getExpression()), aggregationExpression.returnNullOnEmptyGroup());
    }

    private static String stripDoubleQuotes(String expression)
    {
        checkState(expression.startsWith("\"") && expression.endsWith("\"") && expression.length() >= 3, "expression is not enclosed in double quotes");
        return expression.substring(1, expression.length() - 1).replaceAll("\"\"", "\"");
    }

    public String fieldName()
    {
        return format("%s(%s)", function, argument);
    }

    public String expression()
    {
        return format("%s(%s)", function, quoteIdentifier(argument));
    }
}
