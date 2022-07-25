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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.pinot.PinotColumnHandle;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.pinot.query.DynamicTablePqlExtractor.quoteIdentifier;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AggregateExpression
{
    private final String function;
    private final String argument;
    private final boolean returnNullOnEmptyGroup;

    public static AggregateExpression replaceIdentifier(AggregateExpression aggregationExpression, PinotColumnHandle columnHandle)
    {
        return new AggregateExpression(aggregationExpression.getFunction(), stripDoubleQuotes(columnHandle.getExpression()), aggregationExpression.isReturnNullOnEmptyGroup());
    }

    private static String stripDoubleQuotes(String expression)
    {
        checkState(expression.startsWith("\"") && expression.endsWith("\"") && expression.length() >= 3, "expression is not enclosed in double quotes");
        return expression.substring(1, expression.length() - 1).replaceAll("\"\"", "\"");
    }

    @JsonCreator
    public AggregateExpression(@JsonProperty String function, @JsonProperty String argument, @JsonProperty boolean returnNullOnEmptyGroup)
    {
        this.function = requireNonNull(function, "function is null");
        this.argument = requireNonNull(argument, "argument is null");
        this.returnNullOnEmptyGroup = returnNullOnEmptyGroup;
    }

    @JsonProperty
    public String getFunction()
    {
        return function;
    }

    @JsonProperty
    public String getArgument()
    {
        return argument;
    }

    @JsonProperty
    public boolean isReturnNullOnEmptyGroup()
    {
        return returnNullOnEmptyGroup;
    }

    public String toFieldName()
    {
        return format("%s(%s)", function, argument);
    }

    public String toExpression()
    {
        return format("%s(%s)", function, quoteIdentifier(argument));
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof AggregateExpression)) {
            return false;
        }
        AggregateExpression that = (AggregateExpression) other;
        return that.function.equals(function) &&
                that.argument.equals(argument) &&
                that.returnNullOnEmptyGroup == returnNullOnEmptyGroup;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(function, argument, returnNullOnEmptyGroup);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("function", function)
                .add("argument", argument)
                .add("returnNullOnEmptyGroup", returnNullOnEmptyGroup)
                .toString();
    }
}
