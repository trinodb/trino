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
package io.trino.spi.connector;

import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public class AggregateFunction
{
    private final String functionName;
    private final Type outputType;
    private final List<ConnectorExpression> arguments;
    private final List<SortItem> sortItems;
    private final boolean isDistinct;
    private final Optional<ConnectorExpression> filter;

    public AggregateFunction(
            String aggregateFunctionName,
            Type outputType,
            List<ConnectorExpression> arguments,
            List<SortItem> sortItems,
            boolean isDistinct,
            Optional<ConnectorExpression> filter)
    {
        if (isDistinct && arguments.isEmpty()) {
            throw new IllegalArgumentException("DISTINCT requires arguments");
        }

        this.functionName = requireNonNull(aggregateFunctionName, "aggregateFunctionName is null");
        this.outputType = requireNonNull(outputType, "outputType is null");
        requireNonNull(arguments, "arguments is null");
        requireNonNull(sortItems, "sortItems is null");
        this.arguments = List.copyOf(arguments);
        this.sortItems = List.copyOf(sortItems);
        this.isDistinct = isDistinct;
        this.filter = requireNonNull(filter, "filter is null");
    }

    public String getFunctionName()
    {
        return functionName;
    }

    public List<ConnectorExpression> getArguments()
    {
        return arguments;
    }

    public Type getOutputType()
    {
        return outputType;
    }

    public List<SortItem> getSortItems()
    {
        return sortItems;
    }

    public boolean isDistinct()
    {
        return isDistinct;
    }

    public Optional<ConnectorExpression> getFilter()
    {
        return filter;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", AggregateFunction.class.getSimpleName() + "[", "]")
                .add("aggregationName='" + functionName + "'")
                .add("arguments=" + arguments)
                .add("outputType=" + outputType)
                .add("sortOrder=" + sortItems)
                .add("isDistinct=" + isDistinct)
                .add("filter=" + filter)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AggregateFunction that = (AggregateFunction) o;
        return isDistinct == that.isDistinct &&
                Objects.equals(functionName, that.functionName) &&
                Objects.equals(arguments, that.arguments) &&
                Objects.equals(outputType, that.outputType) &&
                Objects.equals(sortItems, that.sortItems) &&
                Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionName, arguments, outputType, sortItems, isDistinct, filter);
    }
}
