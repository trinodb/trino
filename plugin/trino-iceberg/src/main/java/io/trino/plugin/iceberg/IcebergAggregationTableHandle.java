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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record IcebergAggregationTableHandle(
        IcebergTableHandle source,
        List<Aggregation> intermediateAggregations,
        List<IcebergColumnHandle> groupBy)
        implements ConnectorTableHandle
{
    public IcebergAggregationTableHandle
    {
        requireNonNull(source, "source is null");
        intermediateAggregations = ImmutableList.copyOf(requireNonNull(intermediateAggregations, "intermediateAggregations is null"));
        groupBy = ImmutableList.copyOf(requireNonNull(groupBy, "groupBy is null"));
    }

    public record Aggregation(
            AggregationType aggregationType,
            Type outputType,
            List<IcebergColumnHandle> arguments)
            implements ColumnHandle
    {
        public Aggregation
        {
            requireNonNull(aggregationType, "aggregationType is null");
            requireNonNull(outputType, "outputType is null");
            arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
            aggregationType.checkArguments(arguments);
        }

        public AggregateFunction finalAggregation(Variable intermediateResultVariable)
        {
            return new AggregateFunction(
                    aggregationType.getFinalAggregateFunctionName(),
                    aggregationType.getFinalAggregateFunctionType(outputType()),
                    ImmutableList.of(intermediateResultVariable),
                    ImmutableList.of(),
                    false,
                    Optional.empty());
        }

        public boolean nonNullOnEmpty()
        {
            return aggregationType.valueOnEmpty() != null;
        }

        public Object valueOnEmpty()
        {
            return aggregationType.valueOnEmpty();
        }
    }

    public enum AggregationType
    {
        COUNT_ALL("sum", 0, 0L),
        COUNT_NON_NULL("sum", 1, 0L),
        MIN("min", 1, null),
        MAX("max", 1, null),
        /**/;

        private final String finalAggregateFunctionName;
        private final int argumentCount;
        @Nullable
        private final Object valueOnEmpty;

        AggregationType(String finalAggregateFunctionName, int argumentCount, @Nullable Object valueOnEmpty)
        {
            this.finalAggregateFunctionName = requireNonNull(finalAggregateFunctionName, "finalAggregateFunctionName is null");
            this.argumentCount = argumentCount;
            this.valueOnEmpty = valueOnEmpty;
        }

        public String getFinalAggregateFunctionName()
        {
            return finalAggregateFunctionName;
        }

        public Type getFinalAggregateFunctionType(Type connectorProducedType)
        {
            // Currently intermediate and final results are always same type.
            return connectorProducedType;
        }

        private void checkArguments(List<IcebergColumnHandle> arguments)
        {
            checkArgument(arguments.size() == argumentCount, "Wrong argument count, expected %s, got %s: %s", argumentCount, arguments.size(), arguments);
        }

        private Object valueOnEmpty()
        {
            return valueOnEmpty;
        }
    }
}
