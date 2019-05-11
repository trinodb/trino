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
package io.prestosql.metadata;

import com.google.common.collect.ImmutableList;
import io.prestosql.operator.aggregation.AggregationFromAnnotationsParser;
import io.prestosql.operator.aggregation.InternalAggregationFunction;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public abstract class SqlAggregationFunction
        implements SqlFunction
{
    private final FunctionMetadata functionMetadata;

    public static List<SqlAggregationFunction> createFunctionByAnnotations(Class<?> aggregationDefinition)
    {
        return ImmutableList.of(AggregationFromAnnotationsParser.parseFunctionDefinition(aggregationDefinition));
    }

    public static List<SqlAggregationFunction> createFunctionsByAnnotations(Class<?> aggregationDefinition)
    {
        return AggregationFromAnnotationsParser.parseFunctionDefinitions(aggregationDefinition)
                .stream()
                .map(x -> (SqlAggregationFunction) x)
                .collect(toImmutableList());
    }

    protected SqlAggregationFunction(FunctionMetadata functionMetadata)
    {
        this.functionMetadata = requireNonNull(functionMetadata, "functionMetadata is null");
        checkArgument(functionMetadata.isDeterministic(), "Aggregation function must be deterministic");
    }

    @Override
    public FunctionMetadata getFunctionMetadata()
    {
        return functionMetadata;
    }

    public abstract InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, Metadata metadata);
}
