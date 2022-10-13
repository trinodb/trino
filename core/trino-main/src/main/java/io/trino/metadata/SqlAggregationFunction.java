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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import io.trino.operator.aggregation.AggregationFromAnnotationsParser;
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.spi.function.AggregationImplementation;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionMetadata;

import java.util.List;

import static java.util.Objects.requireNonNull;

public abstract class SqlAggregationFunction
        implements SqlFunction
{
    private final FunctionMetadata functionMetadata;
    private final AggregationFunctionMetadata aggregationFunctionMetadata;

    public static List<SqlAggregationFunction> createFunctionsByAnnotations(Class<?> aggregationDefinition)
    {
        try {
            return ImmutableList.copyOf(AggregationFromAnnotationsParser.parseFunctionDefinitions(aggregationDefinition));
        }
        catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid aggregation class " + aggregationDefinition.getSimpleName());
        }
    }

    public SqlAggregationFunction(FunctionMetadata functionMetadata, AggregationFunctionMetadata aggregationFunctionMetadata)
    {
        this.functionMetadata = requireNonNull(functionMetadata, "functionMetadata is null");
        this.aggregationFunctionMetadata = requireNonNull(aggregationFunctionMetadata, "aggregationFunctionMetadata is null");
    }

    @Override
    public FunctionMetadata getFunctionMetadata()
    {
        return functionMetadata;
    }

    public AggregationFunctionMetadata getAggregationMetadata()
    {
        return aggregationFunctionMetadata;
    }

    public AggregationImplementation specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        return specialize(boundSignature);
    }

    protected AggregationImplementation specialize(BoundSignature boundSignature)
    {
        throw new UnsupportedOperationException();
    }
}
