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
package io.trino.spi.function;

import io.trino.spi.Experimental;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;

import java.util.Collection;
import java.util.List;

@Experimental(eta = "2023-03-31")
public interface FunctionProvider
{
    default ScalarFunctionImplementation getScalarFunctionImplementation(
            FunctionId functionId,
            BoundSignature boundSignature,
            FunctionDependencies functionDependencies,
            InvocationConvention invocationConvention)
    {
        throw new UnsupportedOperationException("%s does not provide scalar functions".formatted(getClass().getName()));
    }

    default AggregationImplementation getAggregationImplementation(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        throw new UnsupportedOperationException("%s does not provide aggregation functions".formatted(getClass().getName()));
    }

    default WindowFunctionSupplier getWindowFunctionSupplier(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        throw new UnsupportedOperationException("%s does not provide window functions".formatted(getClass().getName()));
    }

    default TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle functionHandle)
    {
        throw new UnsupportedOperationException("%s does not provide table functions".formatted(getClass().getName()));
    }

    default List<FunctionMetadata> listFunctions()
    {
        throw new UnsupportedOperationException("%s does not provide list functions".formatted(getClass().getName()));
    }

    default Collection<FunctionMetadata> getFunctions(SchemaFunctionName name)
    {
        throw new UnsupportedOperationException("%s does not provide get functions".formatted(getClass().getName()));
    }

    default FunctionMetadata getFunctionMetadata(FunctionId functionId)
    {
        throw new UnsupportedOperationException("%s does not provide function metadata".formatted(getClass().getName()));
    }

    default AggregationFunctionMetadata getAggregationFunctionMetadata(FunctionId functionId)
    {
        throw new UnsupportedOperationException("%s does not provide aggreagtion function metadata".formatted(getClass().getName()));
    }

    default FunctionDependencyDeclaration getFunctionDependencies(FunctionId functionId, BoundSignature boundSignature)
    {
        throw new UnsupportedOperationException("%s does not provide function dependencies".formatted(getClass().getName()));
    }
}
