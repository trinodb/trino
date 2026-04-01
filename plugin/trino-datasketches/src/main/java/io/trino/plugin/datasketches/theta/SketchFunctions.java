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
package io.trino.plugin.datasketches.theta;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.spi.function.AggregationImplementation;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;

import java.util.List;

public class SketchFunctions
        implements FunctionProvider
{
    private static final InternalFunctionBundle FUNCTION_BUNDLE = InternalFunctionBundle.builder()
            .functions(Estimate.class)
            .functions(Union.class)
            .functions(UnionWithParams.class)
            .build();

    public List<FunctionMetadata> getFunctions()
    {
        return ImmutableList.copyOf(FUNCTION_BUNDLE.getFunctions());
    }

    public AggregationFunctionMetadata getAggregationFunctionMetadata(FunctionId functionId)
    {
        return FUNCTION_BUNDLE.getAggregationFunctionMetadata(functionId);
    }

    public FunctionDependencyDeclaration getFunctionDependencies(FunctionId functionId, BoundSignature boundSignature)
    {
        return FUNCTION_BUNDLE.getFunctionDependencies(functionId, boundSignature);
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(
            FunctionId functionId,
            BoundSignature boundSignature,
            FunctionDependencies functionDependencies,
            InvocationConvention invocationConvention)
    {
        return FUNCTION_BUNDLE.getScalarFunctionImplementation(functionId, boundSignature, functionDependencies, invocationConvention);
    }

    @Override
    public AggregationImplementation getAggregationImplementation(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        return FUNCTION_BUNDLE.getAggregationImplementation(functionId, boundSignature, functionDependencies);
    }
}
