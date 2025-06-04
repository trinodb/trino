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
package io.trino.plugin.faker;

import com.google.inject.Inject;
import io.trino.metadata.SqlScalarFunction;
import io.trino.operator.scalar.annotations.ScalarFromAnnotationsParser;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class FakerFunctionProvider
        implements FunctionProvider
{
    private final List<SqlScalarFunction> functions;

    @Inject
    public FakerFunctionProvider()
    {
        functions = ScalarFromAnnotationsParser.parseFunctionDefinitions(FakerFunctions.class);
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(
            FunctionId functionId,
            BoundSignature boundSignature,
            FunctionDependencies functionDependencies,
            InvocationConvention invocationConvention)
    {
        return functions.stream()
                .filter(function -> function.getFunctionMetadata().getFunctionId().equals(functionId))
                .map(function -> function.specialize(boundSignature, functionDependencies))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown function " + functionId))
                .getScalarFunctionImplementation(invocationConvention);
    }

    public List<FunctionMetadata> functionsMetadata()
    {
        return functions.stream()
                .map(SqlScalarFunction::getFunctionMetadata)
                .collect(toImmutableList());
    }
}
