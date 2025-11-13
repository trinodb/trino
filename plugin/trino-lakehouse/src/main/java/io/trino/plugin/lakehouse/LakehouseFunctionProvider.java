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
package io.trino.plugin.lakehouse;

import com.google.inject.Inject;
import io.trino.plugin.iceberg.functions.IcebergFunctionProvider;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;

import static java.util.Objects.requireNonNull;

public class LakehouseFunctionProvider
        implements FunctionProvider
{
    private final IcebergFunctionProvider icebergFunctionProvider;

    @Inject
    public LakehouseFunctionProvider(
            IcebergFunctionProvider icebergFunctionProvider)
    {
        this.icebergFunctionProvider = requireNonNull(icebergFunctionProvider, "icebergFunctionProvider is null");
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(
            FunctionId functionId,
            BoundSignature boundSignature,
            FunctionDependencies functionDependencies,
            InvocationConvention invocationConvention)
    {
        if ("system".equals(boundSignature.getName().getSchemaName()) && "bucket".equals(boundSignature.getName().getFunctionName())) {
            return icebergFunctionProvider.getScalarFunctionImplementation(functionId, boundSignature, functionDependencies, invocationConvention);
        }
        throw new UnsupportedOperationException("%s does not provide %s scalar function".formatted(getClass().getName(), boundSignature.getName()));
    }
}
