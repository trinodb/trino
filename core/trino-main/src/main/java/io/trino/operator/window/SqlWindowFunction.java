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
package io.trino.operator.window;

import io.trino.metadata.SqlFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.function.WindowFunctionSupplier;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SqlWindowFunction
        implements SqlFunction
{
    private final WindowFunctionSupplier supplier;
    private final FunctionMetadata functionMetadata;

    public SqlWindowFunction(Signature signature, Optional<String> description, boolean deprecated, WindowFunctionSupplier supplier)
    {
        this.supplier = requireNonNull(supplier, "supplier is null");
        FunctionMetadata.Builder functionMetadata = FunctionMetadata.windowBuilder()
                .signature(signature);
        if (description.isPresent()) {
            functionMetadata.description(description.get());
        }
        else {
            functionMetadata.noDescription();
        }
        if (deprecated) {
            functionMetadata.deprecated();
        }
        this.functionMetadata = functionMetadata.build();
    }

    @Override
    public FunctionMetadata getFunctionMetadata()
    {
        return functionMetadata;
    }

    public WindowFunctionSupplier specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        return specialize(boundSignature);
    }

    public WindowFunctionSupplier specialize(BoundSignature boundSignature)
    {
        return supplier;
    }
}
