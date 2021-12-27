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

import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlFunction;

import java.util.Optional;

import static io.trino.metadata.FunctionKind.WINDOW;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class SqlWindowFunction
        implements SqlFunction
{
    private final WindowFunctionSupplier supplier;
    private final FunctionMetadata functionMetadata;

    public SqlWindowFunction(Signature signature, Optional<String> description, boolean deprecated, WindowFunctionSupplier supplier)
    {
        this.supplier = requireNonNull(supplier, "supplier is null");
        functionMetadata = new FunctionMetadata(
                signature,
                signature.getName(),
                new FunctionNullability(true, nCopies(signature.getArgumentTypes().size(), true)),
                false,
                true,
                description.orElse(""),
                WINDOW,
                deprecated);
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
