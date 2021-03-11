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

import io.trino.metadata.FunctionArgumentDefinition;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlFunction;

import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.metadata.FunctionKind.WINDOW;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class SqlWindowFunction
        implements SqlFunction
{
    private final WindowFunctionSupplier supplier;
    private final FunctionMetadata functionMetadata;

    public SqlWindowFunction(WindowFunctionSupplier supplier, boolean deprecated)
    {
        this.supplier = requireNonNull(supplier, "supplier is null");
        Signature signature = supplier.getSignature();
        functionMetadata = new FunctionMetadata(
                signature,
                signature.getName(),
                true,
                nCopies(signature.getArgumentTypes().size(), new FunctionArgumentDefinition(true)),
                false,
                true,
                nullToEmpty(supplier.getDescription()),
                WINDOW,
                deprecated);
    }

    @Override
    public FunctionMetadata getFunctionMetadata()
    {
        return functionMetadata;
    }

    public WindowFunctionSupplier specialize(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        return specialize(functionBinding);
    }

    public WindowFunctionSupplier specialize(FunctionBinding functionBinding)
    {
        return supplier;
    }
}
