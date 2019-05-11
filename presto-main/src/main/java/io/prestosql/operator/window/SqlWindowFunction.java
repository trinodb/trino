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
package io.prestosql.operator.window;

import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlFunction;

import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Objects.requireNonNull;

public class SqlWindowFunction
        implements SqlFunction
{
    private final WindowFunctionSupplier supplier;
    private final FunctionMetadata functionMetadata;

    public SqlWindowFunction(WindowFunctionSupplier supplier)
    {
        this.supplier = requireNonNull(supplier, "supplier is null");
        Signature signature = supplier.getSignature();
        functionMetadata = new FunctionMetadata(
                signature,
                false,
                true,
                nullToEmpty(supplier.getDescription()));
    }

    @Override
    public FunctionMetadata getFunctionMetadata()
    {
        return functionMetadata;
    }

    public WindowFunctionSupplier specialize(BoundVariables boundVariables, int arity, Metadata metadata)
    {
        return supplier;
    }
}
