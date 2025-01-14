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

import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.trino.connector.CatalogServiceProvider;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.SchemaFunctionName;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ScalarFunctionRegistry
{
    private final CatalogServiceProvider<CatalogScalarFunctions> tableFunctionsProvider;

    @Inject
    public ScalarFunctionRegistry(CatalogServiceProvider<CatalogScalarFunctions> tableFunctionsProvider)
    {
        this.tableFunctionsProvider = requireNonNull(tableFunctionsProvider, "tableFunctionsProvider is null");
    }

    public List<FunctionMetadata> listFunctions(CatalogHandle catalogHandle)
    {
        return tableFunctionsProvider.getService(catalogHandle).listScalarFunctions().stream()
                .map(ScalarFunctionRegistry::toFunctionMetadata)
                .collect(toImmutableList());
    }

    public List<FunctionMetadata> listFunctions(CatalogHandle catalogHandle, String schemaName)
    {
        return tableFunctionsProvider.getService(catalogHandle).listScalarFunctions(schemaName).stream()
                .map(ScalarFunctionRegistry::toFunctionMetadata)
                .collect(toImmutableList());
    }

    /**
     * Resolve table function with given qualified name.
     * Scalar functions are resolved case-insensitive for consistency with existing scalar function resolution.
     */
    public Optional<SqlScalarFunction> resolve(CatalogHandle catalogHandle, SchemaFunctionName schemaFunctionName, FunctionId functionId)
    {
        return tableFunctionsProvider.getService(catalogHandle)
                .getScalarFunction(schemaFunctionName.getSchemaName(), functionId);
    }

    private static FunctionMetadata toFunctionMetadata(SqlScalarFunction tableFunction)
    {
        return tableFunction.getFunctionMetadata();
    }
}
