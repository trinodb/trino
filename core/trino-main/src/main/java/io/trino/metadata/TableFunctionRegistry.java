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

import com.google.inject.Inject;
import io.trino.connector.CatalogServiceProvider;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.function.table.ConnectorTableFunction;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class TableFunctionRegistry
{
    private final CatalogServiceProvider<CatalogTableFunctions> tableFunctionsProvider;

    @Inject
    public TableFunctionRegistry(CatalogServiceProvider<CatalogTableFunctions> tableFunctionsProvider)
    {
        this.tableFunctionsProvider = requireNonNull(tableFunctionsProvider, "tableFunctionsProvider is null");
    }

    /**
     * Resolve table function with given qualified name.
     * Table functions are resolved case-insensitive for consistency with existing scalar function resolution.
     */
    public Optional<ConnectorTableFunction> resolve(CatalogHandle catalogHandle, SchemaFunctionName schemaFunctionName)
    {
        return tableFunctionsProvider.getService(catalogHandle)
                .getTableFunction(schemaFunctionName);
    }
}
