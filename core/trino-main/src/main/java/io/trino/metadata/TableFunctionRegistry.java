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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.sql.tree.QualifiedName;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.metadata.FunctionResolver.toPath;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class TableFunctionRegistry
{
    // catalog name in the original case; schema and function name in lowercase
    private final Map<CatalogName, Map<SchemaFunctionName, TableFunctionMetadata>> tableFunctions = new ConcurrentHashMap<>();

    public void addTableFunctions(CatalogName catalogName, Collection<ConnectorTableFunction> functions)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(functions, "functions is null");

        ImmutableMap.Builder<SchemaFunctionName, TableFunctionMetadata> builder = ImmutableMap.builder();
        for (ConnectorTableFunction function : functions) {
            builder.put(
                    new SchemaFunctionName(
                            function.getSchema().toLowerCase(ENGLISH),
                            function.getName().toLowerCase(ENGLISH)),
                    new TableFunctionMetadata(catalogName, function));
        }
        checkState(tableFunctions.putIfAbsent(catalogName, builder.buildOrThrow()) == null, "Table functions already registered for catalog: " + catalogName);
    }

    public void removeTableFunctions(CatalogName catalogName)
    {
        tableFunctions.remove(catalogName);
    }

    /**
     * Resolve table function with given qualified name.
     * Table functions are resolved case-insensitive for consistency with existing scalar function resolution.
     */
    public TableFunctionMetadata resolve(Session session, QualifiedName qualifiedName)
    {
        for (CatalogSchemaFunctionName name : toPath(session, qualifiedName)) {
            CatalogName catalogName = new CatalogName(name.getCatalogName());
            Map<SchemaFunctionName, TableFunctionMetadata> catalogFunctions = tableFunctions.get(catalogName);
            if (catalogFunctions != null) {
                String lowercasedSchemaName = name.getSchemaFunctionName().getSchemaName().toLowerCase(ENGLISH);
                String lowercasedFunctionName = name.getSchemaFunctionName().getFunctionName().toLowerCase(ENGLISH);
                TableFunctionMetadata function = catalogFunctions.get(new SchemaFunctionName(lowercasedSchemaName, lowercasedFunctionName));
                if (function != null) {
                    return function;
                }
            }
        }

        return null;
    }
}
