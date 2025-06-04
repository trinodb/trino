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

import com.google.common.collect.Maps;
import com.google.errorprone.annotations.ThreadSafe;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class CatalogTableFunctions
{
    // schema and function name in lowercase
    private final Map<SchemaFunctionName, ConnectorTableFunction> functions;

    public CatalogTableFunctions(Collection<ConnectorTableFunction> functions)
    {
        requireNonNull(functions, "functions is null");
        this.functions = Maps.uniqueIndex(functions, function -> lowerCaseSchemaFunctionName(new SchemaFunctionName(function.getSchema(), function.getName())));
    }

    public Map<SchemaFunctionName, ConnectorTableFunction> listTableFunctions()
    {
        return functions;
    }

    public Optional<ConnectorTableFunction> getTableFunction(SchemaFunctionName schemaFunctionName)
    {
        return Optional.ofNullable(functions.get(lowerCaseSchemaFunctionName(schemaFunctionName)));
    }

    private static SchemaFunctionName lowerCaseSchemaFunctionName(SchemaFunctionName name)
    {
        return new SchemaFunctionName(
                name.getSchemaName().toLowerCase(ENGLISH),
                name.getFunctionName().toLowerCase(ENGLISH));
    }
}
