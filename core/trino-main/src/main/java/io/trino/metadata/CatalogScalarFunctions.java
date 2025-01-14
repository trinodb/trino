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

import com.google.common.base.Functions;
import com.google.common.collect.Table;
import com.google.errorprone.annotations.ThreadSafe;
import io.trino.spi.function.FunctionId;

import java.util.Collection;
import java.util.Optional;

import static com.google.common.collect.ImmutableTable.toImmutableTable;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class CatalogScalarFunctions
{
    private final Table<String, FunctionId, SqlScalarFunction> functions;

    public CatalogScalarFunctions(Collection<SqlScalarFunction> functions)
    {
        this.functions = requireNonNull(functions, "functions is null").stream()
                .collect(toImmutableTable(
                        function -> function.getSchemaName().orElseThrow().toLowerCase(ENGLISH),
                        function -> function.getFunctionMetadata().getFunctionId(),
                        Functions.identity()));
    }

    public Collection<SqlScalarFunction> listScalarFunctions()
    {
        return functions.values();
    }

    public Collection<SqlScalarFunction> listScalarFunctions(String schemaName)
    {
        return functions.row(schemaName.toLowerCase(ENGLISH)).values();
    }

    public Optional<SqlScalarFunction> getScalarFunction(String schemaName, FunctionId functionId)
    {
        return Optional.ofNullable(functions.get(schemaName.toLowerCase(ENGLISH), functionId));
    }
}
