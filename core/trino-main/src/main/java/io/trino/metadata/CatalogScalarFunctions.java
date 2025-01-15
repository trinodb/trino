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

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.errorprone.annotations.ThreadSafe;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.SchemaFunctionName;

import java.util.Collection;
import java.util.Optional;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class CatalogScalarFunctions
{
    // schema and function name in lowercase
    private final Multimap<SchemaFunctionName, SqlScalarFunction> functions;

    public CatalogScalarFunctions(Collection<SqlScalarFunction> functions)
    {
        requireNonNull(functions, "functions is null");
        this.functions = Multimaps.index(functions, function -> lowerCaseSchemaFunctionName(new SchemaFunctionName(function.getSchemaName().orElseThrow(), function.getFunctionMetadata().getCanonicalName())));
    }

    public Multimap<SchemaFunctionName, SqlScalarFunction> listScalarFunctions()
    {
        return functions;
    }

    public Optional<SqlScalarFunction> getScalarFunction(SchemaFunctionName schemaFunctionName, FunctionId functionId)
    {
        return functions.get(lowerCaseSchemaFunctionName(schemaFunctionName)).stream()
                .filter(function -> function.getFunctionMetadata().getFunctionId().equals(functionId))
                .findAny();
    }

    private static SchemaFunctionName lowerCaseSchemaFunctionName(SchemaFunctionName name)
    {
        return new SchemaFunctionName(
                name.getSchemaName().toLowerCase(ENGLISH),
                name.getFunctionName().toLowerCase(ENGLISH));
    }
}
