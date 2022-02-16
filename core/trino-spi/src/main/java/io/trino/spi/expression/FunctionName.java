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

package io.trino.spi.expression;

import io.trino.spi.connector.CatalogSchemaName;

import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public class FunctionName
{
    private final Optional<CatalogSchemaName> catalogSchemaName;
    private final String functionName;

    public FunctionName(String functionName)
    {
        this(Optional.empty(), functionName);
    }

    public FunctionName(Optional<CatalogSchemaName> catalogSchemaName, String functionName)
    {
        this.catalogSchemaName = requireNonNull(catalogSchemaName, "catalogSchemaName is null");
        this.functionName = requireNonNull(functionName, "functionName is null");
    }

    /**
     * @return the catalog and schema of this function, or {@link Optional#empty()} if this is a built-in function
     */
    public Optional<CatalogSchemaName> getCatalogSchemaName()
    {
        return catalogSchemaName;
    }

    /**
     * @return the function's name
     */
    public String getFunctionName()
    {
        return functionName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FunctionName that = (FunctionName) o;
        return Objects.equals(catalogSchemaName, that.catalogSchemaName) &&
                Objects.equals(functionName, that.functionName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogSchemaName, functionName);
    }

    @Override
    public String toString()
    {
        StringJoiner stringJoiner = new StringJoiner(", ");
        catalogSchemaName.ifPresent(name -> stringJoiner.add("catalogSchemaName=" + name));
        return stringJoiner
                .add("functionName='" + functionName + "'")
                .toString();
    }
}
