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
    private final Optional<CatalogSchemaName> catalogSchema;
    private final String name;

    public FunctionName(String name)
    {
        this(Optional.empty(), name);
    }

    public FunctionName(Optional<CatalogSchemaName> catalogSchema, String name)
    {
        this.catalogSchema = requireNonNull(catalogSchema, "catalogSchema is null");
        this.name = requireNonNull(name, "name is null");
    }

    /**
     * @return the catalog and schema of this function, or {@link Optional#empty()} if this is a built-in function
     */
    public Optional<CatalogSchemaName> getCatalogSchema()
    {
        return catalogSchema;
    }

    /**
     * @return the function's name
     */
    public String getName()
    {
        return name;
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
        return Objects.equals(catalogSchema, that.catalogSchema) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogSchema, name);
    }

    @Override
    public String toString()
    {
        StringJoiner stringJoiner = new StringJoiner(", ");
        catalogSchema.ifPresent(value -> stringJoiner.add("catalogSchema=" + value));
        return stringJoiner
                .add("name='" + name + "'")
                .toString();
    }
}
