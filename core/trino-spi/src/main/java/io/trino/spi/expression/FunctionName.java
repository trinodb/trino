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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.CatalogSchemaName;

import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public class FunctionName
{
    @JsonProperty
    private final Optional<CatalogSchemaName> catalogSchema;
    @JsonProperty
    private final String name;

    public FunctionName(String name)
    {
        this(Optional.empty(), name);
    }

    @JsonCreator
    public FunctionName(
            @JsonProperty("catalogSchema") Optional<CatalogSchemaName> catalogSchema,
            @JsonProperty("name") String name)
    {
        this.catalogSchema = requireNonNull(catalogSchema, "catalogSchema is null");
        this.name = requireNonNull(name, "name is null");
    }

    /**
     * @return the catalog and schema of this function, or {@link Optional#empty()} if this is a built-in function
     */
    @JsonProperty
    public Optional<CatalogSchemaName> getCatalogSchema()
    {
        return catalogSchema;
    }

    /**
     * @return the function's name
     */
    @JsonProperty
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
