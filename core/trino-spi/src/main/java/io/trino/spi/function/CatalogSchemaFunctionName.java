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
package io.trino.spi.function;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;

public record CatalogSchemaFunctionName(String catalogName, SchemaFunctionName schemaFunctionName)
{
    public CatalogSchemaFunctionName
    {
        catalogName = catalogName.toLowerCase(ROOT);
        if (catalogName.isEmpty()) {
            throw new IllegalArgumentException("catalogName is empty");
        }
        requireNonNull(schemaFunctionName, "schemaFunctionName is null");
    }

    @JsonCreator
    public CatalogSchemaFunctionName(@JsonProperty String catalogName, @JsonProperty String schemaName, @JsonProperty String functionName)
    {
        this(catalogName, new SchemaFunctionName(schemaName, functionName));
    }

    @Override
    @JsonIgnore
    public SchemaFunctionName schemaFunctionName()
    {
        return schemaFunctionName;
    }

    @JsonProperty
    public String schemaName()
    {
        return schemaFunctionName.schemaName();
    }

    @JsonProperty
    public String functionName()
    {
        return schemaFunctionName.functionName();
    }

    @Override
    public String toString()
    {
        return catalogName + '.' + schemaFunctionName;
    }
}
