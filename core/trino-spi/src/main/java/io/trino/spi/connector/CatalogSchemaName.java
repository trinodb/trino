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
package io.trino.spi.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class CatalogSchemaName
{
    private final ConnectorIdentifier catalog;
    private final ConnectorIdentifier schema;

    @JsonCreator
    public CatalogSchemaName(
            @JsonProperty("catalog") ConnectorIdentifier catalog,
            @JsonProperty("schema") ConnectorIdentifier schema)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
    }

    public CatalogSchemaName(
            String catalog,
            String schema)
    {
        this.catalog = new ConnectorIdentifier(catalog, false);
        this.schema = new ConnectorIdentifier(schema, false);
    }

    @JsonProperty
    public ConnectorIdentifier getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public ConnectorIdentifier getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getCatalogName()
    {
        return catalog.getValue();
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schema.getValue();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        CatalogSchemaName that = (CatalogSchemaName) obj;
        return Objects.equals(catalog, that.catalog) &&
                Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalog, schema);
    }

    @Override
    public String toString()
    {
        return catalog.getValue() + '.' + schema.getValue();
    }
}
