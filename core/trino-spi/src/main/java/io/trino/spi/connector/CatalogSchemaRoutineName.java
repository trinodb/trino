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

import java.util.Objects;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class CatalogSchemaRoutineName
{
    private final String catalogName;
    private final SchemaRoutineName schemaRoutineName;

    public CatalogSchemaRoutineName(String catalogName, SchemaRoutineName schemaRoutineName)
    {
        this.catalogName = catalogName.toLowerCase(ENGLISH);
        this.schemaRoutineName = requireNonNull(schemaRoutineName, "schemaRoutineName is null");
    }

    public CatalogSchemaRoutineName(String catalogName, String schemaName, String routineName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.schemaRoutineName = new SchemaRoutineName(schemaName, routineName);
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public SchemaRoutineName getSchemaRoutineName()
    {
        return schemaRoutineName;
    }

    public String getSchemaName()
    {
        return schemaRoutineName.getSchemaName();
    }

    public String getRoutineName()
    {
        return schemaRoutineName.getRoutineName();
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
        CatalogSchemaRoutineName that = (CatalogSchemaRoutineName) o;
        return Objects.equals(catalogName, that.catalogName) &&
                Objects.equals(schemaRoutineName, that.schemaRoutineName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemaRoutineName);
    }

    @Override
    public String toString()
    {
        return catalogName + "." + schemaRoutineName;
    }
}
