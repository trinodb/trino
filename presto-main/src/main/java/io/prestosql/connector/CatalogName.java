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
package io.prestosql.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.Name;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.Name.createNonDelimitedName;
import static java.util.Objects.requireNonNull;

public final class CatalogName
{
    private static final String INFORMATION_SCHEMA_CONNECTOR_PREFIX = "$info_schema@";
    private static final String SYSTEM_TABLES_CONNECTOR_PREFIX = "$system@";

    private final Name catalogName;

    @JsonCreator
    public CatalogName(@JsonProperty("name") Name catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        checkArgument(!catalogName.getName().isEmpty(), "catalogName is empty");
    }

    public CatalogName(String catalogName)
    {
        this(createNonDelimitedName(requireNonNull(catalogName, "catalogName is null")));
    }

    @JsonProperty("name")
    public Name getCatalogName()
    {
        return catalogName;
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
        CatalogName that = (CatalogName) o;
        return Objects.equals(catalogName, that.catalogName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName);
    }

    @Override
    public String toString()
    {
        return catalogName.getLegacyName();
    }

    public static boolean isInternalSystemConnector(CatalogName catalogName)
    {
        return catalogName.getCatalogName().getLegacyName().startsWith(SYSTEM_TABLES_CONNECTOR_PREFIX) ||
                catalogName.getCatalogName().getLegacyName().startsWith(INFORMATION_SCHEMA_CONNECTOR_PREFIX);
    }

    public static CatalogName createInformationSchemaConnectorId(CatalogName catalogName)
    {
        return new CatalogName(createNonDelimitedName(INFORMATION_SCHEMA_CONNECTOR_PREFIX + catalogName.getCatalogName().getLegacyName()));
    }

    public static CatalogName createSystemTablesConnectorId(CatalogName catalogName)
    {
        return new CatalogName(createNonDelimitedName(SYSTEM_TABLES_CONNECTOR_PREFIX + catalogName.getCatalogName().getLegacyName()));
    }
}
