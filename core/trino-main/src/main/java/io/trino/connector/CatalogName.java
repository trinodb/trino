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
package io.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class CatalogName
{
    private static final String INFORMATION_SCHEMA_CONNECTOR_PREFIX = "$info_schema@";
    private static final String SYSTEM_TABLES_CONNECTOR_PREFIX = "$system@";

    private final String catalogName;

    @JsonCreator
    public CatalogName(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        checkArgument(!catalogName.isEmpty(), "catalogName is empty");
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public CatalogName getActualName()
    {
        if (catalogName.startsWith(SYSTEM_TABLES_CONNECTOR_PREFIX)) {
            return new CatalogName(catalogName.substring(SYSTEM_TABLES_CONNECTOR_PREFIX.length()));
        }
        if (catalogName.startsWith(INFORMATION_SCHEMA_CONNECTOR_PREFIX)) {
            return new CatalogName(catalogName.substring(INFORMATION_SCHEMA_CONNECTOR_PREFIX.length()));
        }
        return this;
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

    @JsonValue
    @Override
    public String toString()
    {
        return catalogName;
    }
}
