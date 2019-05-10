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
package io.prestosql.connector.informationschema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.prestosql.metadata.QualifiedTablePrefix;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;

import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class InformationSchemaTableHandle
        implements ConnectorTableHandle
{
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final Set<QualifiedTablePrefix> prefixes;

    @JsonCreator
    public InformationSchemaTableHandle(
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("prefixes") Set<QualifiedTablePrefix> prefixes)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.prefixes = ImmutableSet.copyOf(requireNonNull(prefixes, "prefixes is null"));
    }

    @JsonProperty
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @JsonProperty
    public Set<QualifiedTablePrefix> getPrefixes()
    {
        return prefixes;
    }

    @Override
    public String toString()
    {
        return catalogName + ":" + schemaName + ":" + tableName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemaName, tableName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        InformationSchemaTableHandle other = (InformationSchemaTableHandle) obj;
        return Objects.equals(this.catalogName, other.catalogName) &&
                Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }
}
