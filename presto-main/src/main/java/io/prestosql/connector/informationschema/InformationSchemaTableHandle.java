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

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class InformationSchemaTableHandle
        implements ConnectorTableHandle
{
    private final String catalogName;
    private final InformationSchemaTable table;
    private final Set<QualifiedTablePrefix> prefixes;
    private final Optional<Set<String>> roles;
    private final Optional<Set<String>> grantees;
    private final OptionalLong limit;

    @JsonCreator
    public InformationSchemaTableHandle(
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("table") InformationSchemaTable table,
            @JsonProperty("prefixes") Set<QualifiedTablePrefix> prefixes,
            @JsonProperty("roles") Optional<Set<String>> roles,
            @JsonProperty("grantees") Optional<Set<String>> grantees,
            @JsonProperty("limit") OptionalLong limit)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.table = requireNonNull(table, "table is null");
        this.prefixes = ImmutableSet.copyOf(requireNonNull(prefixes, "prefixes is null"));
        this.roles = requireNonNull(roles, "roles is null");
        this.grantees = requireNonNull(grantees, "grantees is null");
        this.limit = requireNonNull(limit, "limit is null");
    }

    @JsonProperty
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public InformationSchemaTable getTable()
    {
        return table;
    }

    @JsonProperty
    public Optional<Set<String>> getRoles()
    {
        return roles;
    }

    @JsonProperty
    public Optional<Set<String>> getGrantees()
    {
        return grantees;
    }

    @JsonProperty
    public Set<QualifiedTablePrefix> getPrefixes()
    {
        return prefixes;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", catalogName)
                .add("table", table)
                .add("prefixes", prefixes)
                .add("roles", roles)
                .add("grantees", grantees)
                .add("limit", limit)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, table, prefixes, limit);
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
                this.table == other.table &&
                Objects.equals(this.roles, other.roles) &&
                Objects.equals(this.grantees, other.grantees) &&
                Objects.equals(this.prefixes, other.prefixes) &&
                Objects.equals(this.limit, other.limit);
    }
}
