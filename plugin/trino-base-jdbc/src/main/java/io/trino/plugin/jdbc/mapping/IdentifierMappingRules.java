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
package io.trino.plugin.jdbc.mapping;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class IdentifierMappingRules
{
    private final List<SchemaMappingRule> schemas;
    private final List<TableMappingRule> tables;

    @JsonCreator
    public IdentifierMappingRules(
            @JsonProperty("schemas") List<SchemaMappingRule> schemas,
            @JsonProperty("tables") List<TableMappingRule> tables)
    {
        this.schemas = requireNonNull(schemas, "schemaMappingRules is null");
        this.tables = requireNonNull(tables, "tableMappingRules is null");
    }

    @JsonProperty("schemas")
    public List<SchemaMappingRule> getSchemaMapping()
    {
        return schemas;
    }

    @JsonProperty("tables")
    public List<TableMappingRule> getTableMapping()
    {
        return tables;
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
        IdentifierMappingRules that = (IdentifierMappingRules) o;
        return schemas.equals(that.schemas) && tables.equals(that.tables);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemas, tables);
    }
}
