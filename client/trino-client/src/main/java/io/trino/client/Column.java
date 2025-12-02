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
package io.trino.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class Column
{
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final Optional<String> table;
    private final String name;
    private final Optional<String> label;
    private final String type;
    private final ClientTypeSignature typeSignature;

    public Column(String name, String type, ClientTypeSignature typeSignature)
    {
        this(Optional.empty(), Optional.empty(), Optional.empty(), name, Optional.of(name), type, typeSignature);
    }

    public Column(String catalog, String schema, String table, String name, String label, String type, ClientTypeSignature typeSignature)
    {
        this(Optional.of(catalog), Optional.of(schema), Optional.of(table), name, Optional.of(label), type, typeSignature);
    }

    @JsonCreator
    public Column(
            @JsonProperty("catalog") Optional<String> catalog,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("table") Optional<String> table,
            @JsonProperty("name") String name,
            @JsonProperty("label") Optional<String> label,
            @JsonProperty("type") String type,
            @JsonProperty("typeSignature") ClientTypeSignature typeSignature)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.name = requireNonNull(name, "name is null");
        this.label = requireNonNull(label, "label is null");
        this.type = requireNonNull(type, "type is null");
        this.typeSignature = typeSignature;
    }

    @JsonProperty
    public Optional<String> getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public Optional<String> getSchema()
    {
        return schema;
    }

    @JsonProperty
    public Optional<String> getTable()
    {
        return table;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Optional<String> getLabel()
    {
        return label;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public ClientTypeSignature getTypeSignature()
    {
        return typeSignature;
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
        Column column = (Column) o;
        return Objects.equals(name, column.name)
                && Objects.equals(type, column.type)
                && Objects.equals(typeSignature, column.typeSignature);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, typeSignature);
    }
}
