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
import io.trino.spi.type.TypeId;

import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public class ConnectorViewDefinition
{
    private final String originalSql;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final List<ViewColumn> columns;
    private final Optional<String> comment;
    private final Optional<String> owner;
    private final boolean runAsInvoker;

    @JsonCreator
    public ConnectorViewDefinition(
            @JsonProperty("originalSql") String originalSql,
            @JsonProperty("catalog") Optional<String> catalog,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("columns") List<ViewColumn> columns,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("owner") Optional<String> owner,
            @JsonProperty("runAsInvoker") boolean runAsInvoker)
    {
        this.originalSql = requireNonNull(originalSql, "originalSql is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
        this.comment = requireNonNull(comment, "comment is null");
        this.owner = requireNonNull(owner, "owner is null");
        this.runAsInvoker = runAsInvoker;
        if (catalog.isEmpty() && schema.isPresent()) {
            throw new IllegalArgumentException("catalog must be present if schema is present");
        }
        if (runAsInvoker && owner.isPresent()) {
            throw new IllegalArgumentException("owner cannot be present with runAsInvoker");
        }
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("columns list is empty");
        }
    }

    @JsonProperty
    public String getOriginalSql()
    {
        return originalSql;
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
    public List<ViewColumn> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonProperty
    public Optional<String> getOwner()
    {
        return owner;
    }

    @JsonProperty
    public boolean isRunAsInvoker()
    {
        return runAsInvoker;
    }

    public ConnectorViewDefinition withoutOwner()
    {
        return new ConnectorViewDefinition(
                originalSql,
                catalog,
                schema,
                columns,
                comment,
                Optional.empty(),
                runAsInvoker);
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        owner.ifPresent(value -> joiner.add("owner=" + value));
        comment.ifPresent(value -> joiner.add("comment=" + value));
        joiner.add("runAsInvoker=" + runAsInvoker);
        joiner.add("columns=" + columns);
        catalog.ifPresent(value -> joiner.add("catalog=" + value));
        schema.ifPresent(value -> joiner.add("schema=" + value));
        joiner.add("originalSql=[" + originalSql + "]");
        return getClass().getSimpleName() + joiner.toString();
    }

    public static final class ViewColumn
    {
        private final String name;
        private final TypeId type;
        private final Optional<String> comment;

        @JsonCreator
        public ViewColumn(
                @JsonProperty("name") String name,
                @JsonProperty("type") TypeId type,
                @JsonProperty("comment") Optional<String> comment)

        {
            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
            this.comment = requireNonNull(comment, "comment is null");
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public TypeId getType()
        {
            return type;
        }

        @JsonProperty
        public Optional<String> getComment()
        {
            return comment;
        }

        @Override
        public String toString()
        {
            StringJoiner joiner = new StringJoiner(", ", "[", "]");
            joiner.add("name=" + name);
            joiner.add("type=" + type);
            comment.ifPresent(value -> joiner.add("comment=" + value));
            return getClass().getSimpleName() + joiner;
        }
    }
}
