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

import io.trino.spi.type.TypeId;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

import static io.trino.spi.connector.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public record ConnectorMaterializedViewDefinition(
        String originalSql,
        Optional<CatalogSchemaTableName> storageTable,
        Optional<String> catalog,
        Optional<String> schema,
        List<Column> columns,
        Optional<Duration> gracePeriod,
        Optional<String> comment,
        Optional<String> owner,
        List<CatalogSchemaName> path)
{
    public ConnectorMaterializedViewDefinition(
            String originalSql,
            Optional<CatalogSchemaTableName> storageTable,
            Optional<String> catalog,
            Optional<String> schema,
            List<Column> columns,
            Optional<Duration> gracePeriod,
            Optional<String> comment,
            Optional<String> owner,
            List<CatalogSchemaName> path)
    {
        this.originalSql = requireNonNull(originalSql, "originalSql is null");
        this.storageTable = requireNonNull(storageTable, "storageTable is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
        checkArgument(gracePeriod.isEmpty() || !gracePeriod.get().isNegative(), "gracePeriod cannot be negative: %s", gracePeriod);
        this.gracePeriod = gracePeriod;
        this.comment = requireNonNull(comment, "comment is null");
        this.owner = requireNonNull(owner, "owner is null");
        this.path = List.copyOf(path);

        if (catalog.isEmpty() && schema.isPresent()) {
            throw new IllegalArgumentException("catalog must be present if schema is present");
        }
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("columns list is empty");
        }
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        joiner.add("originalSql=[" + originalSql + "]");
        storageTable.ifPresent(value -> joiner.add("storageTable=" + value));
        catalog.ifPresent(value -> joiner.add("catalog=" + value));
        schema.ifPresent(value -> joiner.add("schema=" + value));
        joiner.add("columns=" + columns);
        gracePeriod.ifPresent(_ -> joiner.add("gracePeriod=" + gracePeriod));
        comment.ifPresent(value -> joiner.add("comment=" + value));
        joiner.add("owner=" + owner);
        joiner.add(path.stream().map(CatalogSchemaName::toString).collect(joining(", ", "path=(", ")")));
        return getClass().getSimpleName() + joiner;
    }

    public record Column(String name, TypeId type, Optional<String> comment)
    {
        public Column(String name, TypeId type, Optional<String> comment)
        {
            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
            this.comment = requireNonNull(comment, "comment is null");
        }

        @Override
        public String toString()
        {
            return name + " " + type;
        }
    }
}
