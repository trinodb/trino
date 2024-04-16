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
package io.trino.metadata;

import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.security.Identity;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ViewDefinition
{
    private final String originalSql;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final List<ViewColumn> columns;
    private final Optional<String> comment;
    private final Optional<Identity> runAsIdentity;
    private final List<CatalogSchemaName> path;

    public ViewDefinition(
            String originalSql,
            Optional<String> catalog,
            Optional<String> schema,
            List<ViewColumn> columns,
            Optional<String> comment,
            Optional<Identity> runAsIdentity,
            List<CatalogSchemaName> path)
    {
        this.originalSql = requireNonNull(originalSql, "originalSql is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
        this.comment = requireNonNull(comment, "comment is null");
        this.runAsIdentity = requireNonNull(runAsIdentity, "runAsIdentity is null");
        this.path = requireNonNull(path, "path is null");
        checkArgument(schema.isEmpty() || catalog.isPresent(), "catalog must be present if schema is present");
        checkArgument(!columns.isEmpty(), "columns list is empty");
    }

    public String getOriginalSql()
    {
        return originalSql;
    }

    public Optional<String> getCatalog()
    {
        return catalog;
    }

    public Optional<String> getSchema()
    {
        return schema;
    }

    public List<ViewColumn> getColumns()
    {
        return columns;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    public boolean isRunAsInvoker()
    {
        return runAsIdentity.isEmpty();
    }

    public Optional<Identity> getRunAsIdentity()
    {
        return runAsIdentity;
    }

    public List<CatalogSchemaName> getPath()
    {
        return path;
    }

    public ConnectorViewDefinition toConnectorViewDefinition()
    {
        return new ConnectorViewDefinition(
                originalSql,
                catalog,
                schema,
                columns.stream()
                        .map(column -> new ConnectorViewDefinition.ViewColumn(column.name(), column.type(), column.comment()))
                        .collect(toImmutableList()),
                comment,
                runAsIdentity.map(Identity::getUser),
                runAsIdentity.isEmpty(),
                path);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).omitNullValues()
                .add("originalSql", originalSql)
                .add("catalog", catalog.orElse(null))
                .add("schema", schema.orElse(null))
                .add("columns", columns)
                .add("comment", comment.orElse(null))
                .add("runAsIdentity", runAsIdentity.orElse(null))
                .add("path", path)
                .toString();
    }
}
