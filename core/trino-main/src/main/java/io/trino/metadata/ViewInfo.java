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

import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;

public final class ViewInfo
{
    private final String originalSql;
    private final List<ViewColumn> columns;
    private final Optional<String> comment;
    private final Optional<CatalogSchemaTableName> storageTable;

    public ViewInfo(ConnectorViewDefinition viewDefinition)
    {
        this.originalSql = viewDefinition.getOriginalSql();
        this.columns = viewDefinition.getColumns().stream()
                .map(column -> new ViewColumn(column.getName(), column.getType(), column.getComment()))
                .collect(toImmutableList());
        this.comment = viewDefinition.getComment();
        this.storageTable = Optional.empty();
    }

    public ViewInfo(ConnectorMaterializedViewDefinition viewDefinition)
    {
        this.originalSql = viewDefinition.getOriginalSql();
        this.columns = viewDefinition.getColumns().stream()
                .map(column -> new ViewColumn(column.getName(), column.getType(), Optional.empty()))
                .collect(toImmutableList());
        this.comment = viewDefinition.getComment();
        this.storageTable = viewDefinition.getStorageTable();
    }

    public String getOriginalSql()
    {
        return originalSql;
    }

    public List<ViewColumn> getColumns()
    {
        return columns;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    public Optional<CatalogSchemaTableName> getStorageTable()
    {
        return storageTable;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).omitNullValues()
                .add("originalSql", originalSql)
                .add("columns", columns)
                .add("comment", comment.orElse(null))
                .add("storageTable", storageTable.orElse(null))
                .toString();
    }
}
