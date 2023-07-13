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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.security.Identity;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MaterializedViewDefinition
        extends ViewDefinition
{
    private final Optional<Duration> gracePeriod;
    private final Optional<CatalogSchemaTableName> storageTable;
    private final Map<String, Object> properties;

    public MaterializedViewDefinition(
            String originalSql,
            Optional<String> catalog,
            Optional<String> schema,
            List<ViewColumn> columns,
            Optional<Duration> gracePeriod,
            Optional<String> comment,
            Identity owner,
            Optional<CatalogSchemaTableName> storageTable,
            Map<String, Object> properties)
    {
        super(originalSql, catalog, schema, columns, comment, Optional.of(owner));
        checkArgument(gracePeriod.isEmpty() || !gracePeriod.get().isNegative(), "gracePeriod cannot be negative: %s", gracePeriod);
        this.gracePeriod = gracePeriod;
        this.storageTable = requireNonNull(storageTable, "storageTable is null");
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
    }

    public MaterializedViewDefinition(ConnectorMaterializedViewDefinition view, Identity runAsIdentity)
    {
        super(
                view.getOriginalSql(),
                view.getCatalog(),
                view.getSchema(),
                view.getColumns().stream()
                        .map(column -> new ViewColumn(column.getName(), column.getType(), Optional.empty()))
                        .collect(toImmutableList()),
                view.getComment(),
                Optional.of(runAsIdentity));
        this.gracePeriod = view.getGracePeriod();
        this.storageTable = view.getStorageTable();
        this.properties = ImmutableMap.copyOf(view.getProperties());
    }

    public Optional<Duration> getGracePeriod()
    {
        return gracePeriod;
    }

    public Optional<CatalogSchemaTableName> getStorageTable()
    {
        return storageTable;
    }

    public Map<String, Object> getProperties()
    {
        return properties;
    }

    public ConnectorMaterializedViewDefinition toConnectorMaterializedViewDefinition()
    {
        return new ConnectorMaterializedViewDefinition(
                getOriginalSql(),
                storageTable,
                getCatalog(),
                getSchema(),
                getColumns().stream()
                        .map(column -> new ConnectorMaterializedViewDefinition.Column(column.getName(), column.getType()))
                        .collect(toImmutableList()),
                getGracePeriod(),
                getComment(),
                getRunAsIdentity().map(Identity::getUser),
                properties);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).omitNullValues()
                .add("originalSql", getOriginalSql())
                .add("catalog", getCatalog().orElse(null))
                .add("schema", getSchema().orElse(null))
                .add("columns", getColumns())
                .add("gracePeriod", gracePeriod.orElse(null))
                .add("comment", getComment().orElse(null))
                .add("runAsIdentity", getRunAsIdentity())
                .add("storageTable", storageTable.orElse(null))
                .add("properties", properties)
                .toString();
    }
}
