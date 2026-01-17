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
package io.trino.plugin.weaviate;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.weaviate.client6.v1.api.collections.CollectionConfig;
import io.weaviate.client6.v1.api.collections.Property;
import io.weaviate.client6.v1.api.collections.VectorConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.weaviate.WeaviateColumnHandle.METADATA_COLUMNS;
import static io.trino.plugin.weaviate.WeaviateMetadata.DEFAULT_SCHEMA;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public record WeaviateTableHandle(
        Optional<String> tenant,
        String tableName,
        String comment,
        OptionalLong limit,
        List<WeaviateColumnHandle> columns)
        implements ConnectorTableHandle
{
    public WeaviateTableHandle
    {
        requireNonNull(tenant, "tenant is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(limit, "limit is null");
        columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    public WeaviateTableHandle(CollectionConfig collection)
    {
        this(
                Optional.of(DEFAULT_SCHEMA),
                requireNonNull(collection, "collection is null").collectionName(),
                collection.description(),
                OptionalLong.empty(),
                makeColumns(collection.properties(), collection.vectors()));
    }

    private static List<WeaviateColumnHandle> makeColumns(List<Property> properties, Map<String, VectorConfig> vectors)
    {
        requireNonNull(properties, "properties is null");
        requireNonNull(vectors, "vectors is null");

        ImmutableList.Builder<WeaviateColumnHandle> columns = ImmutableList.builder();
        columns.addAll(METADATA_COLUMNS);
        if (!vectors.isEmpty()) {
            columns.add(new WeaviateColumnHandle(vectors));
        }
        properties.stream().map(WeaviateColumnHandle::new).forEach(columns::add);
        return columns.build();
    }

    public WeaviateTableHandle withTenant(String tenant)
    {
        requireNonNull(tenant, "tenant is null");
        return new WeaviateTableHandle(Optional.of(tenant), tableName, comment, limit, columns);
    }

    public WeaviateTableHandle withLimit(long limit)
    {
        return new WeaviateTableHandle(tenant, tableName, comment, OptionalLong.of(limit), columns);
    }

    public String tenantIgnoreDefault()
    {
        return tenant.isPresent() && !tenant.get().equals(DEFAULT_SCHEMA)
                ? tenant.get()
                : null;
    }

    ConnectorTableMetadata tableMetadata()
    {
        SchemaTableName name = new SchemaTableName(this.tenant.orElse(DEFAULT_SCHEMA), this.tableName);
        Optional<String> comment = Optional.ofNullable(this.comment);
        List<ColumnMetadata> columns = columnsMetadata();
        return new ConnectorTableMetadata(name, columns, emptyMap(), comment);
    }

    List<ColumnMetadata> columnsMetadata()
    {
        return this.columns.stream().map(WeaviateColumnHandle::columnMetadata).toList();
    }
}
