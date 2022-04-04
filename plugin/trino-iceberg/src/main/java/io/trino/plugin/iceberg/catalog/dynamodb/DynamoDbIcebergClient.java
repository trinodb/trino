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
package io.trino.plugin.iceberg.catalog.dynamodb;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.SchemaTableName;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.dynamodb.IcebergDynamoDbCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class DynamoDbIcebergClient
{
    private final IcebergDynamoDbCatalog dynamoCatalog;

    public DynamoDbIcebergClient(IcebergDynamoDbCatalog dynamoCatalog)
    {
        this.dynamoCatalog = requireNonNull(dynamoCatalog, "dynamoCatalog is null");
    }

    public List<String> getNamespaces()
    {
        return dynamoCatalog.listNamespaces()
                .stream().map(Namespace::toString)
                .collect(toImmutableList());
    }

    public Map<String, Object> getNamespaceProperties(String namespace)
    {
        return dynamoCatalog.loadNamespaceMetadata(Namespace.of(namespace)).entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public boolean namespaceExists(String namespace)
    {
        return dynamoCatalog.namespaceExists(Namespace.of(namespace));
    }

    public Optional<String> getNamespaceLocation(String namespace)
    {
        try {
            Map<String, String> properties = dynamoCatalog.loadNamespaceMetadata(Namespace.of(namespace));
            return Optional.ofNullable(properties.get("location"));
        }
        catch (NoSuchNamespaceException e) {
            return Optional.empty();
        }
    }

    public void createNamespace(String namespace, Map<String, Object> properties)
    {
        ImmutableMap.Builder<String, String> namespaceProperties = ImmutableMap.builderWithExpectedSize(properties.size());
        for (Entry<String, Object> property : properties.entrySet()) {
            namespaceProperties.put(property.getKey(), String.valueOf(property.getValue()));
        }
        dynamoCatalog.createNamespace(Namespace.of(namespace), namespaceProperties.buildOrThrow());
    }

    public void dropNamespace(String namespace)
    {
        dynamoCatalog.dropNamespace(Namespace.of(namespace));
    }

    public List<SchemaTableName> getTables(String namespace)
    {
        return dynamoCatalog.listTables(Namespace.of(namespace)).stream()
                .map(table -> new SchemaTableName(namespace, table.name()))
                .collect(toImmutableList());
    }

    public Optional<Table> getTable(String namespace, String tableName)
    {
        try {
            return Optional.of(dynamoCatalog.loadTable(TableIdentifier.of(namespace, tableName)));
        }
        catch (NoSuchTableException e) {
            return Optional.empty();
        }
    }

    public void createTable(String schemaName, String tableName, Schema schema, String metadataLocation)
    {
        dynamoCatalog.createTable(
                TableIdentifier.of(schemaName, tableName),
                schema,
                PartitionSpec.builderFor(schema).build(),
                Map.of("metadata_location", metadataLocation));
    }

    public void alterTable(String schemaName, String tableName, Schema schema, Map<String, String> metadata, String newMetadataLocation, String previousMetadataLocation)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.putAll(metadata);
        properties.put("metadata_location", newMetadataLocation);
        properties.put("previous_metadata_location", previousMetadataLocation);
        dynamoCatalog.newReplaceTableTransaction(
                        TableIdentifier.of(schemaName, tableName),
                        schema,
                        PartitionSpec.builderFor(schema).build(),
                        properties.buildOrThrow(),
                        false)
                .commitTransaction();
    }

    public Optional<String> getMetadataLocation(String schemaName, String tableName)
    {
        try {
            return Optional.ofNullable(dynamoCatalog.loadTable(TableIdentifier.of(schemaName, tableName)).properties().get("metadata_location"));
        }
        catch (NoSuchTableException e) {
            return Optional.empty();
        }
    }

    public void dropTable(String schemaName, String tableName)
    {
        dynamoCatalog.dropTable(TableIdentifier.of(schemaName, tableName));
    }

    public void renameTable(SchemaTableName from, SchemaTableName to)
    {
        dynamoCatalog.renameTable(TableIdentifier.of(from.getSchemaName(), from.getTableName()), TableIdentifier.of(to.getSchemaName(), to.getTableName()));
    }
}
