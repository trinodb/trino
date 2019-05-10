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
package io.prestosql.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.RowType.Field;
import io.prestosql.spi.type.Type;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ElasticsearchClient
{
    private static final Logger LOG = Logger.get(ElasticsearchClient.class);

    private final ExecutorService executor = newFixedThreadPool(1, daemonThreadsNamed("elasticsearch-metadata-%s"));
    private final ElasticsearchTableDescriptionProvider tableDescriptions;
    private final LoadingCache<ElasticsearchTableDescription, List<ColumnMetadata>> columnMetadataCache;
    private final ElasticsearchMetadataFetcher metadataFetcher;

    @Inject
    public ElasticsearchClient(ElasticsearchTableDescriptionProvider descriptions, ElasticsearchMetadataFetcher metadataFetcher)
            throws IOException
    {
        tableDescriptions = requireNonNull(descriptions, "description is null");
        this.metadataFetcher = metadataFetcher;
        this.columnMetadataCache = CacheBuilder.newBuilder()
                .expireAfterWrite(30, MINUTES)
                .refreshAfterWrite(15, MINUTES)
                .maximumSize(500)
                .build(asyncReloading(CacheLoader.from(this::loadColumns), executor));

        // Set the property so that we don't hit the issue with multiple Netty clients in same JVM
        // More at: https://github.com/elastic/elasticsearch/issues/25741
        System.setProperty("es.set.netty.runtime.available.processors", "false");
    }

    @PreDestroy
    public void tearDown()
    {
        executor.shutdown();
    }

    public List<String> listSchemas()
    {
        return tableDescriptions.getAllSchemaTableNames()
                .stream()
                .map(SchemaTableName::getSchemaName)
                .collect(toImmutableList());
    }

    public List<SchemaTableName> listTables(Optional<String> schemaName)
    {
        return tableDescriptions.getAllSchemaTableNames()
                .stream()
                .filter(schemaTableName -> !schemaName.isPresent() || schemaTableName.getSchemaName().equals(schemaName.get()))
                .collect(toImmutableList());
    }

    private List<ColumnMetadata> loadColumns(ElasticsearchTableDescription table)
    {
        if (table.getColumns().isPresent()) {
            return buildMetadata(table.getColumns().get());
        }
        return buildMetadata(buildColumns(table));
    }

    public List<ColumnMetadata> getColumnMetadata(ElasticsearchTableDescription tableDescription)
    {
        return columnMetadataCache.getUnchecked(tableDescription);
    }

    public ElasticsearchTableDescription getTable(String schemaName, String tableName)
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        ElasticsearchTableDescription table = tableDescriptions.get(new SchemaTableName(schemaName, tableName));
        if (table == null) {
            return null;
        }
        if (table.getColumns().isPresent()) {
            return table;
        }
        return new ElasticsearchTableDescription(
                table.getTableName(),
                table.getSchemaName(),
                table.getHost(),
                table.getPort(),
                table.isHttpPort(),
                table.getPathPrefix(),
                table.getHeaders(),
                table.getClusterName(),
                table.getIndex(),
                table.getIndexExactMatch(),
                table.getType(),
                Optional.of(buildColumns(table)));
    }

    public List<String> getIndices(ElasticsearchTableDescription tableDescription)
    {
        if (tableDescription.getIndexExactMatch()) {
            return ImmutableList.of(tableDescription.getIndex());
        }
        return metadataFetcher.fetchIndices(tableDescription);
    }

    public List<ElasticsearchShard> getShards(String index, ElasticsearchTableDescription tableDescription)
    {
        return metadataFetcher.fetchShards(index, tableDescription);
    }

    private List<ColumnMetadata> buildMetadata(List<ElasticsearchColumn> columns)
    {
        List<ColumnMetadata> result = new ArrayList<>();
        for (ElasticsearchColumn column : columns) {
            Map<String, Object> properties = new HashMap<>();
            properties.put("originalColumnName", column.getName());
            properties.put("jsonPath", column.getJsonPath());
            properties.put("jsonType", column.getJsonType());
            properties.put("isList", column.isList());
            properties.put("ordinalPosition", column.getOrdinalPosition());
            result.add(new ColumnMetadata(column.getName(), column.getType(), "", "", false, properties));
        }
        return result;
    }

    private List<ElasticsearchColumn> buildColumns(ElasticsearchTableDescription tableDescription)
    {
        List<ElasticsearchColumn> columns = new ArrayList<>();
        for (String index : getIndices(tableDescription)) {
            JsonNode mappingNode = metadataFetcher.fetchMapping(index, tableDescription);

            // parse field mapping JSON: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-field-mapping.html
            JsonNode propertiesNode = mappingNode.get("properties");

            List<String> lists = new ArrayList<>();
            JsonNode metaNode = mappingNode.get("_meta");
            if (metaNode != null) {
                JsonNode arrayNode = metaNode.get("lists");
                if (arrayNode != null && arrayNode.isArray()) {
                    ArrayNode arrays = (ArrayNode) arrayNode;
                    for (int i = 0; i < arrays.size(); i++) {
                        lists.add(arrays.get(i).textValue());
                    }
                }
            }
            populateColumns(propertiesNode, lists, columns);
        }
        return columns;
    }

    private List<String> getColumnMetadata(Optional<String> parent, JsonNode propertiesNode)
    {
        ImmutableList.Builder<String> metadata = ImmutableList.builder();
        Iterator<Entry<String, JsonNode>> iterator = propertiesNode.fields();
        while (iterator.hasNext()) {
            Entry<String, JsonNode> entry = iterator.next();
            String key = entry.getKey();
            JsonNode value = entry.getValue();
            String childKey;
            if (parent.isPresent()) {
                if (parent.get().isEmpty()) {
                    childKey = key;
                }
                else {
                    childKey = parent.get().concat(".").concat(key);
                }
            }
            else {
                childKey = key;
            }

            if (value.isObject()) {
                metadata.addAll(getColumnMetadata(Optional.of(childKey), value));
                continue;
            }

            if (!value.isArray()) {
                metadata.add(childKey.concat(":").concat(value.asText()));
            }
        }
        return metadata.build();
    }

    private void populateColumns(JsonNode propertiesNode, List<String> arrays, List<ElasticsearchColumn> columns)
    {
        FieldNestingComparator comparator = new FieldNestingComparator();
        TreeMap<String, Type> fieldsMap = new TreeMap<>(comparator);
        for (String columnMetadata : getColumnMetadata(Optional.empty(), propertiesNode)) {
            int delimiterIndex = columnMetadata.lastIndexOf(":");
            if (delimiterIndex == -1 || delimiterIndex == columnMetadata.length() - 1) {
                LOG.debug("Invalid column path format: %s", columnMetadata);
                continue;
            }
            String fieldName = columnMetadata.substring(0, delimiterIndex);
            String typeName = columnMetadata.substring(delimiterIndex + 1);

            if (!fieldName.endsWith(".type")) {
                LOG.debug("Ignoring column with no type info: %s", columnMetadata);
                continue;
            }
            String propertyName = fieldName.substring(0, fieldName.lastIndexOf('.'));
            String nestedName = propertyName.replaceAll("properties\\.", "");
            if (nestedName.contains(".")) {
                fieldsMap.put(nestedName, getPrestoType(typeName));
            }
            else {
                boolean newColumnFound = columns.stream()
                        .noneMatch(column -> column.getName().equalsIgnoreCase(nestedName));
                if (newColumnFound) {
                    columns.add(new ElasticsearchColumn(nestedName, getPrestoType(typeName), nestedName, typeName, arrays.contains(nestedName), -1));
                }
            }
        }
        processNestedFields(fieldsMap, columns, arrays);
    }

    private void processNestedFields(TreeMap<String, Type> fieldsMap, List<ElasticsearchColumn> columns, List<String> arrays)
    {
        if (fieldsMap.size() == 0) {
            return;
        }
        Entry<String, Type> first = fieldsMap.firstEntry();
        String field = first.getKey();
        Type type = first.getValue();
        if (field.contains(".")) {
            String prefix = field.substring(0, field.lastIndexOf('.'));
            ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();
            int size = field.split("\\.").length;
            Iterator<String> iterator = fieldsMap.navigableKeySet().iterator();
            while (iterator.hasNext()) {
                String name = iterator.next();
                if (name.split("\\.").length == size && name.startsWith(prefix)) {
                    Optional<String> columnName = Optional.of(name.substring(name.lastIndexOf('.') + 1));
                    Type columnType = fieldsMap.get(name);
                    Field column = new Field(columnName, columnType);
                    fieldsBuilder.add(column);
                    iterator.remove();
                }
            }
            fieldsMap.put(prefix, RowType.from(fieldsBuilder.build()));
        }
        else {
            boolean newColumnFound = columns.stream()
                    .noneMatch(column -> column.getName().equalsIgnoreCase(field));
            if (newColumnFound) {
                columns.add(new ElasticsearchColumn(field, type, field, type.getDisplayName(), arrays.contains(field), -1));
            }
            fieldsMap.remove(field);
        }
        processNestedFields(fieldsMap, columns, arrays);
    }

    private static Type getPrestoType(String elasticsearchType)
    {
        switch (elasticsearchType) {
            case "double":
            case "float":
                return DOUBLE;
            case "integer":
                return INTEGER;
            case "long":
                return BIGINT;
            case "string":
            case "text":
            case "keyword":
                return VARCHAR;
            case "boolean":
                return BOOLEAN;
            case "binary":
                return VARBINARY;
            default:
                throw new IllegalArgumentException("Unsupported type: " + elasticsearchType);
        }
    }

    private static class FieldNestingComparator
            implements Comparator<String>
    {
        FieldNestingComparator() {}

        @Override
        public int compare(String left, String right)
        {
            // comparator based on levels of nesting
            int leftLength = left.split("\\.").length;
            int rightLength = right.split("\\.").length;
            if (leftLength == rightLength) {
                return left.compareTo(right);
            }
            return rightLength - leftLength;
        }
    }
}
