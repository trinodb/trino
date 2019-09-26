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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;

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

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_CORRUPTED_MAPPING_METADATA;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ElasticsearchMetadata
        implements ConnectorMetadata
{
    private static final Logger LOG = Logger.get(ElasticsearchClient.class);
    private final ObjectMapper objectMapper = new ObjectMapperProvider().get();

    private final ElasticsearchTableDescriptionProvider tableDescriptions;
    private final LoadingCache<ElasticsearchTableDescription, List<ColumnMetadata>> columnMetadataCache;
    private final ExecutorService executor = newSingleThreadExecutor(daemonThreadsNamed("elasticsearch-metadata-%s"));

    private final ElasticsearchClient client;

    @Inject
    public ElasticsearchMetadata(ElasticsearchClient client, ElasticsearchTableDescriptionProvider descriptions)
    {
        this.client = requireNonNull(client, "client is null");
        this.tableDescriptions = requireNonNull(descriptions, "descriptions is null");

        this.columnMetadataCache = CacheBuilder.newBuilder()
                .expireAfterWrite(30, MINUTES)
                .refreshAfterWrite(15, MINUTES)
                .maximumSize(500)
                .build(asyncReloading(CacheLoader.from(this::loadColumns), executor));
    }

    @PreDestroy
    public void close()
    {
        executor.shutdown();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return tableDescriptions.getAllSchemaTableNames()
                .stream()
                .map(SchemaTableName::getSchemaName)
                .collect(toImmutableList());
    }

    @Override
    public ElasticsearchTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        ElasticsearchTableDescription table = getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new ElasticsearchTableHandle(table.getIndex(), table.getType(), tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;
        SchemaTableName tableName = new SchemaTableName(handle.getSchemaName(), handle.getTableName());
        return getTableMetadata(tableName).get();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return tableDescriptions.getAllSchemaTableNames()
                .stream()
                .filter(schemaTableName -> !schemaName.isPresent() || schemaTableName.getSchemaName().equals(schemaName.get()))
                .collect(toImmutableList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) tableHandle;

        ElasticsearchTableDescription table = getTable(handle.getSchemaName(), handle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(handle.getSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : getColumnMetadata(table)) {
            Map<String, Object> properties = column.getProperties();
            int ordinalPosition = (Integer) properties.get("ordinalPosition");
            int position = ordinalPosition == -1 ? index : ordinalPosition;
            columnHandles.put(column.getName(),
                    new ElasticsearchColumnHandle(
                            String.valueOf(properties.get("originalColumnName")),
                            column.getType(),
                            String.valueOf(properties.get("jsonPath")),
                            position,
                            (Boolean) properties.get("isList")));
            index++;
        }
        return columnHandles.build();
    }

    private List<ColumnMetadata> getColumnMetadata(ElasticsearchTableDescription table)
    {
        try {
            return columnMetadataCache.getUnchecked(table);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((ElasticsearchColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            Optional<ConnectorTableMetadata> tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata.isPresent()) {
                columns.put(tableName, tableMetadata.get().getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;

        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new ElasticsearchTableHandle(
                handle.getIndex(),
                handle.getType(),
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getConstraint());

        return Optional.of(new ConstraintApplicationResult<>(handle, constraint.getSummary()));
    }

    private Optional<ConnectorTableMetadata> getTableMetadata(SchemaTableName tableName)
    {
        ElasticsearchTableDescription table = getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return Optional.empty();
        }
        return Optional.of(new ConnectorTableMetadata(tableName, getColumnMetadata(table)));
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return prefix.toOptionalSchemaTableName()
                .map(schemaTableName -> (List<SchemaTableName>) ImmutableList.of(schemaTableName))
                .orElseGet(() -> listTables(session, prefix.getSchema()));
    }

    private ElasticsearchTableDescription getTable(String schemaName, String tableName)
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
                table.getIndex(),
                table.getType(),
                Optional.of(buildColumns(table)));
    }

    private List<ColumnMetadata> loadColumns(ElasticsearchTableDescription table)
    {
        if (table.getColumns().isPresent()) {
            return buildMetadata(table.getColumns().get());
        }
        return buildMetadata(buildColumns(table));
    }

    private List<ColumnMetadata> buildMetadata(List<ElasticsearchColumn> columns)
    {
        List<ColumnMetadata> result = new ArrayList<>();
        for (ElasticsearchColumn column : columns) {
            Map<String, Object> properties = new HashMap<>();
            properties.put("originalColumnName", column.getName());
            properties.put("jsonPath", column.getJsonPath());
            properties.put("isList", column.isList());
            properties.put("ordinalPosition", column.getOrdinalPosition());
            result.add(new ColumnMetadata(column.getName(), column.getType(), "", "", false, properties));
        }
        return result;
    }

    private List<ElasticsearchColumn> buildColumns(ElasticsearchTableDescription tableDescription)
    {
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = client.getMappings(tableDescription.getIndex(), tableDescription.getType());

        List<ElasticsearchColumn> columns = new ArrayList<>();
        Iterator<String> indexIterator = mappings.keysIt();
        while (indexIterator.hasNext()) {
            // TODO use io.airlift.json.JsonCodec
            MappingMetaData mappingMetaData = mappings.get(indexIterator.next()).get(tableDescription.getType());
            JsonNode rootNode;
            try {
                rootNode = objectMapper.readTree(mappingMetaData.source().uncompressed());
            }
            catch (IOException e) {
                throw new PrestoException(ELASTICSEARCH_CORRUPTED_MAPPING_METADATA, e);
            }
            // parse field mapping JSON: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-field-mapping.html
            JsonNode mappingNode = rootNode.get(tableDescription.getType());
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
                    columns.add(new ElasticsearchColumn(nestedName, getPrestoType(typeName), nestedName, arrays.contains(nestedName), -1));
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
        Map.Entry<String, Type> first = fieldsMap.firstEntry();
        String field = first.getKey();
        Type type = first.getValue();
        if (field.contains(".")) {
            String prefix = field.substring(0, field.lastIndexOf('.'));
            ImmutableList.Builder<RowType.Field> fieldsBuilder = ImmutableList.builder();
            int size = field.split("\\.").length;
            Iterator<String> iterator = fieldsMap.navigableKeySet().iterator();
            while (iterator.hasNext()) {
                String name = iterator.next();
                if (name.split("\\.").length == size && name.startsWith(prefix)) {
                    Optional<String> columnName = Optional.of(name.substring(name.lastIndexOf('.') + 1));
                    Type columnType = fieldsMap.get(name);
                    RowType.Field column = new RowType.Field(columnName, columnType);
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
                columns.add(new ElasticsearchColumn(field, type, field, arrays.contains(field), -1));
            }
            fieldsMap.remove(field);
        }
        processNestedFields(fieldsMap, columns, arrays);
    }

    private List<String> getColumnMetadata(Optional<String> parent, JsonNode propertiesNode)
    {
        ImmutableList.Builder<String> metadata = ImmutableList.builder();
        Iterator<Map.Entry<String, JsonNode>> iterator = propertiesNode.fields();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
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
                metadata.add(childKey.concat(":").concat(value.textValue()));
            }
        }
        return metadata.build();
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
        FieldNestingComparator()
        {
        }

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
