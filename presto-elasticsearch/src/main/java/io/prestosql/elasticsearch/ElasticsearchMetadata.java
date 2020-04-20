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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.prestosql.elasticsearch.client.ElasticsearchClient;
import io.prestosql.elasticsearch.client.IndexMetadata;
import io.prestosql.elasticsearch.client.types.ElasticField;
import io.prestosql.elasticsearch.client.types.ObjectFieldType;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.LimitApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ElasticsearchMetadata
        implements ConnectorMetadata
{
    private static final String ORIGINAL_NAME = "original-name";
    public static final String SUPPORTS_PREDICATES = "supports-predicates";

    private final ElasticsearchTypeHandler typeHandler;
    private final ElasticsearchClient client;
    private final String schemaName;

    @Inject
    public ElasticsearchMetadata(TypeManager typeManager, ElasticsearchClient client, ElasticsearchConfig config)
    {
        requireNonNull(typeManager, "typeManager is null");
        this.typeHandler = new ElasticsearchTypeHandler(typeManager);
        this.client = requireNonNull(client, "client is null");
        requireNonNull(config, "config is null");
        this.schemaName = config.getDefaultSchema();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(schemaName);
    }

    @Override
    public ElasticsearchTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");

        if (tableName.getSchemaName().equals(schemaName)) {
            String[] parts = tableName.getTableName().split(":", 2);
            String table = parts[0];
            Optional<String> query = Optional.empty();
            if (parts.length == 2) {
                query = Optional.of(parts[1]);
            }

            if (listTables(session, Optional.of(schemaName)).contains(new SchemaTableName(schemaName, table))) {
                return new ElasticsearchTableHandle(schemaName, table, query);
            }
        }

        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;
        return getTableMetadata(handle.getSchema(), handle.getIndex());
    }

    private ConnectorTableMetadata getTableMetadata(String schemaName, String tableName)
    {
        IndexMetadata metadata = client.getIndexMetadata(tableName);

        return new ConnectorTableMetadata(
                new SchemaTableName(schemaName, tableName),
                toColumnMetadata(metadata));
    }

    private List<ColumnMetadata> toColumnMetadata(IndexMetadata metadata)
    {
        ImmutableList.Builder<ColumnMetadata> result = ImmutableList.builder();

        result.add(BuiltinColumns.ID.getMetadata());
        result.add(BuiltinColumns.SOURCE.getMetadata());
        result.add(BuiltinColumns.SCORE.getMetadata());

        Map<String, Long> counts = metadata.getSchema()
                .getFields().stream()
                .collect(Collectors.groupingBy(f -> f.getName().toLowerCase(ENGLISH), Collectors.counting()));

        for (ElasticField field : metadata.getSchema().getFields()) {
            Type type = typeHandler.toPrestoType(field);
            if (type == null || counts.get(field.getName().toLowerCase(ENGLISH)) > 1) {
                continue;
            }

            result.add(makeColumnMetadata(field.getName(), type, field.getType().supportsPredicates()));
        }

        return result.build();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent() && !schemaName.get().equals(this.schemaName)) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<SchemaTableName> result = ImmutableList.builder();

        client.getIndexes().stream()
                .map(index -> new SchemaTableName(this.schemaName, index))
                .forEach(result::add);

        client.getAliases().stream()
                .map(index -> new SchemaTableName(this.schemaName, index))
                .forEach(result::add);

        return result.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> results = ImmutableMap.builder();

        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            results.put(column.getName(), new ElasticsearchColumnHandle(
                    (String) column.getProperties().getOrDefault(ORIGINAL_NAME, column.getName()),
                    column.getType(),
                    (Boolean) column.getProperties().get(SUPPORTS_PREDICATES)));
        }

        return results.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ElasticsearchColumnHandle handle = (ElasticsearchColumnHandle) columnHandle;
        return makeColumnMetadata(handle.getName(), handle.getType(), handle.isSupportsPredicates());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchema().isPresent() && !prefix.getSchema().get().equals(schemaName)) {
            return ImmutableMap.of();
        }

        if (prefix.getSchema().isPresent() && prefix.getTable().isPresent()) {
            ConnectorTableMetadata metadata = getTableMetadata(prefix.getSchema().get(), prefix.getTable().get());
            return ImmutableMap.of(metadata.getTable(), metadata.getColumns());
        }

        return listTables(session, prefix.getSchema()).stream()
                .map(name -> getTableMetadata(name.getSchemaName(), name.getTableName()))
                .collect(toImmutableMap(ConnectorTableMetadata::getTable, ConnectorTableMetadata::getColumns));
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;

        return new ConnectorTableProperties(
                handle.getConstraint(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;

        if (handle.getLimit().isPresent() && handle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        handle = new ElasticsearchTableHandle(
                handle.getSchema(),
                handle.getIndex(),
                handle.getConstraint(),
                handle.getQuery(),
                OptionalLong.of(limit));

        return Optional.of(new LimitApplicationResult<>(handle, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;

        Map<ColumnHandle, Domain> supported = new HashMap<>();
        Map<ColumnHandle, Domain> unsupported = new HashMap<>();
        if (constraint.getSummary().getDomains().isPresent()) {
            for (Map.Entry<ColumnHandle, Domain> entry : constraint.getSummary().getDomains().get().entrySet()) {
                ElasticsearchColumnHandle column = (ElasticsearchColumnHandle) entry.getKey();

                if (column.isSupportsPredicates()) {
                    supported.put(column, entry.getValue());
                }
                else {
                    unsupported.put(column, entry.getValue());
                }
            }
        }

        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(TupleDomain.withColumnDomains(supported));
        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new ElasticsearchTableHandle(
                handle.getSchema(),
                handle.getIndex(),
                newDomain,
                handle.getQuery(),
                handle.getLimit());

        return Optional.of(new ConstraintApplicationResult<>(handle, TupleDomain.withColumnDomains(unsupported)));
    }

    private static ColumnMetadata makeColumnMetadata(String name, Type type, boolean supportsPredicates)
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setProperties(ImmutableMap.of(
                        ORIGINAL_NAME, name,
                        SUPPORTS_PREDICATES, supportsPredicates))
                .build();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        client.createTemplate(tableMetadata.getTable().getTableName(), toIndexMetadata(tableMetadata.getColumns()), ignoreExisting);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) tableHandle;
        client.deleteIndex(handle.getIndex());
    }

    private IndexMetadata toIndexMetadata(List<ColumnMetadata> metadata)
    {
        List<ElasticField> fields = metadata.stream()
                .map(col -> new ElasticField(false, col.getName(), typeHandler.toElasticType(col.getType())))
                .collect(Collectors.toList());

        IndexMetadata indexMetadata = new IndexMetadata(new ObjectFieldType(fields));

        return indexMetadata;
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session,
                                                       ConnectorTableMetadata tableMetadata,
                                                       Optional<ConnectorNewTableLayout> layout)
    {
        SchemaTableName table = tableMetadata.getTable();
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnNames.add(column.getName());
            columnTypes.add(column.getType());
        }

        return new ElasticsearchOutputTableHandle(table.getSchemaName(), table.getTableName(), columnNames.build(), columnTypes.build());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }
}
