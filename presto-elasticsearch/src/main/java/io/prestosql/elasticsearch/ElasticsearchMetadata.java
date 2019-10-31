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
import io.prestosql.elasticsearch.client.ElasticsearchClient;
import io.prestosql.elasticsearch.client.IndexMetadata;
import io.prestosql.elasticsearch.client.IndexMetadata.DateTimeType;
import io.prestosql.elasticsearch.client.IndexMetadata.ObjectType;
import io.prestosql.elasticsearch.client.IndexMetadata.PrimitiveType;
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
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class ElasticsearchMetadata
        implements ConnectorMetadata
{
    private static final String ORIGINAL_NAME = "original-name";

    private final ElasticsearchClient client;
    private final String schemaName;

    @Inject
    public ElasticsearchMetadata(ElasticsearchClient client, ElasticsearchConfig config)
    {
        requireNonNull(config, "config is null");

        this.client = requireNonNull(client, "client is null");
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

        for (IndexMetadata.Field field : metadata.getSchema().getFields()) {
            Type type = toPrestoType(field.getType());
            if (type == null) {
                continue;
            }

            result.add(makeColumnMetadata(field.getName(), type));
        }

        return result.build();
    }

    private Type toPrestoType(IndexMetadata.Type type)
    {
        if (type instanceof PrimitiveType) {
            switch (((PrimitiveType) type).getName()) {
                case "float":
                    return REAL;
                case "double":
                    return DOUBLE;
                case "byte":
                    return TINYINT;
                case "short":
                    return SMALLINT;
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
            }
        }
        else if (type instanceof DateTimeType) {
            if (((DateTimeType) type).getFormats().isEmpty()) {
                return TIMESTAMP;
            }
            // otherwise, skip -- we don't support custom formats, yet
        }
        else if (type instanceof ObjectType) {
            ObjectType objectType = (ObjectType) type;

            List<RowType.Field> fields = objectType.getFields().stream()
                    .map(field -> RowType.field(field.getName(), toPrestoType(field.getType())))
                    .collect(toImmutableList());

            return RowType.from(fields);
        }

        return null;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent() && !schemaName.get().equals(this.schemaName)) {
            return ImmutableList.of();
        }

        return client.getIndexes().stream()
                .map(index -> new SchemaTableName(this.schemaName, index))
                .collect(toImmutableList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> results = ImmutableMap.builder();

        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            results.put(column.getName(), new ElasticsearchColumnHandle(
                    (String) column.getProperties().getOrDefault(ORIGINAL_NAME, column.getName()),
                    column.getType()));
        }

        return results.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ElasticsearchColumnHandle handle = (ElasticsearchColumnHandle) columnHandle;
        return makeColumnMetadata(handle.getName(), handle.getType());
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
                handle.getSchema(),
                handle.getIndex(),
                handle.getConstraint(),
                handle.getQuery());

        return Optional.of(new ConstraintApplicationResult<>(handle, constraint.getSummary()));
    }

    private static ColumnMetadata makeColumnMetadata(String name, Type type)
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setProperties(ImmutableMap.of(ORIGINAL_NAME, name))
                .build();
    }
}
