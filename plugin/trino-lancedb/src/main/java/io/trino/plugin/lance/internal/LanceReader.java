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
package io.trino.plugin.lance.internal;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.DatasetFragment;
import com.lancedb.lancedb.Connection;
import io.trino.plugin.lance.LanceColumnHandle;
import io.trino.plugin.lance.LanceConfig;
import io.trino.plugin.lance.LanceTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class LanceReader
{
    // TODO: support schema
    public static final String SCHEMA = "default";
    private static final String TABLE_PATH_SUFFIX = ".lance";
    private static final BufferAllocator allocator = new RootAllocator(
            RootAllocator.configBuilder().from(RootAllocator.defaultConfig()).maxAllocation(4 * 1024 * 1024).build());

    private final Path dbPath;
    // TODO: revisit whether we want to keep long running connection or create one connection per
    private final Connection conn;

    public LanceReader(LanceConfig lanceConfig)
    {
        URI lanceDbURI = lanceConfig.getLanceDbUri();
        dbPath = Path.of(lanceDbURI);
        conn = Connection.connect(lanceDbURI.toString());
    }

    public List<SchemaTableName> listTables(ConnectorSession session, String schema)
    {
        if (SCHEMA.equals(schema)) {
            return conn.tableNames().stream().map(e -> new SchemaTableName(schema, e)).collect(Collectors.toList());
        }
        else {
            return Collections.emptyList();
        }
    }

    public List<ColumnMetadata> getColumnsMetadata(String tableName)
    {
        Map<String, ColumnHandle> columnHandlers = this.getColumnHandle(tableName);
        return columnHandlers.values().stream().map(c -> ((LanceColumnHandle) c).getColumnMetadata())
                .collect(toImmutableList());
    }

    public Map<String, ColumnHandle> getColumnHandle(String tableName)
    {
        Path tablePath = getTablePath(dbPath, tableName);
        Schema arrowSchema = getSchema(tablePath);
        return arrowSchema.getFields().stream().collect(Collectors.toMap(Field::getName,
                f -> new LanceColumnHandle(f.getName(), LanceColumnHandle.toTrinoType(f.getFieldType().getType()),
                        f.getFieldType())));
    }

    public Path getTablePath(ConnectorSession session, SchemaTableName schemaTableName)
    {
        List<SchemaTableName> schemaTableNameList = listTables(session, schemaTableName.getSchemaName());
        if (schemaTableNameList.contains(schemaTableName)) {
            return getTablePath(dbPath, schemaTableName.getTableName());
        }
        else {
            return null;
        }
    }

    public List<DatasetFragment> getFragments(LanceTableHandle tableHandle)
    {
        return getFragments(getTablePath(dbPath, tableHandle.getTableName()));
    }

    private static List<DatasetFragment> getFragments(Path tablePath)
    {
        try (Dataset dataset = Dataset.open(tablePath.toUri().toString(), allocator)) {
            return dataset.getFragments();
        }
    }

    private static Schema getSchema(Path tablePath)
    {
        try (Dataset dataset = Dataset.open(tablePath.toUri().toString(), allocator)) {
            return dataset.getSchema();
        }
    }

    private static Path getTablePath(Path dbPath, String tableName)
    {
        return dbPath.resolve(tableName + TABLE_PATH_SUFFIX);
    }
}
