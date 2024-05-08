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
import io.trino.plugin.lance.LanceColumnHandle;
import io.trino.plugin.lance.LanceConfig;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LanceReader
{
    // TODO: fix allocator should be size of metadata, not actual data
    private static final BufferAllocator allocator = new RootAllocator(
            RootAllocator.configBuilder().from(RootAllocator.defaultConfig()).maxAllocation(Integer.MAX_VALUE).build());

    private final URI lanceDbURI;
    private final String dbScheme;
    private final String dbPath;

    public LanceReader(LanceConfig lanceConfig)
    {
        lanceDbURI = lanceConfig.getLanceDbUri();
        // TODO: use https://lancedb.github.io/lancedb/python/python/#lancedb.db.DBConnection
        dbScheme = lanceDbURI.getScheme();
        dbPath = lanceDbURI.toString();
    }

    private static Schema getSchema(String tablePath)
    {
        try (Dataset dataset = Dataset.open(tablePath, allocator)) {
            return dataset.getSchema();
        }
    }

    private static String getTablePath(String dbPath, String tableName)
    {
        // TODO: local fs impl here to be replaced by
        // https://lancedb.github.io/lancedb/python/python/#lancedb.db.DBConnection.open_table
        Path tablePath = Paths.get(dbPath, tableName);
        return tablePath.toString();
    }

    public List<SchemaTableName> listTables(ConnectorSession session, String schema)
    {
        // TODO: local fs impl here to be replaced by
        // https://lancedb.github.io/lancedb/python/python/#lancedb.db.DBConnection.table_names
        try (Stream<Path> stream = Files.list(Paths.get(dbPath))) {
            return stream.filter(file -> !Files.isDirectory(file))
                    .map(f -> new SchemaTableName(schema, f.getFileName().toString())).collect(Collectors.toList());
        }
        catch (IOException e) {
            return Collections.emptyList();
        }
    }

    public Map<String, ColumnHandle> getColumnHandle(String tableName)
    {
        String tablePath = getTablePath(dbPath, tableName);
        Schema arrowSchema = getSchema(tablePath);
        return arrowSchema.getFields().stream().collect(Collectors.toMap(Field::getName,
                f -> new LanceColumnHandle(f.getName(), LanceColumnHandle.toTrinoType(f.getFieldType().getType()),
                        f.getFieldType())));
    }

    public String getTablePath(SchemaTableName schemaTableName)
    {
        return getTablePath(dbPath, schemaTableName.getTableName());
    }
}
