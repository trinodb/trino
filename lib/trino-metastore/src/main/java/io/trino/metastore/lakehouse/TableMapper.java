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
package io.trino.metastore.lakehouse;

import com.google.cloud.biglake.hive.v1.HiveTable;
import com.google.cloud.biglake.hive.v1.StorageDescriptor;
import com.google.cloud.biglake.hive.v1.TableName;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.Table;
import io.trino.spi.TrinoException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.metastore.MetastoreErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.metastore.lakehouse.StorageMapper.optionalText;

/**
 * Maps a Lakehouse table to the Trino metastore model.
 */
public final class TableMapper
{
    /**
     * The table type to use when Lakehouse reports none. Trino requires a table type, so a table
     * without one is read as an external table.
     */
    private static final String DEFAULT_TABLE_TYPE = "EXTERNAL_TABLE";

    private TableMapper() {}

    public static Table toTrino(HiveTable table)
    {
        TableName name = TableName.parse(table.getName());
        String qualifiedName = name.getDatabase() + "." + name.getTable();

        if (!table.hasStorageDescriptor()) {
            throw new TrinoException(
                    HIVE_UNSUPPORTED_FORMAT, "Table has no storage descriptor: " + qualifiedName);
        }
        StorageDescriptor descriptor = table.getStorageDescriptor();

        return new Table(
                name.getDatabase(),
                name.getTable(),
                // The Lakehouse Hive table message has no owner
                Optional.empty(),
                tableType(table),
                StorageMapper.toTrino(descriptor, qualifiedName),
                StorageMapper.toTrinoColumns(descriptor.getColumnsList(), qualifiedName),
                StorageMapper.toTrinoColumns(table.getPartitionKeysList(), qualifiedName),
                parameters(table),
                optionalText(table.getViewOriginalText()),
                optionalText(table.getViewExpandedText()),
                OptionalLong.empty());
    }

    /**
     * Returns the table name, which Lakehouse reports as part of a resource name.
     */
    public static String tableName(HiveTable table)
    {
        return TableName.parse(table.getName()).getTable();
    }

    static String tableType(HiveTable table)
    {
        return table.getTableType().isEmpty() ? DEFAULT_TABLE_TYPE : table.getTableType();
    }

    /**
     * Returns the table parameters, with the Lakehouse description exposed as the Trino comment.
     */
    private static Map<String, String> parameters(HiveTable table)
    {
        if (table.getDescription().isEmpty()) {
            return table.getParametersMap();
        }
        Map<String, String> merged = new LinkedHashMap<>(table.getParametersMap());
        merged.put(TABLE_COMMENT, table.getDescription());
        return ImmutableMap.copyOf(merged);
    }
}
