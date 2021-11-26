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
package io.trino.plugin.iceberg.serdes;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;

import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.util.SerializationUtil.deserializeFromBase64;
import static org.apache.iceberg.util.SerializationUtil.serializeToBase64;

public class IcebergTableWrapper
{
    private final Table table;

    @JsonCreator
    public IcebergTableWrapper(String serializedTable)
    {
        this.table = deserializeFromBase64(requireNonNull(serializedTable, "serializedTable is null"));
    }

    private IcebergTableWrapper(Table table)
    {
        this.table = requireNonNull(table, "table is null");
    }

    public static IcebergTableWrapper wrap(Table table)
    {
        return new IcebergTableWrapper(table);
    }

    public Table getTable()
    {
        return table;
    }

    @JsonValue
    public String serialize()
    {
        if (table instanceof SerializableTable) {
            return serializeToBase64(table);
        }
        return serializeToBase64(SerializableTable.copyOf(table));
    }
}
