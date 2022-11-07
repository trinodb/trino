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
package io.trino.plugin.kudu;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import org.apache.kudu.client.KuduTable;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class KuduInsertTableHandle
        implements ConnectorInsertTableHandle, KuduTableMapping
{
    private final SchemaTableName schemaTableName;
    private final List<Type> columnTypes;
    private final boolean generateUUID;
    private transient KuduTable table;

    @JsonCreator
    public KuduInsertTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("generateUUID") boolean generateUUID)
    {
        this(schemaTableName, columnTypes, generateUUID, null);
    }

    public KuduInsertTableHandle(
            SchemaTableName schemaTableName,
            List<Type> columnTypes,
            boolean generateUUID,
            KuduTable table)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.generateUUID = generateUUID;
        this.table = table;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @Override
    @JsonProperty
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public List<Type> getOriginalColumnTypes()
    {
        return columnTypes;
    }

    @Override
    @JsonProperty
    public boolean isGenerateUUID()
    {
        return generateUUID;
    }

    public KuduTable getTable(KuduClientSession session)
    {
        if (table == null) {
            table = session.openTable(schemaTableName);
        }
        return table;
    }
}
