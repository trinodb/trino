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
package io.trino.plugin.hudi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorOutputTableHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class HudiOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final HudiTableType tableType;
    private final String tablePath;
    private final String primaryKey;
    private final List<HudiColumnHandle> dataColumns;
    private final List<HudiColumnHandle> partitionColumns;

    private final HudiStorageFormat storageFormat;

    @JsonCreator
    public HudiOutputTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableType") HudiTableType tableType,
            @JsonProperty("tablePath") String tablePath,
            @JsonProperty("dataColumns") List<HudiColumnHandle> dataColumns,
            @JsonProperty("partitionColumns") List<HudiColumnHandle> partitionColumns,
            @JsonProperty("storageFormat") HudiStorageFormat storageFormat,
            @JsonProperty("primaryKey") String primaryKey)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.tablePath = requireNonNull(tablePath, "tablePath is null");
        this.dataColumns = requireNonNull(dataColumns, "dataColumns is null");
        this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null");
        this.storageFormat = requireNonNull(storageFormat, "storageFormat is null");
        this.primaryKey = requireNonNull(primaryKey, "primaryKey is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public HudiTableType getTableType()
    {
        return tableType;
    }

    @JsonProperty
    public List<HudiColumnHandle> getDataColumns()
    {
        return dataColumns;
    }

    @JsonProperty
    public List<HudiColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    @JsonProperty
    public String getTablePath()
    {
        return tablePath;
    }

    @JsonProperty
    public HudiStorageFormat getStorageFormat()
    {
        return storageFormat;
    }

    @JsonProperty
    public String getPrimaryKey()
    {
        return primaryKey;
    }

    @Override
    public String toString()
    {
        return "hudi:" + schemaName + "." + tableName + "." + tableType;
    }
}
