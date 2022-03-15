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
package io.trino.plugin.iceberg.procedure;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergFileFormat;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class IcebergOptimizeHandle
        extends IcebergProcedureHandle
{
    private final String schemaAsJson;
    private final String partitionSpecAsJson;
    private final List<IcebergColumnHandle> tableColumns;
    private final IcebergFileFormat fileFormat;
    private final Map<String, String> tableStorageProperties;
    private final DataSize maxScannedFileSize;
    private final boolean retriesEnabled;

    @JsonCreator
    public IcebergOptimizeHandle(
            String schemaAsJson,
            String partitionSpecAsJson,
            List<IcebergColumnHandle> tableColumns,
            IcebergFileFormat fileFormat,
            Map<String, String> tableStorageProperties,
            DataSize maxScannedFileSize,
            boolean retriesEnabled)
    {
        this.schemaAsJson = requireNonNull(schemaAsJson, "schemaAsJson is null");
        this.partitionSpecAsJson = requireNonNull(partitionSpecAsJson, "partitionSpecAsJson is null");
        this.tableColumns = ImmutableList.copyOf(requireNonNull(tableColumns, "tableColumns is null"));
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.tableStorageProperties = ImmutableMap.copyOf(requireNonNull(tableStorageProperties, "tableStorageProperties is null"));
        this.maxScannedFileSize = requireNonNull(maxScannedFileSize, "maxScannedFileSize is null");
        this.retriesEnabled = retriesEnabled;
    }

    @JsonProperty
    public String getSchemaAsJson()
    {
        return schemaAsJson;
    }

    @JsonProperty
    public String getPartitionSpecAsJson()
    {
        return partitionSpecAsJson;
    }

    @JsonProperty
    public List<IcebergColumnHandle> getTableColumns()
    {
        return tableColumns;
    }

    @JsonProperty
    public IcebergFileFormat getFileFormat()
    {
        return fileFormat;
    }

    @JsonProperty
    public Map<String, String> getTableStorageProperties()
    {
        return tableStorageProperties;
    }

    @JsonProperty
    public DataSize getMaxScannedFileSize()
    {
        return maxScannedFileSize;
    }

    @JsonProperty
    public boolean isRetriesEnabled()
    {
        return retriesEnabled;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaAsJson", schemaAsJson)
                .add("partitionSpecAsJson", partitionSpecAsJson)
                .add("tableColumns", tableColumns)
                .add("fileFormat", fileFormat)
                .add("tableStorageProperties", tableStorageProperties)
                .add("maxScannedFileSize", maxScannedFileSize)
                .add("retriesEnabled", retriesEnabled)
                .toString();
    }
}
