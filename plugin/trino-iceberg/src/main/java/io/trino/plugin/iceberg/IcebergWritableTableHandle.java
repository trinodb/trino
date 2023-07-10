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
package io.trino.plugin.iceberg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class IcebergWritableTableHandle
        implements ConnectorInsertTableHandle, ConnectorOutputTableHandle
{
    private final SchemaTableName name;
    private final String schemaAsJson;
    private final Map<Integer, String> partitionsSpecsAsJson;
    private final int partitionSpecId;
    private final List<TrinoSortField> sortOrder;
    private final List<IcebergColumnHandle> inputColumns;
    private final String outputPath;
    private final IcebergFileFormat fileFormat;
    private final Map<String, String> storageProperties;
    private final RetryMode retryMode;

    @JsonCreator
    public IcebergWritableTableHandle(
            @JsonProperty("name") SchemaTableName name,
            @JsonProperty("schemaAsJson") String schemaAsJson,
            @JsonProperty("partitionSpecsAsJson") Map<Integer, String> partitionsSpecsAsJson,
            @JsonProperty("partitionSpecId") int partitionSpecId,
            @JsonProperty("sortOrder") List<TrinoSortField> sortOrder,
            @JsonProperty("inputColumns") List<IcebergColumnHandle> inputColumns,
            @JsonProperty("outputPath") String outputPath,
            @JsonProperty("fileFormat") IcebergFileFormat fileFormat,
            @JsonProperty("properties") Map<String, String> storageProperties,
            @JsonProperty("retryMode") RetryMode retryMode)
    {
        this.name = requireNonNull(name, "name is null");
        this.schemaAsJson = requireNonNull(schemaAsJson, "schemaAsJson is null");
        this.partitionsSpecsAsJson = ImmutableMap.copyOf(requireNonNull(partitionsSpecsAsJson, "partitionsSpecsAsJson is null"));
        this.partitionSpecId = partitionSpecId;
        this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));
        this.inputColumns = ImmutableList.copyOf(requireNonNull(inputColumns, "inputColumns is null"));
        this.outputPath = requireNonNull(outputPath, "outputPath is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.storageProperties = ImmutableMap.copyOf(requireNonNull(storageProperties, "storageProperties is null"));
        this.retryMode = requireNonNull(retryMode, "retryMode is null");
        checkArgument(partitionsSpecsAsJson.containsKey(partitionSpecId), "partitionSpecId missing from partitionSpecs");
    }

    @JsonProperty
    public SchemaTableName getName()
    {
        return name;
    }

    @JsonProperty
    public String getSchemaAsJson()
    {
        return schemaAsJson;
    }

    @JsonProperty
    public Map<Integer, String> getPartitionsSpecsAsJson()
    {
        return partitionsSpecsAsJson;
    }

    @JsonProperty
    public int getPartitionSpecId()
    {
        return partitionSpecId;
    }

    @JsonProperty
    public List<TrinoSortField> getSortOrder()
    {
        return sortOrder;
    }

    @JsonProperty
    public List<IcebergColumnHandle> getInputColumns()
    {
        return inputColumns;
    }

    @JsonProperty
    public String getOutputPath()
    {
        return outputPath;
    }

    @JsonProperty
    public IcebergFileFormat getFileFormat()
    {
        return fileFormat;
    }

    @JsonProperty
    public Map<String, String> getStorageProperties()
    {
        return storageProperties;
    }

    @JsonProperty
    public RetryMode getRetryMode()
    {
        return retryMode;
    }

    @Override
    public String toString()
    {
        return name.toString();
    }
}
