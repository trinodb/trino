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

public record IcebergWritableTableHandle(
        SchemaTableName name,
        String schemaAsJson,
        Map<Integer, String> partitionsSpecsAsJson,
        int partitionSpecId,
        List<TrinoSortField> sortOrder,
        List<IcebergColumnHandle> inputColumns,
        String outputPath,
        IcebergFileFormat fileFormat,
        Map<String, String> storageProperties,
        RetryMode retryMode,
        Map<String, String> fileIoProperties)
        implements ConnectorInsertTableHandle, ConnectorOutputTableHandle
{
    public IcebergWritableTableHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(schemaAsJson, "schemaAsJson is null");
        partitionsSpecsAsJson = ImmutableMap.copyOf(requireNonNull(partitionsSpecsAsJson, "partitionsSpecsAsJson is null"));
        sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));
        inputColumns = ImmutableList.copyOf(requireNonNull(inputColumns, "inputColumns is null"));
        requireNonNull(outputPath, "outputPath is null");
        requireNonNull(fileFormat, "fileFormat is null");
        storageProperties = ImmutableMap.copyOf(requireNonNull(storageProperties, "storageProperties is null"));
        requireNonNull(retryMode, "retryMode is null");
        checkArgument(partitionsSpecsAsJson.containsKey(partitionSpecId), "partitionSpecId missing from partitionSpecs");
        fileIoProperties = ImmutableMap.copyOf(requireNonNull(fileIoProperties, "fileIoProperties is null"));
    }

    @Override
    public String toString()
    {
        return name.toString();
    }
}
