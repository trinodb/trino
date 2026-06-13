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
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import org.apache.iceberg.RowLevelOperationMode;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class IcebergMergeTableHandle
        implements ConnectorMergeTableHandle
{
    private final IcebergTableHandle tableHandle;
    private final IcebergWritableTableHandle insertTableHandle;
    private final RowLevelOperationMode rowLevelOperationMode;
    // Frozen base table properties captured in beginMerge (CoW only; empty for MERGE_ON_READ).
    private final Map<String, String> baseTableProperties;
    // Pre-existing position/equality delete files per data file, planned once by the
    // coordinator (CoW only; empty for MERGE_ON_READ).
    private final Map<String, List<DeleteFile>> preExistingDeletesByDataFile;

    @JsonCreator
    public IcebergMergeTableHandle(
            @JsonProperty("tableHandle") IcebergTableHandle tableHandle,
            @JsonProperty("insertTableHandle") IcebergWritableTableHandle insertTableHandle,
            @JsonProperty("rowLevelOperationMode") RowLevelOperationMode rowLevelOperationMode,
            @JsonProperty("baseTableProperties") Map<String, String> baseTableProperties,
            @JsonProperty("preExistingDeletesByDataFile") Map<String, List<DeleteFile>> preExistingDeletesByDataFile)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.insertTableHandle = requireNonNull(insertTableHandle, "insertTableHandle is null");
        this.rowLevelOperationMode = requireNonNull(rowLevelOperationMode, "rowLevelOperationMode is null");
        this.baseTableProperties = ImmutableMap.copyOf(requireNonNull(baseTableProperties, "baseTableProperties is null"));
        // Deep-copy the inner lists: ImmutableMap.copyOf only freezes the outer map, but the
        // List<DeleteFile> values are reachable through getPreExistingDeletesByDataFile() and
        // would otherwise alias the caller's mutable lists, opening a path for worker rewrite
        // inputs to be mutated after the handle is built.
        this.preExistingDeletesByDataFile = requireNonNull(preExistingDeletesByDataFile, "preExistingDeletesByDataFile is null")
                .entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableList.copyOf(entry.getValue())));
    }

    @Override
    @JsonProperty
    public IcebergTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public IcebergWritableTableHandle getInsertTableHandle()
    {
        return insertTableHandle;
    }

    @JsonProperty
    public RowLevelOperationMode getRowLevelOperationMode()
    {
        return rowLevelOperationMode;
    }

    @JsonProperty
    public Map<String, String> getBaseTableProperties()
    {
        return baseTableProperties;
    }

    @JsonProperty
    public Map<String, List<DeleteFile>> getPreExistingDeletesByDataFile()
    {
        return preExistingDeletesByDataFile;
    }
}
