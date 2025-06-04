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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.spi.connector.ConnectorOutputTableHandle;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.PARTITION_KEY;
import static java.util.Objects.requireNonNull;

public record DeltaLakeOutputTableHandle(
        String schemaName,
        String tableName,
        List<DeltaLakeColumnHandle> inputColumns,
        String location,
        Optional<Long> checkpointInterval,
        boolean external,
        Optional<String> comment,
        Optional<Boolean> changeDataFeedEnabled,
        boolean deletionVectorsEnabled,
        String schemaString,
        ColumnMappingMode columnMappingMode,
        OptionalInt maxColumnId,
        boolean replace,
        OptionalLong readVersion,
        ProtocolEntry protocolEntry)
        implements ConnectorOutputTableHandle
{
    public DeltaLakeOutputTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        inputColumns = ImmutableList.copyOf(requireNonNull(inputColumns, "inputColumns is null"));
        requireNonNull(location, "location is null");
        requireNonNull(checkpointInterval, "checkpointInterval is null");
        requireNonNull(comment, "comment is null");
        requireNonNull(changeDataFeedEnabled, "changeDataFeedEnabled is null");
        requireNonNull(schemaString, "schemaString is null");
        requireNonNull(columnMappingMode, "columnMappingMode is null");
        requireNonNull(maxColumnId, "maxColumnId is null");
        requireNonNull(readVersion, "readVersion is null");
        requireNonNull(protocolEntry, "protocolEntry is null");
    }

    public List<String> partitionedBy()
    {
        return inputColumns().stream()
                .filter(column -> column.columnType() == PARTITION_KEY)
                .map(DeltaLakeColumnHandle::columnName)
                .collect(toImmutableList());
    }
}
