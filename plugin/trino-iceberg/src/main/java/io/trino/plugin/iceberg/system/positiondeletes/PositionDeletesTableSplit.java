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
package io.trino.plugin.iceberg.system.positiondeletes;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import io.trino.plugin.iceberg.system.IcebergPartitionColumn;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.iceberg.FileFormat;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public record PositionDeletesTableSplit(
        String deleteFilePath,
        FileFormat deleteFileFormat,
        long deleteFileLength,
        String partitionDataJson,
        int specId,
        String schemaJson,
        Map<Integer, String> partitionSpecsByIdJson,
        Optional<Type> partitionColumnType,
        List<Integer> partitionFieldIds,
        OptionalLong contentOffset,
        Optional<Integer> contentSizeInBytes,
        Optional<String> referencedDataFile)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(PositionDeletesTableSplit.class);

    public PositionDeletesTableSplit
    {
        requireNonNull(deleteFilePath, "deleteFilePath is null");
        requireNonNull(deleteFileFormat, "deleteFileFormat is null");
        requireNonNull(partitionDataJson, "partitionDataJson is null");
        requireNonNull(schemaJson, "schemaJson is null");
        partitionSpecsByIdJson = ImmutableMap.copyOf(partitionSpecsByIdJson);
        requireNonNull(partitionColumnType, "partitionColumnType is null");
        partitionFieldIds = ImmutableList.copyOf(partitionFieldIds);
        requireNonNull(contentOffset, "contentOffset is null");
        requireNonNull(contentSizeInBytes, "contentSizeInBytes is null");
        requireNonNull(referencedDataFile, "referencedDataFile is null");
    }

    public Optional<IcebergPartitionColumn> partitionColumn()
    {
        return partitionColumnType.map(type -> new IcebergPartitionColumn((RowType) type, partitionFieldIds));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(deleteFilePath)
                + estimatedSizeOf(partitionDataJson)
                + estimatedSizeOf(schemaJson)
                + estimatedSizeOf(partitionSpecsByIdJson, _ -> 0, SizeOf::estimatedSizeOf);
    }
}
