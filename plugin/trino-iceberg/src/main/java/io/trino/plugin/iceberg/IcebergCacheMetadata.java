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

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.ConnectorCacheMetadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class IcebergCacheMetadata
        implements ConnectorCacheMetadata
{
    private final JsonCodec<IcebergCacheTableId> tableIdCodec;
    private final JsonCodec<IcebergColumnHandle> columnHandleCodec;

    @Inject
    public IcebergCacheMetadata(JsonCodec<IcebergCacheTableId> tableIdCodec, JsonCodec<IcebergColumnHandle> columnHandleCodec)
    {
        this.tableIdCodec = requireNonNull(tableIdCodec, "tableIdCodec is null");
        this.columnHandleCodec = requireNonNull(columnHandleCodec, "columnHandleCodec is null");
    }

    @Override
    public Optional<CacheTableId> getCacheTableId(ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;

        if (icebergTableHandle.getSnapshotId().isEmpty()) {
            // A table with missing snapshot id produces no splits
            return Optional.empty();
        }

        // Ensure cache id generation is revisited whenever handle classes change.
        IcebergTableHandle handle = new IcebergTableHandle(
                icebergTableHandle.getCatalog(),
                icebergTableHandle.getSchemaName(),
                icebergTableHandle.getTableName(),
                icebergTableHandle.getTableType(),
                icebergTableHandle.getSnapshotId(),
                icebergTableHandle.getTableSchemaJson(),
                icebergTableHandle.getPartitionSpecJson(),
                icebergTableHandle.getFormatVersion(),
                icebergTableHandle.getUnenforcedPredicate(),
                icebergTableHandle.getEnforcedPredicate(),
                icebergTableHandle.getLimit(),
                icebergTableHandle.getProjectedColumns(),
                icebergTableHandle.getNameMappingJson(),
                icebergTableHandle.getTableLocation(),
                icebergTableHandle.getStorageProperties(),
                icebergTableHandle.isRecordScannedFiles(),
                icebergTableHandle.getMaxScannedFileSize(),
                icebergTableHandle.getConstraintColumns(),
                icebergTableHandle.getForAnalyze());

        IcebergCacheTableId tableId = new IcebergCacheTableId(
                handle.getCatalog(),
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getTableLocation(),
                handle.getStorageProperties().entrySet().stream()
                        .filter(IcebergCacheTableId::isCacheableStorageProperty)
                        .sorted(Map.Entry.comparingByKey())
                        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));

        return Optional.of(new CacheTableId(tableIdCodec.toJson(tableId)));
    }

    @Override
    public ConnectorTableHandle getCanonicalTableHandle(ConnectorTableHandle handle)
    {
        return ((IcebergTableHandle) handle).toCanonical();
    }

    @Override
    public Optional<CacheColumnId> getCacheColumnId(ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        IcebergColumnHandle icebergColumnHandle = (IcebergColumnHandle) columnHandle;

        // ensure cache id generation is revisited whenever handle classes change
        IcebergColumnHandle canonicalizedHandle = new IcebergColumnHandle(
                icebergColumnHandle.getBaseColumnIdentity(),
                icebergColumnHandle.getBaseType(),
                icebergColumnHandle.getPath(),
                icebergColumnHandle.getType(),
                icebergColumnHandle.isNullable(),
                // comment is irrelevant
                Optional.empty());

        return Optional.of(new CacheColumnId(columnHandleCodec.toJson(canonicalizedHandle)));
    }
}
