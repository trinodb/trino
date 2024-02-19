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

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.ConnectorCacheMetadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DeltaLakeCacheMetadata
        implements ConnectorCacheMetadata
{
    private final JsonCodec<DeltaLakeCacheTableId> tableIdCodec;
    private final JsonCodec<DeltaLakeColumnHandle> columnCodec;

    @Inject
    public DeltaLakeCacheMetadata(JsonCodec<DeltaLakeCacheTableId> tableIdCodec, JsonCodec<DeltaLakeColumnHandle> columnCodec)
    {
        this.tableIdCodec = requireNonNull(tableIdCodec, "tableIdCodec is null");
        this.columnCodec = requireNonNull(columnCodec, "columnCodec is null");
    }

    @Override
    public Optional<CacheTableId> getCacheTableId(ConnectorTableHandle tableHandle)
    {
        DeltaLakeTableHandle deltaLakeTableHandle = (DeltaLakeTableHandle) tableHandle;

        // skip caching if it is UPDATE / INSERT query
        if (((DeltaLakeTableHandle) tableHandle).getWriteType().isPresent()) {
            return Optional.empty();
        }

        // skip caching of analyze queries
        if (deltaLakeTableHandle.getAnalyzeHandle().isPresent()) {
            return Optional.empty();
        }

        // Ensure cache id generation is revisited whenever handle classes change.
        DeltaLakeTableHandle handle = new DeltaLakeTableHandle(
                deltaLakeTableHandle.getSchemaName(),
                deltaLakeTableHandle.getTableName(),
                deltaLakeTableHandle.isManaged(),
                deltaLakeTableHandle.getLocation(),
                deltaLakeTableHandle.getMetadataEntry(),
                deltaLakeTableHandle.getProtocolEntry(),
                deltaLakeTableHandle.getEnforcedPartitionConstraint(),
                deltaLakeTableHandle.getNonPartitionConstraint(),
                deltaLakeTableHandle.getWriteType(),
                deltaLakeTableHandle.getProjectedColumns(),
                deltaLakeTableHandle.getUpdatedColumns(),
                deltaLakeTableHandle.getUpdateRowIdColumns(),
                deltaLakeTableHandle.getAnalyzeHandle(),
                deltaLakeTableHandle.getReadVersion());

        DeltaLakeCacheTableId tableId = new DeltaLakeCacheTableId(
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getLocation(),
                handle.getMetadataEntry());
        return Optional.of(new CacheTableId(tableIdCodec.toJson(tableId)));
    }

    @Override
    public Optional<CacheColumnId> getCacheColumnId(ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return Optional.of(new CacheColumnId(columnCodec.toJson((DeltaLakeColumnHandle) columnHandle)));
    }

    @Override
    public ConnectorTableHandle getCanonicalTableHandle(ConnectorTableHandle handle)
    {
        return ((DeltaLakeTableHandle) handle).toCanonical();
    }
}
