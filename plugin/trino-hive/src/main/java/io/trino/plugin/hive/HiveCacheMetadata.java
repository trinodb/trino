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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.plugin.hive.metastore.HiveCacheTableId;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.ConnectorCacheMetadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.Optional;

import static io.trino.plugin.base.cache.CacheUtils.normalizeTupleDomain;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static java.util.Objects.requireNonNull;

public class HiveCacheMetadata
        implements ConnectorCacheMetadata
{
    private final JsonCodec<HiveCacheTableId> tableIdCodec;
    private final JsonCodec<HiveColumnHandle> columnHandleCodec;

    @Inject
    public HiveCacheMetadata(JsonCodec<HiveCacheTableId> tableIdCodec, JsonCodec<HiveColumnHandle> columnHandleCodec)
    {
        this.tableIdCodec = requireNonNull(tableIdCodec, "tableIdCodec is null");
        this.columnHandleCodec = requireNonNull(columnHandleCodec, "columnHandleCodec is null");
    }

    @Override
    public Optional<CacheTableId> getCacheTableId(ConnectorTableHandle tableHandle)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;

        if (hiveTableHandle.getTransaction().isAcidTransactionRunning()) {
            // skip caching of transactional tables as transaction affects how split rows are read
            return Optional.empty();
        }

        if (hiveTableHandle.getAnalyzePartitionValues().isPresent()) {
            // skip caching of analyze queries
            return Optional.empty();
        }

        // Ensure cache id generation is revisited whenever handle classes change.
        // Only fields that are sent to worker matter for CacheTableId.
        // This constructor is used as JSON deserializer on worker nodes.
        hiveTableHandle = new HiveTableHandle(
                hiveTableHandle.getSchemaName(),
                hiveTableHandle.getTableName(),
                // columns can be skipped from table id as they are obtained separately
                ImmutableList.of(),
                ImmutableList.of(),
                // compactEffectivePredicate is returned as part of ConnectorPageSourceProvider#getUnenforcedPredicate
                TupleDomain.all(),
                // enforced constraint is only enforced on partition columns, therefore it can be skipped
                TupleDomain.all(),
                hiveTableHandle.getBucketHandle(),
                // skip bucket filter as splits are entirely embedded within buckets
                Optional.empty(),
                Optional.empty(),
                NO_ACID_TRANSACTION);

        HiveCacheTableId tableId = new HiveCacheTableId(
                hiveTableHandle.getSchemaName(),
                hiveTableHandle.getTableName(),
                normalizeTupleDomain(hiveTableHandle.getCompactEffectivePredicate()
                        .transformKeys(column -> getCacheColumnId(tableHandle, column).orElseThrow())),
                hiveTableHandle.getBucketHandle());
        return Optional.of(new CacheTableId(tableIdCodec.toJson(tableId)));
    }

    @Override
    public Optional<CacheColumnId> getCacheColumnId(ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        HiveColumnHandle hiveColumnHandle = (HiveColumnHandle) columnHandle;

        // ensure cache id generation is revisited whenever handle classes change
        HiveColumnHandle canonicalizedHandle = new HiveColumnHandle(
                hiveColumnHandle.getBaseColumnName(),
                hiveColumnHandle.getBaseHiveColumnIndex(),
                hiveColumnHandle.getBaseHiveType(),
                hiveColumnHandle.getBaseType(),
                hiveColumnHandle.getHiveColumnProjectionInfo(),
                hiveColumnHandle.getColumnType(),
                // comment is irrelevant
                Optional.empty());
        return Optional.of(new CacheColumnId(columnHandleCodec.toJson(canonicalizedHandle)));
    }

    @Override
    public ConnectorTableHandle getCanonicalTableHandle(ConnectorTableHandle handle)
    {
        return ((HiveTableHandle) handle).toCanonical();
    }
}
