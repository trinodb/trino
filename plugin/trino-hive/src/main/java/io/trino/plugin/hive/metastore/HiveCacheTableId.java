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
package io.trino.plugin.hive.metastore;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.hive.HiveBucketHandle;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.predicate.TupleDomain;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HiveCacheTableId
{
    private final String schemaName;
    private final String tableName;
    private final TupleDomain<CacheColumnId> compactEffectivePredicate;
    private final Optional<HiveBucketHandle> bucketHandle;

    public HiveCacheTableId(
            String schemaName,
            String tableName,
            TupleDomain<CacheColumnId> compactEffectivePredicate,
            Optional<HiveBucketHandle> bucketHandle)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.compactEffectivePredicate = requireNonNull(compactEffectivePredicate, "compactEffectivePredicate is null");
        this.bucketHandle = requireNonNull(bucketHandle, "bucketHandle is null");
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
    public TupleDomain<CacheColumnId> getCompactEffectivePredicate()
    {
        return compactEffectivePredicate;
    }

    @JsonProperty
    public Optional<HiveBucketHandle> getBucketHandle()
    {
        return bucketHandle;
    }
}
