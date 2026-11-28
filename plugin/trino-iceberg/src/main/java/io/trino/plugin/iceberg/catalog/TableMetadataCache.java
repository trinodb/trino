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
package io.trino.plugin.iceberg.catalog;

import com.google.common.cache.Cache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.iceberg.IcebergConfig;
import org.apache.iceberg.TableMetadata;

import java.util.function.Function;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static java.lang.Math.min;

/**
 * Parsed table metadata shared by all transactions of a catalog. Iceberg never rewrites a
 * metadata file, so a metadata location always resolves to the same content and entries never
 * need invalidation. Per-transaction caches still pin the version a query has seen.
 */
public final class TableMetadataCache
{
    // Measured retained sizes: about 19 KB for a table with a single snapshot, and about 2 KB for each
    // snapshot with a typical summary. Snapshots dominate for tables with long histories, so schemas,
    // partition specs and the manifest lists that snapshots cache after planning are not counted.
    private static final long BASE_RETAINED_BYTES = 20 * 1024;
    private static final long SNAPSHOT_RETAINED_BYTES = 2 * 1024;

    private final Cache<String, TableMetadata> cache;

    @Inject
    public TableMetadataCache(IcebergConfig config)
    {
        this(config.isMetadataCacheEnabled() ? config.getMetadataCacheMaxParsedSize() : DataSize.ofBytes(0));
    }

    public TableMetadataCache(DataSize maxSize)
    {
        if (maxSize.toBytes() == 0) {
            cache = EvictableCacheBuilder.newBuilder()
                    .maximumSize(0)
                    .shareNothingWhenDisabled()
                    .build();
            return;
        }
        cache = EvictableCacheBuilder.newBuilder()
                .maximumWeight(maxSize.toBytes())
                .<String, TableMetadata>weigher((_, metadata) -> estimatedRetainedBytes(metadata))
                .build();
    }

    public static TableMetadataCache disabled()
    {
        return new TableMetadataCache(DataSize.ofBytes(0));
    }

    public TableMetadata get(String metadataLocation, Function<String, TableMetadata> loader)
    {
        try {
            return uncheckedCacheGet(cache, metadataLocation, () -> loader.apply(metadataLocation));
        }
        catch (UncheckedExecutionException e) {
            throwIfUnchecked(e.getCause());
            throw e;
        }
    }

    private static int estimatedRetainedBytes(TableMetadata metadata)
    {
        return (int) min(Integer.MAX_VALUE, BASE_RETAINED_BYTES + SNAPSHOT_RETAINED_BYTES * metadata.snapshots().size());
    }
}
