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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.cache.CacheStatsMBean;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;

final class TestTableMetadataCache
{
    // Weight of a table metadata without snapshots, see TableMetadataCache.BASE_RETAINED_BYTES
    private static final long METADATA_WEIGHT = 20 * 1024;

    private static final TableMetadata METADATA = TableMetadata.newTableMetadata(
            new Schema(Types.NestedField.required(1, "x", Types.IntegerType.get())),
            PartitionSpec.unpartitioned(),
            "local:///table",
            ImmutableMap.of());

    @Test
    void testHitsAndMisses()
    {
        TableMetadataCache cache = new TableMetadataCache(DataSize.of(10, MEGABYTE));
        CacheStatsMBean stats = cache.getCacheStats();

        assertThat(stats.getRequestCount()).isZero();
        assertThat(stats.size()).isZero();
        assertThat(cache.getTotalWeight()).isZero();

        assertThat(cache.get("metadata-1.json", _ -> METADATA)).isSameAs(METADATA);
        assertThat(cache.get("metadata-1.json", _ -> METADATA)).isSameAs(METADATA);
        assertThat(cache.get("metadata-1.json", _ -> METADATA)).isSameAs(METADATA);
        assertThat(cache.get("metadata-2.json", _ -> METADATA)).isSameAs(METADATA);

        assertThat(stats.getRequestCount()).isEqualTo(4);
        assertThat(stats.getLoadCount()).isEqualTo(2);
        assertThat(stats.getHitRate()).isEqualTo(0.5);
        assertThat(stats.getMissRate()).isEqualTo(0.5);
        assertThat(stats.getEvictionCount()).isZero();
        assertThat(stats.size()).isEqualTo(2);
        assertThat(cache.getTotalWeight()).isEqualTo(2 * METADATA_WEIGHT);
    }

    @Test
    void testEviction()
    {
        DataSize maxSize = DataSize.of(100, KILOBYTE);
        TableMetadataCache cache = new TableMetadataCache(maxSize);
        CacheStatsMBean stats = cache.getCacheStats();

        int entries = 20;
        for (int i = 0; i < entries; i++) {
            cache.get("metadata-%s.json".formatted(i), _ -> METADATA);
        }

        assertThat(stats.getLoadCount()).isEqualTo(entries);
        assertThat(stats.getEvictionCount()).isPositive();
        assertThat(stats.getEvictionCount()).isEqualTo(entries - stats.size());
        assertThat(cache.getTotalWeight())
                .isEqualTo(stats.size() * METADATA_WEIGHT)
                .isLessThanOrEqualTo(maxSize.toBytes());
    }

    @Test
    void testDisabled()
    {
        TableMetadataCache cache = TableMetadataCache.disabled();
        CacheStatsMBean stats = cache.getCacheStats();

        assertThat(cache.get("metadata-1.json", _ -> METADATA)).isSameAs(METADATA);
        assertThat(cache.get("metadata-1.json", _ -> METADATA)).isSameAs(METADATA);

        assertThat(stats.getRequestCount()).isEqualTo(2);
        assertThat(stats.getLoadCount()).isEqualTo(2);
        assertThat(stats.getMissRate()).isEqualTo(1.0);
        assertThat(stats.size()).isZero();
        assertThat(cache.getTotalWeight()).isZero();
    }
}
