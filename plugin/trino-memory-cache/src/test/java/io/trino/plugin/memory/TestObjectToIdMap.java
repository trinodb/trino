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
package io.trino.plugin.memory;

import org.junit.jupiter.api.Test;

import static io.airlift.slice.SizeOf.LONG_INSTANCE_SIZE;
import static io.trino.plugin.memory.MemoryCacheManager.MAP_ENTRY_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestObjectToIdMap
{
    @Test
    public void testObjectToIdMap()
    {
        ObjectToIdMap<String> idMap = new ObjectToIdMap<>(string -> (long) string.length());

        assertThat(idMap.getRevocableBytes()).isEqualTo(0L);
        assertThat(idMap.getUsageCount(42L)).isEqualTo(0L);

        long cacheEntrySize = 2L * MAP_ENTRY_SIZE + 3L * LONG_INSTANCE_SIZE + "A".length();
        long idA = idMap.allocateId("A");
        assertThat(idA).isEqualTo(0L);
        assertThat(idMap.getUsageCount(idA)).isEqualTo(1L);
        assertThat(idMap.getRevocableBytes()).isEqualTo(cacheEntrySize);

        idMap.acquireId(idA);
        assertThat(idMap.getUsageCount(idA)).isEqualTo(2L);

        long idB = idMap.allocateId("B");
        assertThat(idB).isEqualTo(1L);
        assertThat(idMap.getUsageCount(idB)).isEqualTo(1L);
        assertThat(idMap.getRevocableBytes()).isEqualTo(2 * cacheEntrySize);

        idMap.releaseId(idB);
        assertThat(idMap.getUsageCount(idB)).isEqualTo(0L);
        assertThat(idMap.getRevocableBytes()).isEqualTo(cacheEntrySize);
    }
}
