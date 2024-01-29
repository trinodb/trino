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
package io.trino.operator;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestIdRegistry
{
    @Test
    public void testAllocateDeallocate()
    {
        IdRegistry<String> registry = new IdRegistry<>();
        int id1 = Integer.parseInt(registry.allocateId(Integer::toString));
        assertThat(registry.get(id1)).isEqualTo(Integer.toString(id1));
        int id2 = Integer.parseInt(registry.allocateId(Integer::toString));
        assertThat(registry.get(id1)).isEqualTo(Integer.toString(id1));
        assertThat(registry.get(id2)).isEqualTo(Integer.toString(id2));

        // Should still be able to fetch id2 after deallocating id1
        registry.deallocate(id1);
        assertThat(registry.get(id2)).isEqualTo(Integer.toString(id2));
    }

    @Test
    public void testBulkAllocate()
    {
        IdRegistry<String> registry = new IdRegistry<>();
        IntArrayList ids = new IntArrayList();
        // Bulk allocate
        for (int i = 0; i < 100; i++) {
            ids.add(Integer.parseInt(registry.allocateId(Integer::toString)));
        }
        // Get values
        for (int i = 0; i < 100; i++) {
            assertThat(registry.get(ids.getInt(i))).isEqualTo(Integer.toString(i));
        }
        // Deallocate
        for (int i = 0; i < 100; i++) {
            registry.deallocate(ids.getInt(i));
        }
    }

    @Test
    public void testIdRecycling()
    {
        IdRegistry<String> registry = new IdRegistry<>();
        int id1 = Integer.parseInt(registry.allocateId(Integer::toString));
        registry.deallocate(id1);
        int id2 = Integer.parseInt(registry.allocateId(Integer::toString));
        assertThat(id1).isEqualTo(id2);

        int id3 = Integer.parseInt(registry.allocateId(Integer::toString));
        registry.allocateId(Integer::toString);
        registry.deallocate(id3);
        int id4 = Integer.parseInt(registry.allocateId(Integer::toString));
        assertThat(id3).isEqualTo(id4);
    }
}
