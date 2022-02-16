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
package io.trino.memory;

import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestLocalMemoryManager
{
    @Test
    public void testLocalMemoryManger()
    {
        NodeMemoryConfig config = new NodeMemoryConfig()
                .setHeapHeadroom(DataSize.of(10, GIGABYTE))
                .setMaxQueryMemoryPerNode(DataSize.of(20, GIGABYTE));

        LocalMemoryManager localMemoryManager = new LocalMemoryManager(config, DataSize.of(60, GIGABYTE).toBytes());
        assertEquals(localMemoryManager.getPools().size(), 1);
    }

    @Test
    public void testNotEnoughAvailableMemory()
    {
        NodeMemoryConfig config = new NodeMemoryConfig()
                .setHeapHeadroom(DataSize.of(10, GIGABYTE))
                .setMaxQueryMemoryPerNode(DataSize.of(20, GIGABYTE));

        // 25 GB heap is not sufficient for 10 GB heap headroom and 20 GB query.max-memory-per-node
        assertThatThrownBy(() -> new LocalMemoryManager(config, DataSize.of(25, GIGABYTE).toBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Invalid memory configuration\\. The sum of max query memory per node .* and heap headroom .*" +
                        "cannot be larger than the available heap memory .*");
    }
}
