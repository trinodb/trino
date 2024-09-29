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
package io.trino.plugin.pulsar;

import io.trino.plugin.pulsar.util.NoStrictCacheSizeAllocator;
import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;

/**
 * Cache size allocator test.
 */
public class TestNoStrictCacheSizeAllocator
{
    @Test
    public void allocatorTest()
    {
        NoStrictCacheSizeAllocator noStrictCacheSizeAllocator = new NoStrictCacheSizeAllocator(1000);
        assertEquals(noStrictCacheSizeAllocator.getAvailableCacheSize(), 1000);

        noStrictCacheSizeAllocator.allocate(500);
        assertEquals(noStrictCacheSizeAllocator.getAvailableCacheSize(), 1000 - 500);

        noStrictCacheSizeAllocator.allocate(600);
        assertEquals(noStrictCacheSizeAllocator.getAvailableCacheSize(), 0);

        noStrictCacheSizeAllocator.release(500 + 600);
        assertEquals(noStrictCacheSizeAllocator.getAvailableCacheSize(), 1000);

        noStrictCacheSizeAllocator.release(100);
        assertEquals(noStrictCacheSizeAllocator.getAvailableCacheSize(), 1000);
    }
}
