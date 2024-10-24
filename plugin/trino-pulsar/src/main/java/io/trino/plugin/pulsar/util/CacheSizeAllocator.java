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
package io.trino.plugin.pulsar.util;

/**
 * An allocator manage size of entries read from BookKeeper and messages deserialized from entries.
 */
public interface CacheSizeAllocator
{
    /**
     * Get available cache size.
     *
     * @return available cache size
     */
    long getAvailableCacheSize();

    /**
     * Consume available cache.
     *
     * @param size allocate size
     */
    void allocate(long size);

    /**
     * Release allocated cache size.
     *
     * @param size release size
     */
    void release(long size);
}
