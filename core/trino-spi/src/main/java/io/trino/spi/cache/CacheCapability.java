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
package io.trino.spi.cache;

/**
 * A behavioral fact about the caches a {@link BlobCacheManager} creates, objectively true or
 * false for a given implementation. Consumers pass the capabilities they require to
 * {@link ConnectorCacheFactory#createBlobCache}, and only managers providing every required
 * capability are considered. A value is added here only together with the consumer whose
 * behavior depends on it.
 */
public enum CacheCapability
{
    /**
     * Cache hits are served from process memory, with no disk or network I/O.
     */
    LOW_LATENCY,

    /**
     * Cache capacity is not bounded by the heap size, so it can hold working sets larger
     * than available process memory.
     */
    CAN_EXCEED_HEAP_SIZE,
}
