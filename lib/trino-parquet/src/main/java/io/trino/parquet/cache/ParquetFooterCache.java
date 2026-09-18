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
package io.trino.parquet.cache;

import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.Optional;

public interface ParquetFooterCache
{
    Optional<Slice> get(ParquetFooterCacheKey key)
            throws IOException;

    void put(ParquetFooterCacheKey key, Slice footerBytes);

    default void invalidate(ParquetFooterCacheKey key) {}

    static ParquetFooterCache noop()
    {
        return NoOpParquetFooterCache.INSTANCE;
    }
}
