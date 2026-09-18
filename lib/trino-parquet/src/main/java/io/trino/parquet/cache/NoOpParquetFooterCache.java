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

import java.util.Optional;

enum NoOpParquetFooterCache
        implements ParquetFooterCache
{
    INSTANCE;

    @Override
    public Optional<Slice> get(ParquetFooterCacheKey key)
    {
        return Optional.empty();
    }

    @Override
    public void put(ParquetFooterCacheKey key, Slice footerBytes) {}
}
