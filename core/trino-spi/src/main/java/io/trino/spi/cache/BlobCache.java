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

import java.io.IOException;
import java.util.Collection;

/**
 * A filesystem-agnostic blob cache. Callers identify cached bytes with a
 * {@link CacheKey} and supply a {@link BlobSource} used to populate the entry
 * on miss. The returned {@link BlobSource} reads from the cached entry on
 * subsequent calls.
 */
public interface BlobCache
{
    Blob get(CacheKey key, BlobSource source)
            throws IOException;

    void invalidate(CacheKey key);

    void invalidate(Collection<CacheKey> keys);
}
