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

import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.ValueBlock;

public interface NullSafeHash
{
    long hash(ValueBlock block, int position);

    /**
     * Hashes the block and stores the result in the hashes array.
     * Should be used for single channel Pages and the first channel of multi-channel Pages.
     */
    void hashBatched(ValueBlock block, long[] hashes, int offset, int length);

    void hashBatchedDictionary(DictionaryBlock dictionaryBlock, long[] hashes, int offset, int length);

    /**
     * Hashes the block and combines the result with the existing hash in the hashes array.
     * Should be used for combining hashes for multi-channel Pages after the first channel is processed by hashBatched.
     */
    void hashBatchedWithCombine(ValueBlock block, long[] hashes, int offset, int length);

    void hashBatchedDictionaryWithCombine(DictionaryBlock dictionaryBlock, long[] hashes, int offset, int length);
}
