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
package io.trino.parquet;

import io.airlift.slice.Slice;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.parquet.BloomFilterStore.hash;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBloomFilterHash
{
    @Test
    public void testSliceHashMatchesParquet()
    {
        BlockSplitBloomFilter reference = new BlockSplitBloomFilter(1024);
        Random random = new Random(0);
        for (int length = 0; length <= 1024; length++) {
            byte[] bytes = new byte[length + 19];
            random.nextBytes(bytes);
            Slice base = wrappedBuffer(bytes, 3, length + 11);
            Slice value = base.slice(5, length);
            long expectedHash = reference.hash(Binary.fromConstantByteArray(value.getBytes()));
            assertThat(hash(value, 0, length)).isEqualTo(expectedHash);
            assertThat(hash(base, 5, length)).isEqualTo(expectedHash);
        }
    }
}
