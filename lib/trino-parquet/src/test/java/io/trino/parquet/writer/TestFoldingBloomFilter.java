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
package io.trino.parquet.writer;

import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class TestFoldingBloomFilter
{
    @Test
    void testFold()
            throws IOException
    {
        FoldingBloomFilter builder = new FoldingBloomFilter(6000, 0.05);
        assertThat(builder.filter().getBitsetSize()).isEqualTo(4096);
        assertThat(builder.fold().getBitsetSize()).isEqualTo(32);

        for (int i = 0; i < 500; i++) {
            builder.filter().insertHash(builder.filter().hash(i));
        }
        BlockSplitBloomFilter folded = builder.fold();
        assertThat(folded.getBitsetSize()).isEqualTo(512);

        BlockSplitBloomFilter reference = new BlockSplitBloomFilter(folded.getBitsetSize());
        for (int i = 0; i < 500; i++) {
            reference.insertHash(reference.hash(i));
        }
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        reference.writeTo(expected);
        ByteArrayOutputStream actual = new ByteArrayOutputStream();
        folded.writeTo(actual);
        assertThat(actual.toByteArray()).isEqualTo(expected.toByteArray());
    }

    @Test
    void testSaturatedFilter()
    {
        FoldingBloomFilter builder = new FoldingBloomFilter(256, 0.05);
        for (int i = 0; i < 2000; i++) {
            builder.filter().insertHash(builder.filter().hash(i));
        }
        assertThat(builder.fold().getBitsetSize()).isEqualTo(256);
    }
}
