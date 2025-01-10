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
package io.trino.sql.planner;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class TestLongBloomFilter
{
    @Test
    void testContains()
    {
        LongBloomFilter filter = new LongBloomFilter();
        filter.insert(-10);
        assertThat(filter.contains(-10)).isTrue();
        assertThat(filter.getMinDistinctHashes()).isEqualTo(1);
        filter.insert(-10);
        assertThat(filter.contains(-10)).isTrue();
        assertThat(filter.getMinDistinctHashes()).isEqualTo(1);

        filter.insert(0);
        assertThat(filter.contains(0)).isTrue();
        assertThat(filter.getMinDistinctHashes()).isEqualTo(2);

        filter.insert(2342);
        assertThat(filter.contains(2342)).isTrue();
        assertThat(filter.getMinDistinctHashes()).isEqualTo(3);

        filter.insert(Integer.MAX_VALUE);
        assertThat(filter.contains(Integer.MAX_VALUE)).isTrue();
        assertThat(filter.getMinDistinctHashes()).isEqualTo(4);

        assertThat(filter.contains(Integer.MIN_VALUE)).isFalse();
        assertThat(filter.contains(100)).isFalse();
        assertThat(filter.isEmpty()).isFalse();

        int valuesCount = 1_000_000;
        filter = new LongBloomFilter();
        for (int value = 0; value < valuesCount; value++) {
            if (value % 9 == 0) {
                filter.insert(value);
            }
        }
        assertThat(filter.getMinDistinctHashes()).isGreaterThanOrEqualTo(valuesCount / 10);
        assertThat(filter.isEmpty()).isFalse();

        int hits = 0;
        for (int value = 0; value < valuesCount; value++) {
            boolean contains = filter.contains(value);
            if (value % 9 == 0) {
                // No false negatives
                assertThat(contains).isTrue();
            }
            hits += contains ? 1 : 0;
        }
        assertThat((double) hits / valuesCount).isBetween(0.1, 0.115);
    }

    @Test
    void testIsEmpty()
    {
        LongBloomFilter filter = new LongBloomFilter();
        assertThat(filter.isEmpty()).isTrue();
        filter.insert(0);
        assertThat(filter.isEmpty()).isFalse();
    }

    @Test
    void testGetSizeInBytes()
    {
        LongBloomFilter filter = new LongBloomFilter();
        assertThat(filter.getSizeInBytes()).isEqualTo(2097152L);
        filter.insert(0);
        assertThat(filter.getSizeInBytes()).isEqualTo(2097152L);
        filter.insert(1);
        assertThat(filter.getSizeInBytes()).isEqualTo(2097152L);
    }

    @Test
    void testEquals()
    {
        LongBloomFilter filter1 = new LongBloomFilter();
        LongBloomFilter filter2 = new LongBloomFilter();
        assertThat(filter1).isEqualTo(filter2);
        filter1.insert(0);
        assertThat(filter1).isNotEqualTo(filter2);
        filter2.insert(0);
        assertThat(filter1).isEqualTo(filter2);
        filter2.insert(1);
        assertThat(filter1).isNotEqualTo(filter2);
        filter1.insert(1);
        assertThat(filter1).isEqualTo(filter2);
    }

    @Test
    void testMerge()
    {
        LongBloomFilter bloomFilter1 = new LongBloomFilter();
        LongBloomFilter bloomFilter2 = new LongBloomFilter();

        bloomFilter1.insert(1L);
        bloomFilter1.insert(2L);
        bloomFilter1.insert(3L);
        bloomFilter2.insert(3L);
        bloomFilter2.insert(4L);
        bloomFilter2.insert(5L);
        assertThat(bloomFilter1.getMinDistinctHashes()).isEqualTo(3);
        assertThat(bloomFilter2.getMinDistinctHashes()).isEqualTo(3);
        assertThat(bloomFilter1.contains(4L)).isFalse();
        assertThat(bloomFilter1.contains(5L)).isFalse();

        bloomFilter1.merge(bloomFilter2);

        assertThat(bloomFilter1.getMinDistinctHashes()).isEqualTo(6);
        assertThat(bloomFilter2.getMinDistinctHashes()).isEqualTo(3);
        assertThat(bloomFilter1.contains(1L)).isTrue();
        assertThat(bloomFilter1.contains(2L)).isTrue();
        assertThat(bloomFilter1.contains(3L)).isTrue();
        assertThat(bloomFilter1.contains(4L)).isTrue();
        assertThat(bloomFilter1.contains(5L)).isTrue();
        assertThat(bloomFilter1.contains(0L)).isFalse();
        assertThat(bloomFilter1.contains(6L)).isFalse();
        assertThat(bloomFilter2.contains(1L)).isFalse();
        assertThat(bloomFilter2.contains(2L)).isFalse();
        assertThat(bloomFilter1.getSizeInBytes()).isEqualTo(2097152L);
    }

    @Test
    void testIntersect()
    {
        LongBloomFilter bloomFilter1 = new LongBloomFilter();
        LongBloomFilter bloomFilter2 = new LongBloomFilter();

        bloomFilter1.insert(1L);
        bloomFilter1.insert(2L);
        bloomFilter1.insert(3L);
        bloomFilter2.insert(3L);
        bloomFilter2.insert(4L);
        bloomFilter2.insert(5L);
        assertThat(bloomFilter1.getMinDistinctHashes()).isEqualTo(3);
        assertThat(bloomFilter2.getMinDistinctHashes()).isEqualTo(3);
        assertThat(bloomFilter1.contains(4L)).isFalse();
        assertThat(bloomFilter1.contains(5L)).isFalse();

        bloomFilter1.intersect(bloomFilter2);

        assertThat(bloomFilter1.getMinDistinctHashes()).isEqualTo(3);
        assertThat(bloomFilter2.getMinDistinctHashes()).isEqualTo(3);
        assertThat(bloomFilter1.contains(1L)).isFalse();
        assertThat(bloomFilter1.contains(2L)).isFalse();
        assertThat(bloomFilter1.contains(3L)).isTrue();
        assertThat(bloomFilter1.contains(4L)).isFalse();
        assertThat(bloomFilter1.contains(5L)).isFalse();
        assertThat(bloomFilter1.contains(0L)).isFalse();
        assertThat(bloomFilter1.contains(6L)).isFalse();
        assertThat(bloomFilter1.getSizeInBytes()).isEqualTo(2097152L);

        assertThat(bloomFilter2.contains(3L)).isTrue();
        assertThat(bloomFilter2.contains(4L)).isTrue();
        assertThat(bloomFilter2.contains(5L)).isTrue();
    }
}
