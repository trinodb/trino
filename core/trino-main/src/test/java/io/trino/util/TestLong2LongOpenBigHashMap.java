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
package io.trino.util;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLong2LongOpenBigHashMap
{
    @Test
    public void testBasicOps()
    {
        int expected = 100_000;
        Long2LongOpenBigHashMap map = new Long2LongOpenBigHashMap(expected);
        map.defaultReturnValue(-1);

        assertThat(map.isEmpty()).isTrue();
        assertThat(map.size()).isEqualTo(0);
        assertThat(map.get(0)).isEqualTo(-1);
        assertThat(map.get(1)).isEqualTo(-1);

        List<Long> values = Arrays.asList(Long.MIN_VALUE, -10L, 0L, 10L, Long.MAX_VALUE);

        // Put
        int count = 0;
        for (long key : values) {
            count++;
            assertThat(map.put(key, count - 1)).isEqualTo(-1);
            assertThat(map.isEmpty()).isFalse();
            assertThat(map.size()).isEqualTo(count);
        }

        // Replace
        count = 0;
        for (long key : values) {
            count++;
            assertThat(map.replace(key, count - 1, count)).isTrue();
            assertThat(map.isEmpty()).isFalse();
            assertThat(map.size()).isEqualTo(values.size());
        }

        // Get
        count = 0;
        for (long key : values) {
            count++;
            assertThat(map.containsKey(key)).isTrue();
            assertThat(map.containsValue(count)).isTrue();
            assertThat(map.get(key)).isEqualTo(count);
        }

        // Remove
        count = 0;
        for (long key : values) {
            count++;
            assertThat(map.remove(key)).isEqualTo(count);
        }
    }

    @Test
    public void testRehash()
    {
        int initialCapacity = 1;
        Long2LongOpenBigHashMap map = new Long2LongOpenBigHashMap(initialCapacity);
        map.defaultReturnValue(-1);

        // Inserting 1M elements should be enough to trigger some rehashes given an initial capacity of 1.

        for (long key = 0; key < 1_000_000; key++) {
            assertThat(map.put(key, key + 1)).isEqualTo(-1);
        }

        for (long key = 0; key < 1_000_000; key++) {
            assertThat(map.get(key)).isEqualTo(key + 1);
        }

        // Remove most of the elements and force a trim()
        for (long key = 1; key < 1_000_000; key++) {
            map.remove(key);
        }
        map.trim();

        // Make sure we can still fetch the remaining key
        assertThat(map.get(0)).isEqualTo(1);
    }
}
