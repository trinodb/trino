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

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLongLong2LongOpenCustomBigHashMap
{
    private static final LongLong2LongOpenCustomBigHashMap.HashStrategy DEFAULT_STRATEGY = new LongLong2LongOpenCustomBigHashMap.HashStrategy()
    {
        @Override
        public long hashCode(long e1, long e2)
        {
            return e1 * 31 + e2;
        }

        @Override
        public boolean equals(long a1, long a2, long b1, long b2)
        {
            return a1 == b1 && a2 == b2;
        }
    };

    @DataProvider
    public static Object[][] nullKeyValues()
    {
        return new Object[][] {{0L, 0L}, {1L, 1L}, {-1L, -1L}, {0L, -1L}};
    }

    @Test(dataProvider = "nullKeyValues")
    public void testBasicOps(long nullKey1, long nullKey2)
    {
        int expected = 100_000;
        LongLong2LongOpenCustomBigHashMap map = new LongLong2LongOpenCustomBigHashMap(expected, DEFAULT_STRATEGY, nullKey1, nullKey2);
        map.defaultReturnValue(-1);

        assertThat(map.isEmpty()).isTrue();
        assertThat(map.size()).isEqualTo(0);
        assertThat(map.get(0, 0)).isEqualTo(-1);
        assertThat(map.get(1, -1)).isEqualTo(-1);

        List<Long> values = Arrays.asList(Long.MIN_VALUE, -10L, 0L, 10L, Long.MAX_VALUE);

        // Put
        int count = 0;
        for (long key1 : values) {
            for (long key2 : values) {
                count++;
                assertThat(map.put(key1, key2, count - 1)).isEqualTo(-1);
                assertThat(map.isEmpty()).isFalse();
                assertThat(map.size()).isEqualTo(count);
            }
        }

        // Replace
        count = 0;
        for (long key1 : values) {
            for (long key2 : values) {
                count++;
                assertThat(map.replace(key1, key2, count - 1, count)).isTrue();
                assertThat(map.isEmpty()).isFalse();
                assertThat(map.size()).isEqualTo((long) values.size() * values.size());
            }
        }

        // Get
        count = 0;
        for (long key1 : values) {
            for (long key2 : values) {
                count++;
                assertThat(map.containsKey(key1, key2)).isTrue();
                assertThat(map.containsValue(count)).isTrue();
                assertThat(map.get(key1, key2)).isEqualTo(count);
            }
        }

        // Remove
        count = 0;
        for (long key1 : values) {
            for (long key2 : values) {
                count++;
                assertThat(map.remove(key1, key2)).isEqualTo(count);
            }
        }
    }

    @Test(dataProvider = "nullKeyValues")
    public void testHashCollision(long nullKey1, long nullKey2)
    {
        LongLong2LongOpenCustomBigHashMap.HashStrategy collisionHashStrategy = new LongLong2LongOpenCustomBigHashMap.HashStrategy()
        {
            @Override
            public long hashCode(long e1, long e2)
            {
                // Force collisions
                return 0;
            }

            @Override
            public boolean equals(long a1, long a2, long b1, long b2)
            {
                return a1 == b1 && a2 == b2;
            }
        };

        LongLong2LongOpenCustomBigHashMap map = new LongLong2LongOpenCustomBigHashMap(collisionHashStrategy, nullKey1, nullKey2);
        map.defaultReturnValue(-1);

        List<Long> values = Arrays.asList(Long.MIN_VALUE, -10L, 0L, 10L, Long.MAX_VALUE);

        // Put
        int count = 0;
        for (long key1 : values) {
            for (long key2 : values) {
                count++;
                assertThat(map.put(key1, key2, count - 1)).isEqualTo(-1);
                assertThat(map.isEmpty()).isFalse();
                assertThat(map.size()).isEqualTo(count);
            }
        }

        // Replace
        count = 0;
        for (long key1 : values) {
            for (long key2 : values) {
                count++;
                assertThat(map.replace(key1, key2, count - 1, count)).isTrue();
                assertThat(map.isEmpty()).isFalse();
                assertThat(map.size()).isEqualTo((long) values.size() * values.size());
            }
        }

        // Get
        count = 0;
        for (long key1 : values) {
            for (long key2 : values) {
                count++;
                assertThat(map.containsKey(key1, key2)).isTrue();
                assertThat(map.containsValue(count)).isTrue();
                assertThat(map.get(key1, key2)).isEqualTo(count);
            }
        }

        // Remove
        count = 0;
        for (long key1 : values) {
            for (long key2 : values) {
                count++;
                assertThat(map.remove(key1, key2)).isEqualTo(count);
            }
        }
    }

    @Test(dataProvider = "nullKeyValues")
    public void testRehash(long nullKey1, long nullKey2)
    {
        int initialCapacity = 1;
        LongLong2LongOpenCustomBigHashMap map = new LongLong2LongOpenCustomBigHashMap(initialCapacity, DEFAULT_STRATEGY, nullKey1, nullKey2);
        map.defaultReturnValue(-1);

        // Inserting 1M elements should be enough to trigger some rehashes given an initial capacity of 1.

        int count = 0;
        for (long key1 = 0; key1 < 1000; key1++) {
            for (long key2 = 0; key2 < 1000; key2++) {
                count++;
                assertThat(map.put(key1, key2, count)).isEqualTo(-1);
            }
        }

        count = 0;
        for (long key1 = 0; key1 < 1000; key1++) {
            for (long key2 = 0; key2 < 1000; key2++) {
                count++;
                assertThat(map.get(key1, key2)).isEqualTo(count);
            }
        }

        // Remove most of the elements and force a trim()
        for (long key1 = 1; key1 < 1000; key1++) {
            for (long key2 = 0; key2 < 1000; key2++) {
                map.remove(key1, key2);
            }
        }
        map.trim();

        // Make sure we can still fetch the remaining keys
        count = 0;
        for (long key2 = 0; key2 < 1000; key2++) {
            count++;
            assertThat(map.get(0, key2)).isEqualTo(count);
        }
    }
}
