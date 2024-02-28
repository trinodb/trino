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
package io.trino.spi.block;

// Note: this code was forked from fastutil (http://fastutil.di.unimi.it/) Int2IntOpenHashMap
// and mimics that code style.
// Copyright (C) 2002-2019 Sebastiano Vigna
class Int2IntOpenHashMap
{
    public static final int DEFAULT_RETURN_VALUE = -1;

    /**
     * 2<sup>32</sup> &middot; &phi;, &phi; = (&#x221A;5 &minus; 1)/2.
     */
    private static final int INT_PHI = 0x9E3779B9;
    /**
     * The default load factor of a hash table.
     */
    private static final float DEFAULT_LOAD_FACTOR = .75f;
    /**
     * The array of keys.
     */
    protected transient int[] key;
    /**
     * The array of values.
     */
    protected transient int[] value;
    /**
     * The mask for wrapping a position counter.
     */
    protected transient int mask;
    /**
     * Whether this map contains the key zero.
     */
    protected transient boolean containsNullKey;
    /**
     * The current table size.
     */
    protected transient int n;
    /**
     * Threshold after which we rehash. It must be the table size times {@link #f}.
     */
    protected transient int maxFill;
    /**
     * We never resize below this threshold, which is the construction-time {#n}.
     */
    protected final transient int minN;
    /**
     * Number of entries in the set (including the key zero, if present).
     */
    protected int size;
    /**
     * The acceptable load factor.
     */
    protected final float f;

    public Int2IntOpenHashMap(final int expected)
    {
        this(expected, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Creates a new hash map.
     *
     * <p>
     * The actual table size will be the least power of two greater than
     * {@code expected}/{@code f}.
     *
     * @param expected the expected number of elements in the hash map.
     * @param f the load factor.
     */

    public Int2IntOpenHashMap(final int expected, final float f)
    {
        if (f <= 0 || f > 1) {
            throw new IllegalArgumentException("Load factor must be greater than 0 and smaller than or equal to 1");
        }
        if (expected < 0) {
            throw new IllegalArgumentException("The expected number of elements must be nonnegative");
        }
        this.f = f;
        n = arraySize(expected, f);
        minN = n;
        mask = n - 1;
        maxFill = maxFill(n, f);
        key = new int[n + 1];
        value = new int[n + 1];
    }

    public int putIfAbsent(final int k, final int v)
    {
        final int pos = find(k);
        if (pos >= 0) {
            return value[pos];
        }
        insert(-pos - 1, k, v);
        return DEFAULT_RETURN_VALUE;
    }

    public int get(final int k)
    {
        if (k == 0) {
            return containsNullKey ? value[n] : DEFAULT_RETURN_VALUE;
        }
        final int[] key = this.key;
        // The starting point.
        int pos = mix(k) & mask;
        int curr = key[pos];
        if (curr == 0) {
            return DEFAULT_RETURN_VALUE;
        }
        if (k == curr) {
            return value[pos];
        }
        // There's always an unused entry.
        while (true) {
            pos = (pos + 1) & mask;
            curr = key[pos];
            if (curr == 0) {
                return DEFAULT_RETURN_VALUE;
            }
            if (k == curr) {
                return value[pos];
            }
        }
    }

    public boolean containsKey(final int k)
    {
        if (k == 0) {
            return containsNullKey;
        }
        final int[] key = this.key;
        // The starting point.
        int pos = mix(k) & mask;
        int curr = key[pos];
        if (curr == 0) {
            return false;
        }
        if (k == curr) {
            return true;
        }
        // There's always an unused entry.
        while (true) {
            pos = (pos + 1) & mask;
            curr = key[pos];
            if (curr == 0) {
                return false;
            }
            if (k == curr) {
                return true;
            }
        }
    }

    private void insert(final int pos, final int k, final int v)
    {
        if (pos == n) {
            containsNullKey = true;
        }
        key[pos] = k;
        value[pos] = v;
        if (size++ >= maxFill) {
            rehash(arraySize(size + 1, f));
        }
    }

    private int find(final int k)
    {
        if (k == 0) {
            return containsNullKey ? n : -(n + 1);
        }
        final int[] key = this.key;
        int pos = mix(k) & mask;
        int curr = key[pos];
        // The starting point.
        if (curr == 0) {
            return -(pos + 1);
        }
        if (k == curr) {
            return pos;
        }
        // There's always an unused entry.
        while (true) {
            pos = (pos + 1) & mask;
            curr = key[pos];
            if (curr == 0) {
                return -(pos + 1);
            }
            if (k == curr) {
                return pos;
            }
        }
    }

    /**
     * Rehashes the map.
     *
     * <p>
     * This method implements the basic rehashing strategy, and may be overridden by
     * subclasses implementing different rehashing strategies (e.g., disk-based
     * rehashing). However, you should not override this method unless you
     * understand the internal workings of this class.
     *
     * @param newN the new size
     */
    private void rehash(final int newN)
    {
        final int[] key = this.key;
        final int[] value = this.value;
        final int mask = newN - 1; // Note that this is used by the hashing macro
        final int[] newKey = new int[newN + 1];
        final int[] newValue = new int[newN + 1];
        int i = n;
        int pos;
        for (int j = realSize(); j-- != 0; ) {
            --i;
            while (key[i] == 0) {
                --i;
            }
            pos = mix(key[i]) & mask;
            if (!(newKey[pos] == 0)) {
                pos = (pos + 1) & mask;
                while (!(newKey[pos] == 0)) {
                    pos = (pos + 1) & mask;
                }
            }
            newKey[pos] = key[i];
            newValue[pos] = value[i];
        }
        newValue[newN] = value[n];
        n = newN;
        this.mask = mask;
        maxFill = maxFill(n, f);
        this.key = newKey;
        this.value = newValue;
    }

    private int realSize()
    {
        return containsNullKey ? size - 1 : size;
    }

    private static int mix(final int x)
    {
        final int h = x * INT_PHI;
        return h ^ (h >>> 16);
    }

    private static int maxFill(final int n, final float f)
    {
        /* We must guarantee that there is always at least
         * one free entry (even with pathological load factors). */
        return Math.min((int) Math.ceil(n * f), n - 1);
    }

    private static int arraySize(final int expected, final float f)
    {
        final long s = Math.max(2, nextPowerOfTwo((long) Math.ceil(expected / f)));
        if (s > (1 << 30)) {
            throw new IllegalArgumentException("Too large (" + expected + " expected elements with load factor " + f + ")");
        }
        return (int) s;
    }

    private static long nextPowerOfTwo(long x)
    {
        if (x == 0) {
            return 1;
        }
        x--;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        return (x | x >> 32) + 1;
    }
}
