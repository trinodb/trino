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

import io.trino.array.BigArrays;
import io.trino.array.LongBigArray;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.HashCommon;

import java.util.function.LongBinaryOperator;

import static io.airlift.slice.SizeOf.instanceSize;
import static it.unimi.dsi.fastutil.HashCommon.bigArraySize;
import static it.unimi.dsi.fastutil.HashCommon.maxFill;
import static java.util.Objects.requireNonNull;

// Note: this code was forked from fastutil (http://fastutil.di.unimi.it/) Long2LongOpenCustomHashMap
// and mimics that code style.
// Copyright (C) 2002-2019 Sebastiano Vigna
public class LongLong2LongOpenCustomBigHashMap
        implements Hash
{
    private static final int INSTANCE_SIZE = instanceSize(LongLong2LongOpenCustomBigHashMap.class);

    public interface HashStrategy
    {
        /**
         * Returns the hash code of the specified element with respect to this hash
         * strategy.
         *
         * @param e1 first half of the element.
         * @param e2 second half of the element.
         * @return the hash code of the given element with respect to this hash
         * strategy.
         */
        long hashCode(long e1, long e2);

        /**
         * Returns true if the given elements are equal with respect to this hash
         * strategy.
         *
         * @param a1 first half of an element.
         * @param a2 second half an element.
         * @param b1 first half of another element.
         * @param b2 second half of another element.
         * @return true if the two specified elements are equal with respect to this
         * hash strategy.
         */
        boolean equals(long a1, long a2, long b1, long b2);
    }

    private static final boolean ASSERTS = false;
    /**
     * The array of keys.
     */
    protected LongBigArray key1;
    protected LongBigArray key2;
    /**
     * The array of values.
     */
    protected LongBigArray value;
    /**
     * The mask for wrapping a position counter.
     */
    protected long mask;
    /**
     * Whether this map contains the key zero.
     */
    protected boolean containsNullKey;
    /**
     * The hash strategy of this custom map.
     */
    protected final HashStrategy strategy;
    /**
     * The current table size.
     */
    protected long n;
    /**
     * Threshold after which we rehash. It must be the table size times {@link #f}.
     */
    protected long maxFill;
    /**
     * We never resize below this threshold, which is the construction-time {#n}.
     */
    protected final long minN;
    /**
     * Number of entries in the set (including the key zero, if present).
     */
    protected long size;
    /**
     * The acceptable load factor.
     */
    protected final float f;

    /**
     * The default return value for {@code get()}, {@code put()} and
     * {@code remove()}.
     */
    protected long defRetValue;

    /**
     * The two-part value denoting unmapped keys (or null keys). These values may be passed back via the HashStrategy callback
     * during equality checks, even though no keys with these values have been added.
     */
    protected final long nullKey1;
    protected final long nullKey2;

    /**
     * Creates a new hash map.
     *
     * <p>
     * The actual table size will be the least power of two greater than
     * {@code expected}/{@code f}.
     *
     * @param expected the expected number of elements in the hash map.
     * @param f the load factor.
     * @param strategy the strategy.
     * @param nullKey1 first half of the representation for unmapped keys.
     * @param nullKey2 second half of the representation for unmapped keys.
     */
    public LongLong2LongOpenCustomBigHashMap(final long expected, final float f, final HashStrategy strategy, long nullKey1, long nullKey2)
    {
        this.strategy = strategy;
        if (f <= 0 || f > 1) {
            throw new IllegalArgumentException("Load factor must be greater than 0 and smaller than or equal to 1");
        }
        if (expected < 0) {
            throw new IllegalArgumentException("The expected number of elements must be nonnegative");
        }
        this.f = f;
        n = bigArraySize(expected, f);
        minN = n;
        mask = n - 1;
        maxFill = maxFill(n, f);
        this.nullKey1 = nullKey1;
        this.nullKey2 = nullKey2;
        key1 = new LongBigArray(nullKey1);
        key1.ensureCapacity(n + 1);
        key2 = new LongBigArray(nullKey2);
        key2.ensureCapacity(n + 1);
        value = new LongBigArray();
        value.ensureCapacity(n + 1);
    }

    /**
     * Creates a new hash map with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor.
     *
     * @param expected the expected number of elements in the hash map.
     * @param strategy the strategy.
     */
    public LongLong2LongOpenCustomBigHashMap(final long expected, final HashStrategy strategy, long nullKey1, long nullKey2)
    {
        this(expected, DEFAULT_LOAD_FACTOR, strategy, nullKey1, nullKey2);
    }

    /**
     * Creates a new hash map and zero based null keys.
     *
     * <p>
     * The actual table size will be the least power of two greater than
     * {@code expected}/{@code f}.
     *
     * @param expected the expected number of elements in the hash map.
     * @param f the load factor.
     * @param strategy the strategy.
     */
    public LongLong2LongOpenCustomBigHashMap(final long expected, final float f, final HashStrategy strategy)
    {
        this(expected, f, strategy, 0, 0);
    }

    /**
     * Creates a new hash map with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor
     * and zero based null keys.
     *
     * @param expected the expected number of elements in the hash map.
     * @param strategy the strategy.
     */
    public LongLong2LongOpenCustomBigHashMap(final long expected, final HashStrategy strategy)
    {
        this(expected, DEFAULT_LOAD_FACTOR, strategy);
    }

    /**
     * Creates a new hash map with initial expected
     * {@link BigArrays#SEGMENT_SIZE} entries and
     * {@link Hash#DEFAULT_LOAD_FACTOR} as load factor.
     *
     * @param strategy the strategy.
     * @param nullKey1 first half of the representation for unmapped keys.
     * @param nullKey2 second half of the representation for unmapped keys.
     */
    public LongLong2LongOpenCustomBigHashMap(final HashStrategy strategy, long nullKey1, long nullKey2)
    {
        this(BigArrays.SEGMENT_SIZE, strategy, nullKey1, nullKey2);
    }

    /**
     * Creates a new hash map with initial expected
     * {@link BigArrays#SEGMENT_SIZE} entries,
     * {@link Hash#DEFAULT_LOAD_FACTOR} as load factor,
     * and zero based null keys.
     *
     * @param strategy the strategy.
     */
    public LongLong2LongOpenCustomBigHashMap(final HashStrategy strategy)
    {
        this(BigArrays.SEGMENT_SIZE, DEFAULT_LOAD_FACTOR, strategy);
    }

    public void defaultReturnValue(final long rv)
    {
        defRetValue = rv;
    }

    public long defaultReturnValue()
    {
        return defRetValue;
    }

    /**
     * Returns the size of this hash map in bytes.
     */
    public long sizeOf()
    {
        return INSTANCE_SIZE + key1.sizeOf() + key2.sizeOf() + value.sizeOf();
    }

    /**
     * Returns the hashing strategy.
     *
     * @return the hashing strategy of this custom hash map.
     */
    public HashStrategy strategy()
    {
        return strategy;
    }

    private long realSize()
    {
        return containsNullKey ? size - 1 : size;
    }

    private long removeEntry(final long pos)
    {
        final long oldValue = value.get(pos);
        size--;
        shiftKeys(pos);
        if (n > minN && size < maxFill / 4 && n > BigArrays.SEGMENT_SIZE) {
            rehash(n / 2);
        }
        return oldValue;
    }

    private long removeNullEntry()
    {
        containsNullKey = false;
        final long oldValue = value.get(n);
        size--;
        if (n > minN && size < maxFill / 4 && n > BigArrays.SEGMENT_SIZE) {
            rehash(n / 2);
        }
        return oldValue;
    }

    private long find(final long k1, final long k2)
    {
        if ((strategy.equals((k1), (k2), (nullKey1), (nullKey2)))) {
            return containsNullKey ? n : -(n + 1);
        }
        long curr1;
        long curr2;
        final LongBigArray key1 = this.key1;
        final LongBigArray key2 = this.key2;
        // The starting point.
        long pos = HashCommon.mix(strategy.hashCode(k1, k2)) & mask;
        curr1 = key1.get(pos);
        curr2 = key2.get(pos);
        if ((curr1 == (nullKey1) && curr2 == (nullKey2))) {
            return -(pos + 1);
        }
        if ((strategy.equals((k1), (k2), (curr1), (curr2)))) {
            return pos;
        }
        // There's always an unused entry.
        while (true) {
            pos = (pos + 1) & mask;
            curr1 = key1.get(pos);
            curr2 = key2.get(pos);
            if ((curr1 == (nullKey1) && curr2 == (nullKey2))) {
                return -(pos + 1);
            }
            if ((strategy.equals((k1), (k2), (curr1), (curr2)))) {
                return pos;
            }
        }
    }

    private void insert(final long pos, final long k1, final long k2, final long v)
    {
        if (pos == n) {
            containsNullKey = true;
        }
        key1.set(pos, k1);
        key2.set(pos, k2);
        value.set(pos, v);
        if (size++ >= maxFill) {
            rehash(bigArraySize(size + 1, f));
        }
        if (ASSERTS) {
            checkTable();
        }
    }

    public long put(final long k1, final long k2, final long v)
    {
        final long pos = find(k1, k2);
        if (pos < 0) {
            insert(-pos - 1, k1, k2, v);
            return defRetValue;
        }
        final long oldValue = value.get(pos);
        value.set(pos, v);
        return oldValue;
    }

    private long addToValue(final long pos, final long incr)
    {
        final long oldValue = value.get(pos);
        value.set(pos, oldValue + incr);
        return oldValue;
    }

    /**
     * Adds an increment to value currently associated with a key.
     *
     * <p>
     * Note that this method respects the {@linkplain #defaultReturnValue() default
     * return value} semantics: when called with a key that does not currently
     * appears in the map, the key will be associated with the default return value
     * plus the given increment.
     *
     * @param k1 the first half of key.
     * @param k2 the second half of key.
     * @param incr the increment.
     * @return the old value, or the {@linkplain #defaultReturnValue() default
     * return value} if no value was present for the given key.
     */
    public long addTo(final long k1, final long k2, final long incr)
    {
        long pos;
        if ((strategy.equals((k1), (k2), (nullKey1), (nullKey2)))) {
            if (containsNullKey) {
                return addToValue(n, incr);
            }
            pos = n;
            containsNullKey = true;
        }
        else {
            final LongBigArray key1 = this.key1;
            final LongBigArray key2 = this.key2;
            // The starting point.
            pos = (it.unimi.dsi.fastutil.HashCommon.mix(strategy.hashCode(k1, k2))) & mask;
            long curr1 = key1.get(pos);
            long curr2 = key2.get(pos);
            if (!((curr1) == (nullKey1) && (curr2) == (nullKey2))) {
                if ((strategy.equals((curr1), (curr2), (k1), (k2)))) {
                    return addToValue(pos, incr);
                }
                pos = (pos + 1) & mask;
                curr1 = key1.get(pos);
                curr2 = key2.get(pos);
                while (!((curr1) == (nullKey1) && (curr2) == (nullKey2))) {
                    if ((strategy.equals((curr1), (curr2), (k1), (k2)))) {
                        return addToValue(pos, incr);
                    }
                    pos = (pos + 1) & mask;
                    curr1 = key1.get(pos);
                    curr2 = key2.get(pos);
                }
            }
        }
        key1.set(pos, k1);
        key2.set(pos, k2);
        value.set(pos, defRetValue + incr);
        if (size++ >= maxFill) {
            rehash(bigArraySize(size + 1, f));
        }
        if (ASSERTS) {
            checkTable();
        }
        return defRetValue;
    }

    /**
     * Shifts left entries with the specified hash code, starting at the specified
     * position, and empties the resulting free entry.
     *
     * @param pos a starting position.
     */
    protected final void shiftKeys(long pos)
    {
        // Shift entries with the same hash.
        long last;
        long slot;
        long curr1;
        long curr2;
        final LongBigArray key1 = this.key1;
        final LongBigArray key2 = this.key2;
        for (; ; ) {
            last = pos;
            pos = ((pos) + 1) & mask;
            for (; ; ) {
                curr1 = key1.get(pos);
                curr2 = key2.get(pos);
                if (((curr1) == (nullKey1)) && ((curr2) == (nullKey2))) {
                    key1.set(last, nullKey1);
                    key2.set(last, nullKey2);
                    return;
                }
                slot = (it.unimi.dsi.fastutil.HashCommon.mix(strategy.hashCode(curr1, curr2))) & mask;
                if (last <= pos ? last >= slot || slot > pos : last >= slot && slot > pos) {
                    break;
                }
                pos = (pos + 1) & mask;
            }
            key1.set(last, curr1);
            key2.set(last, curr2);
            value.set(last, value.get(pos));
        }
    }

    public long remove(final long k1, final long k2)
    {
        if ((strategy.equals((k1), (k2), (nullKey1), (nullKey2)))) {
            if (containsNullKey) {
                return removeNullEntry();
            }
            return defRetValue;
        }
        final LongBigArray key1 = this.key1;
        final LongBigArray key2 = this.key2;
        // The starting point.
        long pos = (it.unimi.dsi.fastutil.HashCommon.mix(strategy.hashCode(k1, k2))) & mask;
        long curr1 = key1.get(pos);
        long curr2 = key2.get(pos);
        if (((curr1) == (nullKey1)) && ((curr2) == (nullKey2))) {
            return defRetValue;
        }
        if ((strategy.equals((k1), (k2), (curr1), (curr2)))) {
            return removeEntry(pos);
        }
        while (true) {
            pos = (pos + 1) & mask;
            curr1 = key1.get(pos);
            curr2 = key2.get(pos);
            if (((curr1) == (nullKey1)) && ((curr2) == (nullKey2))) {
                return defRetValue;
            }
            if ((strategy.equals((k1), (k2), (curr1), (curr2)))) {
                return removeEntry(pos);
            }
        }
    }

    public long get(final long k1, final long k2)
    {
        if ((strategy.equals((k1), (k2), (nullKey1), (nullKey2)))) {
            return containsNullKey ? value.get(n) : defRetValue;
        }
        final LongBigArray key1 = this.key1;
        final LongBigArray key2 = this.key2;
        // The starting point.
        long pos = (it.unimi.dsi.fastutil.HashCommon.mix(strategy.hashCode(k1, k2))) & mask;
        long curr1 = key1.get(pos);
        long curr2 = key2.get(pos);
        if (((curr1) == (nullKey1)) && ((curr2) == (nullKey2))) {
            return defRetValue;
        }
        if ((strategy.equals((k1), (k2), (curr1), (curr2)))) {
            return value.get(pos);
        }
        // There's always an unused entry.
        while (true) {
            pos = (pos + 1) & mask;
            curr1 = key1.get(pos);
            curr2 = key2.get(pos);
            if (((curr1) == (nullKey1)) && ((curr2) == (nullKey2))) {
                return defRetValue;
            }
            if ((strategy.equals((k1), (k2), (curr1), (curr2)))) {
                return value.get(pos);
            }
        }
    }

    public boolean containsKey(final long k1, final long k2)
    {
        if ((strategy.equals((k1), (k2), (nullKey1), (nullKey2)))) {
            return containsNullKey;
        }
        final LongBigArray key1 = this.key1;
        final LongBigArray key2 = this.key2;
        // The starting point.
        long pos = (it.unimi.dsi.fastutil.HashCommon.mix(strategy.hashCode(k1, k2))) & mask;
        long curr1 = key1.get(pos);
        long curr2 = key2.get(pos);
        if (((curr1) == (nullKey1)) && ((curr2) == (nullKey2))) {
            return false;
        }
        if ((strategy.equals((k1), (k2), (curr1), (curr2)))) {
            return true;
        }
        // There's always an unused entry.
        while (true) {
            pos = (pos + 1) & mask;
            curr1 = key1.get(pos);
            curr2 = key2.get(pos);
            if (((curr1) == (nullKey1)) && ((curr2) == (nullKey2))) {
                return false;
            }
            if ((strategy.equals((k1), (k2), (curr1), (curr2)))) {
                return true;
            }
        }
    }

    public boolean containsValue(final long v)
    {
        final LongBigArray value = this.value;
        final LongBigArray key1 = this.key1;
        final LongBigArray key2 = this.key2;
        if (containsNullKey && ((value.get(n)) == (v))) {
            return true;
        }
        for (long i = n; i-- != 0; ) {
            if (!((key1.get(i)) == (nullKey1) && (key2.get(i)) == (nullKey2)) && ((value.get(i)) == (v))) {
                return true;
            }
        }
        return false;
    }

    public long getOrDefault(final long k1, final long k2, final long defaultValue)
    {
        if ((strategy.equals((k1), (k2), (nullKey1), (nullKey2)))) {
            return containsNullKey ? value.get(n) : defaultValue;
        }
        final LongBigArray key1 = this.key1;
        final LongBigArray key2 = this.key2;
        // The starting point.
        long pos = (it.unimi.dsi.fastutil.HashCommon.mix(strategy.hashCode(k1, k2))) & mask;
        long curr1 = key1.get(pos);
        long curr2 = key2.get(pos);
        if (((curr1) == (nullKey1)) && ((curr2) == (nullKey2))) {
            return defaultValue;
        }
        if ((strategy.equals((k1), (k2), (curr1), (curr2)))) {
            return value.get(pos);
        }
        // There's always an unused entry.
        while (true) {
            pos = (pos + 1) & mask;
            curr1 = key1.get(pos);
            curr2 = key2.get(pos);
            if (((curr1) == (nullKey1)) && ((curr2) == (nullKey2))) {
                return defaultValue;
            }
            if ((strategy.equals((k1), (k2), (curr1), (curr2)))) {
                return value.get(pos);
            }
        }
    }

    public long putIfAbsent(final long k1, final long k2, final long v)
    {
        final long pos = find(k1, k2);
        if (pos >= 0) {
            return value.get(pos);
        }
        insert(-pos - 1, k1, k2, v);
        return defRetValue;
    }

    public boolean remove(final long k1, final long k2, final long v)
    {
        if ((strategy.equals((k1), (k2), (nullKey1), (nullKey2)))) {
            if (containsNullKey && ((v) == (value.get(n)))) {
                removeNullEntry();
                return true;
            }
            return false;
        }
        final LongBigArray key1 = this.key1;
        final LongBigArray key2 = this.key2;
        // The starting point.
        long pos = (it.unimi.dsi.fastutil.HashCommon.mix(strategy.hashCode(k1, k2))) & mask;
        long curr1 = key1.get(pos);
        long curr2 = key2.get(pos);
        if (((curr1) == (nullKey1)) && ((curr2) == (nullKey2))) {
            return false;
        }
        if ((strategy.equals((k1), (k2), (curr1), (curr2))) && ((v) == (value.get(pos)))) {
            removeEntry(pos);
            return true;
        }
        while (true) {
            pos = (pos + 1) & mask;
            curr1 = key1.get(pos);
            curr2 = key2.get(pos);
            if (((curr1) == (nullKey1)) && ((curr2) == (nullKey2))) {
                return false;
            }
            if ((strategy.equals((k1), (k2), (curr1), (curr2))) && ((v) == (value.get(pos)))) {
                removeEntry(pos);
                return true;
            }
        }
    }

    public boolean replace(final long k1, final long k2, final long oldValue, final long v)
    {
        final long pos = find(k1, k2);
        if (pos < 0 || !((oldValue) == (value.get(pos)))) {
            return false;
        }
        value.set(pos, v);
        return true;
    }

    public long replace(final long k1, final long k2, final long v)
    {
        final long pos = find(k1, k2);
        if (pos < 0) {
            return defRetValue;
        }
        final long oldValue = value.get(pos);
        value.set(pos, v);
        return oldValue;
    }

    public long computeIfAbsent(final long k1, final long k2, final LongBinaryOperator mappingFunction)
    {
        requireNonNull(mappingFunction);
        final long pos = find(k1, k2);
        if (pos >= 0) {
            return value.get(pos);
        }
        final long newValue = mappingFunction.applyAsLong(k1, k2);
        insert(-pos - 1, k1, k2, newValue);
        return newValue;
    }

    public long merge(final long k1, final long k2, final long v,
            final java.util.function.BiFunction<? super Long, ? super Long, ? extends Long> remappingFunction)
    {
        requireNonNull(remappingFunction);
        final long pos = find(k1, k2);
        if (pos < 0) {
            insert(-pos - 1, k1, k2, v);
            return v;
        }
        final Long newValue = remappingFunction.apply(Long.valueOf(value.get(pos)), Long.valueOf(v));
        if (newValue == null) {
            if ((strategy.equals((k1), (k2), (nullKey1), (nullKey2)))) {
                removeNullEntry();
            }
            else {
                removeEntry(pos);
            }
            return defRetValue;
        }
        value.set(pos, (newValue).longValue());
        return (newValue).longValue();
    }

    /*
     * Removes all elements from this map.
     *
     * <p>To increase object reuse, this method does not change the table size. If
     * you want to reduce the table size, you must use {@link #trim()}.
     *
     */
    public void clear()
    {
        if (size == 0) {
            return;
        }
        size = 0;
        containsNullKey = false;
        key1.fill(nullKey1);
        key2.fill(nullKey2);
    }

    public long size()
    {
        return size;
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    /**
     * Rehashes the map, making the table as small as possible.
     *
     * <p>
     * This method rehashes the table to the smallest size satisfying the load
     * factor. It can be used when the set will not be changed anymore, so to
     * optimize access speed and size.
     *
     * <p>
     * If the table size is already the minimum possible, this method does nothing.
     *
     * @return true if there was enough memory to trim the map.
     * @see #trim(long)
     */
    public boolean trim()
    {
        return trim(size);
    }

    /**
     * Rehashes this map if the table is too large.
     *
     * <p>
     * Let <var>N</var> be the smallest table size that can hold
     * <code>max(n,{@link #size()})</code> entries, still satisfying the load
     * factor. If the current table size is smaller than or equal to <var>N</var>,
     * this method does nothing. Otherwise, it rehashes this map in a table of size
     * <var>N</var>.
     *
     * <p>
     * This method is useful when reusing maps. {@linkplain #clear() Clearing a map}
     * leaves the table size untouched. If you are reusing a map many times, you can
     * call this method with a typical size to avoid keeping around a very large
     * table just because of a few large transient maps.
     *
     * @param n the threshold for the trimming.
     * @return true if there was enough memory to trim the map.
     * @see #trim()
     */
    public boolean trim(final long n)
    {
        final long l = bigArraySize(n, f);
        if (l >= this.n || size > maxFill(l, f)) {
            return true;
        }
        try {
            rehash(l);
        }
        catch (OutOfMemoryError cantDoIt) {
            return false;
        }
        return true;
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

    protected void rehash(final long newN)
    {
        final LongBigArray key1 = this.key1;
        final LongBigArray key2 = this.key2;
        final LongBigArray value = this.value;
        final long mask = newN - 1; // Note that this is used by the hashing macro
        final LongBigArray newKey1 = new LongBigArray(nullKey1);
        newKey1.ensureCapacity((newN + 1));
        final LongBigArray newKey2 = new LongBigArray(nullKey2);
        newKey2.ensureCapacity((newN + 1));
        final LongBigArray newValue = new LongBigArray();
        newValue.ensureCapacity(newN + 1);
        long i = n;
        long pos;
        for (long j = realSize(); j-- != 0; ) {
            --i;
            while (((key1.get(i)) == (nullKey1)) && ((key2.get(i)) == (nullKey2))) {
                --i;
            }
            pos = (it.unimi.dsi.fastutil.HashCommon.mix(strategy.hashCode(key1.get(i), key2.get(i)))) & mask;
            if (!((newKey1.get(pos)) == (nullKey1) && (newKey2.get(pos)) == (nullKey2))) {
                pos = (pos + 1) & mask;
                while (!((newKey1.get(pos)) == (nullKey1) && (newKey2.get(pos)) == (nullKey2))) {
                    pos = (pos + 1) & mask;
                }
            }
            newKey1.set(pos, key1.get(i));
            newKey2.set(pos, key2.get(i));
            newValue.set(pos, value.get(i));
        }
        newValue.set(newN, value.get(n));
        n = newN;
        this.mask = mask;
        maxFill = maxFill(n, f);
        this.key1 = newKey1;
        this.key2 = newKey2;
        this.value = newValue;
    }

    private void checkTable()
    {
    }
}
