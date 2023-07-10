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
import it.unimi.dsi.fastutil.longs.AbstractLong2LongMap;
import it.unimi.dsi.fastutil.longs.AbstractLongCollection;
import it.unimi.dsi.fastutil.longs.AbstractLongSet;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongCollection;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.AbstractObjectSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static it.unimi.dsi.fastutil.HashCommon.bigArraySize;
import static it.unimi.dsi.fastutil.HashCommon.maxFill;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

// Note: this code was forked from fastutil (http://fastutil.di.unimi.it/) Long2LongOpenHashMap
// and mimics that code style.
// Copyright (C) 2002-2019 Sebastiano Vigna
public class Long2LongOpenBigHashMap
        extends AbstractLong2LongMap
        implements Hash
{
    private static final long INSTANCE_SIZE = instanceSize(Long2LongOpenBigHashMap.class);
    private static final boolean ASSERTS = false;
    /**
     * The array of keys.
     */
    protected LongBigArray key;
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
     * Cached set of entries.
     */
    protected FastEntrySet entries;
    /**
     * Cached set of keys.
     */
    protected LongSet keys;
    /**
     * Cached collection of values.
     */
    protected LongCollection values;

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
    public Long2LongOpenBigHashMap(final long expected, final float f)
    {
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
        key = new LongBigArray();
        key.ensureCapacity(n + 1);
        value = new LongBigArray();
        value.ensureCapacity(n + 1);
    }

    /**
     * Creates a new hash map with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor.
     *
     * @param expected the expected number of elements in the hash map.
     */
    public Long2LongOpenBigHashMap(final long expected)
    {
        this(expected, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Creates a new hash map with initial expected
     * {@link BigArrays#SEGMENT_SIZE} entries and
     * {@link Hash#DEFAULT_LOAD_FACTOR} as load factor.
     */
    public Long2LongOpenBigHashMap()
    {
        this(BigArrays.SEGMENT_SIZE, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Creates a new hash map copying a given one.
     *
     * @param m a {@link Map} to be copied into the new hash map.
     * @param f the load factor.
     */
    public Long2LongOpenBigHashMap(final Map<? extends Long, ? extends Long> m, final float f)
    {
        this(m.size(), f);
        putAll(m);
    }

    /**
     * Creates a new hash map with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor
     * copying a given one.
     *
     * @param m a {@link Map} to be copied into the new hash map.
     */
    public Long2LongOpenBigHashMap(final Map<? extends Long, ? extends Long> m)
    {
        this(m, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Creates a new hash map copying a given type-specific one.
     *
     * @param m a type-specific map to be copied into the new hash map.
     * @param f the load factor.
     */
    public Long2LongOpenBigHashMap(final Long2LongMap m, final float f)
    {
        this(m.size(), f);
        putAll(m);
    }

    /**
     * Creates a new hash map with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor
     * copying a given type-specific one.
     *
     * @param m a type-specific map to be copied into the new hash map.
     */
    public Long2LongOpenBigHashMap(final Long2LongMap m)
    {
        this(m, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Returns the size of this hash map in bytes.
     */
    public long sizeOf()
    {
        return INSTANCE_SIZE + key.sizeOf() + value.sizeOf();
    }

    private long realSize()
    {
        return containsNullKey ? size - 1 : size;
    }

    private void ensureCapacity(final long capacity)
    {
        final long needed = bigArraySize(capacity, f);
        if (needed > n) {
            rehash(needed);
        }
    }

    private void tryCapacity(final long capacity)
    {
        final long needed = Math.max(2, bigArraySize(capacity, f));
        if (needed > n) {
            rehash(needed);
        }
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

    @Override
    public void putAll(Map<? extends Long, ? extends Long> m)
    {
        if (f <= .5) {
            ensureCapacity(m.size()); // The resulting map will be sized for m.size() elements
        }
        else {
            tryCapacity(size() + m.size()); // The resulting map will be tentatively sized for size() + m.size()
        }
        // elements
        super.putAll(m);
    }

    private long find(final long k)
    {
        if (((k) == (0))) {
            return containsNullKey ? n : -(n + 1);
        }
        final LongBigArray key = this.key;
        // The starting point.
        long pos = it.unimi.dsi.fastutil.HashCommon.mix((k)) & mask;
        long curr = key.get(pos);
        if (((curr) == (0))) {
            return -(pos + 1);
        }
        if (((k) == (curr))) {
            return pos;
        }
        // There's always an unused entry.
        while (true) {
            pos = (pos + 1) & mask;
            curr = key.get(pos);
            if (((curr) == (0))) {
                return -(pos + 1);
            }
            if (((k) == (curr))) {
                return pos;
            }
        }
    }

    private void insert(final long pos, final long k, final long v)
    {
        if (pos == n) {
            containsNullKey = true;
        }
        key.set(pos, k);
        value.set(pos, v);
        if (size++ >= maxFill) {
            rehash(bigArraySize(size + 1, f));
        }
        if (ASSERTS) {
            checkTable();
        }
    }

    @Override
    public long put(final long k, final long v)
    {
        final long pos = find(k);
        if (pos < 0) {
            insert(-pos - 1, k, v);
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
     * @param k the key.
     * @param incr the increment.
     * @return the old value, or the {@linkplain #defaultReturnValue() default
     * return value} if no value was present for the given key.
     */
    public long addTo(final long k, final long incr)
    {
        long pos;
        if (((k) == (0))) {
            if (containsNullKey) {
                return addToValue(n, incr);
            }
            pos = n;
            containsNullKey = true;
        }
        else {
            final LongBigArray key = this.key;
            // The starting point.
            pos = it.unimi.dsi.fastutil.HashCommon.mix((k)) & mask;
            long curr = key.get(pos);
            if (!((curr) == (0))) {
                if (((curr) == (k))) {
                    return addToValue(pos, incr);
                }
                pos = (pos + 1) & mask;
                curr = key.get(pos);
                while (!((curr) == (0))) {
                    if (((curr) == (k))) {
                        return addToValue(pos, incr);
                    }
                    pos = (pos + 1) & mask;
                    curr = key.get(pos);
                }
            }
        }
        key.set(pos, k);
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
        long curr;
        final LongBigArray key = this.key;
        for (; ; ) {
            last = pos;
            pos = (pos + 1) & mask;
            for (; ; ) {
                curr = key.get(pos);
                if (((curr) == (0))) {
                    key.set(last, (0));
                    return;
                }
                slot = it.unimi.dsi.fastutil.HashCommon.mix((curr)) & mask;
                if (last <= pos ? last >= slot || slot > pos : last >= slot && slot > pos) {
                    break;
                }
                pos = (pos + 1) & mask;
            }
            key.set(last, curr);
            value.set(last, value.get(pos));
        }
    }

    @Override
    public long remove(final long k)
    {
        if (((k) == (0))) {
            if (containsNullKey) {
                return removeNullEntry();
            }
            return defRetValue;
        }
        final LongBigArray key = this.key;
        // The starting point.
        long pos = it.unimi.dsi.fastutil.HashCommon.mix((k)) & mask;
        long curr = key.get(pos);
        if (((curr) == (0))) {
            return defRetValue;
        }
        if (((k) == (curr))) {
            return removeEntry(pos);
        }
        while (true) {
            pos = (pos + 1) & mask;
            curr = key.get(pos);
            if (((curr) == (0))) {
                return defRetValue;
            }
            if (((k) == (curr))) {
                return removeEntry(pos);
            }
        }
    }

    @Override
    public long get(final long k)
    {
        if (((k) == (0))) {
            return containsNullKey ? value.get(n) : defRetValue;
        }
        final LongBigArray key = this.key;
        // The starting point.
        long pos = it.unimi.dsi.fastutil.HashCommon.mix((k)) & mask;
        long curr = key.get(pos);
        if (((curr) == (0))) {
            return defRetValue;
        }
        if (((k) == (curr))) {
            return value.get(pos);
        }
        // There's always an unused entry.
        while (true) {
            pos = (pos + 1) & mask;
            curr = key.get(pos);
            if (((curr) == (0))) {
                return defRetValue;
            }
            if (((k) == (curr))) {
                return value.get(pos);
            }
        }
    }

    @Override
    public boolean containsKey(final long k)
    {
        if (((k) == (0))) {
            return containsNullKey;
        }
        final LongBigArray key = this.key;
        // The starting point.
        long pos = it.unimi.dsi.fastutil.HashCommon.mix((k)) & mask;
        long curr = key.get(pos);
        if (((curr) == (0))) {
            return false;
        }
        if (((k) == (curr))) {
            return true;
        }
        // There's always an unused entry.
        while (true) {
            pos = (pos + 1) & mask;
            curr = key.get(pos);
            if (((curr) == (0))) {
                return false;
            }
            if (((k) == (curr))) {
                return true;
            }
        }
    }

    @Override
    public boolean containsValue(final long v)
    {
        final LongBigArray value = this.value;
        final LongBigArray key = this.key;
        if (containsNullKey && ((value.get(n)) == (v))) {
            return true;
        }
        for (long i = n; i-- != 0; ) {
            if (!((key.get(i)) == (0)) && ((value.get(i)) == (v))) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getOrDefault(final long k, final long defaultValue)
    {
        if (((k) == (0))) {
            return containsNullKey ? value.get(n) : defaultValue;
        }
        final LongBigArray key = this.key;
        // The starting point.
        long pos = it.unimi.dsi.fastutil.HashCommon.mix((k)) & mask;
        long curr = key.get(pos);
        if (((curr) == (0))) {
            return defaultValue;
        }
        if (((k) == (curr))) {
            return value.get(pos);
        }
        // There's always an unused entry.
        while (true) {
            pos = (pos + 1) & mask;
            curr = key.get(pos);
            if (((curr) == (0))) {
                return defaultValue;
            }
            if (((k) == (curr))) {
                return value.get(pos);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long putIfAbsent(final long k, final long v)
    {
        final long pos = find(k);
        if (pos >= 0) {
            return value.get(pos);
        }
        insert(-pos - 1, k, v);
        return defRetValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(final long k, final long v)
    {
        if (((k) == (0))) {
            if (containsNullKey && ((v) == (value.get(n)))) {
                removeNullEntry();
                return true;
            }
            return false;
        }
        final LongBigArray key = this.key;
        // The starting point.
        long pos = it.unimi.dsi.fastutil.HashCommon.mix((k)) & mask;
        long curr = key.get(pos);
        if (((curr) == (0))) {
            return false;
        }
        if (((k) == (curr)) && ((v) == (value.get(pos)))) {
            removeEntry(pos);
            return true;
        }
        while (true) {
            pos = (pos + 1) & mask;
            curr = key.get(pos);
            if (((curr) == (0))) {
                return false;
            }
            if (((k) == (curr)) && ((v) == (value.get(pos)))) {
                removeEntry(pos);
                return true;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean replace(final long k, final long oldValue, final long v)
    {
        final long pos = find(k);
        if (pos < 0 || !((oldValue) == (value.get(pos)))) {
            return false;
        }
        value.set(pos, v);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long replace(final long k, final long v)
    {
        final long pos = find(k);
        if (pos < 0) {
            return defRetValue;
        }
        final long oldValue = value.get(pos);
        value.set(pos, v);
        return oldValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long computeIfAbsent(final long k, final java.util.function.LongUnaryOperator mappingFunction)
    {
        requireNonNull(mappingFunction);
        final long pos = find(k);
        if (pos >= 0) {
            return value.get(pos);
        }
        final long newValue = mappingFunction.applyAsLong(k);
        insert(-pos - 1, k, newValue);
        return newValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long computeIfAbsentNullable(final long k,
            final java.util.function.LongFunction<? extends Long> mappingFunction)
    {
        requireNonNull(mappingFunction);
        final long pos = find(k);
        if (pos >= 0) {
            return value.get(pos);
        }
        final Long newValue = mappingFunction.apply(k);
        if (newValue == null) {
            return defRetValue;
        }
        final long v = (newValue).longValue();
        insert(-pos - 1, k, v);
        return v;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long computeIfPresent(final long k,
            final java.util.function.BiFunction<? super Long, ? super Long, ? extends Long> remappingFunction)
    {
        requireNonNull(remappingFunction);
        final long pos = find(k);
        if (pos < 0) {
            return defRetValue;
        }
        final Long newValue = remappingFunction.apply(Long.valueOf(k), Long.valueOf(value.get(pos)));
        if (newValue == null) {
            if (((k) == (0))) {
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

    /**
     * {@inheritDoc}
     */
    @Override
    public long compute(final long k,
            final java.util.function.BiFunction<? super Long, ? super Long, ? extends Long> remappingFunction)
    {
        requireNonNull(remappingFunction);
        final long pos = find(k);
        final Long newValue = remappingFunction.apply(Long.valueOf(k), pos >= 0 ? Long.valueOf(value.get(pos)) : null);
        if (newValue == null) {
            if (pos >= 0) {
                if (((k) == (0))) {
                    removeNullEntry();
                }
                else {
                    removeEntry(pos);
                }
            }
            return defRetValue;
        }
        long newVal = (newValue).longValue();
        if (pos < 0) {
            insert(-pos - 1, k, newVal);
            return newVal;
        }
        value.set(pos, newVal);
        return newVal;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long merge(final long k, final long v,
            final java.util.function.BiFunction<? super Long, ? super Long, ? extends Long> remappingFunction)
    {
        requireNonNull(remappingFunction);
        final long pos = find(k);
        if (pos < 0) {
            insert(-pos - 1, k, v);
            return v;
        }
        final Long newValue = remappingFunction.apply(Long.valueOf(value.get(pos)), Long.valueOf(v));
        if (newValue == null) {
            if (((k) == (0))) {
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
    @Override
    public void clear()
    {
        if (size == 0) {
            return;
        }
        size = 0;
        containsNullKey = false;
        key.fill(0);
    }

    @Override
    public int size()
    {
        return toIntExact(size);
    }

    @Override
    public boolean isEmpty()
    {
        return size == 0;
    }

    /**
     * The entry class for a hash map does not record key and value, but rather the
     * position in the hash table of the corresponding entry. This is necessary so
     * that calls to {@link java.util.Map.Entry#setValue(Object)} are reflected in
     * the map
     */
    final class MapEntry
            implements Long2LongMap.Entry, Map.Entry<Long, Long>
    {
        // The table index this entry refers to, or -1 if this entry has been deleted.
        long index;

        MapEntry(final long index)
        {
            this.index = index;
        }

        MapEntry()
        {
        }

        @Override
        public long getLongKey()
        {
            return key.get(index);
        }

        @Override
        public long getLongValue()
        {
            return value.get(index);
        }

        @Override
        public long setValue(final long v)
        {
            final long oldValue = value.get(index);
            value.set(index, v);
            return oldValue;
        }

        /**
         * {@inheritDoc}
         *
         * @deprecated Please use the corresponding type-specific method instead.
         */
        @Deprecated
        @Override
        public Long getKey()
        {
            return Long.valueOf(key.get(index));
        }

        /**
         * {@inheritDoc}
         *
         * @deprecated Please use the corresponding type-specific method instead.
         */
        @Deprecated
        @Override
        public Long getValue()
        {
            return Long.valueOf(value.get(index));
        }

        /**
         * {@inheritDoc}
         *
         * @deprecated Please use the corresponding type-specific method instead.
         */
        @Deprecated
        @Override
        public Long setValue(final Long v)
        {
            return Long.valueOf(setValue((v).longValue()));
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean equals(final Object o)
        {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            Map.Entry<Long, Long> e = (Map.Entry<Long, Long>) o;
            return ((key.get(index)) == ((e.getKey()).longValue())) && ((value.get(index)) == ((e.getValue()).longValue()));
        }

        @Override
        public int hashCode()
        {
            return it.unimi.dsi.fastutil.HashCommon.long2int(key.get(index))
                    ^ it.unimi.dsi.fastutil.HashCommon.long2int(value.get(index));
        }

        @Override
        public String toString()
        {
            return key.get(index) + "=>" + value.get(index);
        }
    }

    /**
     * An iterator over a hash map.
     */
    private class MapIterator
    {
        /**
         * The index of the last entry returned, if positive or zero; initially,
         * {@link #n}. If negative, the last entry returned was that of the key of index
         * {@code - pos - 1} from the {@link #wrapped} list.
         */
        long pos = n;
        /**
         * The index of the last entry that has been returned (more precisely, the value
         * of {@link #pos} if {@link #pos} is positive, or {@link Long#MIN_VALUE} if
         * {@link #pos} is negative). It is -1 if either we did not return an entry yet,
         * or the last returned entry has been removed.
         */
        long last = -1;
        /**
         * A downward counter measuring how many entries must still be returned.
         */
        long c = size;
        /**
         * A boolean telling us whether we should return the entry with the null key.
         */
        boolean mustReturnNullKey = Long2LongOpenBigHashMap.this.containsNullKey;
        /**
         * A lazily allocated list containing keys of entries that have wrapped around
         * the table because of removals.
         */
        LongBigArrayBigList wrapped;

        public boolean hasNext()
        {
            return c != 0;
        }

        public long nextEntry()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            c--;
            if (mustReturnNullKey) {
                mustReturnNullKey = false;
                last = n;
                return n;
            }
            final LongBigArray key = Long2LongOpenBigHashMap.this.key;
            for (; ; ) {
                if (--pos < 0) {
                    // We are just enumerating elements from the wrapped list.
                    last = Long.MIN_VALUE;
                    final long k = wrapped.getLong(-pos - 1);
                    long p = it.unimi.dsi.fastutil.HashCommon.mix((k)) & mask;
                    while (!((k) == (key.get(p)))) {
                        p = (p + 1) & mask;
                    }
                    return p;
                }
                if (!((key.get(pos)) == (0))) {
                    last = pos;
                    return pos;
                }
            }
        }

        /**
         * Shifts left entries with the specified hash code, starting at the specified
         * position, and empties the resulting free entry.
         *
         * @param pos a starting position.
         */
        private void shiftKeys(long pos)
        {
            // Shift entries with the same hash.
            long last;
            long slot;
            long curr;
            final LongBigArray key = Long2LongOpenBigHashMap.this.key;
            for (; ; ) {
                last = pos;
                pos = (pos + 1) & mask;
                for (; ; ) {
                    curr = key.get(pos);
                    if (((curr) == (0))) {
                        key.set(last, (0));
                        return;
                    }
                    slot = it.unimi.dsi.fastutil.HashCommon.mix((curr)) & mask;
                    if (last <= pos ? last >= slot || slot > pos : last >= slot && slot > pos) {
                        break;
                    }
                    pos = (pos + 1) & mask;
                }
                if (pos < last) { // Wrapped entry.
                    if (wrapped == null) {
                        wrapped = new LongBigArrayBigList(2);
                    }
                    wrapped.add(key.get(pos));
                }
                key.set(last, curr);
                value.set(last, value.get(pos));
            }
        }

        public void remove()
        {
            if (last == -1) {
                throw new IllegalStateException();
            }
            if (last == n) {
                containsNullKey = false;
            }
            else if (pos >= 0) {
                shiftKeys(last);
            }
            else {
                // We're removing wrapped entries.
                Long2LongOpenBigHashMap.this.remove(wrapped.getLong(-pos - 1));
                last = -1; // Note that we must not decrement size
                return;
            }
            size--;
            last = -1; // You can no longer remove this entry.
            if (ASSERTS) {
                checkTable();
            }
        }

        public long skip(final long n)
        {
            long i = n;
            while (i-- != 0 && hasNext()) {
                nextEntry();
            }
            return n - i - 1;
        }
    }

    private class EntryIterator
            extends MapIterator
            implements ObjectIterator<Long2LongMap.Entry>
    {
        private MapEntry entry;

        @Override
        public MapEntry next()
        {
            entry = new MapEntry(nextEntry());
            return entry;
        }

        @Override
        public void remove()
        {
            super.remove();
            entry.index = -1; // You cannot use a deleted entry.
        }
    }

    private class FastEntryIterator
            extends MapIterator
            implements ObjectIterator<Long2LongMap.Entry>
    {
        private final MapEntry entry = new MapEntry();

        @Override
        public MapEntry next()
        {
            entry.index = nextEntry();
            return entry;
        }
    }

    private final class MapEntrySet
            extends AbstractObjectSet<Long2LongMap.Entry>
            implements FastEntrySet
    {
        @Override
        public ObjectIterator<Long2LongMap.Entry> iterator()
        {
            return new EntryIterator();
        }

        @Override
        public ObjectIterator<Long2LongMap.Entry> fastIterator()
        {
            return new FastEntryIterator();
        }

        @Override
        public boolean contains(final Object o)
        {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            final Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            if (e.getKey() == null || !(e.getKey() instanceof Long)) {
                return false;
            }
            if (e.getValue() == null || !(e.getValue() instanceof Long)) {
                return false;
            }
            final long k = ((Long) (e.getKey())).longValue();
            final long v = ((Long) (e.getValue())).longValue();
            if (((k) == (0))) {
                return Long2LongOpenBigHashMap.this.containsNullKey && ((value.get(n)) == (v));
            }
            final LongBigArray key = Long2LongOpenBigHashMap.this.key;
            // The starting point.
            long pos = it.unimi.dsi.fastutil.HashCommon.mix((k)) & mask;
            long curr = key.get(pos);
            if (((curr) == (0))) {
                return false;
            }
            if (((k) == (curr))) {
                return ((value.get(pos)) == (v));
            }
            // There's always an unused entry.
            while (true) {
                pos = (pos + 1) & mask;
                curr = key.get(pos);
                if (((curr) == (0))) {
                    return false;
                }
                if (((k) == (curr))) {
                    return ((value.get(pos)) == (v));
                }
            }
        }

        @Override
        public boolean remove(final Object o)
        {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            final Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            if (e.getKey() == null || !(e.getKey() instanceof Long)) {
                return false;
            }
            if (e.getValue() == null || !(e.getValue() instanceof Long)) {
                return false;
            }
            final long k = ((Long) (e.getKey())).longValue();
            final long v = ((Long) (e.getValue())).longValue();
            if (((k) == (0))) {
                if (containsNullKey && ((value.get(n)) == (v))) {
                    removeNullEntry();
                    return true;
                }
                return false;
            }
            final LongBigArray key = Long2LongOpenBigHashMap.this.key;
            // The starting point.
            long pos = it.unimi.dsi.fastutil.HashCommon.mix((k)) & mask;
            long curr = key.get(pos);
            if (((curr) == (0))) {
                return false;
            }
            if (((curr) == (k))) {
                if (((value.get(pos)) == (v))) {
                    removeEntry(pos);
                    return true;
                }
                return false;
            }
            while (true) {
                pos = (pos + 1) & mask;
                curr = key.get(pos);
                if (((curr) == (0))) {
                    return false;
                }
                if (((curr) == (k))) {
                    if (((value.get(pos)) == (v))) {
                        removeEntry(pos);
                        return true;
                    }
                }
            }
        }

        @Override
        public int size()
        {
            return toIntExact(size);
        }

        @Override
        public void clear()
        {
            Long2LongOpenBigHashMap.this.clear();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void forEach(final Consumer<? super Long2LongMap.Entry> consumer)
        {
            if (containsNullKey) {
                consumer.accept(new AbstractLong2LongMap.BasicEntry(key.get(n), value.get(n)));
            }
            for (long pos = n; pos-- != 0; ) {
                if (!((key.get(pos)) == (0))) {
                    consumer.accept(new AbstractLong2LongMap.BasicEntry(key.get(pos), value.get(pos)));
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void fastForEach(final Consumer<? super Long2LongMap.Entry> consumer)
        {
            if (containsNullKey) {
                consumer.accept(new AbstractLong2LongMap.BasicEntry(key.get(n), value.get(n)));
            }
            for (long pos = n; pos-- != 0; ) {
                if (!((key.get(pos)) == (0))) {
                    consumer.accept(new AbstractLong2LongMap.BasicEntry(key.get(pos), value.get(pos)));
                }
            }
        }
    }

    @Override
    public FastEntrySet long2LongEntrySet()
    {
        if (entries == null) {
            entries = new MapEntrySet();
        }
        return entries;
    }

    /**
     * An iterator on keys.
     *
     * <p>
     * We simply override the
     * {@link java.util.ListIterator#next()}/{@link java.util.ListIterator#previous()}
     * methods (and possibly their type-specific counterparts) so that they return
     * keys instead of entries.
     */
    private final class KeyIterator
            extends MapIterator
            implements LongIterator
    {
        public KeyIterator()
        {
            super();
        }

        @Override
        public long nextLong()
        {
            return key.get(nextEntry());
        }
    }

    private final class KeySet
            extends AbstractLongSet
    {
        @Override
        public LongIterator iterator()
        {
            return new KeyIterator();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void forEach(final java.util.function.LongConsumer consumer)
        {
            if (containsNullKey) {
                consumer.accept(key.get(n));
            }
            for (long pos = n; pos-- != 0; ) {
                final long k = key.get(pos);
                if (!((k) == (0))) {
                    consumer.accept(k);
                }
            }
        }

        @Override
        public int size()
        {
            return toIntExact(size);
        }

        @Override
        public boolean contains(long k)
        {
            return containsKey(k);
        }

        @Override
        public boolean remove(long k)
        {
            final long oldSize = size;
            Long2LongOpenBigHashMap.this.remove(k);
            return size != oldSize;
        }

        @Override
        public void clear()
        {
            Long2LongOpenBigHashMap.this.clear();
        }
    }

    @Override
    public LongSet keySet()
    {
        if (keys == null) {
            keys = new KeySet();
        }
        return keys;
    }

    /**
     * An iterator on values.
     *
     * <p>
     * We simply override the
     * {@link java.util.ListIterator#next()}/{@link java.util.ListIterator#previous()}
     * methods (and possibly their type-specific counterparts) so that they return
     * values instead of entries.
     */
    private final class ValueIterator
            extends MapIterator
            implements LongIterator
    {
        public ValueIterator()
        {
            super();
        }

        @Override
        public long nextLong()
        {
            return value.get(nextEntry());
        }
    }

    @Override
    public LongCollection values()
    {
        if (values == null) {
            values = new AbstractLongCollection()
            {
                @Override
                public LongIterator iterator()
                {
                    return new ValueIterator();
                }

                @Override
                public int size()
                {
                    return toIntExact(size);
                }

                @Override
                public boolean contains(long v)
                {
                    return containsValue(v);
                }

                @Override
                public void clear()
                {
                    Long2LongOpenBigHashMap.this.clear();
                }

                /** {@inheritDoc} */
                @Override
                public void forEach(final java.util.function.LongConsumer consumer)
                {
                    if (containsNullKey) {
                        consumer.accept(value.get(n));
                    }
                    for (long pos = n; pos-- != 0; ) {
                        if (!((key.get(pos)) == (0))) {
                            consumer.accept(value.get(pos));
                        }
                    }
                }
            };
        }
        return values;
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
        final LongBigArray key = this.key;
        final LongBigArray value = this.value;
        final long mask = newN - 1; // Note that this is used by the hashing macro
        final LongBigArray newKey = new LongBigArray();
        newKey.ensureCapacity(newN + 1);
        final LongBigArray newValue = new LongBigArray();
        newValue.ensureCapacity(newN + 1);
        long i = n;
        long pos;
        for (long j = realSize(); j-- != 0; ) {
            while (((key.get(--i)) == (0))) {
                // Skip
            }
            pos = it.unimi.dsi.fastutil.HashCommon.mix((key.get(i))) & mask;
            if (!((newKey.get(pos)) == (0))) {
                pos = (pos + 1) & mask;
                while (!((newKey.get(pos)) == (0))) {
                    pos = (pos + 1) & mask;
                }
            }
            newKey.set(pos, key.get(i));
            newValue.set(pos, value.get(i));
        }
        newValue.set(newN, value.get(n));
        n = newN;
        this.mask = mask;
        maxFill = maxFill(n, f);
        this.key = newKey;
        this.value = newValue;
    }

    private void checkTable()
    {
    }
}
