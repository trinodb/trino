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
package io.trino.parquet.writer.valuewriter;

import io.airlift.log.Logger;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.dictionary.IntList;
import org.apache.parquet.column.values.dictionary.IntList.IntIterator;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.io.ParquetEncodingException;

import java.io.IOException;
import java.util.Arrays;

import static io.airlift.slice.SizeOf.sizeOf;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.mix;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static org.apache.parquet.bytes.BytesInput.concat;

/**
 * Based on org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.
 * Will attempt to encode values using a dictionary and fall back to plain encoding
 * if the dictionary gets too big.
 */
public abstract class DictionaryValuesWriter
        extends ValuesWriter
{
    private static final Logger LOG = Logger.get(DictionaryValuesWriter.class);

    /* max entries allowed for the dictionary will fail over to plain encoding if reached */
    private static final int MAX_DICTIONARY_ENTRIES = Integer.MAX_VALUE - 1;
    private static final int MIN_INITIAL_SLAB_SIZE = 64;

    /* encoding to label the data page */
    private final Encoding encodingForDataPage;

    /* encoding to label the dictionary page */
    protected final Encoding encodingForDictionaryPage;

    /* maximum size in bytes allowed for the dictionary will fail over to plain encoding if reached */
    protected final int maxDictionaryByteSize;

    /* will become true if the dictionary becomes too big */
    protected boolean dictionaryTooBig;

    /* current size in bytes the dictionary will take once serialized */
    protected long dictionaryByteSize;

    /* size in bytes of the dictionary at the end of last dictionary encoded page (in case the current page falls back to PLAIN) */
    protected int lastUsedDictionaryByteSize;

    /* size in items of the dictionary at the end of last dictionary encoded page (in case the current page falls back to PLAIN) */
    protected int lastUsedDictionarySize;

    /* dictionary encoded values */
    protected IntList encodedValues = new IntList();

    protected DictionaryValuesWriter(
            int maxDictionaryByteSize,
            Encoding encodingForDataPage,
            Encoding encodingForDictionaryPage)
    {
        this.maxDictionaryByteSize = maxDictionaryByteSize;
        this.encodingForDataPage = encodingForDataPage;
        this.encodingForDictionaryPage = encodingForDictionaryPage;
    }

    protected DictionaryPage dictPage(org.apache.parquet.column.values.ValuesWriter dictPageWriter)
    {
        return dictPage(dictPageWriter.getBytes());
    }

    protected DictionaryPage dictPage(BytesInput dictionaryBytes)
    {
        return new DictionaryPage(dictionaryBytes, lastUsedDictionarySize, encodingForDictionaryPage);
    }

    protected static BytesInput toBytesInput(Slice slice)
    {
        return BytesInput.from(slice.byteArray(), slice.byteArrayOffset(), slice.length());
    }

    public boolean shouldFallBack()
    {
        // if the dictionary reaches the max byte size or the values can not be encoded on 4 bytes anymore.
        return dictionaryByteSize > maxDictionaryByteSize || getDictionarySize() > MAX_DICTIONARY_ENTRIES;
    }

    public boolean isCompressionSatisfying(long rawSize, long encodedSize)
    {
        return (encodedSize + dictionaryByteSize) < rawSize;
    }

    public void fallBackAllValuesTo(ValuesWriter writer)
    {
        fallBackDictionaryEncodedData(writer);
        if (lastUsedDictionarySize == 0) {
            // if we never used the dictionary
            // we free dictionary encoded data
            clearDictionaryContent();
            dictionaryByteSize = 0;
            encodedValues = new IntList();
        }
    }

    protected abstract void fallBackDictionaryEncodedData(ValuesWriter writer);

    @Override
    public long getBufferedSize()
    {
        return encodedValues.size() * 4L;
    }

    @Override
    public long getAllocatedSize()
    {
        // encoded value ids plus the retained dictionary structures
        return encodedValues.size() * 4L + getDictionaryAllocatedSize();
    }

    // retained bytes of the dictionary lookup structures held in memory
    protected long getDictionaryAllocatedSize()
    {
        return dictionaryByteSize;
    }

    @Override
    public BytesInput getBytes()
    {
        int maxDicId = getDictionarySize() - 1;
        LOG.debug("max dic id %s", maxDicId);
        int bitWidth = BytesUtils.getWidthFromMaxInt(maxDicId);
        int initialSlabSize = CapacityByteArrayOutputStream.initialSlabSizeHeuristic(MIN_INITIAL_SLAB_SIZE, maxDictionaryByteSize, 10);

        RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(bitWidth, initialSlabSize, maxDictionaryByteSize, new HeapByteBufferAllocator());
        IntIterator iterator = encodedValues.iterator();
        try {
            while (iterator.hasNext()) {
                encoder.writeInt(iterator.next());
            }
            // encodes the bit width
            byte[] bytesHeader = new byte[] {(byte) bitWidth};
            BytesInput rleEncodedBytes = encoder.toBytes();
            LOG.debug("rle encoded bytes %s", rleEncodedBytes.size());
            BytesInput bytes = concat(BytesInput.from(bytesHeader), rleEncodedBytes);
            // remember size of dictionary when we last wrote a page
            lastUsedDictionarySize = getDictionarySize();
            lastUsedDictionaryByteSize = toIntExact(dictionaryByteSize);
            return bytes;
        }
        catch (IOException e) {
            throw new ParquetEncodingException("could not encode the values", e);
        }
    }

    @Override
    public Encoding getEncoding()
    {
        return encodingForDataPage;
    }

    @Override
    public void reset()
    {
        close();
        encodedValues = new IntList();
    }

    @Override
    public void close()
    {
        encodedValues = null;
    }

    @Override
    public void resetDictionary()
    {
        lastUsedDictionaryByteSize = 0;
        lastUsedDictionarySize = 0;
        dictionaryTooBig = false;
        dictionaryByteSize = 0;
        clearDictionaryContent();
    }

    /**
     * clear/free the underlying dictionary content
     */
    protected abstract void clearDictionaryContent();

    /**
     * @return size in items
     */
    protected abstract int getDictionarySize();

    @Override
    public String memUsageString(String prefix)
    {
        return format(
                """
                %s DictionaryValuesWriter{
                %s
                %s
                %s}
                """,
                prefix,
                prefix + " dict:" + dictionaryByteSize,
                prefix + " values:" + encodedValues.size() * 4L,
                prefix);
    }

    public static class PlainBinaryDictionaryValuesWriter
            extends DictionaryValuesWriter
    {
        private static final float FILL_RATIO = 0.75f;
        // storage is allocated lazily on the first written value; unused writers hold only object headers
        private static final int INITIAL_ENTRIES = 1024;
        private static final int INITIAL_DICTIONARY_BYTES = 64;
        // golden-ratio odd constant for the multiplicative hash
        private static final long HASH_MULTIPLIER = 0x9E3779B97F4A7C15L;

        // distinct value bytes concatenated in id order; null until the first value is written
        private SliceOutput dictionaryData;
        // prefix offsets: value id occupies dictionaryData[offsets[id], offsets[id + 1])
        private int[] offsets;
        // open-addressing table; each occupied slot packs the value hash (high 32 bits) and id + 1 (low 32 bits).
        // an empty slot has zero in the low 32 bits. the cached hash lets rehashing and probing skip recomputation.
        private long[] table;
        private int hashMask;
        private int maxFill;
        private int dictionarySize;

        public PlainBinaryDictionaryValuesWriter(
                int maxDictionaryByteSize,
                Encoding encodingForDataPage,
                Encoding encodingForDictionaryPage)
        {
            super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage);
        }

        @Override
        public void writeBytes(Slice value)
        {
            encodedValues.add(putIfAbsent(value));
        }

        // serialized size contribution of one distinct value
        protected long entrySize(int valueLength)
        {
            // length prefix (4 bytes) plus the value bytes
            return 4L + valueLength;
        }

        private int putIfAbsent(Slice value)
        {
            if (table == null) {
                allocateStorage();
            }
            int length = value.length();
            int hash = hash(value, length);
            int slot = findSlot(value, length, hash);
            long entry = table[slot];
            if ((int) entry != 0) {
                return (int) entry - 1;
            }
            int id = dictionarySize;
            dictionaryData.writeBytes(value);
            offsets[id + 1] = dictionaryData.size();
            table[slot] = ((long) hash << 32) | (id + 1);
            dictionaryByteSize += entrySize(length);
            dictionarySize++;
            if (dictionarySize >= maxFill) {
                // -1 so arraySize rounds to the next power of two, doubling the table rather than quadrupling it
                rehash(maxFill * 2 - 1);
            }
            return id;
        }

        private void allocateStorage()
        {
            int hashSize = arraySize(INITIAL_ENTRIES, FILL_RATIO);
            this.hashMask = hashSize - 1;
            this.maxFill = calculateMaxFill(hashSize);
            this.table = new long[hashSize];
            this.offsets = new int[maxFill + 1];
            this.dictionaryData = new DynamicSliceOutput(INITIAL_DICTIONARY_BYTES);
        }

        // multiplicative word-at-a-time hash; cheaper than XxHash64 on short values, mixed to spread the low bits used for the slot index
        private static int hash(Slice value, int length)
        {
            long accumulator = length;
            int index = 0;
            for (; index + Long.BYTES <= length; index += Long.BYTES) {
                accumulator = (accumulator ^ value.getLong(index)) * HASH_MULTIPLIER;
            }
            if (index < length) {
                long tail = 0;
                for (int shift = 0; index < length; index++, shift += Byte.SIZE) {
                    tail |= (value.getByte(index) & 0xFFL) << shift;
                }
                accumulator = (accumulator ^ tail) * HASH_MULTIPLIER;
            }
            return (int) mix(accumulator);
        }

        private int findSlot(Slice value, int length, int hash)
        {
            Slice dictionarySlice = dictionaryData.getUnderlyingSlice();
            int slot = hash & hashMask;
            while (true) {
                long entry = table[slot];
                if ((int) entry == 0) {
                    return slot;
                }
                if ((int) (entry >>> 32) == hash) {
                    int id = (int) entry - 1;
                    int start = offsets[id];
                    if (value.equals(0, length, dictionarySlice, start, offsets[id + 1] - start)) {
                        return slot;
                    }
                }
                slot = (slot + 1) & hashMask;
            }
        }

        private void rehash(int newSize)
        {
            int newHashSize = arraySize(newSize + 1, FILL_RATIO);
            hashMask = newHashSize - 1;
            maxFill = calculateMaxFill(newHashSize);
            offsets = Arrays.copyOf(offsets, maxFill + 1);
            long[] newTable = new long[newHashSize];
            for (long entry : table) {
                if ((int) entry == 0) {
                    continue;
                }
                int slot = (int) (entry >>> 32) & hashMask;
                while ((int) newTable[slot] != 0) {
                    slot = (slot + 1) & hashMask;
                }
                newTable[slot] = entry;
            }
            table = newTable;
        }

        // slice view of distinct value id backed by the dictionary data
        protected Slice entry(int id)
        {
            int start = offsets[id];
            return dictionaryData.getUnderlyingSlice().slice(start, offsets[id + 1] - start);
        }

        // first lastUsedDictionarySize concatenated values (no length prefixes), copied out of the reused arena buffer
        protected BytesInput usedDictionaryData()
        {
            return toBytesInput(dictionaryData.getUnderlyingSlice().slice(0, offsets[lastUsedDictionarySize]).copy());
        }

        @Override
        public DictionaryPage toDictPageAndClose()
        {
            if (lastUsedDictionarySize > 0) {
                // PLAIN dictionary page: 4-byte little-endian length prefix then value bytes per entry
                SliceOutput dictionaryPage = new DynamicSliceOutput(lastUsedDictionaryByteSize);
                for (int id = 0; id < lastUsedDictionarySize; id++) {
                    Slice value = entry(id);
                    dictionaryPage.writeInt(value.length());
                    dictionaryPage.writeBytes(value);
                }
                return dictPage(toBytesInput(dictionaryPage.slice()));
            }
            return null;
        }

        @Override
        public int getDictionarySize()
        {
            return dictionarySize;
        }

        @Override
        protected long getDictionaryAllocatedSize()
        {
            if (table == null) {
                return 0;
            }
            return dictionaryData.getRetainedSize() + sizeOf(offsets) + sizeOf(table);
        }

        @Override
        protected void clearDictionaryContent()
        {
            // release grown buffers; allocateStorage reallocates lazily on the next written value
            dictionaryData = null;
            offsets = null;
            table = null;
            dictionarySize = 0;
        }

        @Override
        public void fallBackDictionaryEncodedData(ValuesWriter writer)
        {
            // fall back to plain encoding using the id-ordered dictionary values
            IntIterator iterator = encodedValues.iterator();
            while (iterator.hasNext()) {
                writer.writeBytes(entry(iterator.next()));
            }
        }

        private static int calculateMaxFill(int hashSize)
        {
            int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
            if (maxFill == hashSize) {
                maxFill--;
            }
            return maxFill;
        }
    }

    public static class PlainFixedLenArrayDictionaryValuesWriter
            extends PlainBinaryDictionaryValuesWriter
    {
        private final int length;

        public PlainFixedLenArrayDictionaryValuesWriter(
                int maxDictionaryByteSize,
                int length,
                Encoding encodingForDataPage,
                Encoding encodingForDictionaryPage)
        {
            super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage);
            this.length = length;
        }

        @Override
        protected long entrySize(int valueLength)
        {
            // fixed-length values are serialized without a length prefix
            return length;
        }

        @Override
        public DictionaryPage toDictPageAndClose()
        {
            if (lastUsedDictionarySize > 0) {
                // fixed-length values carry no length prefix, so the stored bytes are already the PLAIN payload
                return dictPage(usedDictionaryData());
            }
            return null;
        }
    }

    public static class PlainLongDictionaryValuesWriter
            extends DictionaryValuesWriter
    {
        /* type specific dictionary content */
        private final Long2IntMap longDictionaryContent = new Long2IntOpenHashMap();
        // dictionary values in id order, id equals list index
        private final LongArrayList dictionaryValues = new LongArrayList();

        public PlainLongDictionaryValuesWriter(
                int maxDictionaryByteSize,
                Encoding encodingForDataPage,
                Encoding encodingForDictionaryPage)
        {
            super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage);
            longDictionaryContent.defaultReturnValue(-1);
        }

        @Override
        public void writeLong(long v)
        {
            int id = longDictionaryContent.get(v);
            if (id == -1) {
                id = dictionaryValues.size();
                longDictionaryContent.put(v, id);
                dictionaryValues.add(v);
                dictionaryByteSize += 8;
            }
            encodedValues.add(id);
        }

        @Override
        public DictionaryPage toDictPageAndClose()
        {
            if (lastUsedDictionarySize > 0) {
                // return a dictionary only if we actually used it
                PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize, maxDictionaryByteSize, new HeapByteBufferAllocator());
                // write only the part of the dict that we used
                for (int i = 0; i < lastUsedDictionarySize; i++) {
                    dictionaryEncoder.writeLong(dictionaryValues.getLong(i));
                }
                return dictPage(dictionaryEncoder);
            }
            return null;
        }

        @Override
        public int getDictionarySize()
        {
            return longDictionaryContent.size();
        }

        @Override
        protected void clearDictionaryContent()
        {
            longDictionaryContent.clear();
            dictionaryValues.clear();
        }

        @Override
        public void fallBackDictionaryEncodedData(ValuesWriter writer)
        {
            // fall back to plain encoding using the id-ordered dictionary values
            IntIterator iterator = encodedValues.iterator();
            while (iterator.hasNext()) {
                int id = iterator.next();
                writer.writeLong(dictionaryValues.getLong(id));
            }
        }
    }

    public static class PlainDoubleDictionaryValuesWriter
            extends DictionaryValuesWriter
    {
        /* type specific dictionary content */
        private final Long2IntMap doubleDictionaryContent = new Long2IntOpenHashMap();
        // dictionary values (raw long bits) in id order, id equals list index
        private final LongArrayList dictionaryValues = new LongArrayList();

        public PlainDoubleDictionaryValuesWriter(
                int maxDictionaryByteSize,
                Encoding encodingForDataPage,
                Encoding encodingForDictionaryPage)
        {
            super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage);
            doubleDictionaryContent.defaultReturnValue(-1);
        }

        @Override
        public void writeDouble(double v)
        {
            long bits = Double.doubleToRawLongBits(v);
            int id = doubleDictionaryContent.get(bits);
            if (id == -1) {
                id = dictionaryValues.size();
                doubleDictionaryContent.put(bits, id);
                dictionaryValues.add(bits);
                dictionaryByteSize += 8;
            }
            encodedValues.add(id);
        }

        @Override
        public DictionaryPage toDictPageAndClose()
        {
            if (lastUsedDictionarySize > 0) {
                // return a dictionary only if we actually used it
                PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize, maxDictionaryByteSize, new HeapByteBufferAllocator());
                // write only the part of the dict that we used
                for (int i = 0; i < lastUsedDictionarySize; i++) {
                    dictionaryEncoder.writeDouble(Double.longBitsToDouble(dictionaryValues.getLong(i)));
                }
                return dictPage(dictionaryEncoder);
            }
            return null;
        }

        @Override
        public int getDictionarySize()
        {
            return doubleDictionaryContent.size();
        }

        @Override
        protected void clearDictionaryContent()
        {
            doubleDictionaryContent.clear();
            dictionaryValues.clear();
        }

        @Override
        public void fallBackDictionaryEncodedData(ValuesWriter writer)
        {
            // fall back to plain encoding using the id-ordered dictionary values
            IntIterator iterator = encodedValues.iterator();
            while (iterator.hasNext()) {
                int id = iterator.next();
                writer.writeDouble(Double.longBitsToDouble(dictionaryValues.getLong(id)));
            }
        }
    }

    public static class PlainIntegerDictionaryValuesWriter
            extends DictionaryValuesWriter
    {
        /* type specific dictionary content */
        private final Int2IntMap intDictionaryContent = new Int2IntOpenHashMap();
        // dictionary values in id order, id equals list index
        private final IntArrayList dictionaryValues = new IntArrayList();

        public PlainIntegerDictionaryValuesWriter(
                int maxDictionaryByteSize,
                Encoding encodingForDataPage,
                Encoding encodingForDictionaryPage)
        {
            super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage);
            intDictionaryContent.defaultReturnValue(-1);
        }

        @Override
        public void writeInteger(int v)
        {
            int id = intDictionaryContent.get(v);
            if (id == -1) {
                id = dictionaryValues.size();
                intDictionaryContent.put(v, id);
                dictionaryValues.add(v);
                dictionaryByteSize += 4;
            }
            encodedValues.add(id);
        }

        @Override
        public DictionaryPage toDictPageAndClose()
        {
            if (lastUsedDictionarySize > 0) {
                // return a dictionary only if we actually used it
                PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize, maxDictionaryByteSize, new HeapByteBufferAllocator());
                // write only the part of the dict that we used
                for (int i = 0; i < lastUsedDictionarySize; i++) {
                    dictionaryEncoder.writeInteger(dictionaryValues.getInt(i));
                }
                return dictPage(dictionaryEncoder);
            }
            return null;
        }

        @Override
        public int getDictionarySize()
        {
            return intDictionaryContent.size();
        }

        @Override
        protected void clearDictionaryContent()
        {
            intDictionaryContent.clear();
            dictionaryValues.clear();
        }

        @Override
        public void fallBackDictionaryEncodedData(ValuesWriter writer)
        {
            // fall back to plain encoding using the id-ordered dictionary values
            IntIterator iterator = encodedValues.iterator();
            while (iterator.hasNext()) {
                int id = iterator.next();
                writer.writeInteger(dictionaryValues.getInt(id));
            }
        }
    }

    public static class PlainFloatDictionaryValuesWriter
            extends DictionaryValuesWriter
    {
        /* type specific dictionary content */
        private final Int2IntMap floatDictionaryContent = new Int2IntOpenHashMap();
        // dictionary values (raw int bits) in id order, id equals list index
        private final IntArrayList dictionaryValues = new IntArrayList();

        public PlainFloatDictionaryValuesWriter(
                int maxDictionaryByteSize,
                Encoding encodingForDataPage,
                Encoding encodingForDictionaryPage)
        {
            super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage);
            floatDictionaryContent.defaultReturnValue(-1);
        }

        @Override
        public void writeFloat(float v)
        {
            int bits = Float.floatToRawIntBits(v);
            int id = floatDictionaryContent.get(bits);
            if (id == -1) {
                id = dictionaryValues.size();
                floatDictionaryContent.put(bits, id);
                dictionaryValues.add(bits);
                dictionaryByteSize += 4;
            }
            encodedValues.add(id);
        }

        @Override
        public DictionaryPage toDictPageAndClose()
        {
            if (lastUsedDictionarySize > 0) {
                // return a dictionary only if we actually used it
                PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize, maxDictionaryByteSize, new HeapByteBufferAllocator());
                // write only the part of the dict that we used
                for (int i = 0; i < lastUsedDictionarySize; i++) {
                    dictionaryEncoder.writeFloat(Float.intBitsToFloat(dictionaryValues.getInt(i)));
                }
                return dictPage(dictionaryEncoder);
            }
            return null;
        }

        @Override
        public int getDictionarySize()
        {
            return floatDictionaryContent.size();
        }

        @Override
        protected void clearDictionaryContent()
        {
            floatDictionaryContent.clear();
            dictionaryValues.clear();
        }

        @Override
        public void fallBackDictionaryEncodedData(ValuesWriter writer)
        {
            // fall back to plain encoding using the id-ordered dictionary values
            IntIterator iterator = encodedValues.iterator();
            while (iterator.hasNext()) {
                int id = iterator.next();
                writer.writeFloat(Float.intBitsToFloat(dictionaryValues.getInt(id)));
            }
        }
    }
}
