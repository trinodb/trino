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
package org.apache.parquet.column.values.dictionary;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.reader.SimpleSliceInputStream;
import io.trino.parquet.reader.decoders.PlainByteArrayDecoders;
import io.trino.parquet.reader.decoders.PlainValueDecoders;
import io.trino.parquet.reader.decoders.ValueDecoder;
import io.trino.parquet.reader.flat.BinaryBuffer;
import io.trino.parquet.reader.flat.ColumnAdapter;
import io.trino.parquet.reader.flat.DictionaryDecoder;
import io.trino.parquet.writer.valuewriter.DictionaryFallbackValuesWriter;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFloatDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainLongDictionaryValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static io.trino.parquet.ParquetTestUtils.toTrinoDictionaryPage;
import static io.trino.parquet.reader.flat.BinaryColumnAdapter.BINARY_ADAPTER;
import static io.trino.parquet.reader.flat.IntColumnAdapter.INT_ADAPTER;
import static io.trino.parquet.reader.flat.LongColumnAdapter.LONG_ADAPTER;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class TestDictionaryWriter
{
    @Test
    public void testBinaryDictionary()
            throws IOException
    {
        int count = 100;
        DictionaryFallbackValuesWriter fallbackValuesWriter = newPlainBinaryDictionaryValuesWriter(200, 10000);
        writeRepeated(count, fallbackValuesWriter, "a");
        BytesInput bytes1 = getBytesAndCheckEncoding(fallbackValuesWriter, getDictionaryEncoding());
        writeRepeated(count, fallbackValuesWriter, "b");
        BytesInput bytes2 = getBytesAndCheckEncoding(fallbackValuesWriter, getDictionaryEncoding());
        // now we will fall back
        writeDistinct(count, fallbackValuesWriter, "c");
        BytesInput bytes3 = getBytesAndCheckEncoding(fallbackValuesWriter, PLAIN);

        ValueDecoder<BinaryBuffer> decoder = getDictionaryDecoder(fallbackValuesWriter, BINARY_ADAPTER, new PlainByteArrayDecoders.BinaryPlainValueDecoder());
        checkRepeated(count, bytes1, decoder, "a");
        checkRepeated(count, bytes2, decoder, "b");
        decoder = new PlainByteArrayDecoders.BinaryPlainValueDecoder();
        checkDistinct(count, bytes3, decoder, "c");
    }

    @Test
    public void testSkipInBinaryDictionary()
            throws Exception
    {
        DictionaryFallbackValuesWriter fallbackValuesWriter = newPlainBinaryDictionaryValuesWriter(1000, 10000);
        writeRepeated(100, fallbackValuesWriter, "a");
        writeDistinct(100, fallbackValuesWriter, "b");
        assertThat(fallbackValuesWriter.getEncoding()).isEqualTo(getDictionaryEncoding());

        // Test skip and skip-n with dictionary encoding
        Slice writtenValues = Slices.wrappedBuffer(fallbackValuesWriter.getBytes().toByteArray());
        ValueDecoder<BinaryBuffer> decoder = getDictionaryDecoder(fallbackValuesWriter, BINARY_ADAPTER, new PlainByteArrayDecoders.BinaryPlainValueDecoder());
        decoder.init(new SimpleSliceInputStream(writtenValues));
        for (int i = 0; i < 100; i += 2) {
            BinaryBuffer buffer = new BinaryBuffer(1);
            decoder.read(buffer, 0, 1);
            assertThat(buffer.asSlice().toStringUtf8()).isEqualTo("a" + i % 10);
            decoder.skip(1);
        }
        int skipcount;
        for (int i = 0; i < 100; i += skipcount + 1) {
            BinaryBuffer buffer = new BinaryBuffer(1);
            decoder.read(buffer, 0, 1);
            assertThat(buffer.asSlice().toStringUtf8()).isEqualTo("b" + i);
            skipcount = (100 - i) / 2;
            decoder.skip(skipcount);
        }

        // Ensure fallback
        writeDistinct(1000, fallbackValuesWriter, "c");
        assertThat(fallbackValuesWriter.getEncoding()).isEqualTo(PLAIN);

        // Test skip and skip-n with plain encoding (after fallback)
        decoder = new PlainByteArrayDecoders.BinaryPlainValueDecoder();
        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(fallbackValuesWriter.getBytes().toByteArray())));
        decoder.skip(200);
        for (int i = 0; i < 100; i += 2) {
            BinaryBuffer buffer = new BinaryBuffer(1);
            decoder.read(buffer, 0, 1);
            assertThat(buffer.asSlice().toStringUtf8()).isEqualTo("c" + i);
            decoder.skip(1);
        }
        for (int i = 100; i < 1000; i += skipcount + 1) {
            BinaryBuffer buffer = new BinaryBuffer(1);
            decoder.read(buffer, 0, 1);
            assertThat(buffer.asSlice().toStringUtf8()).isEqualTo("c" + i);
            skipcount = (1000 - i) / 2;
            decoder.skip(skipcount);
        }
    }

    @Test
    public void testBinaryDictionaryFallBack()
            throws IOException
    {
        int slabSize = 100;
        int maxDictionaryByteSize = 50;
        DictionaryFallbackValuesWriter fallbackValuesWriter = newPlainBinaryDictionaryValuesWriter(maxDictionaryByteSize, slabSize);
        int dataSize = 0;
        for (long i = 0; i < 100; i++) {
            Binary binary = Binary.fromString("str" + i);
            fallbackValuesWriter.writeBytes(binary);
            dataSize += (binary.length() + 4);
            assertThat(fallbackValuesWriter.getEncoding()).isEqualTo(dataSize < maxDictionaryByteSize ? getDictionaryEncoding() : PLAIN);
        }

        // Fallback to Plain encoding, therefore use BinaryPlainValueDecoder to read it back
        ValueDecoder<BinaryBuffer> decoder = new PlainByteArrayDecoders.BinaryPlainValueDecoder();
        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(fallbackValuesWriter.getBytes().toByteArray())));
        BinaryBuffer buffer = new BinaryBuffer(100);
        decoder.read(buffer, 0, 100);
        Slice values = buffer.asSlice();
        int[] offsets = buffer.getOffsets();
        int currentOffset = 0;
        for (int i = 0; i < 100; i++) {
            int length = offsets[i + 1] - offsets[i];
            assertThat(values.slice(currentOffset, length).toStringUtf8()).isEqualTo("str" + i);
            currentOffset += length;
        }

        // simulate cutting the page
        fallbackValuesWriter.reset();
        assertThat(fallbackValuesWriter.getBufferedSize()).isEqualTo(0);
    }

    @Test
    public void testBinaryDictionaryChangedValues()
            throws IOException
    {
        int count = 100;
        DictionaryFallbackValuesWriter fallbackValuesWriter = newPlainBinaryDictionaryValuesWriter(200, 10000);
        writeRepeatedWithReuse(count, fallbackValuesWriter, "a");
        BytesInput bytes1 = getBytesAndCheckEncoding(fallbackValuesWriter, getDictionaryEncoding());
        writeRepeatedWithReuse(count, fallbackValuesWriter, "b");
        BytesInput bytes2 = getBytesAndCheckEncoding(fallbackValuesWriter, getDictionaryEncoding());
        // now we will fall back
        writeDistinct(count, fallbackValuesWriter, "c");
        BytesInput bytes3 = getBytesAndCheckEncoding(fallbackValuesWriter, PLAIN);

        ValueDecoder<BinaryBuffer> decoder = getDictionaryDecoder(fallbackValuesWriter, BINARY_ADAPTER, new PlainByteArrayDecoders.BinaryPlainValueDecoder());
        checkRepeated(count, bytes1, decoder, "a");
        checkRepeated(count, bytes2, decoder, "b");
        decoder = new PlainByteArrayDecoders.BinaryPlainValueDecoder();
        checkDistinct(count, bytes3, decoder, "c");
    }

    @Test
    public void testFirstPageFallBack()
            throws IOException
    {
        int count = 1000;
        DictionaryFallbackValuesWriter fallbackValuesWriter = newPlainBinaryDictionaryValuesWriter(10000, 10000);
        writeDistinct(count, fallbackValuesWriter, "a");
        long dictionaryAllocatedSize = fallbackValuesWriter.getInitialWriter().getAllocatedSize();
        assertThat(fallbackValuesWriter.getAllocatedSize()).isEqualTo(dictionaryAllocatedSize);
        // not efficient so falls back
        BytesInput bytes1 = getBytesAndCheckEncoding(fallbackValuesWriter, PLAIN);
        writeRepeated(count, fallbackValuesWriter, "b");
        assertThat(fallbackValuesWriter.getAllocatedSize()).isEqualTo(fallbackValuesWriter.getFallBackWriter().getAllocatedSize());
        // still plain because we fell back on first page
        BytesInput bytes2 = getBytesAndCheckEncoding(fallbackValuesWriter, PLAIN);

        ValueDecoder<BinaryBuffer> decoder = new PlainByteArrayDecoders.BinaryPlainValueDecoder();
        checkDistinct(count, bytes1, decoder, "a");
        checkRepeated(count, bytes2, decoder, "b");
    }

    @Test
    public void testSecondPageFallBack()
            throws IOException
    {
        int count = 1000;
        DictionaryFallbackValuesWriter fallbackValuesWriter = newPlainBinaryDictionaryValuesWriter(1000, 10000);
        writeRepeated(count, fallbackValuesWriter, "a");
        BytesInput bytes1 = getBytesAndCheckEncoding(fallbackValuesWriter, getDictionaryEncoding());
        writeDistinct(count, fallbackValuesWriter, "b");
        // not efficient so falls back
        BytesInput bytes2 = getBytesAndCheckEncoding(fallbackValuesWriter, PLAIN);
        writeRepeated(count, fallbackValuesWriter, "a");
        // still plain because we fell back on previous page
        BytesInput bytes3 = getBytesAndCheckEncoding(fallbackValuesWriter, PLAIN);

        ValueDecoder<BinaryBuffer> decoder = getDictionaryDecoder(fallbackValuesWriter, BINARY_ADAPTER, new PlainByteArrayDecoders.BinaryPlainValueDecoder());
        checkRepeated(count, bytes1, decoder, "a");
        decoder = new PlainByteArrayDecoders.BinaryPlainValueDecoder();
        checkDistinct(count, bytes2, decoder, "b");
        checkRepeated(count, bytes3, decoder, "a");
    }

    @Test
    public void testLongDictionary()
            throws IOException
    {
        int count = 1000;
        int count2 = 2000;
        DictionaryFallbackValuesWriter fallbackValuesWriter = newPlainLongDictionaryValuesWriter(10000, 10000);
        for (long i = 0; i < count; i++) {
            fallbackValuesWriter.writeLong(i % 50);
        }
        BytesInput bytes1 = getBytesAndCheckEncoding(fallbackValuesWriter, getDictionaryEncoding());
        assertThat(fallbackValuesWriter.getInitialWriter().getDictionarySize()).isEqualTo(50);

        for (long i = count2; i > 0; i--) {
            fallbackValuesWriter.writeLong(i % 50);
        }
        BytesInput bytes2 = getBytesAndCheckEncoding(fallbackValuesWriter, getDictionaryEncoding());
        assertThat(fallbackValuesWriter.getInitialWriter().getDictionarySize()).isEqualTo(50);

        DictionaryDecoder<long[]> dictionaryDecoder = getDictionaryDecoder(fallbackValuesWriter, LONG_ADAPTER, new PlainValueDecoders.LongPlainValueDecoder());

        dictionaryDecoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(bytes1.toByteArray())));
        long[] values = new long[count];
        dictionaryDecoder.read(values, 0, count);
        for (int i = 0; i < count; i++) {
            assertThat(values[i]).isEqualTo(i % 50);
        }

        dictionaryDecoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(bytes2.toByteArray())));
        values = new long[count2];
        dictionaryDecoder.read(values, 0, count2);
        for (int i = count2; i > 0; i--) {
            assertThat(values[count2 - i]).isEqualTo(i % 50);
        }
    }

    @Test
    public void testLongDictionaryFallBack()
            throws IOException
    {
        int slabSize = 100;
        int maxDictionaryByteSize = 50;
        DictionaryFallbackValuesWriter fallbackValuesWriter = newPlainLongDictionaryValuesWriter(maxDictionaryByteSize, slabSize);
        // Fallback to Plain encoding, therefore use LongPlainValueDecoder to read it back
        ValueDecoder<long[]> decoder = new PlainValueDecoders.LongPlainValueDecoder();

        roundTripLong(fallbackValuesWriter, decoder, maxDictionaryByteSize);
        // simulate cutting the page
        fallbackValuesWriter.reset();
        assertThat(fallbackValuesWriter.getBufferedSize()).isEqualTo(0);
        fallbackValuesWriter.resetDictionary();

        roundTripLong(fallbackValuesWriter, decoder, maxDictionaryByteSize);
    }

    @Test
    public void testDoubleDictionary()
            throws IOException
    {
        int count = 1000;
        int count2 = 2000;
        DictionaryFallbackValuesWriter fallbackValuesWriter = newPlainDoubleDictionaryValuesWriter(10000, 10000);

        for (double i = 0; i < count; i++) {
            fallbackValuesWriter.writeDouble(i % 50);
        }

        BytesInput bytes1 = getBytesAndCheckEncoding(fallbackValuesWriter, getDictionaryEncoding());
        assertThat(fallbackValuesWriter.getInitialWriter().getDictionarySize()).isEqualTo(50);

        for (double i = count2; i > 0; i--) {
            fallbackValuesWriter.writeDouble(i % 50);
        }
        BytesInput bytes2 = getBytesAndCheckEncoding(fallbackValuesWriter, getDictionaryEncoding());
        assertThat(fallbackValuesWriter.getInitialWriter().getDictionarySize()).isEqualTo(50);

        DictionaryDecoder<long[]> dictionaryDecoder = getDictionaryDecoder(fallbackValuesWriter, LONG_ADAPTER, new PlainValueDecoders.LongPlainValueDecoder());

        dictionaryDecoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(bytes1.toByteArray())));
        long[] values = new long[count];
        dictionaryDecoder.read(values, 0, count);
        for (int i = 0; i < count; i++) {
            double back = Double.longBitsToDouble(values[i]);
            assertThat(back).isEqualTo(i % 50);
        }

        dictionaryDecoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(bytes2.toByteArray())));
        values = new long[count2];
        dictionaryDecoder.read(values, 0, count2);
        for (int i = count2; i > 0; i--) {
            double back = Double.longBitsToDouble(values[count2 - i]);
            assertThat(back).isEqualTo(i % 50);
        }
    }

    @Test
    public void testDoubleDictionaryFallBack()
            throws IOException
    {
        int slabSize = 100;
        int maxDictionaryByteSize = 50;
        DictionaryFallbackValuesWriter fallbackValuesWriter = newPlainDoubleDictionaryValuesWriter(maxDictionaryByteSize, slabSize);

        // Fallback to Plain encoding, therefore use LongPlainValueDecoder to read it back
        ValueDecoder<long[]> decoder = new PlainValueDecoders.LongPlainValueDecoder();

        roundTripDouble(fallbackValuesWriter, decoder, maxDictionaryByteSize);
        // simulate cutting the page
        fallbackValuesWriter.reset();
        assertThat(fallbackValuesWriter.getBufferedSize()).isEqualTo(0);
        fallbackValuesWriter.resetDictionary();

        roundTripDouble(fallbackValuesWriter, decoder, maxDictionaryByteSize);
    }

    @Test
    public void testIntDictionary()
            throws IOException
    {
        int count = 2000;
        int count2 = 4000;
        DictionaryFallbackValuesWriter fallbackValuesWriter = newPlainIntegerDictionaryValuesWriter(10000, 10000);

        for (int i = 0; i < count; i++) {
            fallbackValuesWriter.writeInteger(i % 50);
        }
        BytesInput bytes1 = getBytesAndCheckEncoding(fallbackValuesWriter, getDictionaryEncoding());
        assertThat(fallbackValuesWriter.getInitialWriter().getDictionarySize()).isEqualTo(50);

        for (int i = count2; i > 0; i--) {
            fallbackValuesWriter.writeInteger(i % 50);
        }
        BytesInput bytes2 = getBytesAndCheckEncoding(fallbackValuesWriter, getDictionaryEncoding());
        assertThat(fallbackValuesWriter.getInitialWriter().getDictionarySize()).isEqualTo(50);

        DictionaryDecoder<int[]> dictionaryDecoder = getDictionaryDecoder(fallbackValuesWriter, INT_ADAPTER, new PlainValueDecoders.IntPlainValueDecoder());

        dictionaryDecoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(bytes1.toByteArray())));
        int[] values = new int[count];
        dictionaryDecoder.read(values, 0, count);
        for (int i = 0; i < count; i++) {
            assertThat(values[i]).isEqualTo(i % 50);
        }

        dictionaryDecoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(bytes2.toByteArray())));
        values = new int[count2];
        dictionaryDecoder.read(values, 0, count2);
        for (int i = count2; i > 0; i--) {
            assertThat(values[count2 - i]).isEqualTo(i % 50);
        }
    }

    @Test
    public void testIntDictionaryFallBack()
            throws IOException
    {
        int slabSize = 100;
        int maxDictionaryByteSize = 50;
        DictionaryFallbackValuesWriter fallbackValuesWriter = newPlainIntegerDictionaryValuesWriter(maxDictionaryByteSize, slabSize);

        // Fallback to Plain encoding, therefore use IntPlainValueDecoder to read it back
        ValueDecoder<int[]> decoder = new PlainValueDecoders.IntPlainValueDecoder();

        roundTripInt(fallbackValuesWriter, decoder, maxDictionaryByteSize);
        // simulate cutting the page
        fallbackValuesWriter.reset();
        assertThat(fallbackValuesWriter.getBufferedSize()).isEqualTo(0);
        fallbackValuesWriter.resetDictionary();

        roundTripInt(fallbackValuesWriter, decoder, maxDictionaryByteSize);
    }

    @Test
    public void testFloatDictionary()
            throws IOException
    {
        int count = 2000;
        int count2 = 4000;
        DictionaryFallbackValuesWriter fallbackValuesWriter = newPlainFloatDictionaryValuesWriter(10000, 10000);

        for (float i = 0; i < count; i++) {
            fallbackValuesWriter.writeFloat(i % 50);
        }
        BytesInput bytes1 = getBytesAndCheckEncoding(fallbackValuesWriter, getDictionaryEncoding());
        assertThat(fallbackValuesWriter.getInitialWriter().getDictionarySize()).isEqualTo(50);

        for (float i = count2; i > 0; i--) {
            fallbackValuesWriter.writeFloat(i % 50);
        }
        BytesInput bytes2 = getBytesAndCheckEncoding(fallbackValuesWriter, getDictionaryEncoding());
        assertThat(fallbackValuesWriter.getInitialWriter().getDictionarySize()).isEqualTo(50);

        DictionaryDecoder<int[]> dictionaryDecoder = getDictionaryDecoder(fallbackValuesWriter, INT_ADAPTER, new PlainValueDecoders.IntPlainValueDecoder());

        dictionaryDecoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(bytes1.toByteArray())));
        int[] values = new int[count];
        dictionaryDecoder.read(values, 0, count);
        for (int i = 0; i < count; i++) {
            float back = Float.intBitsToFloat(values[i]);
            assertThat(back).isEqualTo(i % 50);
        }

        dictionaryDecoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(bytes2.toByteArray())));
        values = new int[count2];
        dictionaryDecoder.read(values, 0, count2);
        for (int i = count2; i > 0; i--) {
            float back = Float.intBitsToFloat(values[count2 - i]);
            assertThat(back).isEqualTo(i % 50);
        }
    }

    @Test
    public void testFloatDictionaryFallBack()
            throws IOException
    {
        int slabSize = 100;
        int maxDictionaryByteSize = 50;
        DictionaryFallbackValuesWriter fallbackValuesWriter = newPlainFloatDictionaryValuesWriter(maxDictionaryByteSize, slabSize);

        // Fallback to Plain encoding, therefore use IntPlainValueDecoder to read it back
        ValueDecoder<int[]> decoder = new PlainValueDecoders.IntPlainValueDecoder();

        roundTripFloat(fallbackValuesWriter, decoder, maxDictionaryByteSize);
        // simulate cutting the page
        fallbackValuesWriter.reset();
        assertThat(fallbackValuesWriter.getBufferedSize()).isEqualTo(0);
        fallbackValuesWriter.resetDictionary();

        roundTripFloat(fallbackValuesWriter, decoder, maxDictionaryByteSize);
    }

    private void roundTripLong(DictionaryFallbackValuesWriter fallbackValuesWriter, ValueDecoder<long[]> decoder, int maxDictionaryByteSize)
            throws IOException
    {
        int fallBackThreshold = maxDictionaryByteSize / 8;
        for (long i = 0; i < 100; i++) {
            fallbackValuesWriter.writeLong(i);
            assertThat(fallbackValuesWriter.getEncoding()).isEqualTo(i < fallBackThreshold ? getDictionaryEncoding() : PLAIN);
        }

        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(fallbackValuesWriter.getBytes().toByteArray())));
        long[] values = new long[100];
        decoder.read(values, 0, 100);
        for (int i = 0; i < 100; i++) {
            assertThat(values[i]).isEqualTo(i);
        }

        // Test skip with plain encoding
        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(fallbackValuesWriter.getBytes().toByteArray())));
        values = new long[1];
        for (int i = 0; i < 100; i += 2) {
            decoder.read(values, 0, 1);
            assertThat(values[0]).isEqualTo(i);
            decoder.skip(1);
        }

        // Test skip-n with plain encoding
        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(fallbackValuesWriter.getBytes().toByteArray())));
        int skipcount;
        for (int i = 0; i < 100; i += skipcount + 1) {
            decoder.read(values, 0, 1);
            assertThat(values[0]).isEqualTo(i);
            skipcount = (100 - i) / 2;
            decoder.skip(skipcount);
        }
    }

    private static void roundTripDouble(DictionaryFallbackValuesWriter fallbackValuesWriter, ValueDecoder<long[]> decoder, int maxDictionaryByteSize)
            throws IOException
    {
        int fallBackThreshold = maxDictionaryByteSize / 8;
        for (double i = 0; i < 100; i++) {
            fallbackValuesWriter.writeDouble(i);
            assertThat(fallbackValuesWriter.getEncoding()).isEqualTo(i < fallBackThreshold ? getDictionaryEncoding() : PLAIN);
        }

        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(fallbackValuesWriter.getBytes().toByteArray())));
        long[] values = new long[100];
        decoder.read(values, 0, 100);
        for (int i = 0; i < 100; i++) {
            assertThat(Double.longBitsToDouble(values[i])).isEqualTo(i);
        }

        // Test skip with plain encoding
        values = new long[1];
        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(fallbackValuesWriter.getBytes().toByteArray())));
        for (int i = 0; i < 100; i += 2) {
            decoder.read(values, 0, 1);
            assertThat(Double.longBitsToDouble(values[0])).isEqualTo(i);
            decoder.skip(1);
        }

        // Test skip-n with plain encoding
        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(fallbackValuesWriter.getBytes().toByteArray())));
        int skipcount;
        for (int i = 0; i < 100; i += skipcount + 1) {
            decoder.read(values, 0, 1);
            assertThat(Double.longBitsToDouble(values[0])).isEqualTo(i);
            skipcount = (100 - i) / 2;
            decoder.skip(skipcount);
        }
    }

    private static void roundTripInt(DictionaryFallbackValuesWriter fallbackValuesWriter, ValueDecoder<int[]> decoder, int maxDictionaryByteSize)
            throws IOException
    {
        int fallBackThreshold = maxDictionaryByteSize / 4;
        for (int i = 0; i < 100; i++) {
            fallbackValuesWriter.writeInteger(i);
            assertThat(fallbackValuesWriter.getEncoding()).isEqualTo(i < fallBackThreshold ? getDictionaryEncoding() : PLAIN);
        }

        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(fallbackValuesWriter.getBytes().toByteArray())));
        int[] values = new int[100];
        decoder.read(values, 0, 100);
        for (int i = 0; i < 100; i++) {
            assertThat(values[i]).isEqualTo(i);
        }

        // Test skip with plain encoding
        values = new int[1];
        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(fallbackValuesWriter.getBytes().toByteArray())));
        for (int i = 0; i < 100; i += 2) {
            decoder.read(values, 0, 1);
            assertThat(values[0]).isEqualTo(i);
            decoder.skip(1);
        }

        // Test skip-n with plain encoding
        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(fallbackValuesWriter.getBytes().toByteArray())));
        int skipcount;
        for (int i = 0; i < 100; i += skipcount + 1) {
            decoder.read(values, 0, 1);
            assertThat(values[0]).isEqualTo(i);
            skipcount = (100 - i) / 2;
            decoder.skip(skipcount);
        }
    }

    private static void roundTripFloat(DictionaryFallbackValuesWriter fallbackValuesWriter, ValueDecoder<int[]> decoder, int maxDictionaryByteSize)
            throws IOException
    {
        int fallBackThreshold = maxDictionaryByteSize / 4;
        for (float i = 0; i < 100; i++) {
            fallbackValuesWriter.writeFloat(i);
            assertThat(fallbackValuesWriter.getEncoding()).isEqualTo(i < fallBackThreshold ? getDictionaryEncoding() : PLAIN);
        }

        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(fallbackValuesWriter.getBytes().toByteArray())));
        int[] values = new int[100];
        decoder.read(values, 0, 100);
        for (int i = 0; i < 100; i++) {
            assertThat(Float.intBitsToFloat(values[i])).isEqualTo(i);
        }

        // Test skip with plain encoding
        values = new int[1];
        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(fallbackValuesWriter.getBytes().toByteArray())));
        for (int i = 0; i < 100; i += 2) {
            decoder.read(values, 0, 1);
            assertThat(Float.intBitsToFloat(values[0])).isEqualTo(i);
            decoder.skip(1);
        }

        // Test skip-n with plain encoding
        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(fallbackValuesWriter.getBytes().toByteArray())));
        int skipcount;
        for (int i = 0; i < 100; i += skipcount + 1) {
            decoder.read(values, 0, 1);
            assertThat(Float.intBitsToFloat(values[0])).isEqualTo(i);
            skipcount = (100 - i) / 2;
            decoder.skip(skipcount);
        }
    }

    private static <T> DictionaryDecoder<T> getDictionaryDecoder(ValuesWriter valuesWriter, ColumnAdapter<T> columnAdapter, ValueDecoder<T> plainValuesDecoder)
            throws IOException
    {
        DictionaryPage dictionaryPage = toTrinoDictionaryPage(valuesWriter.toDictPageAndClose().copy());
        return DictionaryDecoder.getDictionaryDecoder(dictionaryPage, columnAdapter, plainValuesDecoder, true);
    }

    private static void checkDistinct(int count, BytesInput bytes, ValueDecoder<BinaryBuffer> decoder, String prefix)
            throws IOException
    {
        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(bytes.toByteArray())));
        BinaryBuffer buffer = new BinaryBuffer(count);
        decoder.read(buffer, 0, count);
        Slice values = buffer.asSlice();
        int[] offsets = buffer.getOffsets();
        int currentOffset = 0;
        for (int i = 0; i < count; i++) {
            int length = offsets[i + 1] - offsets[i];
            assertThat(values.slice(currentOffset, length).toStringUtf8()).isEqualTo(prefix + i);
            currentOffset += length;
        }
    }

    private static void checkRepeated(int count, BytesInput bytes, ValueDecoder<BinaryBuffer> decoder, String prefix)
            throws IOException
    {
        decoder.init(new SimpleSliceInputStream(Slices.wrappedBuffer(bytes.toByteArray())));
        BinaryBuffer buffer = new BinaryBuffer(count);
        decoder.read(buffer, 0, count);
        Slice values = buffer.asSlice();
        int[] offsets = buffer.getOffsets();
        int currentOffset = 0;
        for (int i = 0; i < count; i++) {
            int length = offsets[i + 1] - offsets[i];
            assertThat(values.slice(currentOffset, length).toStringUtf8()).isEqualTo(prefix + i % 10);
            currentOffset += length;
        }
    }

    private static void writeDistinct(int count, ValuesWriter valuesWriter, String prefix)
    {
        for (int i = 0; i < count; i++) {
            valuesWriter.writeBytes(Binary.fromString(prefix + i));
        }
    }

    private static void writeRepeated(int count, ValuesWriter valuesWriter, String prefix)
    {
        for (int i = 0; i < count; i++) {
            valuesWriter.writeBytes(Binary.fromString(prefix + i % 10));
        }
    }

    private static void writeRepeatedWithReuse(int count, ValuesWriter valuesWriter, String prefix)
    {
        Binary reused = Binary.fromReusedByteArray((prefix + "0").getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < count; i++) {
            Binary content = Binary.fromString(prefix + i % 10);
            System.arraycopy(content.getBytesUnsafe(), 0, reused.getBytesUnsafe(), 0, reused.length());
            valuesWriter.writeBytes(reused);
        }
    }

    private static BytesInput getBytesAndCheckEncoding(ValuesWriter valuesWriter, Encoding encoding)
            throws IOException
    {
        BytesInput bytes = BytesInput.copy(valuesWriter.getBytes());
        assertThat(valuesWriter.getEncoding()).isEqualTo(encoding);
        valuesWriter.reset();
        return bytes;
    }

    private static DictionaryFallbackValuesWriter plainFallBack(DictionaryValuesWriter dictionaryValuesWriter, int initialSize)
    {
        return new DictionaryFallbackValuesWriter(dictionaryValuesWriter, new PlainValuesWriter(initialSize, initialSize * 5, new DirectByteBufferAllocator()));
    }

    private static DictionaryFallbackValuesWriter newPlainBinaryDictionaryValuesWriter(int maxDictionaryByteSize, int initialSize)
    {
        return plainFallBack(new PlainBinaryDictionaryValuesWriter(maxDictionaryByteSize, getDictionaryEncoding(), getDictionaryEncoding(), new DirectByteBufferAllocator()), initialSize);
    }

    private static DictionaryFallbackValuesWriter newPlainLongDictionaryValuesWriter(int maxDictionaryByteSize, int initialSize)
    {
        return plainFallBack(new PlainLongDictionaryValuesWriter(maxDictionaryByteSize, getDictionaryEncoding(), getDictionaryEncoding(), new DirectByteBufferAllocator()), initialSize);
    }

    private static DictionaryFallbackValuesWriter newPlainIntegerDictionaryValuesWriter(int maxDictionaryByteSize, int initialSize)
    {
        return plainFallBack(new PlainIntegerDictionaryValuesWriter(maxDictionaryByteSize, getDictionaryEncoding(), getDictionaryEncoding(), new DirectByteBufferAllocator()), initialSize);
    }

    private static DictionaryFallbackValuesWriter newPlainDoubleDictionaryValuesWriter(int maxDictionaryByteSize, int initialSize)
    {
        return plainFallBack(new PlainDoubleDictionaryValuesWriter(maxDictionaryByteSize, getDictionaryEncoding(), getDictionaryEncoding(), new DirectByteBufferAllocator()), initialSize);
    }

    private static DictionaryFallbackValuesWriter newPlainFloatDictionaryValuesWriter(int maxDictionaryByteSize, int initialSize)
    {
        return plainFallBack(new PlainFloatDictionaryValuesWriter(maxDictionaryByteSize, getDictionaryEncoding(), getDictionaryEncoding(), new DirectByteBufferAllocator()), initialSize);
    }

    @SuppressWarnings("deprecation")
    private static Encoding getDictionaryEncoding()
    {
        return PLAIN_DICTIONARY;
    }
}
