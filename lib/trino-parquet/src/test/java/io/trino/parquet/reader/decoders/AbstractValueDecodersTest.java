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
package io.trino.parquet.reader.decoders;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.dictionary.Dictionary;
import io.trino.parquet.reader.SimpleSliceInputStream;
import io.trino.parquet.reader.TestingColumnReader;
import io.trino.parquet.reader.flat.ColumnAdapter;
import io.trino.parquet.reader.flat.DictionaryDecoder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.parquet.ParquetEncoding.DELTA_BINARY_PACKED;
import static io.trino.parquet.ParquetEncoding.DELTA_BYTE_ARRAY;
import static io.trino.parquet.ParquetEncoding.DELTA_LENGTH_BYTE_ARRAY;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.PLAIN_DICTIONARY;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.ValuesType.VALUES;
import static io.trino.parquet.reader.decoders.ValueDecoder.ValueDecodersProvider;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.concat;
import static io.trino.testing.DataProviders.toDataProvider;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.column.Encoding.RLE;
import static org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter;
import static org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter;
import static org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter;
import static org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFloatDictionaryValuesWriter;
import static org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import static org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainLongDictionaryValuesWriter;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;

public abstract class AbstractValueDecodersTest
{
    private static final int MAX_DATA_SIZE = 1_000_000;

    private static final Object[][] SMALL_SIZE_RUNNERS = Stream.of(
                    ImmutableList.of(fullDecoder()),
                    generateRunnerForBatchSize(AbstractValueDecodersTest::batchDecode, 1, 12),
                    generateRunnerForBatchSize(AbstractValueDecodersTest::skippedBatchDecode, 0, 12))
            .flatMap(List::stream)
            .collect(toDataProvider());

    private static final Object[][] LARGE_SIZE_RUNNERS = Stream.of(
                    ImmutableList.of(fullDecoder()),
                    generateRunnerForBatchSize(AbstractValueDecodersTest::batchDecode, 7, 8, 9, 31, 32, 33, 1024),
                    generateRunnerForBatchSize(AbstractValueDecodersTest::skippedBatchDecode, 7, 8, 9, 31, 32, 33, 1024))
            .flatMap(List::stream)
            .collect(toDataProvider());

    private static final Object[][] RUNNERS_AND_SIZES = concat(
            cartesianProduct(SMALL_SIZE_RUNNERS, Stream.of(10).collect(toDataProvider())),
            cartesianProduct(LARGE_SIZE_RUNNERS, Stream.of(10_000).collect(toDataProvider())));

    abstract Object[][] tests();

    @DataProvider(name = "decoders")
    public Object[][] decoders()
    {
        return cartesianProduct(tests(), RUNNERS_AND_SIZES);
    }

    @Test(dataProvider = "decoders")
    public <T> void testDecoder(
            TestType<T> testType,
            ParquetEncoding encoding,
            InputDataProvider inputDataProvider,
            Runner<T> runner,
            int dataSize)
    {
        PrimitiveField field = testType.field();
        PrimitiveType primitiveType = field.getDescriptor().getPrimitiveType();
        ValuesWriter valuesWriter = getValuesWriter(encoding, primitiveType.getPrimitiveTypeName(), OptionalInt.of(primitiveType.getTypeLength()));
        DataBuffer dataBuffer = inputDataProvider.write(valuesWriter, dataSize);

        Optional<io.trino.parquet.DictionaryPage> dictionaryPage = Optional.ofNullable(dataBuffer.dictionaryPage())
                .map(TestingColumnReader::toTrinoDictionaryPage);
        Optional<Dictionary> dictionary = dictionaryPage.map(page -> {
            try {
                return encoding.initDictionary(field.getDescriptor(), page);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        ValuesReader valuesReader = getApacheParquetReader(encoding, field, dictionary);
        ValueDecoder<T> apacheValuesDecoder = testType.apacheValuesDecoderProvider().apply(valuesReader);

        Optional<ValueDecoder<T>> dictionaryDecoder = dictionaryPage.map(page -> DictionaryDecoder.getDictionaryDecoder(
                page,
                testType.columnAdapter(),
                testType.optimizedValuesDecoderProvider().create(PLAIN),
                field.isRequired()));
        ValueDecoder<T> optimizedValuesDecoder = dictionaryDecoder.orElseGet(() -> testType.optimizedValuesDecoderProvider().create(encoding));

        apacheValuesDecoder.init(new SimpleSliceInputStream(dataBuffer.dataPage()));
        optimizedValuesDecoder.init(new SimpleSliceInputStream(dataBuffer.dataPage()));
        ColumnAdapter<T> columnAdapter = testType.columnAdapter();
        T apacheValuesDecoderResult = columnAdapter.createBuffer(dataSize);
        T optimizedValuesDecoderResult = columnAdapter.createBuffer(dataSize);

        runner.run(apacheValuesDecoder, apacheValuesDecoderResult, dataSize);
        runner.run(optimizedValuesDecoder, optimizedValuesDecoderResult, dataSize);

        testType.assertion().accept(optimizedValuesDecoderResult, apacheValuesDecoderResult);
    }

    interface Runner<T>
    {
        void run(ValueDecoder<T> decoder, T data, int size);
    }

    interface InputDataProvider
    {
        DataBuffer write(ValuesWriter valuesWriter, int dataSize);
    }

    static DataBuffer getWrittenBuffer(ValuesWriter valuesWriter)
    {
        try {
            return new DataBuffer(
                    Slices.wrappedBuffer(valuesWriter.getBytes().toByteArray()),
                    valuesWriter.toDictPageAndClose());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        finally {
            valuesWriter.reset();
        }
    }

    record DataBuffer(Slice dataPage, @Nullable DictionaryPage dictionaryPage)
    {
        DataBuffer(Slice dataPage, @Nullable DictionaryPage dictionaryPage)
        {
            this.dataPage = requireNonNull(dataPage, "dataPage is null");
            this.dictionaryPage = dictionaryPage;
        }
    }

    record TestType<T>(
            PrimitiveField field,
            ValueDecodersProvider<T> optimizedValuesDecoderProvider,
            Function<ValuesReader, ValueDecoder<T>> apacheValuesDecoderProvider,
            ColumnAdapter<T> columnAdapter,
            BiConsumer<T, T> assertion)
    {
        TestType(PrimitiveField field, ValueDecodersProvider<T> optimizedValuesDecoderProvider, Function<ValuesReader, ValueDecoder<T>> apacheValuesDecoderProvider, ColumnAdapter<T> columnAdapter, BiConsumer<T, T> assertion)
        {
            this.field = requireNonNull(field, "field is null");
            this.optimizedValuesDecoderProvider = requireNonNull(optimizedValuesDecoderProvider, "optimizedValuesDecoderProvider is null");
            this.apacheValuesDecoderProvider = requireNonNull(apacheValuesDecoderProvider, "apacheValuesDecoderProvider is null");
            this.columnAdapter = requireNonNull(columnAdapter, "columnAdapter is null");
            this.assertion = requireNonNull(assertion, "assertion is null");
        }

        @Override
        public String toString()
        {
            ColumnDescriptor descriptor = field.getDescriptor();
            return toStringHelper(this)
                    .add("primitiveType", descriptor.getPrimitiveType().getPrimitiveTypeName())
                    .add("trinoType", field.getType())
                    .add("typeLength", descriptor.getPrimitiveType().getTypeLength())
                    .toString();
        }
    }

    static <T> Object[][] testArgs(
            TestType<T> testType,
            List<ParquetEncoding> encodings,
            InputDataProvider[] inputDataProviders)
    {
        return cartesianProduct(
                Stream.of(testType).collect(toDataProvider()),
                encodings.stream().collect(toDataProvider()),
                Arrays.stream(inputDataProviders).collect(toDataProvider()));
    }

    static ValuesReader getApacheParquetReader(ParquetEncoding encoding, PrimitiveField field, Optional<Dictionary> dictionary)
    {
        if (encoding == RLE_DICTIONARY || encoding == PLAIN_DICTIONARY) {
            return encoding.getDictionaryBasedValuesReader(field.getDescriptor(), VALUES, dictionary.orElseThrow());
        }
        checkArgument(dictionary.isEmpty(), "dictionary should be empty");
        return encoding.getValuesReader(field.getDescriptor(), VALUES);
    }

    static void initialize(SimpleSliceInputStream input, ValuesReader reader)
    {
        byte[] buffer = input.readBytes();
        try {
            reader.initFromPage(0, ByteBufferInputStream.wrap(ByteBuffer.wrap(buffer, 0, buffer.length)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static PrimitiveField createField(PrimitiveTypeName typeName, OptionalInt typeLength, Type trinoType)
    {
        Types.PrimitiveBuilder<PrimitiveType> builder = Types.required(typeName);
        if (typeLength.isPresent()) {
            builder = builder.length(typeLength.getAsInt());
        }
        if (trinoType instanceof DecimalType decimalType) {
            builder = builder.as(LogicalTypeAnnotation.decimalType(decimalType.getScale(), decimalType.getPrecision()));
        }
        return new PrimitiveField(
                trinoType,
                true,
                new ColumnDescriptor(new String[] {}, builder.named("dummy"), 0, 0),
                0);
    }

    private static <T> Runner<T> fullDecoder()
    {
        return new Runner<>()
        {
            @Override
            public void run(ValueDecoder<T> decoder, T data, int size)
            {
                decoder.read(data, 0, size);
            }

            @Override
            public String toString()
            {
                return "full";
            }
        };
    }

    private static <T> Runner<T> skippedBatchDecode(int batchSize)
    {
        return new Runner<>()
        {
            @Override
            public void run(ValueDecoder<T> decoder, T data, int size)
            {
                // Small random seeds always produce true as first nextBoolean()
                Random random = new Random(0xFFFFFFFFL * (size + batchSize));
                int i = 0;
                while (i < size) {
                    if (random.nextBoolean()) {
                        int readBatch = random.nextInt(1, batchSize + 2);
                        decoder.read(data, i, min(readBatch, size - i));
                        i += readBatch;
                    }
                    else {
                        decoder.skip(min(batchSize, size - i));
                        i += batchSize;
                    }
                }
            }

            @Override
            public String toString()
            {
                return "skippedBatch(" + batchSize + ")";
            }
        };
    }

    private static <T> Runner<T> batchDecode(int batchSize)
    {
        return new Runner<>()
        {
            @Override
            public void run(ValueDecoder<T> decoder, T data, int size)
            {
                for (int i = 0; i < size; i += batchSize) {
                    decoder.read(data, i, min(batchSize, size - i));
                }
            }

            @Override
            public String toString()
            {
                return "batch(" + batchSize + ")";
            }
        };
    }

    private static <T> List<Runner<T>> generateRunnerForBatchSize(Function<Integer, Runner<T>> runnerProvider, int from, int to)
    {
        return IntStream.range(from, to)
                .mapToObj(runnerProvider::apply)
                .collect(toImmutableList());
    }

    private static <T> List<Runner<T>> generateRunnerForBatchSize(Function<Integer, Runner<T>> runnerProvider, Integer... batchSizes)
    {
        return Arrays.stream(batchSizes)
                .map(runnerProvider)
                .collect(toImmutableList());
    }

    private static ValuesWriter getValuesWriter(ParquetEncoding encoding, PrimitiveTypeName typeName, OptionalInt typeLength)
    {
        if (encoding.equals(ParquetEncoding.RLE)) {
            if (typeName.equals(BOOLEAN)) {
                return new RunLengthBitPackingHybridValuesWriter(1, MAX_DATA_SIZE, MAX_DATA_SIZE, HeapByteBufferAllocator.getInstance());
            }
            throw new IllegalArgumentException("RLE encoding writer is not supported for type " + typeName);
        }
        if (encoding.equals(PLAIN)) {
            return switch (typeName) {
                case BOOLEAN -> new BooleanPlainValuesWriter();
                case FIXED_LEN_BYTE_ARRAY -> new FixedLenByteArrayPlainValuesWriter(typeLength.orElseThrow(), MAX_DATA_SIZE, MAX_DATA_SIZE, HeapByteBufferAllocator.getInstance());
                case BINARY, INT32, INT64, DOUBLE, FLOAT -> new PlainValuesWriter(MAX_DATA_SIZE, MAX_DATA_SIZE, HeapByteBufferAllocator.getInstance());
                case INT96 -> new FixedLenByteArrayPlainValuesWriter(12, MAX_DATA_SIZE, MAX_DATA_SIZE, HeapByteBufferAllocator.getInstance());
            };
        }
        if (encoding.equals(RLE_DICTIONARY) || encoding.equals(PLAIN_DICTIONARY)) {
            return switch (typeName) {
                case BINARY -> new PlainBinaryDictionaryValuesWriter(MAX_VALUE, RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
                case FIXED_LEN_BYTE_ARRAY -> new PlainFixedLenArrayDictionaryValuesWriter(MAX_VALUE, typeLength.orElseThrow(), RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
                case INT32 -> new PlainIntegerDictionaryValuesWriter(MAX_VALUE, RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
                case INT64 -> new PlainLongDictionaryValuesWriter(MAX_VALUE, RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
                case FLOAT -> new PlainFloatDictionaryValuesWriter(MAX_VALUE, RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
                case DOUBLE -> new PlainDoubleDictionaryValuesWriter(MAX_VALUE, RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
                case INT96 -> new PlainFixedLenArrayDictionaryValuesWriter(MAX_VALUE, 12, RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
                default -> throw new IllegalArgumentException("Dictionary encoding writer is not supported for type " + typeName);
            };
        }
        if (encoding.equals(DELTA_BINARY_PACKED)) {
            return switch (typeName) {
                case INT32 -> new DeltaBinaryPackingValuesWriterForInteger(MAX_DATA_SIZE, MAX_DATA_SIZE, HeapByteBufferAllocator.getInstance());
                case INT64 -> new DeltaBinaryPackingValuesWriterForLong(MAX_DATA_SIZE, MAX_DATA_SIZE, HeapByteBufferAllocator.getInstance());
                default -> throw new IllegalArgumentException("Delta binary packing encoding writer is not supported for type " + typeName);
            };
        }
        if (encoding.equals(DELTA_LENGTH_BYTE_ARRAY)) {
            if (typeName.equals(BINARY)) {
                return new DeltaLengthByteArrayValuesWriter(MAX_DATA_SIZE, MAX_DATA_SIZE, HeapByteBufferAllocator.getInstance());
            }
            throw new IllegalArgumentException("Delta length byte array encoding writer is not supported for type " + typeName);
        }
        if (encoding.equals(DELTA_BYTE_ARRAY)) {
            if (typeName.equals(BINARY) || typeName.equals(FIXED_LEN_BYTE_ARRAY)) {
                return new DeltaByteArrayWriter(MAX_DATA_SIZE, MAX_DATA_SIZE, HeapByteBufferAllocator.getInstance());
            }
            throw new IllegalArgumentException("Delta byte array encoding writer is not supported for type " + typeName);
        }
        throw new UnsupportedOperationException(format("Encoding %s is not supported", encoding));
    }
}
